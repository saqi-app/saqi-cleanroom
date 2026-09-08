import {
  ENRICHMENT_PUBLICATION_SCHEMA_ID,
  ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
  EnrichmentPublicationV2RequestSchema,
  EnrichmentPublicationV2ResponseSchema,
  MAX_CORPUS_IMPORT_BYTES,
} from "@saqi/precedent-iso";
import {
  CorpusRevisionConflictError,
  LostPromotionClaimError,
} from "@saqi/precedent-node";

import { getServices } from "@/backend/get-services";
import { getCloudflareEnv } from "@/lib/cloudflare";
import {
  hasJsonContentType,
  isTrustedMutationRequest,
  readBoundedJson,
} from "@/lib/operations-boundary";
import { ProductionDeploymentIdentityRepository } from "@/lib/production-deployment-identity-repository";
import {
  publicCacheConfig,
  PublicCacheInvalidationError,
  purgePublishedPoem,
} from "@/lib/public-cache";

const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;
// Keep this aligned with the public site's 300-second edge max-age plus its
// 60-second stale-while-revalidate window.
const MAXIMUM_NATURAL_CACHE_STALENESS_SECONDS = 360;

// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return failure(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request)) return failure(415, "INVALID_CONTENT_TYPE");
  const env = getCloudflareEnv();
  if (
    !(await new ProductionDeploymentIdentityRepository(
      env.DB
    ).matchesProduction())
  ) {
    return failure(503, "PRODUCTION_DATABASE_IDENTITY_MISMATCH");
  }

  try {
    const input = await parseRequest(request);
    const cache = publicCacheConfig(env);
    const { corpusImport, poemStore } = getServices();
    const results = [];
    let cacheInvalidationDeferred = false;
    for (const item of input.items) {
      try {
        // eslint-disable-next-line no-await-in-loop -- Per-item commits and cache invalidations retain request order and replay identity.
        const receipt = await corpusImport.publishBoundEnrichment(item);
        if (cache.state === "enabled") {
          if (!cacheInvalidationDeferred) {
            try {
              // eslint-disable-next-line no-await-in-loop -- Exact purges are intentionally serialized with their receipts.
              const authorSlug = await poemStore.getAuthorSlugForPoem(
                receipt.poemId
              );
              if (!authorSlug)
                throw new PublicCacheInvalidationError(
                  "PUBLIC_CACHE_ROUTE_INVALID"
                );
              // eslint-disable-next-line no-await-in-loop -- Exact purges are intentionally serialized with their receipts.
              await purgePublishedPoem(cache.config, {
                authorSlug,
                poemId: receipt.poemId,
              });
            } catch (error) {
              if (!(error instanceof PublicCacheInvalidationError)) throw error;
              if (error.message !== "PUBLIC_CACHE_PURGE_UNAVAILABLE")
                throw error;
              cacheInvalidationDeferred = true;
              // Publication is committed durably before invalidation begins. A
              // transient purge outage must not make the caller replay committed
              // work forever. It also opens a request-scoped circuit breaker so
              // one purge outage cannot add the same timeout to every item in a
              // batch; the public site's bounded cache policy will revalidate
              // each route independently.
              console.error("[ops] Public cache invalidation deferred", {
                code: error.message,
                maximumNaturalStalenessSeconds:
                  MAXIMUM_NATURAL_CACHE_STALENESS_SECONDS,
                poemId: receipt.poemId,
                publicationIntentId: item.publicationIntentId,
              });
            }
          }
        } else {
          console.warn("[ops] Public cache purge skipped", {
            code: "PUBLIC_CACHE_PURGE_NOT_CONFIGURED",
            poemId: receipt.poemId,
          });
        }
        results.push({ receipt, status: "published" as const });
      } catch (error) {
        if (error instanceof PublicCacheInvalidationError) throw error;
        if (
          !(error instanceof LostPromotionClaimError) &&
          !(error instanceof CorpusRevisionConflictError)
        ) {
          // A partially committed batch is safe to replay because publication
          // intents are idempotent. Do not misclassify transient D1 or runtime
          // failures as terminal binding defects on otherwise valid work.
          throw error;
        }
        results.push(rejectedResult(item.publicationIntentId, error));
      }
    }
    const body = EnrichmentPublicationV2ResponseSchema.parse({
      results,
      schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
      schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
    });
    return Response.json(body, {
      headers: {
        ...NO_STORE_HEADERS,
        ...(cacheInvalidationDeferred
          ? { "x-saqi-cache-invalidation": "deferred" }
          : {}),
      },
    });
  } catch (error) {
    console.error(
      "[ops] Bound enrichment publication rejected",
      publicationFailureDiagnostic(error)
    );
    if (error instanceof PublicCacheInvalidationError) {
      return Response.json(
        { error: error.message, ok: false, retryable: true },
        { headers: { ...NO_STORE_HEADERS, "retry-after": "30" }, status: 503 }
      );
    }
    if (error instanceof LostPromotionClaimError) {
      return failure(409, "PUBLICATION_POINTER_CONFLICT", true);
    }
    if (error instanceof CorpusRevisionConflictError) {
      return failure(409, error.message || "PUBLICATION_REJECTED", false);
    }
    if (error instanceof InvalidPublicationRequestError) {
      return failure(400, "INVALID_PUBLICATION_REQUEST", false);
    }
    return failure(503, "PUBLICATION_UNAVAILABLE", true);
  }
}

type PublicationFailureDiagnostic = Readonly<{
  causeCode:
    | "D1_BUSY"
    | "D1_CONSTRAINT"
    | "D1_QUERY_LIMIT"
    | "DATABASE_FAILURE"
    | "PUBLICATION_FAILURE";
  causeMessage: string;
  code: "PUBLICATION_UNAVAILABLE";
}>;

/**
 * Classify database failures without logging the underlying error. Drizzle's
 * error message can contain SQL and bound parameters, while a nested D1 cause
 * can contain request metadata. Keep both fields fixed and low-cardinality so
 * production logs are useful without becoming a data-exfiltration surface.
 */
function publicationFailureDiagnostic(
  error: unknown
): PublicationFailureDiagnostic {
  const signal = errorCauseSignal(error);
  if (
    /7429|expression tree is too large|query (?:is )?too large|sqlite_toobig|statement too long|too many sql variables/u.test(
      signal
    )
  ) {
    return diagnostic("D1_QUERY_LIMIT", "Database query limit exceeded");
  }
  if (
    /database (?:is )?(?:busy|locked)|d1[^\n]*timed? ?out|sqlite_busy/u.test(
      signal
    )
  ) {
    return diagnostic("D1_BUSY", "Database temporarily busy");
  }
  if (
    /constraint failed|foreign key constraint|not null constraint|sqlite_constraint|unique constraint/u.test(
      signal
    )
  ) {
    return diagnostic("D1_CONSTRAINT", "Database constraint violation");
  }
  if (/d1_error|drizzle|failed query|sqlite/u.test(signal)) {
    return diagnostic("DATABASE_FAILURE", "Database operation failed");
  }
  return diagnostic("PUBLICATION_FAILURE", "Publication operation failed");
}

function diagnostic(
  causeCode: PublicationFailureDiagnostic["causeCode"],
  causeMessage: string
): PublicationFailureDiagnostic {
  return { causeCode, causeMessage, code: "PUBLICATION_UNAVAILABLE" };
}

function errorCauseSignal(error: unknown): string {
  const parts: string[] = [];
  const seen = new Set<object>();
  let current = error;
  for (let depth = 0; depth < 4 && current instanceof Error; depth += 1) {
    if (seen.has(current)) break;
    seen.add(current);
    parts.push(current.name.toLowerCase(), current.message.toLowerCase());
    current = current.cause;
  }
  return parts.join("\n");
}

class InvalidPublicationRequestError extends Error {}

async function parseRequest(request: Request) {
  try {
    return EnrichmentPublicationV2RequestSchema.parse(
      await readBoundedJson(request, MAX_CORPUS_IMPORT_BYTES)
    );
  } catch (error) {
    throw new InvalidPublicationRequestError("INVALID_PUBLICATION_REQUEST", {
      cause: error,
    });
  }
}

function rejectedResult(publicationIntentId: string, error: unknown) {
  const message =
    error instanceof Error ? error.message : "PUBLICATION_REJECTED";
  if (error instanceof LostPromotionClaimError) {
    return {
      code: "POINTER_CONFLICT" as const,
      message,
      publicationIntentId,
      retryable: true,
      status: "rejected" as const,
    };
  }
  const code =
    message.includes("ACTION_HASH") || message.includes("INTENT")
      ? "ACTION_HASH_CONFLICT"
      : message.includes("VALIDATION")
        ? "VALIDATION_INVALID"
        : message.includes("ARTIFACT")
          ? "ARTIFACT_INVALID"
          : message.includes("TOMBSTON")
            ? "SOURCE_TOMBSTONED"
            : message.includes("CURRENT") || message.includes("SOURCE_CHANGED")
              ? "SOURCE_CHANGED"
              : "BINDING_INVALID";
  return {
    code,
    message,
    publicationIntentId,
    retryable: code === "SOURCE_CHANGED",
    status: "rejected" as const,
  };
}

function failure(status: number, error: string, retryable = false): Response {
  return Response.json(
    { error, ok: false, retryable },
    { headers: NO_STORE_HEADERS, status }
  );
}
