import {
  CorpusImportActionSchema,
  MAX_CORPUS_IMPORT_BYTES,
  SAQI_PRODUCTION_DATABASE_ID,
} from "@saqi/precedent-iso";
import {
  CorpusRevisionConflictError,
  LostPromotionClaimError,
  LostWriterEpochError,
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

// Cloudflare Access protects this operations origin. This read-only canary
// proves that publication credentials reached the exact protected service
// before the local rig is allowed to attempt a D1 mutation.
// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function GET(): Promise<Response> {
  const identity = {
    databaseId: SAQI_PRODUCTION_DATABASE_ID,
    schemaId: "saqi.publication-identity",
    schemaVersion: 1,
    service: "saqi-production",
  } as const;
  if (!(await productionDatabaseMatches())) {
    return Response.json(
      { error: "PRODUCTION_DATABASE_IDENTITY_MISMATCH", ok: false },
      { headers: NO_STORE_HEADERS, status: 503 }
    );
  }
  return Response.json(identity, { headers: NO_STORE_HEADERS });
}

// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return response(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request))
    return response(415, "INVALID_CONTENT_TYPE");
  if (!(await productionDatabaseMatches()))
    return Response.json(
      { error: "PRODUCTION_DATABASE_IDENTITY_MISMATCH", ok: false },
      { headers: NO_STORE_HEADERS, status: 503 }
    );
  try {
    const action = CorpusImportActionSchema.parse(
      await readBoundedJson(request, MAX_CORPUS_IMPORT_BYTES)
    );
    const { corpusImport } = getServices();
    switch (action.action) {
      case "stage-and-plan":
        return Response.json({
          ok: true,
          result: await corpusImport.stageAndPlan({
            ...action.input,
            records: action.input.records.map((record) => ({
              ...record,
              observedAt: new Date(record.observedAt),
            })),
          }),
        });
      case "promote":
        return Response.json({
          ok: true,
          result: await corpusImport.promote(
            action.bundleId,
            action.writerEpoch,
            action.expectedPlanHash
          ),
        });
      case "publish-enrichment": {
        // Reject partial configuration before the idempotent D1 write.
        const cache = publicCacheConfig(getCloudflareEnv());
        const result = await corpusImport.publishEnrichment(action.input);
        if (cache.state === "enabled") {
          await purgePublishedPoem(cache.config, result);
        } else {
          console.warn("[ops] Public cache purge skipped", {
            code: "PUBLIC_CACHE_PURGE_NOT_CONFIGURED",
          });
        }
        return Response.json({ ok: true, result });
      }
    }
  } catch (error) {
    console.error("[ops] Corpus import rejected", {
      code: error instanceof Error ? error.message : "UNKNOWN",
    });
    if (error instanceof LostWriterEpochError)
      return conflict("PUBLICATION_WRITER_EPOCH_CONFLICT", "writer_epoch");
    if (error instanceof LostPromotionClaimError)
      return conflict("PUBLICATION_POINTER_CONFLICT", "pointer");
    if (error instanceof PublicCacheInvalidationError)
      return Response.json(
        { error: error.message, ok: false, retryable: true },
        { headers: { "retry-after": "30" }, status: 503 }
      );
    if (
      error instanceof CorpusRevisionConflictError &&
      error.message === "ENRICHMENT_SOURCE_NOT_CURRENT"
    )
      return conflict(
        "PUBLICATION_SOURCE_REVISION_CONFLICT",
        "source_revision"
      );
    return response(409, "CORPUS_IMPORT_REJECTED");
  }
}

async function productionDatabaseMatches(): Promise<boolean> {
  return new ProductionDeploymentIdentityRepository(
    getCloudflareEnv().DB
  ).matchesProduction();
}

function conflict(
  error: string,
  kind: "pointer" | "source_revision" | "writer_epoch"
): Response {
  return Response.json(
    { conflict: { kind, retryable: true }, error, ok: false },
    { status: 409 }
  );
}

function response(status: number, code: string): Response {
  return Response.json({ error: code, ok: false }, { status });
}
