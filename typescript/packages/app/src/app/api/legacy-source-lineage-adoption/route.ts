import { LegacySourceLineageAdoptionRequestSchema } from "@saqi/precedent-iso";
import {
  CorpusRevisionConflictError,
  LostPromotionClaimError,
  LostWriterEpochError,
} from "@saqi/precedent-node";
import { ZodError } from "zod";

import { getServices } from "@/backend/get-services";
import { getCloudflareEnv } from "@/lib/cloudflare";
import {
  hasJsonContentType,
  isTrustedMutationRequest,
  readBoundedJson,
} from "@/lib/operations-boundary";
import { ProductionDeploymentIdentityRepository } from "@/lib/production-deployment-identity-repository";

const MAXIMUM_REQUEST_BYTES = 1_024;
const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;
// Explicit targeted repair endpoint. Each request adopts at most ten named
// legacy rows and every mutation remains behind the production identity fence.
// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return failure(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request)) return failure(415, "INVALID_CONTENT_TYPE");
  const { DB } = getCloudflareEnv();
  if (
    !(await new ProductionDeploymentIdentityRepository(DB).matchesProduction())
  ) {
    return failure(503, "PRODUCTION_DATABASE_IDENTITY_MISMATCH");
  }
  try {
    const input = LegacySourceLineageAdoptionRequestSchema.parse(
      await readBoundedJson(request, MAXIMUM_REQUEST_BYTES)
    );
    const { corpusRevision } = getServices();
    return Response.json(
      {
        ok: true,
        result: await corpusRevision.adoptLegacySourceLineage({
          poemIds: input.poemIds,
        }),
      },
      { headers: NO_STORE_HEADERS }
    );
  } catch (error) {
    console.error("[ops] Legacy source lineage adoption rejected", {
      code: error instanceof Error ? error.message : "UNKNOWN",
    });
    if (error instanceof ZodError || error instanceof SyntaxError) {
      return failure(400, "LEGACY_SOURCE_LINEAGE_REQUEST_INVALID");
    }
    if (
      error instanceof CorpusRevisionConflictError ||
      error instanceof LostPromotionClaimError
    ) {
      return failure(
        409,
        error instanceof Error
          ? error.message
          : "LEGACY_SOURCE_LINEAGE_CONFLICT"
      );
    }
    if (error instanceof LostWriterEpochError) {
      return failure(503, "WRITER_EPOCH_MISMATCH");
    }
    return failure(503, "LEGACY_SOURCE_LINEAGE_UNAVAILABLE");
  }
}

function failure(status: number, error: string): Response {
  return Response.json(
    { error, ok: false, retryable: status >= 500 },
    { headers: NO_STORE_HEADERS, status }
  );
}
