import { z } from "zod";

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
const RequestSchema = z.strictObject({
  cursor: z
    .strictObject({
      createdAt: z.number().int().nonnegative(),
      sourceRevisionId: z.string().regex(/^[a-f\d]{64}$/),
    })
    .optional(),
  limit: z.number().int().min(1).max(20).default(20),
});

// Explicit, resumable maintenance operation. Invoke repeatedly with the
// returned nextCursor until complete=true; restarting without a cursor safely
// catches active revisions created during an earlier pass.
// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return response(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request))
    return response(415, "INVALID_CONTENT_TYPE");
  const environment = getCloudflareEnv();
  if (
    !(await new ProductionDeploymentIdentityRepository(
      environment.DB
    ).matchesProduction())
  ) {
    return response(503, "PRODUCTION_DATABASE_IDENTITY_MISMATCH");
  }
  try {
    const input = RequestSchema.parse(
      await readBoundedJson(request, MAXIMUM_REQUEST_BYTES)
    );
    const { corpusRevision } = getServices();
    return Response.json(
      {
        ok: true,
        result: await corpusRevision.backfillActiveSourceFingerprints({
          ...(input.cursor ? { cursor: input.cursor } : {}),
          limit: input.limit,
        }),
      },
      { headers: NO_STORE_HEADERS }
    );
  } catch (error) {
    console.error("[ops] Source fingerprint backfill rejected", {
      code: error instanceof Error ? error.message : "UNKNOWN",
    });
    if (error instanceof z.ZodError)
      return response(400, "SOURCE_FINGERPRINT_BACKFILL_REQUEST_INVALID");
    return response(409, "SOURCE_FINGERPRINT_BACKFILL_REJECTED");
  }
}

function response(status: number, error: string): Response {
  return Response.json(
    { error, ok: false },
    { headers: NO_STORE_HEADERS, status }
  );
}
