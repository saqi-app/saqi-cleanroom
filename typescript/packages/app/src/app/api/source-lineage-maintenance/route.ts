import { z } from "zod";

import { getCloudflareEnv } from "@/lib/cloudflare";
import { getSourceLineageMaintenance } from "@/lib/get-source-lineage-maintenance";
import {
  hasJsonContentType,
  isTrustedMutationRequest,
  readBoundedJson,
} from "@/lib/operations-boundary";
import { ProductionDeploymentIdentityRepository } from "@/lib/production-deployment-identity-repository";

const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;
const RequestSchema = z.strictObject({
  maxPages: z.number().int().min(1).max(2).default(2),
});

// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return failure(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request)) return failure(415, "INVALID_CONTENT_TYPE");
  if (!(await productionDatabaseMatches()))
    return failure(503, "PRODUCTION_DATABASE_IDENTITY_MISMATCH");
  try {
    const input = RequestSchema.parse(await readBoundedJson(request, 256));
    return Response.json(
      {
        ok: true,
        result: await getSourceLineageMaintenance().run({
          maxPages: input.maxPages,
        }),
      },
      { headers: NO_STORE_HEADERS }
    );
  } catch (error) {
    if (error instanceof z.ZodError || error instanceof SyntaxError)
      return failure(400, "SOURCE_LINEAGE_MAINTENANCE_REQUEST_INVALID");
    return failure(503, "SOURCE_LINEAGE_MAINTENANCE_UNAVAILABLE");
  }
}

async function productionDatabaseMatches(): Promise<boolean> {
  return new ProductionDeploymentIdentityRepository(
    getCloudflareEnv().DB
  ).matchesProduction();
}

function failure(status: number, error: string): Response {
  return Response.json(
    { error, ok: false, retryable: status >= 500 },
    { headers: NO_STORE_HEADERS, status }
  );
}
