import { getCloudflareEnv } from "@/lib/cloudflare";
import { getSourceLineageMaintenance } from "@/lib/get-source-lineage-maintenance";
import { ProductionDeploymentIdentityRepository } from "@/lib/production-deployment-identity-repository";

const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;

// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function GET(): Promise<Response> {
  if (
    !(await new ProductionDeploymentIdentityRepository(
      getCloudflareEnv().DB
    ).matchesProduction())
  ) {
    return failure(503, "PRODUCTION_DATABASE_IDENTITY_MISMATCH");
  }
  try {
    return Response.json(
      { ok: true, result: await getSourceLineageMaintenance().status() },
      { headers: NO_STORE_HEADERS }
    );
  } catch {
    return failure(503, "SOURCE_LINEAGE_MAINTENANCE_UNAVAILABLE");
  }
}

function failure(status: number, error: string): Response {
  return Response.json(
    { error, ok: false, retryable: true },
    { headers: NO_STORE_HEADERS, status }
  );
}
