import {
  MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES,
  ProductionResolutionRequestSchema,
} from "@saqi/precedent-iso";
import { ProductionResolutionConflictError } from "@saqi/precedent-node";
import { ZodError } from "zod";

import { getServices } from "@/backend/get-services";
import {
  hasJsonContentType,
  isTrustedMutationRequest,
  readBoundedJson,
} from "@/lib/operations-boundary";

const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;

// eslint-disable-next-line @typescript-eslint/naming-convention -- Next.js route handlers use HTTP method exports.
export async function POST(request: Request): Promise<Response> {
  if (!isTrustedMutationRequest(request))
    return response(403, "UNTRUSTED_MUTATION");
  if (!hasJsonContentType(request))
    return response(415, "INVALID_CONTENT_TYPE");
  try {
    const input = ProductionResolutionRequestSchema.parse(
      await readBoundedJson(request, MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES)
    );
    const { productionResolution } = getServices();
    return Response.json(
      { ok: true, result: await productionResolution.resolve(input) },
      { headers: NO_STORE_HEADERS }
    );
  } catch (error) {
    console.error("[ops] Production resolution rejected", {
      code: error instanceof Error ? error.message : "UNKNOWN",
    });
    if (error instanceof ZodError) {
      return response(400, "INVALID_PRODUCTION_RESOLUTION_REQUEST");
    }
    if (
      error instanceof Error &&
      ["Invalid content length", "Request body too large"].includes(
        error.message
      )
    ) {
      return response(413, "PRODUCTION_RESOLUTION_REQUEST_TOO_LARGE");
    }
    if (error instanceof ProductionResolutionConflictError) {
      return response(409, error.message);
    }
    return response(503, "PRODUCTION_RESOLUTION_UNAVAILABLE");
  }
}

function response(status: number, code: string): Response {
  return Response.json(
    { error: code, ok: false },
    { headers: NO_STORE_HEADERS, status }
  );
}
