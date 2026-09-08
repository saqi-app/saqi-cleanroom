import {
  MAX_CORPUS_IMPORT_BYTES,
  SOURCE_ADMISSION_SCHEMA_ID,
  SOURCE_ADMISSION_SCHEMA_VERSION,
  SourceAdmissionV2RequestSchema,
  SourceAdmissionV2ResponseSchema,
} from "@saqi/precedent-iso";
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

const NO_STORE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
} as const;

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
    const input = SourceAdmissionV2RequestSchema.parse(
      await readBoundedJson(request, MAX_CORPUS_IMPORT_BYTES)
    );
    const observedAt = new Date(Math.floor(Date.now() / 1_000) * 1_000);
    const { corpusImport, corpusRevision } = getServices();
    const results = [];
    for (const item of input.items) {
      try {
        // eslint-disable-next-line no-await-in-loop -- Each bounded admission preserves request order and independent evidence.
        const result = await corpusImport.admitSource(item);
        results.push({
          admissionId: item.admissionId,
          binding: result.binding,
          status: result.state,
        });
      } catch (error) {
        results.push(admissionRejection(item.admissionId, error));
      }
    }
    const writerEpoch = await corpusRevision.currentWriterEpoch();
    const body = SourceAdmissionV2ResponseSchema.parse({
      observedAt: observedAt.toISOString(),
      results,
      schemaId: SOURCE_ADMISSION_SCHEMA_ID,
      schemaVersion: SOURCE_ADMISSION_SCHEMA_VERSION,
      writerEpoch,
    });
    return Response.json(body, { headers: NO_STORE_HEADERS });
  } catch (error) {
    console.error("[ops] Source admission rejected", {
      code: error instanceof Error ? error.message : "UNKNOWN",
    });
    if (error instanceof LostWriterEpochError) {
      return failure(503, "WRITER_EPOCH_MISMATCH");
    }
    if (error instanceof ZodError || error instanceof SyntaxError) {
      return failure(400, "INVALID_SOURCE_ADMISSION_REQUEST");
    }
    return failure(503, "SOURCE_ADMISSION_UNAVAILABLE");
  }
}

function admissionRejection(admissionId: string, error: unknown) {
  if (
    !(error instanceof CorpusRevisionConflictError) &&
    !(error instanceof LostWriterEpochError) &&
    !(error instanceof LostPromotionClaimError)
  ) {
    throw error;
  }
  const message =
    error instanceof Error ? error.message : "SOURCE_VALIDATION_FAILED";
  const code =
    message === "AUTHOR_NOT_FOUND"
      ? "AUTHOR_NOT_FOUND"
      : message.includes("TOMBSTON")
        ? "SOURCE_TOMBSTONED"
        : error instanceof LostWriterEpochError ||
            error instanceof LostPromotionClaimError
          ? "WRITER_EPOCH_MISMATCH"
          : message.includes("CONFLICT")
            ? "IDENTITY_CONFLICT"
            : "SOURCE_VALIDATION_FAILED";
  return {
    admissionId,
    code,
    message,
    retryable: code === "WRITER_EPOCH_MISMATCH",
    status: "rejected" as const,
  };
}

function failure(status: number, error: string): Response {
  return Response.json(
    { error, ok: false, retryable: status >= 500 },
    { headers: NO_STORE_HEADERS, status }
  );
}
