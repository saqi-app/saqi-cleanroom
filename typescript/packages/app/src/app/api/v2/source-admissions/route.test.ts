import {
  CorpusRevisionConflictError,
  LostWriterEpochError,
} from "@saqi/precedent-node";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { admitSource, currentWriterEpoch, getCloudflareEnv } = vi.hoisted(
  () => ({
    admitSource: vi.fn(),
    currentWriterEpoch: vi.fn(),
    getCloudflareEnv: vi.fn(),
  })
);

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({
    corpusImport: { admitSource },
    corpusRevision: { currentWriterEpoch },
  }),
}));
vi.mock("@/lib/cloudflare", () => ({ getCloudflareEnv }));

import { POST } from "./route";

const HASH = "a".repeat(64);

describe("source admission v2 route", () => {
  beforeEach(() => {
    admitSource.mockReset();
    currentWriterEpoch.mockReset().mockResolvedValue(7);
    getCloudflareEnv.mockReturnValue({
      DB: {
        prepare: (statement: string) => ({
          first: () =>
            Promise.resolve(
              statement.includes("production_deployment_identity")
                ? { databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5" }
                : { writer_epoch: 7 }
            ),
        }),
      },
    });
  });

  it.each([
    [
      new CorpusRevisionConflictError("AUTHOR_NOT_FOUND"),
      "AUTHOR_NOT_FOUND",
      false,
    ],
    [
      new CorpusRevisionConflictError("SOURCE_AUTHOR_IDENTITY_CONFLICT"),
      "IDENTITY_CONFLICT",
      false,
    ],
    [
      new CorpusRevisionConflictError("SOURCE_TOMBSTONED"),
      "SOURCE_TOMBSTONED",
      false,
    ],
    [new LostWriterEpochError(), "WRITER_EPOCH_MISMATCH", true],
    [
      new CorpusRevisionConflictError("SOURCE_FINGERPRINT_MISMATCH"),
      "SOURCE_VALIDATION_FAILED",
      false,
    ],
  ] as const)(
    "maps %s to the per-item %s result",
    async (error, code, retryable) => {
      admitSource.mockRejectedValueOnce(error);

      const response = await POST(request());

      expect(response.status).toBe(200);
      expect(currentWriterEpoch).toHaveBeenCalledOnce();
      const [writerReadOrder] = currentWriterEpoch.mock.invocationCallOrder;
      if (writerReadOrder === undefined)
        throw new Error("Expected writer read");
      expect(admitSource.mock.invocationCallOrder[0]).toBeLessThan(
        writerReadOrder
      );
      await expect(response.json()).resolves.toMatchObject({
        results: [
          {
            admissionId: HASH,
            code,
            retryable,
            status: "rejected",
          },
        ],
        writerEpoch: 7,
      });
    }
  );

  it("returns a retryable service failure for unclassified store errors", async () => {
    admitSource.mockRejectedValueOnce(new Error("D1 unavailable"));

    const response = await POST(request());

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: "SOURCE_ADMISSION_UNAVAILABLE",
      retryable: true,
    });
  });
});

function request(): Request {
  return new Request("https://ops.saqi.app/api/v2/source-admissions", {
    body: JSON.stringify({
      items: [
        {
          admissionId: HASH,
          externalPoemId: "101680",
          lineNfcHash: "b".repeat(64),
          linesArabic: ["بيت"],
          sourceAuthorId: "495",
          sourceAuthorUrl: "https://source.invalid/writers/495",
          sourceContentSha256: "c".repeat(64),
          sourceName: "source",
          sourcePoemUrl: "https://source.invalid/works/101680",
          sourceRevisionId: "d".repeat(64),
          titleArabic: "عنوان",
        },
      ],
      schemaId: "saqi.source-admission",
      schemaVersion: 2,
    }),
    headers: {
      "content-type": "application/json",
      host: "ops.saqi.app",
      origin: "https://ops.saqi.app",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
    },
    method: "POST",
  });
}
