import { CorpusRevisionConflictError } from "@saqi/precedent-node";
import { beforeEach, describe, expect, it, vi } from "vitest";

const { getAuthorSlugForPoem, getCloudflareEnv, publishBoundEnrichment } =
  vi.hoisted(() => ({
    getAuthorSlugForPoem: vi.fn(),
    getCloudflareEnv: vi.fn(),
    publishBoundEnrichment: vi.fn(),
  }));

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({
    corpusImport: { publishBoundEnrichment },
    poemStore: { getAuthorSlugForPoem },
  }),
}));
vi.mock("@/lib/cloudflare", () => ({ getCloudflareEnv }));

import { POST } from "./route";

const HASH_A = "a".repeat(64);
const HASH_B = "b".repeat(64);
const HASH_C = "c".repeat(64);
const HASH_D = "d".repeat(64);
const HASH_E = "e".repeat(64);

describe("bound enrichment publication v2 route", () => {
  beforeEach(() => {
    publishBoundEnrichment.mockReset();
    getAuthorSlugForPoem.mockReset().mockResolvedValue("poet");
    vi.unstubAllGlobals();
    getCloudflareEnv.mockReturnValue({
      DB: {
        prepare: () => ({
          first: () =>
            Promise.resolve({
              databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
            }),
        }),
      },
    });
  });

  it("returns a retryable service failure for unclassified store errors", async () => {
    publishBoundEnrichment.mockRejectedValueOnce(new Error("D1 unavailable"));
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await POST(request());

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      error: "PUBLICATION_UNAVAILABLE",
      ok: false,
      retryable: true,
    });
    expect(log).toHaveBeenCalledWith(
      "[ops] Bound enrichment publication rejected",
      {
        causeCode: "PUBLICATION_FAILURE",
        causeMessage: "Publication operation failed",
        code: "PUBLICATION_UNAVAILABLE",
      }
    );
    log.mockRestore();
  });

  it.each([
    {
      cause: "D1_ERROR: UNIQUE constraint failed: secret_table.private_id",
      causeCode: "D1_CONSTRAINT",
      causeMessage: "Database constraint violation",
    },
    {
      cause: "D1_ERROR: database is locked; retry token=secret-token",
      causeCode: "D1_BUSY",
      causeMessage: "Database temporarily busy",
    },
    {
      cause: "D1_ERROR 7429: Expression tree is too large (maximum depth 1000)",
      causeCode: "D1_QUERY_LIMIT",
      causeMessage: "Database query limit exceeded",
    },
  ])(
    "logs a bounded $causeCode diagnostic without Drizzle query details",
    async ({ cause, causeCode, causeMessage }) => {
      const databaseError = new Error(cause);
      const drizzleError = new Error(
        "Failed query: INSERT INTO secret_table VALUES (?) " +
          "params: https://user:password@example.com/private?token=secret",
        { cause: databaseError }
      );
      publishBoundEnrichment.mockRejectedValueOnce(drizzleError);
      const log = vi
        .spyOn(console, "error")
        .mockImplementation(() => undefined);

      const response = await POST(request());

      expect(response.status).toBe(503);
      expect(log).toHaveBeenCalledWith(
        "[ops] Bound enrichment publication rejected",
        {
          causeCode,
          causeMessage,
          code: "PUBLICATION_UNAVAILABLE",
        }
      );
      const renderedLog = JSON.stringify(log.mock.calls);
      expect(renderedLog).not.toContain("secret_table");
      expect(renderedLog).not.toContain("secret-token");
      expect(renderedLog).not.toContain("example.com");
      expect(renderedLog.length).toBeLessThan(300);
      log.mockRestore();
    }
  );

  it("keeps provenance conflicts as terminal per-item rejections", async () => {
    publishBoundEnrichment.mockRejectedValueOnce(
      new CorpusRevisionConflictError("SOURCE_CHANGED")
    );

    const response = await POST(request());

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      results: [
        {
          code: "SOURCE_CHANGED",
          publicationIntentId: HASH_D,
          retryable: true,
          status: "rejected",
        },
      ],
    });
  });

  it("acknowledges a committed publication when cache invalidation is deferred", async () => {
    const purge = vi.fn().mockRejectedValueOnce(new Error("timed out"));
    vi.stubGlobal("fetch", purge);
    getCloudflareEnv.mockReturnValue({
      CF_CACHE_PURGE_TOKEN: "cache-token",
      CF_ZONE_ID: "b".repeat(32),
      DB: {
        prepare: (query: string) => ({
          bind: () => ({
            first: () => Promise.resolve({ author_slug: "poet" }),
          }),
          first: () =>
            Promise.resolve(
              query.includes("production_deployment_identity")
                ? { databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5" }
                : undefined
            ),
        }),
      },
      SAQI_PUBLIC_ORIGIN: "https://www.saqi.app",
    });
    publishBoundEnrichment.mockResolvedValueOnce(publicationReceipt());
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await POST(request());

    expect(response.status).toBe(200);
    expect(response.headers.get("x-saqi-cache-invalidation")).toBe("deferred");
    await expect(response.json()).resolves.toMatchObject({
      results: [{ status: "published" }],
    });
    expect(log).toHaveBeenCalledWith(
      "[ops] Public cache invalidation deferred",
      expect.objectContaining({
        code: "PUBLIC_CACHE_PURGE_UNAVAILABLE",
        maximumNaturalStalenessSeconds: 360,
        publicationIntentId: HASH_D,
      })
    );
    log.mockRestore();
  });

  it("attempts cache purge only once after a transport failure in a batch", async () => {
    const purge = vi.fn().mockRejectedValue(new Error("timed out"));
    vi.stubGlobal("fetch", purge);
    getCloudflareEnv.mockReturnValue(cacheEnabledEnvironment());
    publishBoundEnrichment.mockResolvedValue(publicationReceipt());
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await POST(request(3));

    expect(response.status).toBe(200);
    expect(response.headers.get("x-saqi-cache-invalidation")).toBe("deferred");
    await expect(response.json()).resolves.toMatchObject({
      results: [
        { status: "published" },
        { status: "published" },
        { status: "published" },
      ],
    });
    expect(publishBoundEnrichment).toHaveBeenCalledTimes(3);
    expect(purge).toHaveBeenCalledOnce();
    expect(getAuthorSlugForPoem).toHaveBeenCalledExactlyOnceWith(
      publicationReceipt().poemId
    );
    const [slugReadOrder] = getAuthorSlugForPoem.mock.invocationCallOrder;
    const [purgeOrder] = purge.mock.invocationCallOrder;
    if (slugReadOrder === undefined || purgeOrder === undefined)
      throw new Error("Expected slug read and purge");
    expect(publishBoundEnrichment.mock.invocationCallOrder[0]).toBeLessThan(
      slugReadOrder
    );
    expect(slugReadOrder).toBeLessThan(purgeOrder);
    expect(log).toHaveBeenCalledOnce();
    log.mockRestore();
  });

  it("keeps a rejected cache purge retryable after publication commits", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          Response.json(
            { errors: [{ code: 9109 }], success: false },
            { status: 403 }
          )
        )
    );
    getCloudflareEnv.mockReturnValue(cacheEnabledEnvironment());
    publishBoundEnrichment.mockResolvedValueOnce(publicationReceipt());
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await POST(request());

    expect(response.status).toBe(503);
    expect(response.headers.get("x-saqi-cache-invalidation")).toBeNull();
    await expect(response.json()).resolves.toEqual({
      error: "PUBLIC_CACHE_PURGE_REJECTED",
      ok: false,
      retryable: true,
    });
    log.mockRestore();
  });

  it("keeps an invalid purge response retryable after publication commits", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValueOnce(new Response("not-json", { status: 200 }))
    );
    getCloudflareEnv.mockReturnValue(cacheEnabledEnvironment());
    publishBoundEnrichment.mockResolvedValueOnce(publicationReceipt());
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await POST(request());

    expect(response.status).toBe(503);
    expect(response.headers.get("x-saqi-cache-invalidation")).toBeNull();
    await expect(response.json()).resolves.toEqual({
      error: "PUBLIC_CACHE_PURGE_INVALID_RESPONSE",
      ok: false,
      retryable: true,
    });
    log.mockRestore();
  });

  it("keeps exact purge success on the synchronous success path", async () => {
    const purge = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ success: true }));
    vi.stubGlobal("fetch", purge);
    getCloudflareEnv.mockReturnValue(cacheEnabledEnvironment());
    publishBoundEnrichment.mockResolvedValueOnce(publicationReceipt());

    const response = await POST(request());

    expect(response.status).toBe(200);
    expect(response.headers.get("x-saqi-cache-invalidation")).toBeNull();
    await expect(response.json()).resolves.toMatchObject({
      results: [{ status: "published" }],
    });
    expect(purge).toHaveBeenCalledOnce();
  });
});

function cacheEnabledEnvironment() {
  return {
    CF_CACHE_PURGE_TOKEN: "cache-token",
    CF_ZONE_ID: "b".repeat(32),
    DB: {
      prepare: (query: string) => ({
        bind: () => ({
          first: () => Promise.resolve({ author_slug: "poet" }),
        }),
        first: () =>
          Promise.resolve(
            query.includes("production_deployment_identity")
              ? { databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5" }
              : undefined
          ),
      }),
    },
    SAQI_PUBLIC_ORIGIN: "https://www.saqi.app",
  };
}

function publicationReceipt() {
  return {
    actionHash: HASH_E,
    artifactId: "artifact-1",
    committedAt: "2026-09-01T12:00:00Z",
    modelKey: "sol-5.6",
    outcome: "published",
    poemId: "poem-1",
    pointerVersion: 1,
    promptVersion: "sol-word-gloss-v2",
    publicationIntentId: HASH_D,
    schemaId: "saqi.enrichment-publication-receipt",
    schemaVersion: 1,
    sourceRevisionId: HASH_A,
    writerEpoch: 1,
  } as const;
}

function request(itemCount = 1): Request {
  const validation = {
    artifactId: "artifact-1",
    attempt: 1,
    highestSeverity: "none",
    id: "validation-1",
    outcome: "pass",
    report: {
      fidelityScore: 100,
      findings: [],
      insightScore: 100,
      verdict: "pass",
    },
    reportHash: HASH_E,
    validatorKey: "sol-fidelity-review",
    validatorVersion: "sol-word-gloss-v2",
  };
  return new Request("https://ops.saqi.app/api/v2/enrichment-publications", {
    body: JSON.stringify({
      items: Array.from({ length: itemCount }, (_, index) =>
        String(index + 1)
      ).map((suffix, index) => ({
        actionHash: HASH_E,
        artifact: {
          id: `artifact-${suffix}`,
          model: "gpt-5.6-sol",
          modelKey: "sol-5.6",
          payload: {
            schemaId: "saqi.poem-enrichment-output",
            schemaVersion: 2,
            translation: { lines: ["verse"] },
            wordGlosses: {
              lines: [{ lineIndex: 0, segments: [] }],
              tokenizerVersion: "saqi-orthographic-v1",
            },
          },
          payloadHash: HASH_D,
          promptVersion: "sol-word-gloss-v2",
          reasoningEffort: "high",
          schemaVersion: 2,
          sourceRevisionId: HASH_A,
          taskKey: `task-${suffix}`,
          variant: 0,
        },
        binding: {
          admissionEvidence: {
            databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
            issuedAt: "2026-09-01T12:00:00Z",
            sourcePointerVersion: 2,
          },
          authorId: "author-1",
          authorNameArabic: "شاعر",
          bindingId: HASH_C,
          externalPoemId: "101680",
          lineNfcHash: HASH_B,
          poemId: "poem-1",
          promptMaterialHash: HASH_C,
          schemaId: "saqi.canonical-poem-binding",
          schemaVersion: 1,
          sourceName: "source",
          sourceRevisionId: HASH_A,
        },
        publicationIntentId: [HASH_D, HASH_C, HASH_B][index] ?? HASH_D,
        validations: [
          { ...validation, artifactId: `artifact-${suffix}` },
          {
            ...validation,
            artifactId: `artifact-${suffix}`,
            attempt: 2,
            id: "validation-2",
            validatorKey: "sol-grounding-review",
          },
        ],
      })),
      schemaId: "saqi.enrichment-publication",
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
