import {
  CorpusRevisionConflictError,
  LostPromotionClaimError,
} from "@saqi/precedent-node";
import { beforeEach, describe, expect, it, vi } from "vitest";

const {
  databaseFirst,
  getCloudflareEnv,
  promote,
  publishEnrichment,
  stageAndPlan,
} = vi.hoisted(() => ({
  databaseFirst: vi.fn(),
  getCloudflareEnv: vi.fn(),
  promote: vi.fn(),
  publishEnrichment: vi.fn(),
  stageAndPlan: vi.fn(),
}));

vi.mock("@/lib/cloudflare", () => ({ getCloudflareEnv }));

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({
    corpusImport: {
      promote,
      publishEnrichment,
      stageAndPlan,
    },
  }),
}));

import { GET, POST } from "./route";

const HASH = "a".repeat(64);

describe("corpus import route", () => {
  beforeEach(() => {
    promote.mockReset();
    promote.mockResolvedValue({ planHash: HASH });
    publishEnrichment.mockReset();
    stageAndPlan.mockReset();
    publishEnrichment.mockResolvedValue({
      authorSlug: "poet name",
      poemId: "poem/1",
      pointerVersion: 1,
      state: "published",
    });
    getCloudflareEnv.mockReset();
    databaseFirst.mockReset();
    databaseFirst.mockResolvedValue({
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
    });
    getCloudflareEnv.mockReturnValue({
      CF_CACHE_PURGE_TOKEN: "cache-token",
      CF_ZONE_ID: "b".repeat(32),
      DB: { prepare: () => ({ first: databaseFirst }) },
      SAQI_PUBLIC_ORIGIN: "https://saqi.app",
    });
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          Response.json({ errors: [], messages: [], result: {}, success: true })
        )
    );
  });

  it("exposes a no-store D1-bound publication identity canary", async () => {
    const response = await GET();

    expect(response.headers.get("cache-control")).toBe(
      "private, no-store, max-age=0"
    );
    await expect(response.json()).resolves.toEqual({
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
      schemaId: "saqi.publication-identity",
      schemaVersion: 1,
      service: "saqi-production",
    });
  });

  it("fails the identity canary closed for a missing or wrong D1 sentinel", async () => {
    databaseFirst.mockResolvedValueOnce(null);
    const missing = await GET();
    expect(missing.status).toBe(503);

    databaseFirst.mockResolvedValueOnce({ databaseId: crypto.randomUUID() });
    const wrong = await GET();
    expect(wrong.status).toBe(503);
  });

  it("fails the identity canary closed when the D1 read fails", async () => {
    databaseFirst.mockRejectedValueOnce(new Error("D1 unavailable"));

    const response = await GET();

    expect(response.status).toBe(503);
  });

  it.each([
    ["missing", null],
    ["wrong", { databaseId: crypto.randomUUID() }],
  ] as const)(
    "rejects every mutation before writes when the D1 sentinel is %s",
    async (_description, sentinel) => {
      databaseFirst.mockResolvedValueOnce(sentinel);

      const response = await POST(request());

      expect(response.status).toBe(503);
      expect(promote).not.toHaveBeenCalled();
      expect(stageAndPlan).not.toHaveBeenCalled();
      expect(publishEnrichment).not.toHaveBeenCalled();
    }
  );

  it("rejects every mutation before writes when the D1 identity read fails", async () => {
    databaseFirst.mockRejectedValueOnce(new Error("D1 unavailable"));

    const response = await POST(request());

    expect(response.status).toBe(503);
    expect(promote).not.toHaveBeenCalled();
    expect(stageAndPlan).not.toHaveBeenCalled();
    expect(publishEnrichment).not.toHaveBeenCalled();
  });

  it("rejects mutation requests outside the trusted same-origin boundary", async () => {
    const response = await POST(request({ origin: "https://evil.example" }));

    expect(response.status).toBe(403);
    expect(promote).not.toHaveBeenCalled();
  });

  it("rejects request bodies larger than two MiB before parsing", async () => {
    const response = await POST(
      request({ "content-length": String(2 * 1_024 * 1_024 + 1) })
    );

    expect(response.status).toBe(409);
    expect(promote).not.toHaveBeenCalled();
  });

  it("passes a bounded, schema-validated promotion to the coordinator", async () => {
    const response = await POST(request());

    expect(response.status).toBe(200);
    expect(promote).toHaveBeenCalledExactlyOnceWith("bundle-1", 1, HASH);
  });

  it("returns a typed retryable pointer conflict", async () => {
    promote.mockRejectedValueOnce(new LostPromotionClaimError());
    const response = await POST(request());

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toEqual({
      conflict: { kind: "pointer", retryable: true },
      error: "PUBLICATION_POINTER_CONFLICT",
      ok: false,
    });
  });

  it("fails a legacy Sol downgrade closed without purging public cache", async () => {
    publishEnrichment.mockRejectedValueOnce(
      new CorpusRevisionConflictError(
        "LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER"
      )
    );

    const response = await POST(request({}, publicationAction()));

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toEqual({
      error: "CORPUS_IMPORT_REJECTED",
      ok: false,
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(["published", "already_current"] as const)(
    "purges the exact poem URL after a %s enrichment result",
    async (state) => {
      publishEnrichment.mockResolvedValueOnce({
        authorSlug: "poet name",
        poemId: "poem/1",
        pointerVersion: 1,
        state,
      });

      const response = await POST(request({}, publicationAction()));

      expect(response.status).toBe(200);
      expect(fetch).toHaveBeenCalledExactlyOnceWith(
        `https://api.cloudflare.com/client/v4/zones/${"b".repeat(32)}/purge_cache`,
        expect.objectContaining({
          body: JSON.stringify({
            files: ["https://saqi.app/author/poet%20name/poem/poem%2F1"],
          }),
          method: "POST",
        })
      );
    }
  );

  it("publishes with a warning when purge is fully unconfigured", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    getCloudflareEnv.mockReturnValue({
      CF_CACHE_PURGE_TOKEN: undefined,
      CF_ZONE_ID: undefined,
      DB: { prepare: () => ({ first: databaseFirst }) },
      SAQI_PUBLIC_ORIGIN: "https://saqi.app",
    });

    const response = await POST(request({}, publicationAction()));

    expect(response.status).toBe(200);
    expect(publishEnrichment).toHaveBeenCalledOnce();
    expect(fetch).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledExactlyOnceWith(
      "[ops] Public cache purge skipped",
      { code: "PUBLIC_CACHE_PURGE_NOT_CONFIGURED" }
    );
    warn.mockRestore();
  });

  it("fails before publication when purge configuration is partial", async () => {
    getCloudflareEnv.mockReturnValue({
      CF_CACHE_PURGE_TOKEN: undefined,
      CF_ZONE_ID: "b".repeat(32),
      DB: { prepare: () => ({ first: databaseFirst }) },
      SAQI_PUBLIC_ORIGIN: "https://saqi.app",
    });

    const response = await POST(request({}, publicationAction()));

    expect(response.status).toBe(503);
    expect(response.headers.get("retry-after")).toBe("30");
    expect(publishEnrichment).not.toHaveBeenCalled();
  });

  it("replays an already-current publication until exact purge succeeds", async () => {
    publishEnrichment
      .mockResolvedValueOnce({
        authorSlug: "author-1",
        poemId: "poem-1",
        pointerVersion: 1,
        state: "published",
      })
      .mockResolvedValueOnce({
        authorSlug: "author-1",
        poemId: "poem-1",
        pointerVersion: 1,
        state: "already_current",
      });
    vi.mocked(fetch)
      .mockResolvedValueOnce(
        Response.json({ errors: [], messages: [], success: false })
      )
      .mockResolvedValueOnce(Response.json({ result: {}, success: true }));

    const first = await POST(request({}, publicationAction()));
    const second = await POST(request({}, publicationAction()));

    expect(first.status).toBe(503);
    expect(first.headers.get("retry-after")).toBe("30");
    expect(second.status).toBe(200);
    expect(publishEnrichment).toHaveBeenCalledTimes(2);
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});

function request(
  overrides: Record<string, string> = {},
  body: unknown = {
    action: "promote",
    bundleId: "bundle-1",
    expectedPlanHash: HASH,
    writerEpoch: 1,
  }
): Request {
  return new Request("https://ops.saqi.app/api/corpus-import", {
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
      host: "ops.saqi.app",
      origin: "https://ops.saqi.app",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
      ...overrides,
    },
    method: "POST",
  });
}

function publicationAction() {
  const artifactId = "artifact-1";
  const review = {
    fidelityScore: 100,
    findings: [],
    insightScore: 100,
    verdict: "pass",
  } as const;
  const validations = [
    {
      attempt: 1,
      validatorKey: "sol-fidelity-review",
      validatorVersion: "sol-enrichment-v1",
    },
    {
      attempt: 2,
      validatorKey: "sol-grounding-review",
      validatorVersion: "sol-enrichment-v1",
    },
  ];
  return {
    action: "publish-enrichment",
    input: {
      artifact: {
        id: artifactId,
        model: "gpt-5.6-sol",
        modelKey: "sol-5.6",
        payload: {
          insights: {
            culturalSignificance: "c",
            historicalContext: "h",
            literaryDevices: ["device"],
            notableLines: [{ explanation: "e", line: "بيت" }],
            summary: "s",
            themes: ["theme"],
          },
          translation: { lines: ["line"] },
        },
        payloadHash: HASH,
        promptVersion: "sol-enrichment-v1",
        reasoningEffort: "high",
        schemaVersion: 1,
        sourceRevisionId: "revision-1",
        taskKey: "task-1",
        variant: 0,
      },
      publication: {
        artifactId,
        expectedPointerVersion: null,
        poemId: "poem-1",
        requiredValidations: validations,
        writerEpoch: 1,
      },
      validations: validations.map((validation, index) => ({
        ...validation,
        artifactId,
        highestSeverity: "none",
        id: `validation-${String(index + 1)}`,
        outcome: "pass",
        report: review,
        reportHash: index === 0 ? HASH : "c".repeat(64),
      })),
    },
  };
}
