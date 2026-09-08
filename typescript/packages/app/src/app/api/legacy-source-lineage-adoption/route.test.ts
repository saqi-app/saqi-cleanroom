import { beforeEach, describe, expect, it, vi } from "vitest";

const adoptLegacySourceLineage = vi.fn();
const matchesProduction = vi.fn();

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({ corpusRevision: { adoptLegacySourceLineage } }),
}));
vi.mock("@/lib/cloudflare", () => ({
  getCloudflareEnv: () => ({ DB: {} }),
}));
vi.mock("@/lib/production-deployment-identity-repository", () => ({
  ProductionDeploymentIdentityRepository: class {
    matchesProduction = matchesProduction;
  },
}));

import { POST } from "./route";

const POEM_ID = "22222222-2222-4222-8222-222222222222";

describe("legacy source lineage adoption route", () => {
  beforeEach(() => {
    adoptLegacySourceLineage.mockReset();
    adoptLegacySourceLineage.mockResolvedValue({
      adopted: 1,
      conflicts: [],
      scanned: 1,
      unchanged: 0,
    });
    matchesProduction.mockReset();
    matchesProduction.mockResolvedValue(true);
  });

  it("requires the protected same-origin boundary", async () => {
    const response = await POST(
      request({}, { origin: "https://evil.example" })
    );
    expect(response.status).toBe(403);
    expect(adoptLegacySourceLineage).not.toHaveBeenCalled();
  });

  it("accepts an explicit bounded set of completed Sol poem UUIDs", async () => {
    const response = await POST(request({ poemIds: [POEM_ID] }));

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toContain("no-store");
    expect(adoptLegacySourceLineage).toHaveBeenCalledExactlyOnceWith({
      poemIds: [POEM_ID],
    });
  });

  it("returns exact per-poem conflicts without rejecting successful peers", async () => {
    const conflictPoemId = "33333333-3333-4333-8333-333333333333";
    adoptLegacySourceLineage.mockResolvedValue({
      adopted: 9,
      conflicts: [
        {
          code: "LEGACY_SOURCE_AUTHOR_OWNERSHIP_CONFLICT",
          poemId: conflictPoemId,
        },
      ],
      scanned: 10,
      unchanged: 0,
    });

    const response = await POST(request({ poemIds: [POEM_ID] }));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      ok: true,
      result: {
        adopted: 9,
        conflicts: [
          {
            code: "LEGACY_SOURCE_AUTHOR_OWNERSHIP_CONFLICT",
            poemId: conflictPoemId,
          },
        ],
        scanned: 10,
        unchanged: 0,
      },
    });
  });

  it("rejects empty batches before any mutation", async () => {
    const response = await POST(request({ poemIds: [] }));
    expect(response.status).toBe(400);
    expect(adoptLegacySourceLineage).not.toHaveBeenCalled();
  });

  it("fails closed on the wrong production database", async () => {
    matchesProduction.mockResolvedValue(false);
    const response = await POST(request({ poemIds: [POEM_ID] }));
    expect(response.status).toBe(503);
    expect(adoptLegacySourceLineage).not.toHaveBeenCalled();
  });
});

function request(
  body: Record<string, unknown>,
  headers: Record<string, string> = {}
): Request {
  return new Request(
    "https://ops.saqi.app/api/legacy-source-lineage-adoption",
    {
      body: JSON.stringify(body),
      headers: {
        "content-type": "application/json",
        host: "ops.saqi.app",
        origin: "https://ops.saqi.app",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        ...headers,
      },
      method: "POST",
    }
  );
}
