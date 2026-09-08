import { beforeEach, describe, expect, it, vi } from "vitest";

const backfillActiveSourceFingerprints = vi.fn();
const matchesProduction = vi.fn();

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({
    corpusRevision: { backfillActiveSourceFingerprints },
  }),
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

describe("source fingerprint backfill route", () => {
  beforeEach(() => {
    backfillActiveSourceFingerprints.mockReset();
    backfillActiveSourceFingerprints.mockResolvedValue({
      complete: true,
      existing: 0,
      inserted: 1,
      nextCursor: null,
      scanned: 1,
    });
    matchesProduction.mockReset();
    matchesProduction.mockResolvedValue(true);
  });

  it("requires the protected same-origin boundary", async () => {
    const result = await POST(request({}, { origin: "https://evil.example" }));
    expect(result.status).toBe(403);
    expect(backfillActiveSourceFingerprints).not.toHaveBeenCalled();
  });

  it("runs one bounded page and forwards its durable cursor", async () => {
    const cursor = {
      createdAt: 123,
      sourceRevisionId: "a".repeat(64),
    };
    const result = await POST(request({ cursor, limit: 7 }));

    expect(result.status).toBe(200);
    expect(result.headers.get("cache-control")).toContain("no-store");
    expect(backfillActiveSourceFingerprints).toHaveBeenCalledExactlyOnceWith({
      cursor,
      limit: 7,
    });
    await expect(result.json()).resolves.toMatchObject({
      ok: true,
      result: { inserted: 1, scanned: 1 },
    });
  });

  it("fails closed on the wrong production database", async () => {
    matchesProduction.mockResolvedValue(false);
    const result = await POST(request({}));
    expect(result.status).toBe(503);
    expect(backfillActiveSourceFingerprints).not.toHaveBeenCalled();
  });
});

function request(
  body: Record<string, unknown>,
  headers: Record<string, string> = {}
): Request {
  return new Request("https://ops.saqi.app/api/corpus-fingerprint-backfill", {
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
  });
}
