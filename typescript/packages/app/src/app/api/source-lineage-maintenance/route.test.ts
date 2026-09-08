import { beforeEach, describe, expect, it, vi } from "vitest";

const matchesProduction = vi.fn();
const run = vi.fn();
const status = vi.fn();

vi.mock("@/lib/cloudflare", () => ({ getCloudflareEnv: () => ({ DB: {} }) }));
vi.mock("@/lib/get-source-lineage-maintenance", () => ({
  getSourceLineageMaintenance: () => ({ run, status }),
}));
vi.mock("@/lib/production-deployment-identity-repository", () => ({
  ProductionDeploymentIdentityRepository: class {
    matchesProduction = matchesProduction;
  },
}));

import { POST } from "./route";

describe("source lineage maintenance route", () => {
  beforeEach(() => {
    matchesProduction.mockReset().mockResolvedValue(true);
    run.mockReset().mockResolvedValue({ remaining: 7, state: "active" });
    status.mockReset().mockResolvedValue({ remaining: 7, state: "active" });
  });

  it("rejects an untrusted mutation", async () => {
    const response = await POST(request({ maxPages: 2 }, "https://evil.test"));
    expect(response.status).toBe(403);
    expect(run).not.toHaveBeenCalled();
  });

  it("runs bounded maintenance behind the operations boundary", async () => {
    const response = await POST(request({ maxPages: 2 }));
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toContain("no-store");
    expect(run).toHaveBeenCalledExactlyOnceWith({ maxPages: 2 });
  });

  it("fails closed on the wrong database", async () => {
    matchesProduction.mockResolvedValue(false);
    const postResponse = await POST(request({ maxPages: 2 }));
    expect(postResponse.status).toBe(503);
    expect(run).not.toHaveBeenCalled();
  });
});

function request(body: unknown, origin = "https://ops.saqi.app"): Request {
  return new Request("https://ops.saqi.app/api/source-lineage-maintenance", {
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
      host: "ops.saqi.app",
      origin,
      "sec-fetch-mode": "cors",
      "sec-fetch-site":
        origin === "https://ops.saqi.app" ? "same-origin" : "cross-site",
    },
    method: "POST",
  });
}
