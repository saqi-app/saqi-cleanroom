import { beforeEach, describe, expect, it, vi } from "vitest";

const matchesProduction = vi.fn();
const status = vi.fn();

vi.mock("@/lib/cloudflare", () => ({ getCloudflareEnv: () => ({ DB: {} }) }));
vi.mock("@/lib/get-source-lineage-maintenance", () => ({
  getSourceLineageMaintenance: () => ({ status }),
}));
vi.mock("@/lib/production-deployment-identity-repository", () => ({
  ProductionDeploymentIdentityRepository: class {
    matchesProduction = matchesProduction;
  },
}));

import { GET } from "./route";

describe("source lineage maintenance status route", () => {
  beforeEach(() => {
    matchesProduction.mockReset().mockResolvedValue(true);
    status.mockReset().mockResolvedValue({ remaining: 7, state: "active" });
  });

  it("exposes exact health status", async () => {
    const response = await GET();
    await expect(response.json()).resolves.toEqual({
      ok: true,
      result: { remaining: 7, state: "active" },
    });
  });

  it("fails closed on the wrong database", async () => {
    matchesProduction.mockResolvedValue(false);
    const response = await GET();
    expect(response.status).toBe(503);
    expect(status).not.toHaveBeenCalled();
  });
});
