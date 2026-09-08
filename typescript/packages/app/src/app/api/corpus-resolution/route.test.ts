import {
  MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES,
  MAX_PRODUCTION_RESOLUTION_TARGETS,
  PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
  PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID,
  PRODUCTION_RESOLUTION_SCHEMA_VERSION,
} from "@saqi/precedent-iso";
import { ProductionResolutionConflictError } from "@saqi/precedent-node";
import { beforeEach, describe, expect, it, vi } from "vitest";

const resolve = vi.fn();

vi.mock("@/backend/get-services", () => ({
  getServices: () => ({ productionResolution: { resolve } }),
}));

import { POST } from "./route";

const HASH = "a".repeat(64);

describe("corpus resolution route", () => {
  beforeEach(() => {
    resolve.mockReset();
    resolve.mockResolvedValue({
      expiresAt: "2026-08-28T12:15:00.000Z",
      manifestHash: HASH,
      observedAt: "2026-08-28T12:00:00.000Z",
      schemaId: PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID,
      schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
      scopeHash: HASH,
      targets: [],
      writerEpoch: 1,
    });
  });

  it("requires the protected same-origin operations boundary", async () => {
    const result = await POST(request({ origin: "https://evil.example" }));

    expect(result.status).toBe(403);
    expect(resolve).not.toHaveBeenCalled();
  });

  it("returns a no-store exact-target resolution envelope", async () => {
    const result = await POST(request());

    expect(result.status).toBe(200);
    expect(result.headers.get("cache-control")).toContain("no-store");
    expect(resolve).toHaveBeenCalledExactlyOnceWith(body());
    await expect(result.json()).resolves.toMatchObject({ ok: true });
  });

  it("classifies ownership conflicts separately from transient failures", async () => {
    resolve.mockRejectedValueOnce(
      new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED"
      )
    );
    const conflict = await POST(request());
    expect(conflict.status).toBe(409);

    resolve.mockRejectedValueOnce(new Error("D1 transient"));
    const transient = await POST(request());
    expect(transient.status).toBe(503);
    await expect(transient.json()).resolves.toEqual({
      error: "PRODUCTION_RESOLUTION_UNAVAILABLE",
      ok: false,
    });
  });

  it("rejects invalid and oversized request bodies before resolution", async () => {
    const invalid = await POST(request({}, { targets: [] }));
    expect(invalid.status).toBe(400);

    const oversized = await POST(
      request({
        "content-length": String(MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES + 1),
      })
    );
    expect(oversized.status).toBe(413);
    expect(resolve).not.toHaveBeenCalled();
  });

  it("accepts the schema maximum target set within the shared byte limit", async () => {
    const maximum = {
      ...body(),
      targets: Array.from(
        { length: MAX_PRODUCTION_RESOLUTION_TARGETS },
        (_, index) => ({
          modelKeys: ["sol-5.6"],
          sourceAuthorSlug: `${"a".repeat(990)}-${String(index)}`,
          sourcePoemId: String(index + 1),
        })
      ),
    };
    expect(Buffer.byteLength(JSON.stringify(maximum))).toBeLessThanOrEqual(
      MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES
    );

    const result = await POST(request({}, maximum));

    expect(result.status).toBe(200);
    expect(resolve).toHaveBeenCalledExactlyOnceWith(maximum);
  });
});

function body() {
  return {
    schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
    schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
    targets: [
      {
        modelKeys: ["sol-5.6"],
        sourceAuthorSlug: "poet-test",
        sourcePoemId: "82737",
      },
    ],
  };
}

function request(
  headers: Record<string, string> = {},
  content: Record<string, unknown> = body()
): Request {
  return new Request("https://ops.saqi.app/api/corpus-resolution", {
    body: JSON.stringify(content),
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
