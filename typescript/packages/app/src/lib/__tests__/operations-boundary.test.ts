import { describe, expect, it, vi } from "vitest";

import {
  isImmutableNextAsset,
  isTrustedMutationRequest,
  MAX_JSON_BODY_BYTES,
  readBoundedJson,
  secureOperationsResponse,
} from "../operations-boundary";

function mutationRequest(overrides: Record<string, string> = {}) {
  return new Request("https://ops.saqi.app/api/corpus-import", {
    method: "POST",
    headers: {
      host: "ops.saqi.app",
      origin: "https://ops.saqi.app",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
      ...overrides,
    },
    body: "{}",
  });
}

describe("mutation request checks", () => {
  it("accepts exact same-origin browser metadata", () => {
    expect(isTrustedMutationRequest(mutationRequest())).toBe(true);
  });

  it.each([
    ["host", "evil.example"],
    ["origin", "https://evil.example"],
    ["sec-fetch-site", "cross-site"],
    ["sec-fetch-mode", "navigate"],
  ])("rejects invalid %s metadata", (header, value) => {
    expect(isTrustedMutationRequest(mutationRequest({ [header]: value }))).toBe(
      false
    );
  });
});

describe("response cache boundary", () => {
  it("allows only successful framework static assets to be immutable", () => {
    const asset = new Request(
      "https://ops.saqi.app/_next/static/chunks/app-deadbeef.js"
    );
    expect(isImmutableNextAsset(asset, new Response("ok"))).toBe(true);
    expect(
      isImmutableNextAsset(
        new Request("https://ops.saqi.app/tasks"),
        new Response("ok")
      )
    ).toBe(false);
    expect(
      isImmutableNextAsset(asset, new Response("missing", { status: 404 }))
    ).toBe(false);
  });

  it("applies immutable caching only to successful static responses", () => {
    const asset = new Request(
      "https://ops.saqi.app/_next/static/chunks/app-deadbeef.js"
    );
    const immutable = secureOperationsResponse(new Response("ok"), asset);
    expect(immutable.headers.get("cache-control")).toBe(
      "private, max-age=31536000, immutable"
    );
    expect(immutable.headers.has("expires")).toBe(false);
    expect(immutable.headers.has("pragma")).toBe(false);
    expect(immutable.headers.get("x-robots-tag")).toContain("noindex");

    const html = secureOperationsResponse(
      new Response("ok"),
      new Request("https://ops.saqi.app/tasks")
    );
    expect(html.headers.get("cache-control")).toBe(
      "private, no-store, max-age=0"
    );
    expect(html.headers.get("pragma")).toBe("no-cache");
    expect(html.headers.get("content-security-policy")).toContain(
      "default-src 'self'"
    );
    expect(html.headers.get("content-security-policy")).toContain(
      "connect-src 'self'"
    );
  });
});

describe("bounded JSON reader", () => {
  it.each([undefined, "1"])(
    "cancels a multi-chunk overflow with content-length %s",
    async (contentLength) => {
      const cancel = vi.fn();
      const sizes = [MAX_JSON_BODY_BYTES / 2, MAX_JSON_BODY_BYTES / 2, 1];
      let pulls = 0;
      const body = new ReadableStream<Uint8Array>(
        {
          cancel,
          pull(controller) {
            const size = sizes[pulls++];
            if (size === undefined) throw new Error("Read beyond overflow");
            controller.enqueue(new Uint8Array(size));
          },
        },
        { highWaterMark: 0 }
      );
      const init = {
        body,
        duplex: "half",
        headers: contentLength ? { "content-length": contentLength } : {},
        method: "POST",
      } satisfies RequestInit & { duplex: "half" };
      const request = new Request("https://ops.saqi.app/api", init);
      await expect(readBoundedJson(request)).rejects.toThrow(
        "Request body too large"
      );
      expect(pulls).toBe(3);
      expect(cancel).toHaveBeenCalledOnce();
      expect(body.locked).toBe(false);
    }
  );

  it("parses a small JSON body", async () => {
    const request = mutationRequest({ "content-type": "application/json" });
    await expect(readBoundedJson(request)).resolves.toEqual({});
  });

  it("rejects bodies over the byte limit", async () => {
    const request = new Request("https://ops.saqi.app/api", {
      method: "POST",
      body: `"${"a".repeat(MAX_JSON_BODY_BYTES)}"`,
    });
    await expect(readBoundedJson(request)).rejects.toThrow();
  });
});
