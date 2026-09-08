import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { withResponseHeaders } from "../src/lib/with-response-headers";

void describe("withResponseHeaders", () => {
  for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
    void it(`prevents caching successful ${method} responses`, () => {
      const response = withResponseHeaders(
        new Response(null, { status: 204 }),
        method,
        true,
      );

      assert.equal(response.headers.get("Cache-Control"), "no-store");
      assert.equal(
        response.headers.get("Cloudflare-CDN-Cache-Control"),
        "no-store",
      );
    });
  }

  void it("allows public caching for successful reads", () => {
    const response = withResponseHeaders(new Response("ok"), "GET", true);

    assert.equal(
      response.headers.get("Cache-Control"),
      "public, max-age=60, stale-while-revalidate=300, stale-if-error=86400",
    );
    assert.equal(
      response.headers.get("Cloudflare-CDN-Cache-Control") ?? "",
      "public, max-age=300, stale-while-revalidate=60, stale-if-error=86400",
    );
    assert.equal(response.headers.get("Cache-Tag"), "saqi-corpus");
  });
});
