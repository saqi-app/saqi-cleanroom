import assert from "node:assert/strict";
import test from "node:test";

import worker from "../www-worker";

void test("redirects www to the canonical HTTPS origin", () => {
  const requestUrl = new URL("https://www.saqi.app/author/poet?source=test");
  requestUrl.protocol = "http:";
  const response = worker.fetch(new Request(requestUrl));

  assert.equal(response.status, 308);
  assert.equal(
    response.headers.get("Location"),
    "https://saqi.app/author/poet?source=test",
  );
  assert.equal(response.headers.get("X-Content-Type-Options"), "nosniff");
});

void test("rejects method-preserving redirects for non-read requests", () => {
  const response = worker.fetch(
    new Request("https://www.saqi.app/author/poet", { method: "POST" }),
  );

  assert.equal(response.status, 405);
  assert.equal(response.headers.get("Allow"), "GET, HEAD");
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.equal(
    response.headers.get("Cloudflare-CDN-Cache-Control"),
    "no-store",
  );
});
