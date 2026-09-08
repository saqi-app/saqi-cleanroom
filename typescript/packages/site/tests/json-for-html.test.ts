import assert from "node:assert/strict";
import test from "node:test";

import { serializeJsonForHtml } from "../src/lib/serialize-json-for-html";

await test("serializes hostile JSON without creating an HTML script boundary", () => {
  const hostile = {
    name: "</script><script>globalThis.compromised = true</script>",
    separators: `before\u{2028}middle\u{2029}after`,
    entity: "Tom & <Jerry>",
  };

  const serialized = serializeJsonForHtml(hostile);

  assert.doesNotMatch(serialized, /[<>&\u{2028}\u{2029}]/u);
  assert.doesNotMatch(serialized, /<\/script/iu);
  assert.deepEqual(JSON.parse(serialized), hostile);
});

await test("rejects values JSON cannot represent", () => {
  assert.throws(() => serializeJsonForHtml(undefined), TypeError);
});
