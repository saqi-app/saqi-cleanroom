import assert from "node:assert/strict";
import test from "node:test";

import { normalizeSearch } from "../src/scripts/normalize-search.js";

test("normalizes search text independently of the runtime locale", () => {
  assert.equal(normalizeSearch("  ISTANBUL  "), "istanbul");
  assert.equal(normalizeSearch("Cafe\u{301}"), normalizeSearch("Caf\u{E9}"));
  assert.equal(normalizeSearch("\u{FF21}\u{FF22}\u{FF23}"), "abc");
});

test("folds common Arabic spelling and presentation variants", () => {
  assert.equal(normalizeSearch("آمِنَة"), "امنة");
  assert.equal(normalizeSearch("عَلِيّ"), normalizeSearch("علي"));
  assert.equal(normalizeSearch("مسـؤول"), normalizeSearch("مسوول"));
  assert.equal(normalizeSearch("شاطئ"), normalizeSearch("شاطي"));
  assert.equal(normalizeSearch("عبد  الله"), normalizeSearch("عبد الله"));
  assert.equal(normalizeSearch("مالک"), normalizeSearch("مالك"));
  assert.equal(normalizeSearch("علی"), normalizeSearch("علي"));
});
