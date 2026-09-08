import assert from "node:assert/strict";
import test from "node:test";

import {
  contextualPageTitle,
  metaDescription,
  pageTitle,
} from "../src/lib/metadata";

function codePointLength(value: string): number {
  let length = 0;
  for (const _codePoint of value) length += 1;
  return length;
}

void test("contextual poem titles preserve author identity within the limit", () => {
  const title = contextualPageTitle(
    "A very long poem title that would otherwise consume the entire title budget",
    "Khalil Gibran",
  );
  assert.ok(title.length <= 65);
  assert.match(title, /Khalil Gibran — Saqi$/);
});

void test("metadata preserves exact code-point limits and normalized whitespace", () => {
  assert.equal(pageTitle("a".repeat(58)), `${"a".repeat(58)} — Saqi`);
  assert.equal(pageTitle("a".repeat(59)), `${"a".repeat(57)}… — Saqi`);
  assert.equal(metaDescription("a".repeat(155)), "a".repeat(155));
  assert.equal(metaDescription("a".repeat(156)), `${"a".repeat(154)}…`);
  assert.equal(metaDescription("  first\n\tsecond  "), "first second");
});

void test("Arabic combining marks stay attached without expanding title budgets", () => {
  const syllable = "مُ";
  const title = pageTitle(syllable.repeat(40));
  assert.equal(title, `${syllable.repeat(28)}… — Saqi`);
  assert.ok(codePointLength(title) <= 65);
  const contextual = contextualPageTitle(
    syllable.repeat(40),
    syllable.repeat(20),
  );
  assert.equal(
    contextual,
    `${syllable.repeat(15)}… — ${syllable.repeat(11)}… — Saqi`,
  );
  assert.ok(codePointLength(contextual) <= 65);
});

void test("ZWJ emoji remain whole within Unicode code-point rather than grapheme budgets", () => {
  const family = "👩‍👩‍👧‍👦";
  const title = pageTitle(family.repeat(10));
  const description = metaDescription(family.repeat(23));
  assert.equal(title, `${family.repeat(8)}… — Saqi`);
  assert.equal(description, `${family.repeat(22)}…`);
  assert.equal(codePointLength(description), 155);
  assert.ok(codePointLength(title) <= 65);
  assert.equal(
    metaDescription(`${"a".repeat(154)}${family}`),
    `${"a".repeat(154)}…`,
  );
});
