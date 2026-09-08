import assert from "node:assert/strict";
import test from "node:test";

import {
  canonicalRedirectUrl,
  isReadMethod,
} from "../src/lib/canonical-request";

void test("classifies only GET and HEAD as read methods", () => {
  assert.equal(isReadMethod("GET"), true);
  assert.equal(isReadMethod("HEAD"), true);
  assert.equal(isReadMethod("POST"), false);
  assert.equal(isReadMethod("get"), false);
});

void test("GET and HEAD query strings redirect to one cacheable URL", () => {
  assert.equal(
    canonicalRedirectUrl(
      "https://saqi.app/author/poet?page=2&utm_source=test",
      "GET",
    )?.href,
    "https://saqi.app/author/poet",
  );
  assert.equal(
    canonicalRedirectUrl("https://preview.example/author/poet?random=1", "HEAD")
      ?.href,
    "https://preview.example/author/poet",
  );
});

void test("canonical redirect combines host and query normalization", () => {
  assert.equal(
    canonicalRedirectUrl("https://www.saqi.app/author/poet?random=1", "GET")
      ?.href,
    "https://saqi.app/author/poet",
  );
  assert.equal(
    canonicalRedirectUrl("https://saqi.app/author/poet", "GET"),
    undefined,
  );
  assert.equal(
    canonicalRedirectUrl("https://saqi.app/api?command=1", "POST"),
    undefined,
  );
});

void test("canonical redirect normalizes duplicate slashes and unreserved escapes", () => {
  assert.equal(
    canonicalRedirectUrl(
      "https://saqi.app//%61uthor//poet%2dabn-rumi/page/%32",
      "GET",
    )?.href,
    "https://saqi.app/author/poet-abn-rumi/page/2",
  );
  assert.equal(
    canonicalRedirectUrl("https://saqi.app/author/poet%2Fabn-rumi", "GET"),
    undefined,
  );
});
