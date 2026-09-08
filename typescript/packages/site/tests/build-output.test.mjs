import assert from "node:assert/strict";
import { hash } from "node:crypto";
import { readdir, readFile, stat } from "node:fs/promises";
import test from "node:test";

const siteRoot = new URL("../", import.meta.url);

async function filesBelow(url) {
  const entries = await readdir(url, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map((entry) => {
      const child = new URL(entry.name + (entry.isDirectory() ? "/" : ""), url);
      return entry.isDirectory() ? filesBelow(child) : [child];
    }),
  );
  return nested.flat();
}

void test("build emits a compact Cloudflare SSR Worker", async () => {
  const [entry, config, clientFiles, serverFiles] = await Promise.all([
    readFile(new URL("dist/server/entry.mjs", siteRoot), "utf8"),
    readFile(new URL("dist/server/wrangler.json", siteRoot), "utf8").then(
      JSON.parse,
    ),
    filesBelow(new URL("dist/client/", siteRoot)),
    filesBelow(new URL("dist/server/", siteRoot)),
  ]);

  assert.equal(config.main, "entry.mjs");
  assert.equal(config.assets.binding, "ASSETS");
  assert.equal(config.d1_databases[0]?.binding, "DB");
  assert.equal(config.d1_databases[0]?.database_name, "saqi-db");
  assert.equal(
    config.kv_namespaces.length,
    0,
    "unused sessions must remain disabled",
  );
  assert.ok(clientFiles.length <= 10, "public client artifact must stay tiny");
  assert.doesNotMatch(entry, /_next\/|elevenlabs|favorite/iu);
  const serverSource = await Promise.all(
    serverFiles
      .filter((file) => file.pathname.endsWith(".mjs"))
      .map((file) => readFile(file, "utf8")),
  );
  assert.doesNotMatch(
    serverSource.join("\n"),
    /data-search=/u,
    "search terms must be derived from visible names instead of duplicated in HTML",
  );
  assert.doesNotMatch(
    serverSource.join("\n"),
    /no-transform/u,
    "edge HTML must remain eligible for Brotli and gzip compression",
  );

  let bytes = 0;
  for (const file of clientFiles) {
    const fileStat = await stat(file);
    bytes += fileStat.size;
  }
  assert.ok(bytes < 140_000, "public browser assets must stay under 140 KiB");
});

void test("static assets retain immutable and strict security headers", async () => {
  const headers = await readFile(
    new URL("dist/client/_headers", siteRoot),
    "utf8",
  );

  assert.match(headers, /Content-Security-Policy: .*script-src 'self'/u);
  assert.doesNotMatch(headers, /'unsafe-inline'|'unsafe-eval'/u);
  assert.match(headers, /Referrer-Policy: no-referrer/u);
  assert.match(headers, /X-Content-Type-Options: nosniff/u);
  assert.match(headers, /\/_astro\/\*/u);
  assert.match(headers, /max-age=31536000, immutable/u);
  assert.match(headers, /\/favicon\.svg/u);
  assert.match(headers, /max-age=86400, stale-while-revalidate=604800/u);
});

void test("one verified Amiri subset serves poems and the bilingual wordmark", async () => {
  const fontUrl = new URL("src/assets/fonts/amiri-saqi-v1.003.woff2", siteRoot);
  const [font, fontBytes, fontFiles] = await Promise.all([
    stat(fontUrl),
    readFile(fontUrl),
    filesBelow(new URL("src/assets/fonts/", siteRoot)),
  ]);

  assert.ok(
    font.size <= 103_000,
    "shared Amiri subset must stay at or below 103 KiB",
  );
  assert.equal(
    hash("sha256", fontBytes, "hex"),
    "bc016b972dfd59f89aa988dd6083f6b6168856875a1c5885871ca30deba43133",
  );
  assert.equal(
    fontFiles.filter((file) => file.pathname.endsWith(".woff2")).length,
    1,
    "the public site must ship a single shared font",
  );
});

void test("redistributed fonts retain discoverable OFL notices", async () => {
  const clientFiles = await filesBelow(new URL("dist/client/", siteRoot));
  const notices = await Promise.all(
    clientFiles
      .filter((file) => file.pathname.endsWith(".txt"))
      .map((file) => readFile(file, "utf8")),
  );
  const noticeText = notices.join("\n");

  assert.match(noticeText, /Copyright 2010-2022 The Amiri Project Authors/u);
  assert.match(noticeText, /SIL OPEN FONT LICENSE Version 1\.1/u);
});
