import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";

const repositoryRoot = new URL("../../", import.meta.url);
const [configuration, configurator, workflow] = await Promise.all([
  readFile(
    new URL("typescript/packages/app/wrangler.jsonc", repositoryRoot),
    "utf8",
  ),
  readFile(
    new URL(".github/scripts/configure-ops-secrets.mjs", repositoryRoot),
    "utf8",
  ),
  readFile(new URL(".github/workflows/deploy.yml", repositoryRoot), "utf8"),
]);

const requiredSecrets = [
  ["CF_CACHE_PURGE_TOKEN", "CF_CACHE_PURGE_TOKEN"],
  ["CF_ZONE_ID", "CF_ZONE_ID"],
  ["SAQI_ACCESS_AUDIENCE", "CF_ACCESS_AUD"],
  ["SAQI_ACCESS_TEAM_ORIGIN", "CF_ACCESS_TEAM_DOMAIN"],
  ["SAQI_SOURCE_BASE_URL", "SAQI_SOURCE_BASE_URL"],
  ["SAQI_SOURCE_ADAPTER_CONFIG", "SAQI_SOURCE_ADAPTER_CONFIG"],
  ["SAQI_SOURCE_NAME", "SAQI_SOURCE_NAME"],
];

for (const [binding, environmentName] of requiredSecrets) {
  assert.match(configurator, new RegExp(`"${binding}"`));
  assert.match(configuration, new RegExp(`"${binding}"`));
  assert.match(
    workflow,
    new RegExp(
      `${environmentName}: \\$\\{\\{ secrets\\.${environmentName} \\}\\}`,
    ),
  );
}

assert.doesNotMatch(
  configuration,
  /"CF_ACCESS_(?:AUD|SERVICE_TOKEN_COMMON_NAMES|TEAM_DOMAIN)"\s*:\s*"[^\n]+"/u,
);

const validEnvironment = {
  CF_ACCESS_AUD: "a".repeat(64),
  CF_ACCESS_TEAM_DOMAIN: "https://example.cloudflareaccess.com",
  CF_CACHE_PURGE_TOKEN: "cache-token",
  CF_ZONE_ID: "b".repeat(32),
  SAQI_SOURCE_ADAPTER_CONFIG: JSON.stringify({
    dom: Object.fromEntries(
      [
        "authorContainerSelector",
        "authorLinkSelector",
        "detailAuthorLinkSelector",
        "detailClassicalLineSelector",
        "detailContentSelector",
        "detailFallbackLineSelector",
        "detailStructureLabelSelector",
        "manifestContainerSelector",
        "manifestPoemLinkSelector",
        "nextPageLinkSelector",
      ].map((field) => [field, "main"]),
    ),
    feed: {
      authorIdFormat: "slug",
      cursorKeys: ["cursor"],
      cursorParameter: "cursor",
      endpointMarker: "feed",
      responseHtmlKey: "html",
      responseNextCursorKey: "next_cursor",
      tokenKeys: ["token"],
      tokenParameter: "token",
    },
    labels: {
      freeVerse: ["free verse"],
      poemCount: ["poems"],
      verseCount: ["verses"],
    },
    routes: {
      authorPath: "/writers/{slug}",
      feedPath: "/writers/{authorId}/feed",
      inventoryPath: "/directory/{page}",
      poemPath: "/works/{id}",
      poemSlug: "work-{id}",
    },
    schemaVersion: 1,
  }),
  SAQI_SOURCE_BASE_URL: "https://source.example",
  SAQI_SOURCE_NAME: "source",
};

function validate(overrides = {}) {
  return spawnSync(
    process.execPath,
    [
      new URL("configure-ops-secrets.mjs", import.meta.url).pathname,
      "--validate-only",
    ],
    {
      encoding: "utf8",
      env: { ...validEnvironment, ...overrides },
    },
  );
}

const validResult = validate();
assert.equal(validResult.status, 0, validResult.stderr);
for (const [name, value] of [
  ["CF_ACCESS_AUD", "not-an-audience"],
  ["CF_ACCESS_TEAM_DOMAIN", "https://example.com"],
  ["CF_ZONE_ID", "not-a-zone"],
  ["SAQI_SOURCE_NAME", "INVALID NAME"],
  ["SAQI_SOURCE_BASE_URL", "not-a-url"],
  ["SAQI_SOURCE_ADAPTER_CONFIG", "{}"],
]) {
  const result = validate({ [name]: value });
  assert.notEqual(result.status, 0, `${name} must fail closed`);
  assert.doesNotMatch(result.stderr, new RegExp(value.replaceAll("/", "\\/")));
}
