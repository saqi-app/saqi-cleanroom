import { spawnSync } from "node:child_process";

const SECRET_BINDINGS = [
  ["CF_CACHE_PURGE_TOKEN", "CF_CACHE_PURGE_TOKEN"],
  ["CF_ZONE_ID", "CF_ZONE_ID"],
  ["SAQI_ACCESS_AUDIENCE", "CF_ACCESS_AUD"],
  ["SAQI_ACCESS_TEAM_ORIGIN", "CF_ACCESS_TEAM_DOMAIN"],
  ["SAQI_SOURCE_BASE_URL", "SAQI_SOURCE_BASE_URL"],
  ["SAQI_SOURCE_ADAPTER_CONFIG", "SAQI_SOURCE_ADAPTER_CONFIG"],
  ["SAQI_SOURCE_NAME", "SAQI_SOURCE_NAME"],
];

const SOURCE_PROFILE_FIELDS = {
  dom: [
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
  ],
  feed: [
    "authorIdFormat",
    "cursorKeys",
    "cursorParameter",
    "endpointMarker",
    "responseHtmlKey",
    "responseNextCursorKey",
    "tokenKeys",
    "tokenParameter",
  ],
  labels: ["freeVerse", "poemCount", "verseCount"],
  routes: ["authorPath", "feedPath", "inventoryPath", "poemPath", "poemSlug"],
};

function invalid(name) {
  throw new Error(`Invalid required operations secret: ${name}`);
}

function cleanHttpsOrigin(value, cloudflareAccess = false) {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      !url.username &&
      !url.password &&
      !url.port &&
      (url.pathname === "/" || url.pathname === "") &&
      !url.search &&
      !url.hash &&
      (!cloudflareAccess ||
        (url.hostname.endsWith(".cloudflareaccess.com") &&
          url.hostname !== "cloudflareaccess.com"))
    );
  } catch {
    return false;
  }
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactFields(value, fields) {
  return (
    isRecord(value) &&
    Object.keys(value).toSorted().join("\0") === fields.toSorted().join("\0")
  );
}

function isBoundedText(value, maximum = 512) {
  return (
    typeof value === "string" &&
    value.trim().length > 0 &&
    value.length <= maximum
  );
}

function isLexemeList(value) {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.length <= 16 &&
    value.every((item) => isBoundedText(item, 64))
  );
}

function validSourceProfile(raw) {
  try {
    const profile = JSON.parse(raw);
    if (
      !hasExactFields(profile, [
        "dom",
        "feed",
        "labels",
        "routes",
        "schemaVersion",
      ]) ||
      profile.schemaVersion !== 1
    ) {
      return false;
    }
    if (
      !hasExactFields(profile.dom, SOURCE_PROFILE_FIELDS.dom) ||
      !SOURCE_PROFILE_FIELDS.dom.every((field) =>
        isBoundedText(profile.dom[field]),
      )
    ) {
      return false;
    }
    if (
      !hasExactFields(profile.labels, SOURCE_PROFILE_FIELDS.labels) ||
      !SOURCE_PROFILE_FIELDS.labels.every((field) =>
        isLexemeList(profile.labels[field]),
      )
    ) {
      return false;
    }
    if (!hasExactFields(profile.feed, SOURCE_PROFILE_FIELDS.feed)) return false;
    if (
      !["positive_integer", "slug"].includes(profile.feed.authorIdFormat) ||
      !isLexemeList(profile.feed.cursorKeys) ||
      !isLexemeList(profile.feed.tokenKeys) ||
      ![
        "cursorParameter",
        "endpointMarker",
        "responseHtmlKey",
        "responseNextCursorKey",
        "tokenParameter",
      ].every((field) => isBoundedText(profile.feed[field], 128))
    ) {
      return false;
    }
    if (!hasExactFields(profile.routes, SOURCE_PROFILE_FIELDS.routes))
      return false;
    return SOURCE_PROFILE_FIELDS.routes.every((field) => {
      const value = profile.routes[field];
      return isBoundedText(value, 256) && !/[\\\s]/u.test(value);
    });
  } catch {
    return false;
  }
}

function validateSecrets(secrets) {
  if (!/^[\da-f]{64}$/u.test(secrets.SAQI_ACCESS_AUDIENCE))
    invalid("CF_ACCESS_AUD");
  if (!cleanHttpsOrigin(secrets.SAQI_ACCESS_TEAM_ORIGIN, true))
    invalid("CF_ACCESS_TEAM_DOMAIN");
  if (!/^[\da-f]{32}$/u.test(secrets.CF_ZONE_ID)) invalid("CF_ZONE_ID");
  if (!/^[a-z][a-z0-9_-]{1,63}$/u.test(secrets.SAQI_SOURCE_NAME))
    invalid("SAQI_SOURCE_NAME");
  if (!cleanHttpsOrigin(secrets.SAQI_SOURCE_BASE_URL))
    invalid("SAQI_SOURCE_BASE_URL");
  if (!validSourceProfile(secrets.SAQI_SOURCE_ADAPTER_CONFIG))
    invalid("SAQI_SOURCE_ADAPTER_CONFIG");
}

const secrets = Object.fromEntries(
  SECRET_BINDINGS.map(([binding, environmentName]) => {
    const value = process.env[environmentName];
    if (value === undefined || value.length === 0) {
      throw new Error(`Missing required operations secret: ${environmentName}`);
    }
    return [binding, value];
  }),
);

validateSecrets(secrets);

if (process.argv[2] === "--validate-only") process.exit(0);

const result = spawnSync(
  "yarn",
  ["workspace", "@saqi/app", "wrangler", "secret", "bulk"],
  {
    cwd: new URL("../../typescript/", import.meta.url),
    input: JSON.stringify(secrets),
    stdio: ["pipe", "inherit", "inherit"],
  },
);

if (result.error !== undefined) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
