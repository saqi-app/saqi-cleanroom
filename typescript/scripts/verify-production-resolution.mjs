import { hash } from "node:crypto";
import { readFile } from "node:fs/promises";

import {
  normalizeProductionResolutionRequest,
  ProductionResolutionApiResponseSchema,
  ProductionResolutionRequestSchema,
} from "@saqi/precedent-iso";
import { canonicalJson } from "@saqi/source-adapter";

const maximumClockSkewMs = 5 * 60_000;
const maximumScopeLifetimeMs = 60 * 60_000;

const [mode, serializedRequest, headersPath, responsePath] =
  process.argv.slice(2);
if (
  !["canonical", "fingerprint", "source"].includes(mode) ||
  serializedRequest === undefined ||
  headersPath === undefined ||
  responsePath === undefined
) {
  throw new Error(
    "usage: verify-production-resolution.mjs <source|canonical|fingerprint> <request-json> <headers-path> <response-path>",
  );
}

const request = normalizeProductionResolutionRequest(
  ProductionResolutionRequestSchema.parse(JSON.parse(serializedRequest)),
);
const headers = await readFile(headersPath, "utf8");
const response = ProductionResolutionApiResponseSchema.parse(
  JSON.parse(await readFile(responsePath, "utf8")),
).result;

if (
  !/^cache-control:\s*private,\s*no-store,\s*max-age=0\s*$/imu.test(headers)
) {
  throw new Error("PRODUCTION_RESOLUTION_CACHE_CONTROL_INVALID");
}
if (response.scopeHash !== sha256(canonicalJson(request))) {
  throw new Error("PRODUCTION_RESOLUTION_SCOPE_HASH_MISMATCH");
}
const { manifestHash, ...body } = response;
if (manifestHash !== sha256(canonicalJson(body))) {
  throw new Error("PRODUCTION_RESOLUTION_MANIFEST_HASH_MISMATCH");
}
const observedAt = Date.parse(response.observedAt);
const expiresAt = Date.parse(response.expiresAt);
const now = Date.now();
if (
  expiresAt <= observedAt ||
  expiresAt - observedAt > maximumScopeLifetimeMs
) {
  throw new Error("PRODUCTION_RESOLUTION_EXPIRY_INVALID");
}
if (observedAt > now + maximumClockSkewMs) {
  throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE");
}
if (expiresAt <= now) {
  throw new Error("PRODUCTION_RESOLUTION_EXPIRED");
}
if (response.targets.length !== 1 || request.targets.length !== 1) {
  throw new Error("PRODUCTION_RESOLUTION_CANARY_SCOPE_INVALID");
}

const [requested] = request.targets;
const [resolved] = response.targets;
if (requested === undefined || resolved === undefined) {
  throw new Error("PRODUCTION_RESOLUTION_CANARY_TARGET_MISSING");
}
if (resolved.currentSourceRevisionId === null) {
  throw new Error("PRODUCTION_RESOLUTION_CURRENT_REVISION_MISSING");
}
if (
  resolved.modelPointers.some(
    ({ modelKey }) => !requested.modelKeys.includes(modelKey),
  )
) {
  throw new Error("PRODUCTION_RESOLUTION_MODEL_SCOPE_MISMATCH");
}

if (mode === "source") {
  if (response.schemaVersion !== 1) {
    throw new Error("PRODUCTION_RESOLUTION_LEGACY_RESPONSE_VERSION_INVALID");
  }
  if (!("sourcePoemId" in requested)) {
    throw new Error("PRODUCTION_RESOLUTION_SOURCE_REQUEST_REQUIRED");
  }
  if (
    resolved.sourcePoemId !== requested.sourcePoemId ||
    resolved.sourceAuthorSlug !== requested.sourceAuthorSlug
  ) {
    throw new Error("PRODUCTION_RESOLUTION_SOURCE_IDENTITY_MISMATCH");
  }
  process.stdout.write(
    JSON.stringify({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 2,
      targets: [
        {
          modelKeys: requested.modelKeys,
          poemId: resolved.poemId,
          sourceRevisionId: resolved.currentSourceRevisionId,
        },
      ],
    }),
  );
} else if (mode === "canonical") {
  if (response.schemaVersion !== 1) {
    throw new Error("PRODUCTION_RESOLUTION_LEGACY_RESPONSE_VERSION_INVALID");
  }
  if (!("poemId" in requested)) {
    throw new Error("PRODUCTION_RESOLUTION_CANONICAL_REQUEST_REQUIRED");
  }
  if (
    resolved.poemId !== requested.poemId ||
    resolved.currentSourceRevisionId !== requested.sourceRevisionId
  ) {
    throw new Error("PRODUCTION_RESOLUTION_CANONICAL_IDENTITY_MISMATCH");
  }
} else {
  if (!("fingerprintAlgorithm" in requested)) {
    throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_REQUEST_REQUIRED");
  }
  if (
    response.schemaVersion !== 2 ||
    !("activeSourceFingerprint" in resolved) ||
    resolved.activeSourceFingerprint.algorithm !==
      requested.fingerprintAlgorithm ||
    resolved.activeSourceFingerprint.lineNfcHash !== requested.lineNfcHash ||
    resolved.activeSourceFingerprint.promptMaterialHash !==
      requested.promptMaterialHash ||
    resolved.currentSourceNfcSha256 !== requested.lineNfcHash ||
    resolved.sourcePointerVersion === null
  ) {
    throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_IDENTITY_MISMATCH");
  }
}

function sha256(value) {
  return hash("sha256", value, "hex");
}
