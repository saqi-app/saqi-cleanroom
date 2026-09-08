import { describe, expect, it } from "vitest";

import {
  normalizeProductionResolutionRequest,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
  PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_SCHEMA_VERSION,
  ProductionResolutionRequestSchema,
  ProductionResolutionResponseSchema,
  ProductionResolutionResultTargetSchema,
} from "./production-resolution";

describe("production resolution contracts", () => {
  it("normalizes target order and admits only the Codex model", () => {
    const normalized = normalizeProductionResolutionRequest({
      schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
      schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
      targets: [
        {
          modelKeys: ["sol-5.6"],
          sourceAuthorSlug: "poet-b",
          sourcePoemId: "9",
        },
        {
          modelKeys: ["sol-5.6"],
          sourceAuthorSlug: "poet-a",
          sourcePoemId: "10",
        },
      ],
    });

    expect(
      normalized.targets.map((target) =>
        "sourcePoemId" in target
          ? target.sourcePoemId
          : "poemId" in target
            ? target.poemId
            : target.lineNfcHash,
      ),
    ).toEqual(["10", "9"]);
    expect(normalized.targets[1]?.modelKeys).toEqual(["sol-5.6"]);
    expect(
      ProductionResolutionRequestSchema.safeParse({
        schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
        schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
        targets: [
          {
            modelKeys: ["retired-model"],
            sourceAuthorSlug: "poet-b",
            sourcePoemId: "9",
          },
        ],
      }).success,
    ).toBe(false);
  });

  it("accepts an exact canonical target without changing legacy normalization", () => {
    const legacy = {
      schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
      schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
      targets: [
        {
          modelKeys: ["sol-5.6" as const],
          sourceAuthorSlug: "poet-a",
          sourcePoemId: "10",
        },
      ],
    };
    expect(normalizeProductionResolutionRequest(legacy)).toEqual(legacy);
    expect(
      ProductionResolutionRequestSchema.safeParse({
        ...legacy,
        schemaVersion: 2,
        targets: [
          {
            modelKeys: ["sol-5.6"],
            poemId: "b".repeat(64),
            sourceRevisionId: "c".repeat(64),
          },
        ],
      }).success,
    ).toBe(true);
    expect(
      ProductionResolutionRequestSchema.safeParse({
        ...legacy,
        schemaVersion: 2,
        targets: [
          {
            modelKeys: ["sol-5.6"],
            poemId: "b".repeat(64),
            sourcePoemId: "10",
            sourceRevisionId: "c".repeat(64),
          },
        ],
      }).success,
    ).toBe(false);
  });

  it("normalizes exact two-hash fingerprint targets deterministically", () => {
    const normalized = normalizeProductionResolutionRequest({
      schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
      schemaVersion: PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION,
      targets: [
        {
          fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash: "b".repeat(64),
          modelKeys: ["sol-5.6"],
          promptMaterialHash: "d".repeat(64),
        },
        {
          fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash: "a".repeat(64),
          modelKeys: ["sol-5.6"],
          promptMaterialHash: "c".repeat(64),
        },
      ],
    });

    expect(
      normalized.targets.map((target) =>
        "lineNfcHash" in target ? target.lineNfcHash : null,
      ),
    ).toEqual(["a".repeat(64), "b".repeat(64)]);
    expect(normalized.targets[1]?.modelKeys).toEqual(["sol-5.6"]);
    expect(
      ProductionResolutionRequestSchema.safeParse({
        ...normalized,
        targets: [normalized.targets[0], normalized.targets[0]],
      }).success,
    ).toBe(false);
  });

  it("rejects duplicate targets, model keys, and partial source pointers", () => {
    const target = {
      modelKeys: ["sol-5.6"],
      sourceAuthorSlug: "poet-a",
      sourcePoemId: "10",
    };
    expect(
      ProductionResolutionRequestSchema.safeParse({
        schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
        schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
        targets: [target, target],
      }).success,
    ).toBe(false);
    expect(
      ProductionResolutionRequestSchema.safeParse({
        schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
        schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
        targets: [{ ...target, modelKeys: ["sol-5.6", "sol-5.6"] }],
      }).success,
    ).toBe(false);
    expect(
      ProductionResolutionResultTargetSchema.safeParse({
        authorId: "11111111-1111-4111-8111-111111111111",
        authorNameArabic: "شاعر",
        currentSourceRevisionId: "a".repeat(64),
        modelPointers: [],
        poemId: "22222222-2222-4222-8222-222222222222",
        sourceAuthorSlug: "poet-a",
        sourcePoemId: "10",
        sourcePointerVersion: null,
      }).success,
    ).toBe(false);
  });

  it("accepts deterministic SHA-256 canonical IDs but rejects other ID forms", () => {
    const response = {
      expiresAt: "2026-08-28T12:15:00.000Z",
      manifestHash: "a".repeat(64),
      observedAt: "2026-08-28T12:00:00.000Z",
      schemaId: "saqi.production-resolution-response",
      schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
      scopeHash: "b".repeat(64),
      targets: [
        {
          authorId: "c".repeat(64),
          authorNameArabic: "شاعر",
          currentSourceRevisionId: null,
          modelPointers: [],
          poemId: "d".repeat(64),
          sourceAuthorSlug: "poet-a",
          sourcePoemId: "10",
          sourcePointerVersion: null,
        },
      ],
      writerEpoch: 1,
    };

    expect(ProductionResolutionResponseSchema.safeParse(response).success).toBe(
      true,
    );
    expect(
      ProductionResolutionResponseSchema.safeParse({
        ...response,
        targets: [{ ...response.targets[0], poemId: "source:poem:10" }],
      }).success,
    ).toBe(false);
  });

  it("requires authoritative active fingerprints on response v2", () => {
    const target = {
      activeSourceFingerprint: {
        algorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
        lineNfcHash: "a".repeat(64),
        promptMaterialHash: "b".repeat(64),
      },
      authorId: "11111111-1111-4111-8111-111111111111",
      authorNameArabic: "شاعر",
      currentSourceNfcSha256: "a".repeat(64),
      currentSourceRevisionId: "c".repeat(64),
      modelPointers: [],
      poemId: "22222222-2222-4222-8222-222222222222",
      sourceAuthorSlug: "poet-a",
      sourcePoemId: "10",
      sourcePointerVersion: 1,
    };
    const response = {
      expiresAt: "2026-08-28T12:15:00.000Z",
      manifestHash: "d".repeat(64),
      observedAt: "2026-08-28T12:00:00.000Z",
      schemaId: "saqi.production-resolution-response",
      schemaVersion: PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
      scopeHash: "e".repeat(64),
      targets: [target],
      writerEpoch: 1,
    };

    expect(ProductionResolutionResponseSchema.safeParse(response).success).toBe(
      true,
    );
    expect(
      ProductionResolutionResponseSchema.safeParse({
        ...response,
        targets: [{ ...target, activeSourceFingerprint: undefined }],
      }).success,
    ).toBe(false);
    expect(
      ProductionResolutionResponseSchema.safeParse({
        ...response,
        targets: [{ ...target, currentSourceRevisionId: null }],
      }).success,
    ).toBe(false);
  });
});
