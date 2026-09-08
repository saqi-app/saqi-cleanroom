import { describe, expect, it } from "vitest";

import {
  type D1CapacityPreflightInput,
  planD1CapacityPreflight,
  SAQI_CURRENT_D1_BYTES,
  SAQI_CURRENT_POEMS,
} from "../publication/d1-capacity-preflight";

const NOW = Date.parse("2026-08-25T22:00:00Z");

function input(
  overrides: Partial<D1CapacityPreflightInput> = {},
): D1CapacityPreflightInput {
  return {
    actions: [
      {
        fixedQueries: 4,
        maximumRecords: 50,
        name: "stage_and_plan",
        queriesPerRecord: 7,
        rowsWrittenPerRecord: 8,
      },
      {
        fixedQueries: 3,
        maximumRecords: 50,
        name: "seal_and_promote",
        queriesPerRecord: 3,
        rowsWrittenPerRecord: 4,
      },
    ],
    backupProof: {
      exportBytes: SAQI_CURRENT_D1_BYTES,
      exportSha256: "a".repeat(64),
      exportVerifiedAt: NOW - 60_000,
      timeTravelBookmark: "00000080-ffffffff-proof",
      timeTravelVerifiedAt: NOW - 60_000,
    },
    currentDatabaseBytes: SAQI_CURRENT_D1_BYTES,
    currentEnrichments: 0,
    currentPoems: SAQI_CURRENT_POEMS,
    estimates: {
      averageEnrichmentPayloadBytes: 12_000,
      enrichmentMetadataBytes: 1_200,
      indexExpansionBytes: 40_000_000,
      indexOverheadBasisPoints: 2_000,
      migrationFixedBytes: 2_000_000,
      reviewBytesPerEnrichment: 2_000,
    },
    observedAt: NOW,
    safetyMarginBasisPoints: 2_000,
    targetEnrichments: SAQI_CURRENT_POEMS,
    tier: "paid",
    ...overrides,
  };
}

describe("D1 capacity and import-cost preflight", () => {
  it("models the current corpus and recommends safe paid chunks", () => {
    const report = planD1CapacityPreflight(input());
    expect(report).toMatchObject({
      blockers: [],
      currentDatabaseBytes: 375_873_536,
      currentPoems: 100_064,
      databaseLimitBytes: 10 * 1024 * 1024 * 1024,
      go: true,
      projectedRemainingEnrichments: 100_064,
      projectedBytesPerEnrichment: 18_240,
      projectedMigrationAndIndexBytes: 42_000_000,
      queryBudgetPerInvocation: 800,
      queryCeilingPerInvocation: 1_000,
      recommendedChunk: 50,
      recommendedConcurrency: 1,
      tier: "paid",
    });
    expect(report.projectedDatabaseBytes).toBeGreaterThan(
      SAQI_CURRENT_D1_BYTES,
    );
    expect(report.actionRecommendations[0]).toMatchObject({
      queriesAtRecommendedChunk: 354,
      recommendedChunk: 50,
      rowsWrittenAtRecommendedChunk: 400,
    });
  });

  it("fails closed for unknown tier without borrowing paid limits", () => {
    expect(planD1CapacityPreflight(input({ tier: "unknown" }))).toMatchObject({
      blockers: ["QUERY_BUDGET_EXHAUSTED", "TIER_UNKNOWN"],
      databaseLimitBytes: null,
      go: false,
      queryCeilingPerInvocation: null,
      recommendedChunk: 0,
      recommendedConcurrency: 0,
    });
  });

  it("rejects the 100k enrichment projection on free storage", () => {
    const report = planD1CapacityPreflight(input({ tier: "free" }));
    expect(report).toMatchObject({
      blockers: ["PROJECTED_DATABASE_EXCEEDS_SAFE_LIMIT"],
      databaseLimitBytes: 500 * 1024 * 1024,
      go: false,
      queryBudgetPerInvocation: 40,
      queryCeilingPerInvocation: 50,
      recommendedChunk: 5,
      recommendedConcurrency: 0,
      recommendedMaximumDailyRecords: 12_500,
    });
    expect(report.actionRecommendations[0]).toMatchObject({
      queriesAtRecommendedChunk: 39,
      recommendedChunk: 5,
    });
  });

  it("requires fresh export and Time Travel evidence", () => {
    expect(
      planD1CapacityPreflight(input({ backupProof: null })).blockers,
    ).toEqual(["BACKUP_PROOF_MISSING"]);
    const stale = input();
    stale.backupProof = {
      ...stale.backupProof!,
      exportVerifiedAt: NOW - 25 * 60 * 60_000,
      timeTravelVerifiedAt: NOW + 1,
    };
    expect(planD1CapacityPreflight(stale).blockers).toEqual([
      "BACKUP_EXPORT_STALE",
      "TIME_TRAVEL_PROOF_STALE",
    ]);
  });

  it("fails when fixed action cost consumes the safe query budget", () => {
    const constrained = input({
      actions: [
        {
          fixedQueries: 40,
          maximumRecords: 50,
          name: "unsafe_action",
          queriesPerRecord: 1,
          rowsWrittenPerRecord: 1,
        },
      ],
      tier: "free",
    });
    expect(planD1CapacityPreflight(constrained)).toMatchObject({
      blockers: [
        "PROJECTED_DATABASE_EXCEEDS_SAFE_LIMIT",
        "QUERY_BUDGET_EXHAUSTED",
      ],
      go: false,
      recommendedChunk: 0,
    });
  });
});
