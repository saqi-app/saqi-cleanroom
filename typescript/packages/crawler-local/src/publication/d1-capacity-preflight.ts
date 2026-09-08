import { z } from "zod";

export const SAQI_CURRENT_D1_BYTES = 375_873_536;
export const SAQI_CURRENT_POEMS = 100_064;

const FREE_DATABASE_LIMIT_BYTES = 500 * 1024 * 1024;
const PAID_DATABASE_LIMIT_BYTES = 10 * 1024 * 1024 * 1024;
const FREE_QUERY_LIMIT = 50;
const PAID_QUERY_LIMIT = 1_000;
const FREE_DAILY_ROWS_WRITTEN = 100_000;
const PROOF_MAXIMUM_AGE_MS = 24 * 60 * 60_000;
const D1TierSchema = z.enum(["free", "paid", "unknown"]);

const ActionSchema = z
  .object({
    fixedQueries: z.int().nonnegative().max(10_000),
    maximumRecords: z.int().positive().max(10_000),
    name: z.string().regex(/^[a-z][a-z0-9_-]{0,63}$/),
    queriesPerRecord: z.int().positive().max(10_000),
    rowsWrittenPerRecord: z.int().positive().max(10_000),
  })
  .strict();

export const D1CapacityPreflightInputSchema = z
  .object({
    actions: z.array(ActionSchema).min(1).max(32),
    backupProof: z
      .object({
        exportBytes: z.int().positive(),
        exportSha256: z.string().regex(/^[a-f\d]{64}$/),
        exportVerifiedAt: z.int().nonnegative(),
        timeTravelBookmark: z.string().min(1).max(4_096),
        timeTravelVerifiedAt: z.int().nonnegative(),
      })
      .strict()
      .nullable(),
    currentDatabaseBytes: z.int().nonnegative(),
    currentEnrichments: z.int().nonnegative(),
    currentPoems: z.int().nonnegative(),
    estimates: z
      .object({
        averageEnrichmentPayloadBytes: z.int().positive(),
        enrichmentMetadataBytes: z.int().nonnegative(),
        indexExpansionBytes: z.int().nonnegative(),
        indexOverheadBasisPoints: z.int().min(0).max(100_000),
        migrationFixedBytes: z.int().nonnegative(),
        reviewBytesPerEnrichment: z.int().nonnegative(),
      })
      .strict(),
    observedAt: z.int().nonnegative(),
    safetyMarginBasisPoints: z.int().min(500).max(5_000),
    targetEnrichments: z.int().nonnegative(),
    tier: D1TierSchema,
  })
  .strict()
  .superRefine((input, context) => {
    if (input.currentEnrichments > input.targetEnrichments) {
      context.addIssue({
        code: "custom",
        message: "currentEnrichments cannot exceed targetEnrichments",
      });
    }
    if (
      new Set(input.actions.map(({ name }) => name)).size !==
      input.actions.length
    ) {
      context.addIssue({
        code: "custom",
        message: "action names must be unique",
      });
    }
  });

export type D1CapacityPreflightInput = z.infer<
  typeof D1CapacityPreflightInputSchema
>;

export interface D1ActionRecommendation {
  readonly fixedQueries: number;
  readonly maximumRecords: number;
  readonly name: string;
  readonly queriesAtRecommendedChunk: number;
  readonly queriesPerRecord: number;
  readonly recommendedChunk: number;
  readonly rowsWrittenAtRecommendedChunk: number;
}

export type D1PreflightBlocker =
  | "BACKUP_EXPORT_STALE"
  | "BACKUP_PROOF_MISSING"
  | "CURRENT_DATABASE_OVER_LIMIT"
  | "PROJECTED_DATABASE_EXCEEDS_SAFE_LIMIT"
  | "QUERY_BUDGET_EXHAUSTED"
  | "TIER_UNKNOWN"
  | "TIME_TRAVEL_PROOF_STALE";

export interface D1CapacityPreflightReport {
  readonly actionRecommendations: readonly D1ActionRecommendation[];
  readonly blockers: readonly D1PreflightBlocker[];
  readonly currentDatabaseBytes: number;
  readonly currentPoems: number;
  readonly databaseLimitBytes: null | number;
  readonly go: boolean;
  readonly maximumSafeDatabaseBytes: null | number;
  readonly projectedBytesPerEnrichment: number;
  readonly projectedDatabaseBytes: number;
  readonly projectedGrowthBytes: number;
  readonly projectedMigrationAndIndexBytes: number;
  readonly projectedRemainingEnrichments: number;
  readonly queryBudgetPerInvocation: null | number;
  readonly queryCeilingPerInvocation: null | number;
  readonly recommendedChunk: number;
  readonly recommendedConcurrency: 0 | 1;
  readonly recommendedMaximumDailyRecords: null | number;
  readonly safetyMarginBytes: null | number;
  readonly schemaVersion: 1;
  readonly tier: "free" | "paid" | "unknown";
}

export function planD1CapacityPreflight(
  rawInput: D1CapacityPreflightInput,
): D1CapacityPreflightReport {
  const input = D1CapacityPreflightInputSchema.parse(rawInput);
  const limits =
    input.tier === "free"
      ? {
          databaseBytes: FREE_DATABASE_LIMIT_BYTES,
          queryCeiling: FREE_QUERY_LIMIT,
        }
      : input.tier === "paid"
        ? {
            databaseBytes: PAID_DATABASE_LIMIT_BYTES,
            queryCeiling: PAID_QUERY_LIMIT,
          }
        : null;
  const remaining = input.targetEnrichments - input.currentEnrichments;
  const enrichmentBaseBytes =
    input.estimates.averageEnrichmentPayloadBytes +
    input.estimates.enrichmentMetadataBytes +
    input.estimates.reviewBytesPerEnrichment;
  const enrichmentIndexBytes = Math.ceil(
    (enrichmentBaseBytes * input.estimates.indexOverheadBasisPoints) / 10_000,
  );
  const projectedBytesPerEnrichment =
    enrichmentBaseBytes + enrichmentIndexBytes;
  const projectedMigrationAndIndexBytes =
    input.estimates.migrationFixedBytes + input.estimates.indexExpansionBytes;
  const projectedGrowthBytes = safeInteger(
    projectedMigrationAndIndexBytes + remaining * projectedBytesPerEnrichment,
    "D1_PROJECTED_GROWTH_RANGE",
  );
  const projectedDatabaseBytes = safeInteger(
    input.currentDatabaseBytes + projectedGrowthBytes,
    "D1_PROJECTED_SIZE_RANGE",
  );
  const blockers = new Set<D1PreflightBlocker>();
  if (!limits) blockers.add("TIER_UNKNOWN");

  const maximumSafeDatabaseBytes = limits
    ? Math.floor(
        limits.databaseBytes * (1 - input.safetyMarginBasisPoints / 10_000),
      )
    : null;
  if (limits && input.currentDatabaseBytes >= limits.databaseBytes) {
    blockers.add("CURRENT_DATABASE_OVER_LIMIT");
  }
  if (
    maximumSafeDatabaseBytes !== null &&
    projectedDatabaseBytes > maximumSafeDatabaseBytes
  ) {
    blockers.add("PROJECTED_DATABASE_EXCEEDS_SAFE_LIMIT");
  }

  const proof = input.backupProof;
  if (!proof) {
    blockers.add("BACKUP_PROOF_MISSING");
  } else {
    if (!freshProof(proof.exportVerifiedAt, input.observedAt)) {
      blockers.add("BACKUP_EXPORT_STALE");
    }
    if (!freshProof(proof.timeTravelVerifiedAt, input.observedAt)) {
      blockers.add("TIME_TRAVEL_PROOF_STALE");
    }
  }

  const queryBudgetPerInvocation = limits
    ? Math.floor(
        limits.queryCeiling * (1 - input.safetyMarginBasisPoints / 10_000),
      )
    : null;
  const actionRecommendations = input.actions.map((action) => {
    const budgetRecords =
      queryBudgetPerInvocation === null
        ? 0
        : Math.floor(
            (queryBudgetPerInvocation - action.fixedQueries) /
              action.queriesPerRecord,
          );
    const recommendedChunk = Math.max(
      0,
      Math.min(action.maximumRecords, budgetRecords),
    );
    if (recommendedChunk === 0) blockers.add("QUERY_BUDGET_EXHAUSTED");
    return {
      fixedQueries: action.fixedQueries,
      maximumRecords: action.maximumRecords,
      name: action.name,
      queriesAtRecommendedChunk:
        action.fixedQueries + action.queriesPerRecord * recommendedChunk,
      queriesPerRecord: action.queriesPerRecord,
      recommendedChunk,
      rowsWrittenAtRecommendedChunk:
        action.rowsWrittenPerRecord * recommendedChunk,
    };
  });
  const recommendedChunk = Math.min(
    ...actionRecommendations.map(
      ({ recommendedChunk: actionChunk }) => actionChunk,
    ),
  );
  const maximumRowsWrittenPerRecord = Math.max(
    ...input.actions.map(({ rowsWrittenPerRecord }) => rowsWrittenPerRecord),
  );
  const recommendedMaximumDailyRecords =
    input.tier === "free"
      ? Math.floor(FREE_DAILY_ROWS_WRITTEN / maximumRowsWrittenPerRecord)
      : null;
  const orderedBlockers = [...blockers].toSorted();
  return {
    actionRecommendations,
    blockers: orderedBlockers,
    currentDatabaseBytes: input.currentDatabaseBytes,
    currentPoems: input.currentPoems,
    databaseLimitBytes: limits?.databaseBytes ?? null,
    go: orderedBlockers.length === 0,
    maximumSafeDatabaseBytes,
    projectedDatabaseBytes,
    projectedBytesPerEnrichment,
    projectedGrowthBytes,
    projectedMigrationAndIndexBytes,
    projectedRemainingEnrichments: remaining,
    queryBudgetPerInvocation,
    queryCeilingPerInvocation: limits?.queryCeiling ?? null,
    recommendedChunk,
    recommendedConcurrency: orderedBlockers.length === 0 ? 1 : 0,
    recommendedMaximumDailyRecords,
    safetyMarginBytes:
      limits && maximumSafeDatabaseBytes !== null
        ? limits.databaseBytes - maximumSafeDatabaseBytes
        : null,
    schemaVersion: 1,
    tier: input.tier,
  };
}

function freshProof(proofAt: number, observedAt: number): boolean {
  return proofAt <= observedAt && observedAt - proofAt <= PROOF_MAXIMUM_AGE_MS;
}

function safeInteger(value: number, code: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(code);
  return value;
}
