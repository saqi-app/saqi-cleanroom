import { randomUUID } from "node:crypto";
// eslint-disable-next-line @sarj/prefer-node-fs-promises -- Synchronous SQLite construction requires the directory and existing-file state before opening the connection.
import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

import {
  canonicalPoemBindingIdBody,
  type CanonicalPoemBindingV1,
  CanonicalPoemBindingV1Schema,
  PoemEnrichmentInputSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { currentSource } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { z } from "zod";

import { collectionWorkKinds } from "../collection/collection-scheduler.js";
import { LedgerMigrator } from "./migrations.js";
import { type PauseControlPort, PauseControls } from "./pause-controls.js";
import {
  type Checkpoint,
  CheckpointSchema,
  type HealthReport,
  type KindProgress,
  type LedgerStatus,
  type StoredCheckpoint,
  type WorkClaim,
  type WorkDefinition,
  WorkDefinitionSchema,
  type WorkItem,
  type WorkState,
  WorkStateSchema,
} from "./schema.js";
import { SolOperationStore } from "./sol-operation-store.js";
import {
  queryMany,
  queryOptional,
  queryRequired,
  SqliteBooleanSchema,
  sqliteJsonText,
  SqliteSafeIntegerSchema,
} from "./sqlite-query.js";
import { canonicalJson, inputHash, sha256, workKey } from "./work-key.js";

interface WorkRow {
  attempt_count: number;
  available_at: number;
  created_at: number;
  implementation_version: string;
  input_hash: string;
  input_json: string;
  kind: string;
  last_error_code: null | string;
  lease_epoch: number;
  lease_expires_at: null | number;
  lease_owner: null | string;
  lease_token: null | string;
  output_artifact_hash: null | string;
  priority: number;
  schema_version: string;
  state: string;
  updated_at: number;
  work_key: string;
}

const JsonRecordSchema = z.record(z.string(), z.unknown());
const Sha256Schema = z.string().regex(/^[a-f\d]{64}$/);
const NullableSha256Schema = Sha256Schema.nullable();
const LedgerStateKeySchema = z.string().trim().min(1).max(128);
const LedgerTimestampSchema = z.number().int().nonnegative();
const LedgerIntegerSchema = z.number().int();
const LedgerPrioritySchema = z.number().int().min(-1_000_000).max(1_000_000);
const LedgerModelKeySchema = z.string().trim().min(1).max(100);
const LedgerBatchLimitSchema = z.number().int().min(1).max(500);
const ReconciliationBatchLimitSchema = z.number().int().min(1).max(100);
const LedgerErrorCodeSchema = z.string().regex(/^[A-Z][A-Z\d_:.-]{0,255}$/);
const SqlitePragmaValueSchema = z.string();
const NullableSqlitePragmaValueSchema = z.string().nullish();
const SchedulerSerializedStateSchema = z.string().refine((value) => {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
}, "Scheduler state must be valid JSON");
const ExpiredWorkRowSchema = z.strictObject({
  attemptId: z.string().nullable(),
  leaseEpoch: SqliteSafeIntegerSchema.nonnegative(),
  reservedSolAttempt: z.literal([0, 1]),
  workKey: z.string(),
});
const BudgetStateSchema = z.literal(["active", "closed", "exhausted"]);
const BudgetCapacityRowSchema = z.strictObject({
  budget_id: z.string(),
  maximum_operations: SqliteSafeIntegerSchema.positive(),
});
const LatestBudgetRowSchema = BudgetCapacityRowSchema.extend({
  state: BudgetStateSchema,
}).transform((row) => ({
  budgetId: row.budget_id,
  maximumOperations: row.maximum_operations,
  state: row.state,
}));
const BudgetAccountingRowSchema = BudgetCapacityRowSchema.extend({
  reserved_operations: SqliteSafeIntegerSchema.nonnegative(),
});
const BudgetReservationRowSchema = BudgetAccountingRowSchema.refine(
  (row) => row.reserved_operations <= row.maximum_operations,
).transform((row) => ({
  budgetId: row.budget_id,
  maximumOperations: row.maximum_operations,
  reservedOperations: row.reserved_operations,
}));
const BudgetStatusRowSchema = BudgetAccountingRowSchema.extend({
  state: BudgetStateSchema,
})
  .refine((row) => row.reserved_operations <= row.maximum_operations)
  .transform((row) => ({
    budgetId: row.budget_id,
    maximumOperations: row.maximum_operations,
    reservedOperations: row.reserved_operations,
    state: row.state,
  }));
const SchedulerStateRowSchema = z
  .strictObject({
    state_digest: Sha256Schema,
    state_json: z.string(),
  })
  .transform((row) => ({
    digest: row.state_digest,
    serialized: row.state_json,
  }));
const LegacySolFanoutRecoveryRowSchema = z
  .strictObject({
    bound: SqliteBooleanSchema,
    derivation: SqliteBooleanSchema,
    kind: z.string(),
    model_key: z.string().nullable(),
    output_artifact_hash: Sha256Schema.nullable(),
    rejected: SqliteBooleanSchema,
    source_kind: z.string().nullable(),
    source_state: WorkStateSchema.nullable(),
    state: WorkStateSchema,
  })
  .transform((row) => ({
    bound: row.bound,
    derivation: row.derivation,
    kind: row.kind,
    modelKey: row.model_key,
    outputArtifactHash: row.output_artifact_hash,
    rejected: row.rejected,
    sourceKind: row.source_kind,
    sourceState: row.source_state,
    state: row.state,
  }));
const CountRowSchema = z.strictObject({
  count: z.number().int().nonnegative(),
});
const SolMilestoneBackfillRowSchema = z
  .strictObject({
    completed_at: SqliteSafeIntegerSchema.nullable(),
    cursor_sequence: SqliteSafeIntegerSchema.nonnegative(),
    high_watermark: SqliteSafeIntegerSchema.nonnegative(),
  })
  .transform((row) => ({
    completedAt: row.completed_at,
    cursorSequence: row.cursor_sequence,
    highWatermark: row.high_watermark,
  }));
const SolMilestoneEventRowSchema = z
  .strictObject({
    completed_at: SqliteSafeIntegerSchema.nonnegative(),
    implementation_version: z.string().trim().min(1).max(100),
    payload_json: z.string(),
    schema_version: z.string().trim().min(1).max(100),
    sequence: SqliteSafeIntegerSchema.nonnegative(),
    work_key: Sha256Schema,
  })
  .transform((row) => ({
    completedAt: row.completed_at,
    implementationVersion: row.implementation_version,
    payloadJson: row.payload_json,
    schemaVersion: row.schema_version,
    sequence: row.sequence,
    workKey: row.work_key,
  }));
const SolImportedMilestonePayloadSchema = sqliteJsonText(
  z.union([
    z.strictObject({ artifactHash: Sha256Schema }),
    z.strictObject({
      publicationWorkKey: Sha256Schema,
      receiptArtifactHash: Sha256Schema,
    }),
  ]),
);
const SolMilestoneWindowRowSchema = z
  .strictObject({
    last_15m: SqliteSafeIntegerSchema.nonnegative(),
    last_1h: SqliteSafeIntegerSchema.nonnegative(),
    last_5m: SqliteSafeIntegerSchema.nonnegative(),
    last_at: SqliteSafeIntegerSchema.nonnegative().nullable(),
  })
  .transform((row) => ({
    last15m: row.last_15m,
    last1h: row.last_1h,
    last5m: row.last_5m,
    lastAt: row.last_at,
  }));
const DirectPublicationOptionsSchema = z.strictObject({
  implementationVersion: z.string().trim().min(1).max(100),
  priority: z.int().min(-1_000_000).max(1_000_000).default(0),
});
const SourceAuthorMetadataInputSchema = z.strictObject({
  authorHref: z.url().max(2_048),
  authorNameArabic: z.string().trim().min(1).max(512),
  refreshGeneration: z.string().regex(/^[\w.-]{1,64}$/),
});
const CLASSIFY_UNCLAIMED_SQL = `UPDATE work_item
  SET state = 'dead_letter', last_error_code = ?, available_at = ?, updated_at = ?
  WHERE work_key = ? AND state IN ('pending','retry_wait','quota_wait')`;
const REQUEUE_DEAD_LETTER_SQL = `UPDATE work_item
  SET state = 'pending', available_at = ?, last_error_code = NULL, updated_at = ?
  WHERE work_key = ? AND state = 'dead_letter'`;
const RETIRE_UNCLAIMED_SQL = `UPDATE work_item
  SET state = 'imported', last_error_code = NULL, available_at = ?, updated_at = ?
  WHERE work_key = ? AND attempt_count = 0
    AND NOT EXISTS (SELECT 1 FROM checkpoint WHERE checkpoint.work_key = work_item.work_key)
    AND state IN ('pending','retry_wait','quota_wait')`;
const ReadyWorkExistsSchema = z.object({ ready: z.literal([0, 1]) });
const StartedWorkAvailabilitySchema = z.object({
  earliest: z.number().nullable(),
  ready: z.number(),
});
const MigrationProfileSchema = z.object({
  schema_version: z.string().trim().min(1).max(100),
});
const PoemIdentityInputSchema = z
  .object({
    authorHref: z.url(),
    authorNameArabic: z.string().trim().min(1).max(512).optional(),
    poemHref: z.url(),
    refreshGeneration: z
      .string()
      .regex(/^[\w.-]{1,64}$/)
      .optional(),
  })
  .strict();
const WORK_ROW_COLUMNS = `work_item.work_key, work_item.kind,
  work_item.input_json, work_item.input_hash, work_item.schema_version,
  work_item.implementation_version, work_item.priority, work_item.state,
  work_item.attempt_count, work_item.available_at, work_item.lease_owner,
  work_item.lease_token, work_item.lease_epoch, work_item.lease_expires_at,
  work_item.output_artifact_hash, work_item.last_error_code,
  work_item.created_at, work_item.updated_at`;
const WORK_ITEM_INSERT_SQL = `INSERT INTO work_item(
  work_key, kind, input_json, input_hash, schema_version,
  implementation_version, priority, available_at, created_at, updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(work_key) DO NOTHING`;
const CANONICAL_TRANSLATION_BINDING_INSERT_SQL = `INSERT INTO canonical_translation_binding(
     translation_work_key, binding_id, binding_json, poem_id,
     source_revision_id, line_nfc_hash, prompt_material_hash, created_at
   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT(translation_work_key) DO NOTHING`;
const PUBLICATION_DERIVATION_INSERT_SQL = `INSERT INTO publication_derivation(
  translation_work_key, binding_id, publication_work_key,
  approved_artifact_hash, created_at
) VALUES(?, ?, ?, ?, ?)
ON CONFLICT(translation_work_key) DO NOTHING`;
const SUCCEED_LEASED_WORK_SQL = `UPDATE work_item
  SET state = 'succeeded', available_at = ?, output_artifact_hash = ?,
      last_error_code = NULL, lease_owner = NULL, lease_token = NULL,
      lease_expires_at = NULL, updated_at = ?
  WHERE work_key = ? AND state = 'running' AND lease_token = ?
    AND lease_epoch = ? AND lease_expires_at > ?`;
const RETRYABLE_ORIGIN_STOP_REASONS: ReadonlySet<string> = new Set([
  "SOURCE_HUMAN_REQUIRED",
  "SOURCE_NETWORK_UNAVAILABLE",
  "SOURCE_RATE_LIMITED",
]);
const RETRYABLE_ORIGIN_STOP_SUFFIXES = [
  "_HUMAN_REQUIRED",
  "_NETWORK_UNAVAILABLE",
  "_RATE_LIMITED",
] as const;
function isRetryableOriginStopReason(reason: string): boolean {
  return (
    RETRYABLE_ORIGIN_STOP_REASONS.has(reason) ||
    RETRYABLE_ORIGIN_STOP_SUFFIXES.some((suffix) => reason.endsWith(suffix))
  );
}
const RELEASE_RETRYABLE_ORIGIN_STOP_SQL = `UPDATE origin_gate
  SET stop_reason = NULL, updated_at = ?
  WHERE origin = ? AND stop_reason = ? AND active_token IS NULL
    AND cooldown_until <= ? AND next_allowed_at <= ?`;
const COUNT_FANOUT_PRIORITY_HINTS_SQL =
  "SELECT COUNT(*) AS count FROM fanout_priority_hint";
const FIND_FANOUT_PRIORITY_HINT_SQL =
  "SELECT 1 FROM fanout_priority_hint WHERE work_key = ?";
export interface OriginLease {
  readonly leaseEpoch: number;
  readonly origin: string;
  readonly token: string;
}

export type OriginClaimResult =
  | { readonly lease: OriginLease; readonly state: "claimed" }
  | { readonly reason: string; readonly state: "stopped" }
  | { readonly retryAt: number; readonly state: "busy" | "waiting" };

export type OriginGuardedWorkClaimResult =
  | { readonly claim: WorkClaim; readonly state: "claimed" }
  | { readonly reason: string; readonly state: "stopped" }
  | {
      readonly reason?: string;
      readonly retryAt: number;
      readonly state: "waiting";
    }
  | { readonly state: "idle" };

export interface OriginFailurePolicy {
  readonly circuitBreakerAfter: number;
  readonly circuitBreakerCooldownMs: number;
  readonly retryAt: number;
  readonly stopReason?: string;
}

export interface OriginFailureResult {
  readonly consecutiveFailures: number;
  readonly nextAllowedAt: number;
  readonly stopped: boolean;
}

export interface ResolutionPendingCursor {
  readonly availableAt: number;
  readonly createdAt: number;
  readonly priority: number;
  readonly workKey: string;
}

export interface ResolutionPendingPage {
  readonly complete: boolean;
  readonly cursor: null | ResolutionPendingCursor;
  readonly items: readonly WorkItem[];
}

export type ImportResult = "already_imported" | "imported";

export interface LedgerOptions {
  readonly readonly?: boolean;
}

export interface LedgerConnectionOptions extends LedgerOptions {
  /** Whether the injected file connection existed before this process opened it. */
  readonly existing?: boolean;
  /** Explicit path identity for WAL validation; defaults to the driver name. */
  readonly path?: string;
}

export interface ClaimRequirements {
  readonly implementationVersion?: string;
  readonly schemaVersion?: string;
}

export interface StartedClaimRequirements extends ClaimRequirements {
  readonly minimumAttemptCount?: number;
}

export interface SeedResult {
  readonly inserted: boolean;
  readonly workKey: string;
}

export interface DirectPublicationOptions {
  readonly implementationVersion: string;
  readonly priority?: number;
}

export interface ApprovedPublicationSeedResult {
  readonly publicationWorkKey: string;
  readonly translationWorkKey: string;
}

export interface ApprovedPublishedTranslationAttachment {
  readonly binding: CanonicalPoemBindingV1;
  readonly publicationPriority: number;
}

export type DirectPublicationConfirmationResult =
  "already_confirmed" | "confirmed";

export const DIRECT_ENRICHMENT_PUBLICATION_KIND =
  "corpus-publication-enrichment-v2";
export const DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION =
  "bound-enrichment-publication-v1";
export const MAX_DURABLE_FANOUT_PRIORITY_HINTS = 1_000;

export interface PoemSeedBatchResult {
  readonly conflicts: readonly PoemIdentityConflictError[];
  readonly results: readonly SeedResult[];
}

export interface SourceAuthorMetadata {
  readonly authorNameArabic: string;
  readonly refreshGeneration: string;
}

export interface WorkAvailability {
  readonly earliestAvailableAt: null | number;
  readonly ready: number;
}

export interface ProviderProfileDiagnostics {
  readonly availability: WorkAvailability;
  readonly budget: SolPaidUsageBudgetStatus;
  readonly poemThroughput: SolPoemThroughput;
  readonly progress: KindProgress;
  readonly quarantinedOperations: number;
  readonly recoverableUnknownOperations: number;
  readonly semanticFailures: number;
}

export interface PoemMilestoneWindow {
  readonly last15m: number;
  readonly last1h: number;
  readonly last5m: number;
  readonly lastAt: null | number;
}

export interface SolPoemThroughput {
  readonly coverage: {
    readonly backfillComplete: boolean;
    readonly highWatermark: number;
  };
  readonly generated: PoemMilestoneWindow;
  readonly published: PoemMilestoneWindow;
  readonly remaining: {
    readonly active: number;
    readonly delayed: number;
    readonly endToEndPublication: number;
    readonly generatedAwaitingPublication: number;
    readonly generation: number;
    readonly ready: number;
    readonly terminalDead: number;
  };
}

export interface SolPoemMilestoneBackfillResult {
  readonly complete: boolean;
  readonly eventType: "imported" | "succeeded";
  readonly highWatermark: number;
  readonly inserted: number;
  readonly invalid: number;
  readonly processed: number;
}

export interface WakeResolutionPendingResult {
  /** Signals that are safe for a durable outbox consumer to acknowledge. */
  readonly acknowledge: readonly string[];
  /** Signals that must remain in the outbox and be retried. */
  readonly retry: readonly string[];
}

export interface SchedulerStateRecord {
  readonly digest: string;
  readonly serialized: string;
}

export interface DeadLetterCohortFilter {
  readonly errorCodes: readonly string[];
  readonly implementationVersion: string;
  readonly kind: string;
  readonly schemaVersion: string;
}

export interface ReservedDeadLetterCohort extends DeadLetterCohortFilter {
  readonly expectedStateDigest: null | string;
  readonly reservationId: string;
  readonly schedulerStateDigest: string;
  readonly schedulerStateKey: string;
  readonly schedulerStateSerialized: string;
  readonly workKeys: readonly string[];
}

export interface PaidOperationReconciliationStatus {
  readonly due: number;
  readonly quarantined: number;
  readonly reconciled: number;
  readonly unknown: number;
}

export interface AttemptRetentionEligibility {
  readonly eligibleAttemptIds: ReadonlySet<string>;
  readonly nextEligibleAt: null | number;
  readonly protectedAttemptIds: ReadonlySet<string>;
  readonly withAttemptReservation: (
    attemptId: string,
    operation: () => void,
  ) => boolean;
}

export interface CompletedWorkScan {
  readonly cursor: number;
  readonly items: readonly Readonly<{
    eventSequence: number;
    work: WorkItem;
  }>[];
}

export interface LegacySolFallbackScan extends CompletedWorkScan {
  readonly done: boolean;
}

export interface WorkDefinitionScan {
  readonly cursor: null | string;
  readonly done: boolean;
  readonly items: readonly WorkItem[];
}

export interface ReadyWorkCursor {
  readonly createdAt: number;
  readonly priority: number;
  readonly workKey: string;
}

export interface ReadyWorkScan {
  readonly cursor: null | ReadyWorkCursor;
  readonly done: boolean;
  readonly items: readonly WorkItem[];
}

export class LostLeaseError extends Error {
  constructor(message = "The fenced lease is no longer current") {
    super(message);
    this.name = "LostLeaseError";
  }
}

export class LedgerIntegrityError extends Error {
  readonly result: string;

  constructor(result: string, options?: ErrorOptions) {
    super(`Ledger integrity check failed: ${result}`, options);
    this.name = "LedgerIntegrityError";
    this.result = result;
  }
}

export interface SolPaidUsageBudgetStatus {
  readonly budgetId: null | string;
  readonly maximumOperations: number;
  readonly remainingOperations: number;
  readonly reservedOperations: number;
  readonly state: "active" | "closed" | "exhausted" | "unarmed";
}

export class PoemIdentityConflictError extends Error {
  readonly authorHref: string;
  readonly existingAuthorHref: null | string;
  readonly poemHref: string;

  constructor(
    poemHref: string,
    authorHref: string,
    existingAuthorHref: null | string,
    options?: ErrorOptions,
  ) {
    super(
      `Canonical poem ${poemHref} is already owned by ${existingAuthorHref ?? "another author"}; rejected owner ${authorHref}`,
      options,
    );
    this.name = "PoemIdentityConflictError";
    this.poemHref = poemHref;
    this.authorHref = authorHref;
    this.existingAuthorHref = existingAuthorHref;
  }
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete SQLite aggregate owns connection lifecycle and transactions; a duplicate interface would mirror its entire repository API.
export class Ledger {
  readonly #database: Database.Database;
  #solOperations: SolOperationStore | undefined;
  readonly pauseControls: PauseControlPort;
  readonly #workClaimer: WorkClaimer;

  constructor(
    database: Database.Database,
    options: LedgerConnectionOptions = {},
  ) {
    const path = options.path ?? database.name;
    const existed = options.existing ?? false;
    this.#workClaimer = new WorkClaimer(database);
    try {
      // eslint-disable-next-line @sarj/require-sql-access-class -- Initialize the injected connection inside its owning Ledger constructor.
      database.pragma("foreign_keys = ON");
      // eslint-disable-next-line @sarj/require-sql-access-class -- Initialize the injected connection inside its owning Ledger constructor.
      database.pragma("busy_timeout = 5000");
      if (!options.readonly) {
        if (existed) new LedgerIntegrityInspector(database).assertHealthy();
        const journalMode = SqlitePragmaValueSchema.parse(
          // eslint-disable-next-line @sarj/require-sql-access-class -- Validate WAL initialization before exposing the injected Ledger connection.
          database.pragma("journal_mode = WAL", { simple: true }),
        );
        if (path !== ":memory:" && journalMode.toLowerCase() !== "wal") {
          throw new Error(`Ledger requires WAL mode; received ${journalMode}`);
        }
        // eslint-disable-next-line @sarj/require-sql-access-class -- Establish durability on the injected Ledger connection before migrations.
        database.pragma("synchronous = FULL");
        // eslint-disable-next-line @sarj/require-sql-access-class -- Bound WAL growth on the injected Ledger connection before migrations.
        database.pragma("wal_autocheckpoint = 4096");
        // eslint-disable-next-line @sarj/require-sql-access-class -- Bound journal size on the injected Ledger connection before migrations.
        database.pragma("journal_size_limit = 67108864");
        // eslint-disable-next-line @sarj/require-sql-access-class -- Bound cache memory on the injected Ledger connection before migrations.
        database.pragma("cache_size = -32768");
        // eslint-disable-next-line @sarj/require-sql-access-class -- Initialize temporary storage on the injected Ledger connection before migrations.
        database.pragma("temp_store = MEMORY");
        new LedgerMigrator(database).migrate();
      } else {
        new LedgerMigrator(database).assertConfiguredSourceIdentity();
      }
    } catch (error) {
      try {
        database.close();
      } catch (closeError) {
        throw new AggregateError(
          [error, closeError],
          "Ledger initialization and cleanup both failed",
        );
      }
      throw error;
    }
    this.#database = database;
    this.pauseControls = new PauseControls(database, dirname(path));
  }

  static open(path: string, options: LedgerOptions = {}): Ledger {
    const existed = path !== ":memory:" && existsSync(path);
    if (path !== ":memory:" && !options.readonly) {
      mkdirSync(dirname(path), { mode: 0o700, recursive: true });
    }
    const database = new Database(path, {
      fileMustExist: options.readonly ?? false,
      readonly: options.readonly ?? false,
    });
    return new Ledger(database, { ...options, existing: existed, path });
  }

  get solOperations(): SolOperationStore {
    this.#solOperations ??= new SolOperationStore(this.#database);
    return this.#solOperations;
  }

  static initialize(path: string): Ledger {
    return this.open(path);
  }

  close(): void {
    this.#database.close();
  }

  armSolPaidUsageBudget(
    maximumOperations: number,
    rearm = false,
    now = Date.now(),
  ): SolPaidUsageBudgetStatus {
    if (
      !Number.isSafeInteger(maximumOperations) ||
      maximumOperations <= 0 ||
      maximumOperations % 3 !== 0
    ) {
      throw new Error(
        "maximum Sol operations must be a positive multiple of 3",
      );
    }
    return this.#immediate(() => {
      const latest = queryOptional(
        { operation: "Ledger.armSolPaidUsageBudget" },
        () =>
          this.#database
            .prepare(
              `SELECT budget_id, maximum_operations, state
             FROM sol_paid_usage_budget
            ORDER BY created_at DESC, budget_id DESC LIMIT 1`,
            )
            .get(),
        LatestBudgetRowSchema,
      );
      if (latest?.state === "active") {
        if (rearm) throw new Error("SOL_PAID_USAGE_BUDGET_ALREADY_ACTIVE");
        if (maximumOperations < latest.maximumOperations) {
          throw new Error("SOL_PAID_USAGE_BUDGET_CANNOT_DECREASE");
        }
        if (maximumOperations > latest.maximumOperations) {
          this.#database
            .prepare(
              `UPDATE sol_paid_usage_budget
                  SET maximum_operations = ?, updated_at = max(updated_at, ?)
                WHERE budget_id = ? AND state = 'active'`,
            )
            .run(maximumOperations, now, latest.budgetId);
        }
        return this.solPaidUsageBudgetStatus();
      }
      if (latest && !rearm)
        throw new Error("SOL_PAID_USAGE_BUDGET_REARM_REQUIRED");
      const budgetId = randomUUID();
      this.#database
        .prepare(
          `INSERT INTO sol_paid_usage_budget(
             budget_id, maximum_operations, reserved_operations, state,
             created_at, updated_at
           ) VALUES(?, ?, 0, 'active', ?, ?)`,
        )
        .run(budgetId, maximumOperations, now, now);
      return this.solPaidUsageBudgetStatus();
    });
  }

  solPaidUsageBudgetStatus(): SolPaidUsageBudgetStatus {
    const row = queryOptional(
      { operation: "Ledger.solPaidUsageBudgetStatus" },
      () =>
        this.#database
          .prepare(
            `SELECT budget_id, maximum_operations, reserved_operations, state
           FROM sol_paid_usage_budget
          ORDER BY created_at DESC, budget_id DESC LIMIT 1`,
          )
          .get(),
      BudgetStatusRowSchema,
    );
    if (!row)
      return {
        budgetId: null,
        maximumOperations: 0,
        remainingOperations: 0,
        reservedOperations: 0,
        state: "unarmed",
      };
    return {
      budgetId: row.budgetId,
      maximumOperations: row.maximumOperations,
      remainingOperations: Math.max(
        0,
        row.maximumOperations - row.reservedOperations,
      ),
      reservedOperations: row.reservedOperations,
      state: row.state,
    };
  }

  reserveSolPaidClaim(claim: WorkClaim, now = Date.now()): boolean {
    return this.#immediate(() => {
      const budget = queryOptional(
        { operation: "Ledger.reserveSolPaidClaim" },
        () =>
          this.#database
            .prepare(
              `SELECT budget_id, maximum_operations, reserved_operations
             FROM sol_paid_usage_budget WHERE state = 'active' LIMIT 1`,
            )
            .get(),
        BudgetReservationRowSchema,
      );
      if (!budget || budget.maximumOperations - budget.reservedOperations < 3)
        return false;
      const inserted = this.#database
        .prepare(
          `INSERT INTO sol_paid_usage_reservation(
             budget_id, attempt_id, work_key, reserved_operations, created_at
           ) VALUES(?, ?, ?, 3, ?)
           ON CONFLICT(budget_id, attempt_id) DO NOTHING`,
        )
        .run(budget.budgetId, claim.attemptId, claim.work.workKey, now);
      if (inserted.changes === 0) return true;
      const next = budget.reservedOperations + 3;
      this.#database
        .prepare(
          `UPDATE sol_paid_usage_budget
              SET reserved_operations = ?,
                  state = CASE WHEN ? >= maximum_operations THEN 'exhausted' ELSE 'active' END,
                  updated_at = MAX(updated_at, ?)
            WHERE budget_id = ? AND state = 'active'`,
        )
        .run(next, next, now, budget.budgetId);
      return true;
    });
  }

  loadSchedulerState(stateKey: string): null | SchedulerStateRecord {
    const key = LedgerStateKeySchema.parse(stateKey);
    return (
      queryOptional(
        { operation: "Ledger.loadSchedulerState" },
        () =>
          this.#database
            .prepare(
              `SELECT state_json, state_digest
         FROM scheduler_state
         WHERE state_key = ?`,
            )
            .get(key),
        SchedulerStateRowSchema,
      ) ?? null
    );
  }

  saveSchedulerState(
    stateKey: string,
    serialized: string,
    digest: string,
    expectedDigest: null | string,
    now = Date.now(),
  ): boolean {
    const key = LedgerStateKeySchema.parse(stateKey);
    const payload = SchedulerSerializedStateSchema.parse(serialized);
    const nextDigest = Sha256Schema.parse(digest);
    const previousDigest = NullableSha256Schema.parse(expectedDigest);
    const updatedAt = LedgerTimestampSchema.parse(now);
    if (previousDigest === null) {
      return (
        this.#database
          .prepare(
            `INSERT INTO scheduler_state(
               state_key, state_json, state_digest, updated_at
             ) VALUES(?, ?, ?, ?)
             ON CONFLICT(state_key) DO NOTHING`,
          )
          .run(key, payload, nextDigest, updatedAt).changes === 1
      );
    }
    return (
      this.#database
        .prepare(
          `UPDATE scheduler_state
           SET state_json = ?, state_digest = ?, updated_at = ?
           WHERE state_key = ? AND state_digest = ?`,
        )
        .run(payload, nextDigest, updatedAt, key, previousDigest).changes === 1
    );
  }

  recordPaidOperationUnknown(
    workKeyValue: string,
    operationKey: string,
    attemptId: string,
    nextReconcileAt: number,
    now = Date.now(),
  ): void {
    const workKeyParsed = Sha256Schema.parse(workKeyValue);
    const operationKeyParsed = Sha256Schema.parse(operationKey);
    const attemptIdParsed = LedgerStateKeySchema.parse(attemptId);
    const observedAt = LedgerTimestampSchema.parse(now);
    const reconcileAt = z.number().int().min(observedAt).parse(nextReconcileAt);
    this.#immediate(() => {
      const changed = this.#database
        .prepare(
          `INSERT INTO paid_operation_reconciliation(
             operation_key, work_key, attempt_id, state, next_reconcile_at,
             first_observed_at, updated_at
           ) VALUES(?, ?, ?, 'unknown', ?, ?, ?)
           ON CONFLICT(operation_key) DO UPDATE SET
             next_reconcile_at = CASE
               WHEN paid_operation_reconciliation.state = 'reconciled'
                 THEN paid_operation_reconciliation.next_reconcile_at
               ELSE excluded.next_reconcile_at
             END,
             updated_at = excluded.updated_at
           WHERE paid_operation_reconciliation.work_key = excluded.work_key
             AND paid_operation_reconciliation.attempt_id = excluded.attempt_id`,
        )
        .run(
          operationKeyParsed,
          workKeyParsed,
          attemptIdParsed,
          reconcileAt,
          observedAt,
          observedAt,
        );
      if (changed.changes !== 1)
        throw new Error("PAID_OPERATION_IDENTITY_CONFLICT");
    });
  }

  recordPaidOperationReconciled(
    operationKey: string,
    now = Date.now(),
  ): boolean {
    const key = Sha256Schema.parse(operationKey);
    const reconciledAt = LedgerTimestampSchema.parse(now);
    return this.#immediate(
      () =>
        this.#database
          .prepare(
            `UPDATE paid_operation_reconciliation
             SET state = 'reconciled', reconciliation_count = reconciliation_count + 1,
                 last_reconciled_at = ?, next_reconcile_at = ?, updated_at = ?
             WHERE operation_key = ? AND state IN ('unknown','quarantined')`,
          )
          .run(reconciledAt, reconciledAt, reconciledAt, key).changes === 1,
    );
  }

  recordPaidOperationQuarantined(
    operationKey: string,
    now: number,
    reason: string,
  ): boolean {
    const key = Sha256Schema.parse(operationKey);
    const quarantinedAt = LedgerTimestampSchema.parse(now);
    requireErrorCode(reason);
    // The reason is durably retained by the paired work-item dead-letter
    // event. This table remains the immutable paid-operation identity and
    // outcome index, avoiding a second mutable copy of diagnostic detail.
    return this.#immediate(
      () =>
        this.#database
          .prepare(
            `UPDATE paid_operation_reconciliation
             SET state = 'quarantined', next_reconcile_at = ?, updated_at = ?
             WHERE operation_key = ? AND state = 'unknown'`,
          )
          .run(quarantinedAt, quarantinedAt, key).changes === 1,
    );
  }

  paidOperationReconciliationStatus(
    now = Date.now(),
  ): PaidOperationReconciliationStatus {
    const observedAt = LedgerTimestampSchema.parse(now);
    const rows = this.#database
      .prepare<[], { item_count: number; state: string }>(
        `SELECT state, item_count FROM paid_operation_state_count`,
      )
      .all();
    const due = this.#database
      .prepare<[number], { item_count: number }>(
        `SELECT COUNT(*) AS item_count
         FROM paid_operation_reconciliation
         WHERE state = 'unknown' AND next_reconcile_at <= ?`,
      )
      .get(observedAt);
    if (!due) throw new Error("Paid-operation due aggregate missing");
    const counts = new Map(rows.map((row) => [row.state, row.item_count]));
    return {
      due: due.item_count,
      quarantined: counts.get("quarantined") ?? 0,
      reconciled: counts.get("reconciled") ?? 0,
      unknown: counts.get("unknown") ?? 0,
    };
  }

  seed(definition: WorkDefinition, now = Date.now()): SeedResult {
    const result = this.seedMany([definition], now)[0];
    if (!result) throw new Error("Single seed unexpectedly produced no result");
    return result;
  }

  seedMany(
    definitions: readonly WorkDefinition[],
    now = Date.now(),
  ): SeedResult[] {
    return [...this.#seedMany(definitions, now, false).results];
  }

  seedPoems(
    definitions: readonly WorkDefinition[],
    now = Date.now(),
  ): PoemSeedBatchResult {
    if (
      definitions.some(({ kind }) => kind !== collectionWorkKinds().poemDetail)
    ) {
      throw new Error("Poem batch contains a non-poem work definition");
    }
    return this.#seedMany(definitions, now, true);
  }

  recordSourceAuthorMetadata(
    authorHref: string,
    authorNameArabic: string,
    refreshGeneration: string,
    now = Date.now(),
  ): void {
    const value = SourceAuthorMetadataInputSchema.parse({
      authorHref,
      authorNameArabic,
      refreshGeneration,
    });
    this.#immediate(() => {
      const result = this.#database
        .prepare(
          `INSERT INTO source_author_metadata(
          source_name, author_href, author_name_arabic,
          refresh_generation, observed_at
        ) VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(source_name, author_href) DO UPDATE SET
          author_name_arabic = excluded.author_name_arabic,
          refresh_generation = excluded.refresh_generation,
          observed_at = excluded.observed_at
        WHERE (source_author_metadata.observed_at < excluded.observed_at
          OR (source_author_metadata.observed_at = excluded.observed_at
            AND source_author_metadata.refresh_generation < excluded.refresh_generation))
          AND (source_author_metadata.author_name_arabic <> excluded.author_name_arabic
            OR source_author_metadata.refresh_generation <> excluded.refresh_generation)`,
        )
        .run(
          currentSource().name,
          value.authorHref,
          value.authorNameArabic,
          value.refreshGeneration,
          now,
        );
      if (result.changes > 1)
        throw new Error("SOURCE_AUTHOR_METADATA_WRITE_UNBOUNDED");
    });
  }

  sourceAuthorMetadataRevision(): number {
    const row = this.#database
      .prepare<[], { revision: number }>(
        `SELECT revision FROM source_author_metadata_revision
         WHERE singleton = 1`,
      )
      .get();
    if (!row) throw new Error("SOURCE_AUTHOR_METADATA_REVISION_MISSING");
    return z.int().nonnegative().parse(row.revision);
  }

  sourceAuthorMetadata(authorHref: string): null | SourceAuthorMetadata {
    const row = this.#database
      .prepare<
        [string, string],
        { author_name_arabic: string; refresh_generation: string }
      >(
        `SELECT author_name_arabic, refresh_generation
         FROM source_author_metadata
         WHERE source_name = ? AND author_href = ?`,
      )
      .get(currentSource().name, authorHref);
    return row
      ? {
          authorNameArabic: row.author_name_arabic,
          refreshGeneration: row.refresh_generation,
        }
      : null;
  }

  get(key: string): null | WorkItem {
    const row = this.#database
      .prepare<[string], WorkRow>(
        `SELECT ${WORK_ROW_COLUMNS} FROM work_item WHERE work_key = ?`,
      )
      .get(key);
    return row ? rowToWork(row) : null;
  }

  successfulArtifactForInput(
    kind: string,
    schemaVersion: string,
    expectedInputHash: string,
  ): null | string {
    return (
      this.successfulArtifactsForInputs(kind, schemaVersion, [
        expectedInputHash,
      ]).get(expectedInputHash) ?? null
    );
  }

  successfulArtifactsForInputs(
    kind: string,
    schemaVersion: string,
    expectedInputHashes: readonly string[],
  ): ReadonlyMap<string, string> {
    if (kind.trim().length === 0 || schemaVersion.trim().length === 0) {
      throw new Error(
        "Completed-input lookup requires kind and schema version",
      );
    }
    if (expectedInputHashes.some((hash) => !/^[a-f\d]{64}$/.test(hash))) {
      throw new Error("Completed-input lookup requires a valid input hash");
    }
    const uniqueHashes = [...new Set(expectedInputHashes)];
    const completed = new Map<
      string,
      { hash: string; updatedAt: number; workKey: string }
    >();
    // Stay well below SQLite's host-parameter limit and keep each indexed IN
    // probe bounded even for unusually large author manifests.
    const chunkSize = 500;
    for (let offset = 0; offset < uniqueHashes.length; offset += chunkSize) {
      const chunk = uniqueHashes.slice(offset, offset + chunkSize);
      if (chunk.length === 0) continue;
      const rows = this.#database
        .prepare<
          string[],
          {
            hash: string;
            input_hash: string;
            updated_at: number;
            work_key: string;
          }
        >(
          `SELECT input_hash, output_artifact_hash AS hash, updated_at, work_key
           FROM work_item
           WHERE kind = ? AND schema_version = ?
             AND input_hash IN (${chunk.map(() => "?").join(",")})
             AND state IN ('succeeded','imported')
             AND output_artifact_hash IS NOT NULL`,
        )
        .all(kind, schemaVersion, ...chunk);
      for (const row of rows) {
        const current = completed.get(row.input_hash);
        if (
          !current ||
          row.updated_at > current.updatedAt ||
          (row.updated_at === current.updatedAt &&
            row.work_key < current.workKey)
        ) {
          completed.set(row.input_hash, {
            hash: row.hash,
            updatedAt: row.updated_at,
            workKey: row.work_key,
          });
        }
      }
    }
    return new Map(
      [...completed].map(([inputHashValue, { hash }]) => [
        inputHashValue,
        hash,
      ]),
    );
  }

  /** Stable event-keyset scan for newly completed work. The returned cursor
   * advances to the page tail, or to the captured high watermark when the
   * page is exhausted, so concurrent completions are never skipped. */
  listSucceededAfter(
    cursor: number,
    kinds: readonly string[],
    limit: number,
    requirements: ClaimRequirements = {},
  ): CompletedWorkScan {
    if (!Number.isSafeInteger(cursor) || cursor < 0)
      throw new Error("Completed-work cursor must be nonnegative");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Completed-work limit must be between 1 and 1000");
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Completed-work kinds are required");
    const implementationFilter =
      requirements.implementationVersion === undefined
        ? ""
        : " AND work_item.implementation_version = ?";
    const schemaFilter =
      requirements.schemaVersion === undefined
        ? ""
        : " AND work_item.schema_version = ?";
    const values = [
      ...kinds,
      ...(requirements.implementationVersion === undefined
        ? []
        : [requirements.implementationVersion]),
      ...(requirements.schemaVersion === undefined
        ? []
        : [requirements.schemaVersion]),
    ];
    return this.#immediate(() => {
      const high = this.#database
        .prepare<[], { value: number }>(
          "SELECT COALESCE(MAX(sequence), 0) AS value FROM work_event",
        )
        .get();
      if (!high) throw new Error("Completed-work high watermark missing");
      const rows = this.#database
        .prepare<(number | string)[], WorkRow & { event_sequence: number }>(
          `SELECT work_event.sequence AS event_sequence, ${WORK_ROW_COLUMNS}
         FROM work_event
         JOIN work_item ON work_item.work_key = work_event.work_key
         WHERE work_event.sequence > ? AND work_event.sequence <= ?
           AND work_event.event_type = 'succeeded'
           AND work_item.kind IN (${kinds.map(() => "?").join(",")})
           ${implementationFilter}${schemaFilter}
         ORDER BY work_event.sequence LIMIT ?`,
        )
        .all(cursor, high.value, ...values, limit);
      const items = rows.map((row) => ({
        eventSequence: row.event_sequence,
        work: rowToWork(row),
      }));
      const tail = rows.at(-1);
      return {
        cursor:
          rows.length === limit && tail ? tail.event_sequence : high.value,
        items,
      };
    });
  }

  /** Pages a legacy Sol completion stream while suppressing poems that already
   * have a completed preferred implementation. The returned cursor advances
   * over the unfiltered legacy page so suppressed rows cannot pin a backfill.
   */
  listSucceededSolFallbackAfter(
    cursor: number,
    kind: string,
    limit: number,
    fallbackImplementationVersion: string,
    preferredImplementationVersion: string,
  ): LegacySolFallbackScan {
    const page = this.listSucceededAfter(cursor, [kind], limit, {
      implementationVersion: fallbackImplementationVersion,
    });
    const poemIds = page.items.map(
      ({ work }) => PoemEnrichmentInputSchema.parse(work.input).poemId,
    );
    if (poemIds.length === 0) return { ...page, done: true };
    const completed = new Set<string>();
    const uniquePoemIds = [...new Set(poemIds)];
    const chunkSize = 500;
    for (let offset = 0; offset < uniquePoemIds.length; offset += chunkSize) {
      const chunk = uniquePoemIds.slice(offset, offset + chunkSize);
      if (chunk.length === 0) continue;
      const rows = this.#database
        .prepare<string[], { poem_id: string }>(
          `SELECT DISTINCT json_extract(input_json, '$.poemId') AS poem_id
             FROM work_item
            WHERE kind = ? AND implementation_version = ?
              AND state IN ('succeeded','imported')
              AND output_artifact_hash IS NOT NULL
              AND json_extract(input_json, '$.poemId')
                  IN (${chunk.map(() => "?").join(",")})`,
        )
        .all(kind, preferredImplementationVersion, ...chunk);
      for (const row of rows) completed.add(row.poem_id);
    }
    return {
      cursor: page.cursor,
      done: page.items.length < limit,
      items: page.items.filter(
        ({ work }) =>
          !completed.has(PoemEnrichmentInputSchema.parse(work.input).poemId),
      ),
    };
  }

  /** Stable, bounded keyset scan of one exact work profile. Intended for
   * startup reconciliation where loading the full corpus would be unsafe. */
  listWorkDefinitionsAfter(
    cursor: null | string,
    kind: string,
    limit: number,
    requirements: Required<ClaimRequirements>,
  ): WorkDefinitionScan {
    if (cursor !== null && !/^[a-f\d]{64}$/.test(cursor))
      throw new Error("Work-definition cursor must be a work key");
    if (kind.trim().length === 0)
      throw new Error("Work-definition kind is required");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Work-definition limit must be between 1 and 1000");
    if (
      requirements.implementationVersion.trim().length === 0 ||
      requirements.schemaVersion.trim().length === 0
    ) {
      throw new Error("Work-definition versions are required");
    }
    const cursorFilter = cursor === null ? "" : " AND work_key > ?";
    const values: (number | string)[] = [
      kind,
      requirements.implementationVersion,
      requirements.schemaVersion,
      ...(cursor === null ? [] : [cursor]),
      limit + 1,
    ];
    const rows = this.#database
      .prepare<(number | string)[], WorkRow>(
        `SELECT work_key, kind, input_json, input_hash, schema_version,
                implementation_version, priority, state, attempt_count,
                available_at, lease_owner, lease_token, lease_epoch,
                lease_expires_at, output_artifact_hash, last_error_code,
                created_at, updated_at
         FROM work_item
         WHERE kind = ? AND implementation_version = ? AND schema_version = ?
           ${cursorFilter}
         ORDER BY work_key LIMIT ?`,
      )
      .all(...values);
    const done = rows.length <= limit;
    const items = rows.slice(0, limit).map(rowToWork);
    return {
      cursor: items.at(-1)?.workKey ?? cursor,
      done,
      items,
    };
  }

  /** Bounded priority scan for unclaimed work in one exact profile. */
  hasReadyStartedSolV2Work(now = Date.now()): boolean {
    const row = this.#database
      .prepare(
        `SELECT EXISTS(SELECT 1 FROM work_item
       WHERE kind = 'poem-enrichment-sol' AND implementation_version = 'sol-word-gloss-v2'
         AND schema_version = 'saqi.poem-enrichment-input@2'
         AND (last_error_code IS NULL OR last_error_code <> 'CODEX_OPERATION_OUTCOME_UNKNOWN')
         AND attempt_count > 0 AND state IN ('pending','retry_wait','quota_wait')
         AND available_at <= ?) AS ready`,
      )
      .get(LedgerTimestampSchema.parse(now));
    return ReadyWorkExistsSchema.parse(row).ready === 1;
  }

  migrateUnattemptedSolV2Work(
    limit = 50,
    now = Date.now(),
  ): { inserted: number; migrated: number } {
    const batchSize = z.int().min(1).max(1000).parse(limit);
    const timestamp = LedgerTimestampSchema.parse(now);
    const profiles = queryMany(
      { operation: "ledger.solProfileMigration.profiles" },
      () =>
        this.#database
          .prepare(
            `SELECT schema_version FROM ledger_profile_state_count
         WHERE kind = 'poem-enrichment-sol' AND implementation_version = 'sol-word-gloss-v2'
           AND state = 'pending' AND item_count > 0`,
          )
          .all(),
      MigrationProfileSchema,
    );
    if (profiles.length === 0) return { inserted: 0, migrated: 0 };
    // Limit each exact profile in index order before merging their global top
    // batch. A state/available_at scan otherwise visits unrelated pending work.
    const profileQuery = this.#database.prepare<
      [string, number, number],
      WorkRow
    >(
      `SELECT ${WORK_ROW_COLUMNS} FROM work_item INDEXED BY work_item_ready_priority
       WHERE kind = 'poem-enrichment-sol' AND implementation_version = 'sol-word-gloss-v2'
         AND schema_version = ? AND attempt_count = 0 AND state = 'pending'
         AND state IN ('pending','retry_wait','quota_wait') AND available_at <= ?
         AND NOT EXISTS (SELECT 1 FROM checkpoint WHERE checkpoint.work_key = work_item.work_key)
       ORDER BY priority DESC, created_at, work_key LIMIT ?`,
    );
    const rows = profiles
      .flatMap((profile) =>
        profileQuery.all(profile.schema_version, timestamp, batchSize),
      )
      .toSorted(
        (left, right) =>
          right.priority - left.priority ||
          left.created_at - right.created_at ||
          (left.work_key < right.work_key
            ? -1
            : left.work_key > right.work_key
              ? 1
              : 0),
      )
      .slice(0, batchSize)
      .map(rowToWork);
    let inserted = 0;
    for (const source of rows) {
      const result = this.supersedeUnclaimed(
        source.workKey,
        {
          implementationVersion: "sol-word-gloss-v3",
          input: source.input,
          inputHash: source.inputHash,
          kind: source.kind,
          priority: source.priority,
          schemaVersion: source.schemaVersion,
        },
        "SOL_UNATTEMPTED_PROFILE_UPGRADE",
        timestamp,
      );
      if (result.inserted) inserted += 1;
    }
    return { inserted, migrated: rows.length };
  }

  /** Bounded priority scan for unclaimed work in one exact profile. */
  listReadyWork(
    kind: string,
    limit: number,
    requirements: Required<ClaimRequirements>,
    now = Date.now(),
  ): WorkItem[] {
    if (kind.trim().length === 0)
      throw new Error("Ready-work kind is required");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Ready-work limit must be between 1 and 1000");
    return this.#database
      .prepare<[string, string, string, number, number], WorkRow>(
        `SELECT ${WORK_ROW_COLUMNS} FROM work_item
         WHERE kind = ? AND implementation_version = ? AND schema_version = ?
           AND state IN ('pending','retry_wait','quota_wait')
           AND available_at <= ?
         ORDER BY priority DESC, created_at, work_key LIMIT ?`,
      )
      .all(
        kind,
        requirements.implementationVersion,
        requirements.schemaVersion,
        now,
        limit,
      )
      .map(rowToWork);
  }

  /** Bounded priority/keyset scan for unclaimed work in one exact profile.
   * The split ranges preserve the mixed priority-descending/created-ascending
   * index order without rescanning the prefix before the cursor. */
  listReadyWorkAfter(
    cursor: null | ReadyWorkCursor,
    kind: string,
    limit: number,
    requirements: Required<ClaimRequirements>,
    now = Date.now(),
  ): ReadyWorkScan {
    if (kind.trim().length === 0)
      throw new Error("Ready-work kind is required");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Ready-work limit must be between 1 and 1000");
    if (
      requirements.implementationVersion.trim().length === 0 ||
      requirements.schemaVersion.trim().length === 0
    ) {
      throw new Error("Ready-work versions are required");
    }
    const parsedCursor =
      cursor === null
        ? null
        : {
            createdAt: LedgerTimestampSchema.parse(cursor.createdAt),
            priority: LedgerPrioritySchema.parse(cursor.priority),
            workKey: Sha256Schema.parse(cursor.workKey),
          };
    const profile = [
      kind,
      requirements.implementationVersion,
      requirements.schemaVersion,
      now,
    ] as const;
    const pageSize = limit + 1;
    const rows: WorkRow[] = [];
    if (parsedCursor === null) {
      rows.push(
        ...this.#database
          .prepare<(number | string)[], WorkRow>(
            `SELECT ${WORK_ROW_COLUMNS} FROM work_item
             WHERE kind = ? AND implementation_version = ? AND schema_version = ?
               AND state IN ('pending','retry_wait','quota_wait')
               AND available_at <= ?
             ORDER BY priority DESC, created_at, work_key LIMIT ?`,
          )
          .all(...profile, pageSize),
      );
    } else {
      rows.push(
        ...this.#database
          .prepare<(number | string)[], WorkRow>(
            `SELECT ${WORK_ROW_COLUMNS} FROM work_item
             WHERE kind = ? AND implementation_version = ? AND schema_version = ?
               AND state IN ('pending','retry_wait','quota_wait')
               AND available_at <= ? AND priority = ? AND created_at = ?
               AND work_key > ?
             ORDER BY work_key LIMIT ?`,
          )
          .all(
            ...profile,
            parsedCursor.priority,
            parsedCursor.createdAt,
            parsedCursor.workKey,
            pageSize,
          ),
      );
      if (rows.length < pageSize) {
        rows.push(
          ...this.#database
            .prepare<(number | string)[], WorkRow>(
              `SELECT ${WORK_ROW_COLUMNS} FROM work_item
               WHERE kind = ? AND implementation_version = ? AND schema_version = ?
                 AND state IN ('pending','retry_wait','quota_wait')
                 AND available_at <= ? AND priority = ? AND created_at > ?
               ORDER BY created_at, work_key LIMIT ?`,
            )
            .all(
              ...profile,
              parsedCursor.priority,
              parsedCursor.createdAt,
              pageSize - rows.length,
            ),
        );
      }
      if (rows.length < pageSize) {
        rows.push(
          ...this.#database
            .prepare<(number | string)[], WorkRow>(
              `SELECT ${WORK_ROW_COLUMNS} FROM work_item
               WHERE kind = ? AND implementation_version = ? AND schema_version = ?
                 AND state IN ('pending','retry_wait','quota_wait')
                 AND available_at <= ? AND priority < ?
               ORDER BY priority DESC, created_at, work_key LIMIT ?`,
            )
            .all(...profile, parsedCursor.priority, pageSize - rows.length),
        );
      }
    }
    const done = rows.length <= limit;
    const items = rows.slice(0, limit).map(rowToWork);
    const tail = items.at(-1);
    return {
      cursor: tail
        ? {
            createdAt: tail.createdAt,
            priority: tail.priority,
            workKey: tail.workKey,
          }
        : parsedCursor,
      done,
      items,
    };
  }

  /** Atomically replaces unclaimed legacy work with a fully validated new
   * definition. The old row remains as durable retirement evidence. */
  supersedeUnclaimed(
    sourceWorkKey: string,
    replacementInput: WorkDefinition,
    reason: string,
    now = Date.now(),
  ): SeedResult {
    const sourceKey = Sha256Schema.parse(sourceWorkKey);
    const replacement = WorkDefinitionSchema.parse(replacementInput);
    const errorCode = LedgerErrorCodeSchema.parse(reason);
    const replacementKey = workKey(replacement);
    return this.#immediate(() => {
      const retired = this.#database
        .prepare(RETIRE_UNCLAIMED_SQL)
        .run(now, now, sourceKey);
      if (retired.changes !== 1)
        throw new Error("Legacy work supersession lost its source row");
      const inserted = this.#database
        .prepare(WORK_ITEM_INSERT_SQL)
        .run(
          replacementKey,
          replacement.kind,
          canonicalJson(replacement.input),
          replacement.inputHash,
          replacement.schemaVersion,
          replacement.implementationVersion,
          replacement.priority,
          now,
          now,
          now,
        );
      if (inserted.changes === 1)
        this.#appendEvent(
          replacementKey,
          null,
          null,
          "seeded",
          { supersedesWorkKey: sourceKey },
          now,
        );
      this.#appendEvent(
        sourceKey,
        null,
        null,
        "version_retired",
        { reason: errorCode, replacementWorkKey: replacementKey },
        now,
      );
      return { inserted: inserted.changes === 1, workKey: replacementKey };
    });
  }

  /** Reverses an unattempted definition supersession. This is deliberately
   * fenced: once replacement work has started, rollback requires an explicit
   * operational repair instead of risking duplicate paid work. */
  rollbackUnattemptedSupersession(
    sourceWorkKey: string,
    replacementWorkKey: string,
    now = Date.now(),
  ): boolean {
    const sourceKey = Sha256Schema.parse(sourceWorkKey);
    const replacementKey = Sha256Schema.parse(replacementWorkKey);
    return this.#immediate(() => {
      const replacement = this.#database
        .prepare(
          `UPDATE work_item
              SET state = 'imported', last_error_code = NULL,
                  available_at = ?, updated_at = ?
            WHERE work_key = ? AND state = 'pending' AND attempt_count = 0`,
        )
        .run(now, now, replacementKey);
      if (replacement.changes !== 1) return false;
      const source = this.#database
        .prepare(
          `UPDATE work_item
              SET state = 'pending', last_error_code = NULL,
                  available_at = ?, updated_at = ?
            WHERE work_key = ? AND state = 'imported' AND attempt_count = 0`,
        )
        .run(now, now, sourceKey);
      if (source.changes !== 1)
        throw new Error("Supersession rollback lost its retired source row");
      this.#appendEvent(
        replacementKey,
        null,
        null,
        "version_rollback_retired",
        { restoresWorkKey: sourceKey },
        now,
      );
      this.#appendEvent(
        sourceKey,
        null,
        null,
        "version_rollback_restored",
        { replacementWorkKey: replacementKey },
        now,
      );
      return true;
    });
  }

  classifyUnclaimedTerminal(
    sourceWorkKey: string,
    reason: string,
    now = Date.now(),
  ): boolean {
    const sourceKey = Sha256Schema.parse(sourceWorkKey);
    const errorCode = LedgerErrorCodeSchema.parse(reason);
    return this.#immediate(() => {
      const classified = this.#database
        .prepare(CLASSIFY_UNCLAIMED_SQL)
        .run(errorCode, now, now, sourceKey);
      if (classified.changes === 1)
        this.#appendEvent(
          sourceKey,
          null,
          null,
          "failed",
          { errorCode, terminal: true },
          now,
        );
      return classified.changes === 1;
    });
  }

  /** Atomically terminalizes one ready, unclaimed item whose retry budget was
   * exhausted under a newer worker policy. The empty fast path stays
   * read-only so ordinary workers do not contend on the ledger writer lock. */
  classifyReadyErrorAtAttemptThreshold(
    kind: string,
    requirements: Required<ClaimRequirements>,
    sourceErrorCode: string,
    minimumAttempts: number,
    terminalErrorCode: string,
    now = Date.now(),
  ): null | string {
    validateClaim("terminal-classifier", 1, [kind], requirements);
    requireErrorCode(sourceErrorCode);
    requireErrorCode(terminalErrorCode);
    if (!Number.isSafeInteger(minimumAttempts) || minimumAttempts < 1)
      throw new Error("Attempt threshold must be a positive integer");
    const select = () =>
      this.#database
        .prepare<
          [string, string, string, string, number, number],
          { work_key: string }
        >(
          `SELECT work_key FROM work_item INDEXED BY work_item_kind_error_code
           WHERE kind = ? AND last_error_code = ?
             AND implementation_version = ? AND schema_version = ?
             AND state IN ('pending','retry_wait','quota_wait')
             AND attempt_count >= ? AND available_at <= ?
           ORDER BY priority DESC, created_at, work_key LIMIT 1`,
        )
        .get(
          kind,
          sourceErrorCode,
          requirements.implementationVersion,
          requirements.schemaVersion,
          minimumAttempts,
          now,
        );
    if (!select()) return null;
    return this.#immediate(() => {
      const row = select();
      if (!row) return null;
      const changed = this.#database
        .prepare(
          `UPDATE work_item
              SET state = 'dead_letter', last_error_code = ?,
                  available_at = ?, updated_at = ?
            WHERE work_key = ? AND kind = ? AND last_error_code = ?
              AND implementation_version = ? AND schema_version = ?
              AND state IN ('pending','retry_wait','quota_wait')
              AND attempt_count >= ? AND available_at <= ?`,
        )
        .run(
          terminalErrorCode,
          now,
          now,
          row.work_key,
          kind,
          sourceErrorCode,
          requirements.implementationVersion,
          requirements.schemaVersion,
          minimumAttempts,
          now,
        );
      if (changed.changes !== 1) return null;
      this.#appendEvent(
        row.work_key,
        null,
        null,
        "dead_letter",
        {
          errorCode: terminalErrorCode,
          minimumAttempts,
          previousErrorCode: sourceErrorCode,
          terminal: true,
        },
        now,
      );
      return row.work_key;
    });
  }

  claim(
    owner: string,
    now: number,
    leaseDurationMs: number,
    kinds: readonly string[] = [],
    requirements: StartedClaimRequirements = {},
    excludedErrorCodes: readonly string[] = [],
  ): null | WorkClaim {
    validateClaim(owner, leaseDurationMs, kinds, requirements);
    for (const code of excludedErrorCodes) requireErrorCode(code);
    return this.#immediate(() =>
      this.#workClaimer.claim({
        appendEvent: (...event) => this.#appendEvent(...event),
        excludedErrorCodes,
        kinds,
        leaseDurationMs,
        lookup: (key) => this.get(key),
        now,
        owner,
        requirements,
      }),
    );
  }

  /** Claims a bounded page in one durable transaction.
   *
   * Queue consumers that already process a bounded cooperative batch should
   * not pay one FULL-synchronous commit per item merely to establish leases.
   * Every claimed item still receives its own fenced token, epoch, attempt,
   * and append-only event; only the commit boundary is shared.
   */
  claimMany(
    owner: string,
    now: number,
    leaseDurationMs: number,
    kinds: readonly string[],
    limit: number,
    requirements: ClaimRequirements = {},
    excludedErrorCodes: readonly string[] = [],
    includedWorkKeys?: readonly string[],
  ): readonly WorkClaim[] {
    validateClaim(owner, leaseDurationMs, kinds, requirements);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Claim limit must be between 1 and 1000");
    for (const code of excludedErrorCodes) requireErrorCode(code);
    const exactWorkKeys =
      includedWorkKeys === undefined
        ? undefined
        : includedWorkKeys.map((workKeyValue) =>
            Sha256Schema.parse(workKeyValue),
          );
    if (exactWorkKeys && exactWorkKeys.length > 100)
      throw new Error("Exact claim cohort cannot exceed 100 work keys");
    if (exactWorkKeys && new Set(exactWorkKeys).size !== exactWorkKeys.length)
      throw new Error("Exact claim cohort contains duplicate work keys");
    return this.#immediate(() =>
      this.#workClaimer.claimMany({
        appendEvent: (...event) => this.#appendEvent(...event),
        excludedErrorCodes,
        ...(exactWorkKeys === undefined
          ? {}
          : { includedWorkKeys: exactWorkKeys }),
        kinds,
        leaseDurationMs,
        limit,
        lookup: (key) => this.get(key),
        now,
        owner,
        requirements,
      }),
    );
  }

  /** Claims one aged ambiguous operation for an exact immutable worker profile.
   *
   * This is deliberately separate from the ordinary priority queue: recovery
   * cannot rewrite user priority, starve fresh work, or expose ambiguous paid
   * operations to every worker lane. The read-only fast path also avoids a
   * write transaction when there is no recovery work. */
  claimUnknownOperationRecovery(
    owner: string,
    now: number,
    leaseDurationMs: number,
    kind: string,
    updatedBefore: number,
    requirements: Required<ClaimRequirements>,
  ): null | WorkClaim {
    validateClaim(owner, leaseDurationMs, [kind], requirements);
    if (!Number.isSafeInteger(updatedBefore) || updatedBefore > now)
      throw new Error("Unknown-operation recovery cutoff must not exceed now");
    const values = [
      kind,
      requirements.implementationVersion,
      requirements.schemaVersion,
      updatedBefore,
    ] as const;
    const select = () =>
      this.#database
        .prepare<(number | string)[], WorkRow>(
          `SELECT ${WORK_ROW_COLUMNS} FROM work_item INDEXED BY work_item_unknown_recovery
           WHERE state IN ('pending','retry_wait','quota_wait','dead_letter')
             AND kind = ?
             AND implementation_version = ? AND schema_version = ?
             AND last_error_code = 'CODEX_OPERATION_OUTCOME_UNKNOWN'
             AND updated_at <= ?
             AND (state = 'dead_letter' OR available_at <= ?)
           ORDER BY updated_at, created_at, work_key LIMIT 1`,
        )
        .get(...values, now);
    // At most one designated maintenance lane calls this method. Keeping the
    // overwhelmingly common empty path read-only avoids 256 lanes contending
    // for SQLite's writer lock after the recovery queue drains.
    if (!select()) return null;
    return this.#immediate(() => {
      const row = select();
      if (!row) return null;
      const token = randomUUID();
      const attemptId = randomUUID();
      const epoch = row.lease_epoch + 1;
      const changed = this.#database
        .prepare(
          `UPDATE work_item SET state = 'running', attempt_count = attempt_count + 1,
               lease_owner = ?, lease_token = ?, lease_epoch = ?, lease_expires_at = ?, updated_at = ?
           WHERE work_key = ? AND lease_epoch = ?
             AND state IN ('pending','retry_wait','quota_wait','dead_letter')
             AND implementation_version = ? AND schema_version = ?
             AND last_error_code = 'CODEX_OPERATION_OUTCOME_UNKNOWN'
             AND updated_at <= ?`,
        )
        .run(
          owner,
          token,
          epoch,
          now + leaseDurationMs,
          now,
          row.work_key,
          row.lease_epoch,
          requirements.implementationVersion,
          requirements.schemaVersion,
          updatedBefore,
        );
      if (changed.changes !== 1) return null;
      this.#appendEvent(
        row.work_key,
        attemptId,
        epoch,
        "claimed",
        {
          errorCode: "CODEX_OPERATION_OUTCOME_UNKNOWN",
          owner,
          recovery: true,
          updatedBefore,
        },
        now,
      );
      const work = this.get(row.work_key);
      if (!work) throw new Error("Recovery claim disappeared");
      return { attemptId, leaseEpoch: epoch, leaseToken: token, work };
    });
  }

  claimUnlessOriginStopped(
    origin: string,
    owner: string,
    now: number,
    leaseDurationMs: number,
    kinds: readonly string[] = [],
    requirements: ClaimRequirements = {},
    includedWorkKeys?: readonly string[],
  ): OriginGuardedWorkClaimResult {
    requireOrigin(origin);
    validateClaim(owner, leaseDurationMs, kinds, requirements);
    const exactWorkKeys =
      includedWorkKeys === undefined
        ? undefined
        : includedWorkKeys.map((workKeyValue) =>
            Sha256Schema.parse(workKeyValue),
          );
    if (exactWorkKeys && exactWorkKeys.length > 100)
      throw new Error("Exact claim cohort cannot exceed 100 work keys");
    if (exactWorkKeys && new Set(exactWorkKeys).size !== exactWorkKeys.length)
      throw new Error("Exact claim cohort contains duplicate work keys");
    return this.#immediate(() => {
      const gate = this.#database
        .prepare<
          [string],
          {
            cooldown_until: number;
            next_allowed_at: number;
            stop_reason: null | string;
          }
        >(
          `SELECT cooldown_until, next_allowed_at, stop_reason
           FROM origin_gate WHERE origin = ?`,
        )
        .get(origin);
      if (gate?.stop_reason) {
        const retryAt = Math.max(gate.cooldown_until, gate.next_allowed_at);
        if (!isRetryableOriginStopReason(gate.stop_reason)) {
          return { reason: gate.stop_reason, state: "stopped" };
        }
        if (retryAt > now)
          return { reason: gate.stop_reason, retryAt, state: "waiting" };
        const released = this.#database
          .prepare(RELEASE_RETRYABLE_ORIGIN_STOP_SQL)
          .run(now, origin, gate.stop_reason, now, now);
        if (released.changes !== 1) {
          return { reason: gate.stop_reason, state: "stopped" };
        }
      }
      const retryAt = gate
        ? Math.max(gate.cooldown_until, gate.next_allowed_at)
        : now;
      if (retryAt > now) return { retryAt, state: "waiting" };
      const claim = this.#workClaimer.claim({
        appendEvent: (...event) => this.#appendEvent(...event),
        excludedErrorCodes: [],
        ...(exactWorkKeys === undefined
          ? {}
          : { includedWorkKeys: exactWorkKeys }),
        kinds,
        leaseDurationMs,
        lookup: (key) => this.get(key),
        now,
        owner,
        requirements,
      });
      return claim ? { claim, state: "claimed" } : { state: "idle" };
    });
  }

  availability(
    kinds: readonly string[],
    now = Date.now(),
    requirements: StartedClaimRequirements = {},
  ): WorkAvailability {
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Availability kinds are required");
    const implementationFilter =
      requirements.implementationVersion === undefined
        ? ""
        : " AND implementation_version = ?";
    const schemaFilter =
      requirements.schemaVersion === undefined ? "" : " AND schema_version = ?";
    const values = [
      ...kinds,
      ...(requirements.implementationVersion === undefined
        ? []
        : [requirements.implementationVersion]),
      ...(requirements.schemaVersion === undefined
        ? []
        : [requirements.schemaVersion]),
    ];
    if (requirements.minimumAttemptCount !== undefined) {
      const minimum = z
        .int()
        .nonnegative()
        .parse(requirements.minimumAttemptCount);
      const row = StartedWorkAvailabilitySchema.parse(
        this.#database
          .prepare(
            `SELECT COALESCE(SUM(CASE WHEN available_at <= ? THEN 1 ELSE 0 END), 0) AS ready,
                MIN(available_at) AS earliest FROM work_item
         WHERE state IN ('pending','retry_wait','quota_wait')
           AND kind IN (${kinds.map(() => "?").join(",")})
           ${implementationFilter}${schemaFilter} AND attempt_count >= ?`,
          )
          .get(now, ...values, minimum),
      );
      return { earliestAvailableAt: row.earliest, ready: row.ready };
    }
    const row = this.#database
      .prepare<(number | string)[], { earliest: null | number; ready: number }>(
        `SELECT COALESCE(SUM(CASE WHEN available_at <= ? THEN item_count ELSE 0 END), 0) AS ready,
                MIN(available_at) AS earliest
         FROM ledger_profile_availability_count
         WHERE kind IN (${kinds.map(() => "?").join(",")})
           ${implementationFilter}${schemaFilter}`,
      )
      .get(now, ...values);
    if (!row) throw new Error("Availability aggregate missing");
    return { earliestAvailableAt: row.earliest, ready: row.ready };
  }

  /** Counts errors for one exact immutable worker profile. Aggregate ledger
   * status intentionally spans history; live provider health must not treat
   * obsolete implementations as actionable by the current worker. */
  countErrors(
    kind: string,
    errorCodes: readonly string[],
    requirements: Required<ClaimRequirements>,
  ): number {
    validateClaim("diagnostics", 1, [kind], requirements);
    if (errorCodes.length === 0) return 0;
    for (const code of errorCodes) requireErrorCode(code);
    const row = this.#database
      .prepare<string[], { count: number }>(
        `SELECT COALESCE(SUM(item_count), 0) AS count
         FROM ledger_profile_error_count
         WHERE kind = ? AND implementation_version = ? AND schema_version = ?
           AND error_code IN (${errorCodes.map(() => "?").join(",")})`,
      )
      .get(
        kind,
        requirements.implementationVersion,
        requirements.schemaVersion,
        ...errorCodes,
      );
    if (!row) throw new Error("Error count missing");
    return row.count;
  }

  /** Returns active progress for one immutable worker profile. The aggregate
   * status() API intentionally remains available for historical diagnostics. */
  profileProgress(
    kind: string,
    requirements: Required<ClaimRequirements>,
  ): KindProgress {
    validateClaim("diagnostics", 1, [kind], requirements);
    const counts = emptyWorkStateCounts();
    const rows = this.#database
      .prepare<string[], { count: number; state: string }>(
        `SELECT state, item_count AS count
         FROM ledger_profile_state_count
         WHERE kind = ? AND implementation_version = ? AND schema_version = ?
         ORDER BY state`,
      )
      .all(
        kind,
        requirements.implementationVersion,
        requirements.schemaVersion,
      );
    for (const row of rows) {
      counts[WorkStateSchema.parse(row.state)] = row.count;
    }
    const latestEvent = this.#database
      .prepare<string[], { created_at: number }>(
        `SELECT last_success_at AS created_at
         FROM ledger_profile_success_clock
         WHERE kind = ? AND implementation_version = ? AND schema_version = ?`,
      )
      .get(
        kind,
        requirements.implementationVersion,
        requirements.schemaVersion,
      );
    const total = Object.values(counts).reduce((sum, count) => sum + count, 0);
    return {
      byState: counts,
      completed: counts.succeeded + counts.imported,
      kind,
      lastSuccessAt: latestEvent?.created_at ?? null,
      terminal: counts.succeeded + counts.imported + counts.dead_letter,
      total,
    };
  }

  /** One coherent, indexed read for the lightweight provider heartbeat. */
  providerProfileDiagnostics(
    kind: string,
    requirements: Required<ClaimRequirements>,
    now: number,
    errorCodes: {
      readonly quarantined: readonly string[];
      readonly recoverableUnknown: readonly string[];
      readonly semantic: readonly string[];
    },
  ): ProviderProfileDiagnostics {
    return this.#readTransaction(() => ({
      availability: this.availability([kind], now, requirements),
      budget: this.solPaidUsageBudgetStatus(),
      poemThroughput: this.#poemThroughput(requirements, now),
      progress: this.profileProgress(kind, requirements),
      quarantinedOperations: this.countErrors(
        kind,
        errorCodes.quarantined,
        requirements,
      ),
      recoverableUnknownOperations: this.countErrors(
        kind,
        errorCodes.recoverableUnknown,
        requirements,
      ),
      semanticFailures: this.countErrors(
        kind,
        errorCodes.semantic,
        requirements,
      ),
    }));
  }

  poemThroughput(
    requirements: Required<ClaimRequirements>,
    now = Date.now(),
  ): SolPoemThroughput {
    validateClaim("diagnostics", 1, ["poem-enrichment-sol"], requirements);
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Throughput time must be nonnegative");
    return this.#readTransaction(() => this.#poemThroughput(requirements, now));
  }

  backfillSolPoemMilestones(
    eventType: "imported" | "succeeded",
    limit: number,
    now = Date.now(),
  ): SolPoemMilestoneBackfillResult {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Milestone backfill limit must be between 1 and 1000");
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Milestone backfill time must be nonnegative");
    const milestone = eventType === "succeeded" ? "generated" : "published";
    return this.#immediate(() => {
      const checkpoint = queryRequired(
        { operation: "ledger.solMilestoneBackfill.checkpoint" },
        () =>
          this.#database
            .prepare(
              `SELECT cursor_sequence, high_watermark, completed_at
                 FROM sol_poem_milestone_backfill WHERE event_type = ?`,
            )
            .get(eventType),
        SolMilestoneBackfillRowSchema,
      );
      if (checkpoint.completedAt !== null)
        return {
          complete: true,
          eventType,
          highWatermark: checkpoint.highWatermark,
          invalid: 0,
          inserted: 0,
          processed: 0,
        };
      const rows = queryMany(
        { operation: "ledger.solMilestoneBackfill.events" },
        () =>
          this.#database
            .prepare(
              `SELECT event.sequence, event.created_at AS completed_at,
                      event.payload_json, work.work_key,
                      work.implementation_version, work.schema_version
                 FROM work_event AS event INDEXED BY work_event_completed_scan
                 JOIN work_item AS work ON work.work_key = event.work_key
                WHERE event.event_type = ? AND event.sequence > ?
                  AND event.sequence <= ? AND work.kind = 'poem-enrichment-sol'
                ORDER BY event.sequence LIMIT ?`,
            )
            .all(
              eventType,
              checkpoint.cursorSequence,
              checkpoint.highWatermark,
              limit,
            ),
        SolMilestoneEventRowSchema,
      );
      const insert = this.#database.prepare(
        `INSERT INTO sol_poem_milestone(
           work_key, milestone, implementation_version, schema_version, completed_at
         ) VALUES(?, ?, ?, ?, ?) ON CONFLICT(work_key, milestone) DO NOTHING`,
      );
      let inserted = 0;
      let invalid = 0;
      for (const row of rows) {
        if (
          eventType === "imported" &&
          !SolImportedMilestonePayloadSchema.safeParse(row.payloadJson).success
        ) {
          invalid += 1;
          continue;
        }
        inserted += insert.run(
          row.workKey,
          milestone,
          row.implementationVersion,
          row.schemaVersion,
          row.completedAt,
        ).changes;
      }
      const complete = rows.length < limit;
      const cursorSequence = complete
        ? checkpoint.highWatermark
        : (rows.at(-1)?.sequence ?? checkpoint.cursorSequence);
      this.#database
        .prepare(
          `UPDATE sol_poem_milestone_backfill
              SET cursor_sequence = ?, completed_at = ?
            WHERE event_type = ? AND cursor_sequence = ? AND completed_at IS NULL`,
        )
        .run(
          cursorSequence,
          complete ? now : null,
          eventType,
          checkpoint.cursorSequence,
        );
      return {
        complete,
        eventType,
        highWatermark: checkpoint.highWatermark,
        invalid,
        inserted,
        processed: rows.length,
      };
    });
  }

  attemptRetentionEligibility(
    kind: string,
    implementationVersion: string,
    now: number,
    safetyAgeMs: number,
    attemptIds?: readonly string[],
  ): AttemptRetentionEligibility {
    if (kind.trim().length === 0 || implementationVersion.trim().length === 0)
      throw new Error("Retention kind and implementation version are required");
    requirePositiveDuration(safetyAgeMs);
    if (
      attemptIds &&
      (attemptIds.length > 1_000 ||
        attemptIds.some(
          (attemptId) =>
            !/^[\da-f]{8}-[\da-f]{4}-[1-8][\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/iu.test(
              attemptId,
            ),
        ))
    ) {
      throw new Error("Retention attempt IDs are invalid");
    }
    const attemptFilter =
      attemptIds === undefined
        ? ""
        : attemptIds.length === 0
          ? " AND 0"
          : ` AND checkpoint_attempt_reference.attempt_id IN (${attemptIds.map(() => "?").join(",")})`;
    const query = this.#database.prepare<
      string[],
      { attempt_id: null | string; state: string; updated_at: number }
    >(
      `SELECT work_item.state, work_item.updated_at,
              checkpoint_attempt_reference.attempt_id
         FROM checkpoint_attempt_reference
         CROSS JOIN work_item
           ON work_item.work_key = checkpoint_attempt_reference.work_key
        WHERE work_item.kind = ? AND work_item.implementation_version = ?
          ${attemptFilter}`,
    );
    const reservationQuery = this.#database.prepare<
      [string, string, string],
      { state: string; updated_at: number }
    >(
      `SELECT DISTINCT work_item.state, work_item.updated_at
         FROM checkpoint_attempt_reference
         CROSS JOIN work_item
           ON work_item.work_key = checkpoint_attempt_reference.work_key
         WHERE work_item.kind = ? AND work_item.implementation_version = ?
           AND checkpoint_attempt_reference.attempt_id = ?`,
    );
    const ambiguousAttemptQuery = this.#database.prepare<
      string[],
      { attempt_id: string }
    >(
      `SELECT paid_operation_reconciliation.attempt_id
       FROM paid_operation_reconciliation
       INNER JOIN work_item USING(work_key)
       WHERE paid_operation_reconciliation.state IN ('unknown','quarantined')
         AND work_item.kind = ? AND work_item.implementation_version = ?`,
    );
    const ambiguousReservationQuery = this.#database.prepare<
      [string, string, string],
      { protected: number }
    >(
      `SELECT 1 AS protected
       FROM paid_operation_reconciliation
       INNER JOIN work_item USING(work_key)
       WHERE paid_operation_reconciliation.state IN ('unknown','quarantined')
         AND work_item.kind = ? AND work_item.implementation_version = ?
         AND paid_operation_reconciliation.attempt_id = ?
       LIMIT 1`,
    );
    const calculate = (): Omit<
      AttemptRetentionEligibility,
      "withAttemptReservation"
    > => {
      // eslint-disable-next-line @sarj/require-sql-access-class -- This closure executes a Ledger-owned statement inside the attempt-retention reservation transaction.
      const rows = query.all(
        kind,
        implementationVersion,
        ...(attemptIds ?? []),
      );
      const eligibleAttemptIds = new Set<string>();
      const protectedAttemptIds = new Set<string>();
      let nextEligibleAt: null | number = null;
      for (const row of rows) {
        const terminal = ["succeeded", "imported", "dead_letter"].includes(
          row.state,
        );
        const eligibleAt = row.updated_at + safetyAgeMs;
        const eligible = terminal && eligibleAt <= now;
        if (terminal && !eligible)
          nextEligibleAt =
            nextEligibleAt === null
              ? eligibleAt
              : Math.min(nextEligibleAt, eligibleAt);
        const ids = new Set<string>();
        collectAttemptIds(row.attempt_id, ids);
        for (const id of ids)
          (eligible ? eligibleAttemptIds : protectedAttemptIds).add(id);
      }
      for (const { attempt_id: attemptId } of ambiguousAttemptQuery.all(
        kind,
        implementationVersion,
      )) {
        if (attemptIds && !attemptIds.includes(attemptId)) continue;
        protectedAttemptIds.add(attemptId);
      }
      for (const id of protectedAttemptIds) eligibleAttemptIds.delete(id);
      return { eligibleAttemptIds, nextEligibleAt, protectedAttemptIds };
    };
    return {
      ...calculate(),
      withAttemptReservation: (attemptId, operation) =>
        this.#immediate(() => {
          if (
            ambiguousReservationQuery.get(
              kind,
              implementationVersion,
              attemptId,
            )
          ) {
            return false;
          }
          let eligible = false;
          for (const row of reservationQuery.all(
            kind,
            implementationVersion,
            attemptId,
          )) {
            const terminal = ["succeeded", "imported", "dead_letter"].includes(
              row.state,
            );
            if (!terminal || row.updated_at + safetyAgeMs > now) return false;
            eligible = true;
          }
          if (!eligible) return false;
          operation();
          return true;
        }),
    };
  }

  retireIncompatible(
    kinds: readonly string[],
    requirements: Required<ClaimRequirements>,
    now = Date.now(),
  ): number {
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0)) {
      throw new Error("Retirement kinds are required");
    }
    if (
      requirements.implementationVersion.trim().length === 0 ||
      requirements.schemaVersion.trim().length === 0
    ) {
      throw new Error("Retirement versions are required");
    }
    return this.#immediate(() => {
      const rows = this.#database
        .prepare<string[], WorkRow>(
          `SELECT ${WORK_ROW_COLUMNS} FROM work_item
           WHERE kind IN (${kinds.map(() => "?").join(",")})
             AND state IN ('pending','retry_wait','quota_wait')
             AND (implementation_version <> ? OR schema_version <> ?)
           ORDER BY work_key`,
        )
        .all(
          ...kinds,
          requirements.implementationVersion,
          requirements.schemaVersion,
        );
      const insert = this.#database.prepare(WORK_ITEM_INSERT_SQL);
      const update = this.#database.prepare(
        `UPDATE work_item SET state = 'dead_letter', last_error_code = 'WORKER_VERSION_RETIRED',
           available_at = ?, updated_at = ?
         WHERE work_key = ? AND state IN ('pending','retry_wait','quota_wait')`,
      );
      for (const row of rows) {
        const definition = WorkDefinitionSchema.parse({
          implementationVersion: requirements.implementationVersion,
          input: JsonRecordSchema.parse(JSON.parse(row.input_json)),
          inputHash: row.input_hash,
          kind: row.kind,
          priority: row.priority,
          schemaVersion: requirements.schemaVersion,
        });
        const replacementKey = workKey(definition);
        const changed = update.run(now, now, row.work_key);
        if (changed.changes !== 1) {
          throw new Error("Incompatible work retirement lost its transaction");
        }
        const inserted = insert.run(
          replacementKey,
          definition.kind,
          canonicalJson(definition.input),
          definition.inputHash,
          definition.schemaVersion,
          definition.implementationVersion,
          definition.priority,
          Math.min(row.available_at, now),
          row.created_at,
          now,
        );
        if (inserted.changes === 1) {
          this.#appendEvent(
            replacementKey,
            null,
            null,
            "seeded",
            { supersedesWorkKey: row.work_key },
            now,
          );
        }
        this.#appendEvent(
          row.work_key,
          null,
          null,
          "version_retired",
          { ...requirements, replacementWorkKey: replacementKey },
          now,
        );
      }
      return rows.length;
    });
  }

  renew(
    claim: WorkClaim,
    now: number,
    leaseDurationMs: number,
    allowExpired = false,
  ): void {
    requirePositiveDuration(leaseDurationMs);
    this.#immediate(() => {
      const changed = this.#database
        .prepare(
          `UPDATE work_item SET lease_expires_at = ?, updated_at = ?
         WHERE work_key = ? AND state = 'running' AND lease_token = ? AND lease_epoch = ?
           AND (? = 1 OR lease_expires_at > ?)`,
        )
        .run(
          now + leaseDurationMs,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          allowExpired ? 1 : 0,
          now,
        );
      if (changed.changes !== 1) throw new LostLeaseError();
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        "renewed",
        {},
        now,
      );
    });
  }

  /** Renews every still-current claim while explicitly reporting stale peers.
   * A stale member never rolls back renewal of another independently fenced
   * lease, so bounded batch consumers can safely protect queued claims.
   */
  renewMany(
    claims: readonly WorkClaim[],
    now: number,
    leaseDurationMs: number,
    allowExpired = false,
  ): {
    readonly renewed: readonly string[];
    readonly stale: readonly string[];
  } {
    requirePositiveDuration(leaseDurationMs);
    if (claims.length > 1_000)
      throw new Error("Renew limit must not exceed 1000");
    if (new Set(claims.map(({ work }) => work.workKey)).size !== claims.length)
      throw new Error("Renew claims must be unique");
    if (claims.length === 0) return { renewed: [], stale: [] };
    return this.#immediate(() => {
      const renew = this.#database.prepare(
        `UPDATE work_item SET lease_expires_at = ?, updated_at = ?
         WHERE work_key = ? AND state = 'running' AND lease_token = ?
           AND lease_epoch = ? AND (? = 1 OR lease_expires_at > ?)`,
      );
      const renewed: string[] = [];
      const stale: string[] = [];
      for (const claim of claims) {
        const changed = renew.run(
          now + leaseDurationMs,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          allowExpired ? 1 : 0,
          now,
        );
        if (changed.changes !== 1) {
          stale.push(claim.work.workKey);
          continue;
        }
        this.#appendEvent(
          claim.work.workKey,
          claim.attemptId,
          claim.leaseEpoch,
          "renewed",
          {},
          now,
        );
        renewed.push(claim.work.workKey);
      }
      return { renewed, stale };
    });
  }

  checkpoint(claim: WorkClaim, value: Checkpoint, now = Date.now()): number {
    const parsed = CheckpointSchema.parse(value);
    return this.#immediate(() => {
      this.#assertLease(claim, now);
      const result = this.#database
        .prepare(
          `INSERT INTO checkpoint(
             checkpoint_id, work_key, attempt_id, lease_epoch, kind,
             payload_json, artifact_hash, created_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          randomUUID(),
          claim.work.workKey,
          claim.attemptId,
          claim.leaseEpoch,
          parsed.kind,
          canonicalJson(parsed.payload),
          parsed.artifactHash,
          now,
        );
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        "checkpointed",
        {
          checkpointSequence: Number(result.lastInsertRowid),
          kind: parsed.kind,
        },
        now,
      );
      return Number(result.lastInsertRowid);
    });
  }

  succeed(claim: WorkClaim, artifactHash: string, now = Date.now()): void {
    if (!/^[a-f\d]{64}$/.test(artifactHash))
      throw new Error("Invalid artifact hash");
    this.#transitionClaim(claim, "succeeded", now, artifactHash, null, now);
  }

  attachApprovedBindingAndSeedPublication(
    translationWorkKey: string,
    artifactHash: string,
    bindingInput: CanonicalPoemBindingV1,
    options: DirectPublicationOptions,
    now = Date.now(),
  ): ApprovedPublicationSeedResult {
    const translationKey = Sha256Schema.parse(translationWorkKey);
    const approvedArtifactHash = Sha256Schema.parse(artifactHash);
    const binding = CanonicalPoemBindingV1Schema.parse(bindingInput);
    const {
      admissionEvidence: _admissionEvidence,
      bindingId: _bindingId,
      ...bindingIdentity
    } = binding;
    if (
      sha256(canonicalPoemBindingIdBody(bindingIdentity)) !== binding.bindingId
    ) {
      throw new Error(
        "Canonical translation binding ID does not match identity",
      );
    }
    const configuration = DirectPublicationOptionsSchema.parse(options);
    const publicationInput = {
      binding,
      jobType: "bound-translation" as const,
      source: {
        artifactHash: approvedArtifactHash,
        workKey: translationKey,
      },
    };
    const publicationDefinition = WorkDefinitionSchema.parse({
      implementationVersion: configuration.implementationVersion,
      input: publicationInput,
      inputHash: inputHash(publicationInput),
      kind: DIRECT_ENRICHMENT_PUBLICATION_KIND,
      priority: configuration.priority,
      schemaVersion: DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
    });
    const publicationWorkKey = workKey(publicationDefinition);
    const bindingJson = canonicalJson(binding);
    const publicationInputJson = canonicalJson(publicationDefinition.input);

    return this.#immediate(() => {
      const translation = this.#database
        .prepare<
          string,
          {
            implementation_version: string;
            input_json: string;
            kind: string;
            output_artifact_hash: null | string;
            schema_version: string;
            state: string;
          }
        >(
          `SELECT kind, implementation_version, input_json, schema_version,
                  state, output_artifact_hash
           FROM work_item WHERE work_key = ?`,
        )
        .get(translationKey);
      if (
        translation?.kind !== "poem-enrichment-sol" ||
        !["sol-word-gloss-v2", "sol-word-gloss-v3"].includes(
          translation.implementation_version,
        ) ||
        translation.schema_version !== "saqi.poem-enrichment-input@1" ||
        translation.output_artifact_hash !== approvedArtifactHash
      ) {
        throw new Error("Approved legacy Sol translation does not match");
      }
      const translationInput = PoemEnrichmentInputSchema.safeParse(
        JSON.parse(translation.input_json),
      );
      if (
        !translationInput.success ||
        sha256(sourceLineNfcHashBody(translationInput.data.linesArabic)) !==
          binding.lineNfcHash ||
        sha256(sourcePromptMaterialHashBody(translationInput.data)) !==
          binding.promptMaterialHash
      ) {
        throw new Error(
          "Approved legacy Sol translation material does not match",
        );
      }

      const existingBinding = this.#database
        .prepare<string, { binding_id: string; binding_json: string }>(
          `SELECT binding_id, binding_json
           FROM canonical_translation_binding
           WHERE translation_work_key = ?`,
        )
        .get(translationKey);
      const existingDerivation = this.#database
        .prepare<
          string,
          {
            approved_artifact_hash: string;
            binding_id: string;
            publication_work_key: string;
          }
        >(
          `SELECT approved_artifact_hash, binding_id, publication_work_key
           FROM publication_derivation
           WHERE translation_work_key = ?`,
        )
        .get(translationKey);
      const existingPublication = this.#database
        .prepare<string, WorkRow>(
          `SELECT ${WORK_ROW_COLUMNS} FROM work_item
           WHERE work_item.work_key = ?`,
        )
        .get(publicationWorkKey);
      if (
        existingBinding !== undefined ||
        existingDerivation !== undefined ||
        existingPublication !== undefined
      ) {
        if (
          existingBinding?.binding_id !== binding.bindingId ||
          existingBinding.binding_json !== bindingJson ||
          existingDerivation?.approved_artifact_hash !== approvedArtifactHash ||
          existingDerivation.binding_id !== binding.bindingId ||
          existingDerivation.publication_work_key !== publicationWorkKey ||
          existingPublication?.kind !== publicationDefinition.kind ||
          existingPublication.input_json !== publicationInputJson ||
          existingPublication.input_hash !== publicationDefinition.inputHash ||
          existingPublication.schema_version !==
            publicationDefinition.schemaVersion ||
          existingPublication.implementation_version !==
            publicationDefinition.implementationVersion ||
          existingPublication.priority !== publicationDefinition.priority
        ) {
          throw new Error("Approved publication attachment conflicts");
        }
        return { publicationWorkKey, translationWorkKey: translationKey };
      }
      if (!["succeeded", "imported"].includes(translation.state)) {
        throw new Error("Approved legacy Sol translation is not completed");
      }

      const bindingInsert = this.#database
        .prepare(CANONICAL_TRANSLATION_BINDING_INSERT_SQL)
        .run(
          translationKey,
          binding.bindingId,
          bindingJson,
          binding.poemId,
          binding.sourceRevisionId,
          binding.lineNfcHash,
          binding.promptMaterialHash,
          now,
        );
      if (bindingInsert.changes !== 1) {
        throw new Error("Approved translation binding was not inserted");
      }
      const publicationInsert = this.#database
        .prepare(WORK_ITEM_INSERT_SQL)
        .run(
          publicationWorkKey,
          publicationDefinition.kind,
          publicationInputJson,
          publicationDefinition.inputHash,
          publicationDefinition.schemaVersion,
          publicationDefinition.implementationVersion,
          publicationDefinition.priority,
          now,
          now,
          now,
        );
      if (publicationInsert.changes !== 1) {
        throw new Error("Approved publication work was not inserted");
      }
      const derivationInsert = this.#database
        .prepare(PUBLICATION_DERIVATION_INSERT_SQL)
        .run(
          translationKey,
          binding.bindingId,
          publicationWorkKey,
          approvedArtifactHash,
          now,
        );
      if (derivationInsert.changes !== 1) {
        throw new Error("Approved publication derivation was not inserted");
      }
      this.#appendEvent(
        publicationWorkKey,
        null,
        null,
        "seeded",
        {
          bindingId: binding.bindingId,
          translationWorkKey: translationKey,
        },
        now,
      );
      return { publicationWorkKey, translationWorkKey: translationKey };
    });
  }

  approvedPublicationAttachmentForTranslation(
    translationWorkKey: string,
    artifactHash: string,
  ): ApprovedPublishedTranslationAttachment | null {
    const translationKey = Sha256Schema.parse(translationWorkKey);
    const approvedArtifactHash = Sha256Schema.parse(artifactHash);
    const row = this.#database
      .prepare<
        string,
        {
          approved_artifact_hash: null | string;
          binding_id: null | string;
          binding_json: null | string;
          derivation_binding_id: null | string;
          output_artifact_hash: null | string;
          publication_priority: null | number;
        }
      >(
        `SELECT translation.output_artifact_hash,
                binding.binding_id, binding.binding_json,
                derivation.binding_id AS derivation_binding_id,
                derivation.approved_artifact_hash,
                publication.priority AS publication_priority
           FROM work_item AS translation
           LEFT JOIN canonical_translation_binding AS binding
             ON binding.translation_work_key = translation.work_key
           LEFT JOIN publication_derivation AS derivation
             ON derivation.translation_work_key = translation.work_key
           LEFT JOIN work_item AS publication
             ON publication.work_key = derivation.publication_work_key
          WHERE translation.work_key = ?`,
      )
      .get(translationKey);
    if (
      !row ||
      (row.binding_json === null && row.approved_artifact_hash === null)
    )
      return null;
    if (
      row.output_artifact_hash !== approvedArtifactHash ||
      row.approved_artifact_hash !== approvedArtifactHash ||
      row.binding_id === null ||
      row.derivation_binding_id !== row.binding_id ||
      row.binding_json === null ||
      row.publication_priority === null
    ) {
      throw new Error("APPROVED_PUBLICATION_ATTACHMENT_CONFLICT");
    }
    const binding = CanonicalPoemBindingV1Schema.parse(
      JSON.parse(row.binding_json),
    );
    if (binding.bindingId !== row.binding_id)
      throw new Error("APPROVED_PUBLICATION_ATTACHMENT_CONFLICT");
    return { binding, publicationPriority: row.publication_priority };
  }

  completeApprovedAndSeedPublication(
    claim: WorkClaim,
    artifactHash: string,
    bindingInput: CanonicalPoemBindingV1,
    options: DirectPublicationOptions,
    now = Date.now(),
  ): ApprovedPublicationSeedResult {
    const approvedArtifactHash = Sha256Schema.parse(artifactHash);
    const binding = CanonicalPoemBindingV1Schema.parse(bindingInput);
    const {
      admissionEvidence: _admissionEvidence,
      bindingId: _bindingId,
      ...bindingIdentity
    } = binding;
    if (
      sha256(canonicalPoemBindingIdBody(bindingIdentity)) !== binding.bindingId
    ) {
      throw new Error(
        "Canonical translation binding ID does not match identity",
      );
    }
    if (claim.work.kind !== "poem-enrichment-sol") {
      throw new Error("Only Sol enrichment work can seed direct publication");
    }
    const configuration = DirectPublicationOptionsSchema.parse(options);
    const publicationInput = {
      binding,
      jobType: "bound-translation" as const,
      source: {
        artifactHash: approvedArtifactHash,
        workKey: claim.work.workKey,
      },
    };
    const publicationDefinition = WorkDefinitionSchema.parse({
      implementationVersion: configuration.implementationVersion,
      input: publicationInput,
      inputHash: inputHash(publicationInput),
      kind: DIRECT_ENRICHMENT_PUBLICATION_KIND,
      priority: configuration.priority,
      schemaVersion: DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
    });
    const publicationWorkKey = workKey(publicationDefinition);
    const publicationInputJson = canonicalJson(publicationDefinition.input);

    return this.#immediate(() => {
      this.#assertLease(claim, now);
      const bindingInsert = this.#database
        .prepare(CANONICAL_TRANSLATION_BINDING_INSERT_SQL)
        .run(
          claim.work.workKey,
          binding.bindingId,
          canonicalJson(binding),
          binding.poemId,
          binding.sourceRevisionId,
          binding.lineNfcHash,
          binding.promptMaterialHash,
          now,
        );
      if (bindingInsert.changes !== 1) {
        throw new Error("Canonical translation binding was not inserted");
      }

      const completed = this.#database
        .prepare(SUCCEED_LEASED_WORK_SQL)
        .run(
          now,
          approvedArtifactHash,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          now,
        );
      if (completed.changes !== 1) throw new LostLeaseError();

      const publicationInsert = this.#database
        .prepare(WORK_ITEM_INSERT_SQL)
        .run(
          publicationWorkKey,
          publicationDefinition.kind,
          publicationInputJson,
          publicationDefinition.inputHash,
          publicationDefinition.schemaVersion,
          publicationDefinition.implementationVersion,
          publicationDefinition.priority,
          now,
          now,
          now,
        );
      if (publicationInsert.changes !== 1) {
        throw new Error(
          "Direct publication work already exists without derivation",
        );
      }

      const derivationInsert = this.#database
        .prepare(PUBLICATION_DERIVATION_INSERT_SQL)
        .run(
          claim.work.workKey,
          binding.bindingId,
          publicationWorkKey,
          approvedArtifactHash,
          now,
        );
      if (derivationInsert.changes !== 1) {
        throw new Error("Publication derivation was not inserted");
      }
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        "succeeded",
        {
          artifactHash: approvedArtifactHash,
          publicationWorkKey,
        },
        now,
      );
      this.#appendEvent(
        publicationWorkKey,
        null,
        null,
        "seeded",
        {
          bindingId: binding.bindingId,
          translationWorkKey: claim.work.workKey,
        },
        now,
      );
      return {
        publicationWorkKey,
        translationWorkKey: claim.work.workKey,
      };
    });
  }

  confirmDirectPublication(
    claim: WorkClaim,
    translationWorkKey: string,
    approvedArtifactHash: string,
    receiptArtifactHash: string,
    now = Date.now(),
  ): DirectPublicationConfirmationResult {
    const translationKey = Sha256Schema.parse(translationWorkKey);
    const approvedHash = Sha256Schema.parse(approvedArtifactHash);
    const receiptHash = Sha256Schema.parse(receiptArtifactHash);
    if (claim.work.kind !== DIRECT_ENRICHMENT_PUBLICATION_KIND) {
      throw new Error("Claim is not direct enrichment publication work");
    }
    return this.#immediate(() => {
      const relationship = this.#database
        .prepare<
          [string, string],
          {
            approved_artifact_hash: string;
            publication_artifact_hash: null | string;
            publication_state: string;
            translation_artifact_hash: null | string;
            translation_state: string;
          }
        >(
          `SELECT derivation.approved_artifact_hash,
                  publication.state AS publication_state,
                  publication.output_artifact_hash AS publication_artifact_hash,
                  translation.state AS translation_state,
                  translation.output_artifact_hash AS translation_artifact_hash
           FROM publication_derivation AS derivation
           JOIN work_item AS publication
             ON publication.work_key = derivation.publication_work_key
           JOIN work_item AS translation
             ON translation.work_key = derivation.translation_work_key
           WHERE derivation.publication_work_key = ?
             AND derivation.translation_work_key = ?`,
        )
        .get(claim.work.workKey, translationKey);
      if (
        relationship?.approved_artifact_hash !== approvedHash ||
        relationship.translation_artifact_hash !== approvedHash
      ) {
        throw new Error(
          "Direct publication derivation does not match artifacts",
        );
      }
      if (
        relationship.publication_state === "succeeded" &&
        relationship.publication_artifact_hash === receiptHash &&
        relationship.translation_state === "imported"
      ) {
        return "already_confirmed";
      }
      if (
        relationship.publication_state !== "running" ||
        relationship.translation_state !== "succeeded"
      ) {
        throw new Error("Direct publication derivation is not confirmable");
      }
      this.#assertLease(claim, now);
      const imported = this.#database
        .prepare(
          `UPDATE work_item SET state = 'imported', updated_at = ?
           WHERE work_key = ? AND state = 'succeeded'
             AND output_artifact_hash = ?`,
        )
        .run(now, translationKey, approvedHash);
      if (imported.changes !== 1) {
        throw new Error("Direct publication translation import was lost");
      }
      const completed = this.#database
        .prepare(SUCCEED_LEASED_WORK_SQL)
        .run(
          now,
          receiptHash,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          now,
        );
      if (completed.changes !== 1) throw new LostLeaseError();
      this.#appendEvent(
        translationKey,
        null,
        null,
        "imported",
        {
          publicationWorkKey: claim.work.workKey,
          receiptArtifactHash: receiptHash,
        },
        now,
      );
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        "succeeded",
        {
          receiptArtifactHash: receiptHash,
          translationWorkKey: translationKey,
        },
        now,
      );
      return "confirmed";
    });
  }

  retry(
    claim: WorkClaim,
    errorCode: string,
    retryAt: number,
    now = Date.now(),
  ): void {
    requireErrorCode(errorCode);
    requireDeferredTimestamp(retryAt, now);
    this.#transitionClaim(claim, "retry_wait", now, null, errorCode, retryAt);
  }

  quotaWait(
    claim: WorkClaim,
    errorCode: string,
    retryAt: number,
    now = Date.now(),
  ): void {
    requireErrorCode(errorCode);
    requireDeferredTimestamp(retryAt, now);
    this.#transitionClaim(claim, "quota_wait", now, null, errorCode, retryAt);
  }

  deadLetter(claim: WorkClaim, errorCode: string, now = Date.now()): void {
    requireErrorCode(errorCode);
    this.#transitionClaim(claim, "dead_letter", now, null, errorCode, now);
  }

  operatorRelease(
    claim: WorkClaim,
    reason = "OPERATOR_RELEASED",
    now = Date.now(),
    availableAt = now,
  ): void {
    requireErrorCode(reason);
    if (!Number.isSafeInteger(availableAt) || availableAt < now)
      throw new Error("Operator release availability must not precede now");
    this.#immediate(() => {
      const changed = this.#database
        .prepare(
          `UPDATE work_item SET state = 'pending', available_at = ?,
             attempt_count = MAX(0, attempt_count - 1), last_error_code = ?,
             lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
             updated_at = ?
           WHERE work_key = ? AND state = 'running' AND lease_token = ?
             AND lease_epoch = ? AND lease_expires_at > ?`,
        )
        .run(
          availableAt,
          reason,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          now,
        );
      if (changed.changes !== 1) throw new LostLeaseError();
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        "operator_released",
        { reason },
        now,
      );
    });
  }

  /** Wakes exact fanout jobs after their external resolution becomes ready.
   *
   * Running claims are deliberately returned for retry instead of being
   * mutated. Keeping their outbox signal unacknowledged closes the race where
   * a live worker could otherwise overwrite the wake with a deferred release.
   */
  wakeResolutionPending(
    workKeys: readonly string[],
    kinds: readonly string[],
    now = Date.now(),
  ): WakeResolutionPendingResult {
    if (workKeys.length > 500)
      throw new Error("Resolution wake batch must not exceed 500 work keys");
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Resolution wake kinds are required");
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error(
        "Resolution wake timestamp must be a nonnegative integer",
      );
    const keys = [...new Set(workKeys)];
    for (const key of keys) {
      if (!/^[a-f\d]{64}$/.test(key))
        throw new Error("Invalid resolution wake work key");
    }
    if (keys.length === 0) return { acknowledge: [], retry: [] };
    const allowedKinds = new Set(kinds);
    return this.#immediate(() => {
      const acknowledge: string[] = [];
      const retry: string[] = [];
      const select = this.#database.prepare<
        [string],
        Pick<WorkRow, "available_at" | "kind" | "last_error_code" | "state">
      >(
        `SELECT kind, state, available_at, last_error_code
         FROM work_item WHERE work_key = ?`,
      );
      const wake = this.#database.prepare(
        `UPDATE work_item SET available_at = ?, updated_at = ?
         WHERE work_key = ?
           AND state IN ('pending','retry_wait','quota_wait')
           AND last_error_code = 'FANOUT_RESOLUTION_PENDING'
           AND available_at > ?`,
      );
      const prioritize = this.#database.prepare(
        `INSERT INTO fanout_priority_hint(
           work_key, kind, state, available_at, created_at, updated_at
         )
         SELECT work_key, kind, state, available_at, ?, ?
           FROM work_item WHERE work_key = ?
         ON CONFLICT(work_key) DO UPDATE SET
           kind = excluded.kind,
           state = excluded.state,
           available_at = excluded.available_at,
           updated_at = excluded.updated_at`,
      );
      let priorityHintCount = CountRowSchema.parse(
        this.#database.prepare(COUNT_FANOUT_PRIORITY_HINTS_SQL).get(),
      ).count;
      const prioritizeKey = (key: string): boolean => {
        const existing = this.#database
          .prepare(FIND_FANOUT_PRIORITY_HINT_SQL)
          .get(key);
        if (
          existing === undefined &&
          priorityHintCount >= MAX_DURABLE_FANOUT_PRIORITY_HINTS
        )
          return false;
        prioritize.run(now, now, key);
        if (existing === undefined) priorityHintCount += 1;
        return true;
      };
      for (const key of keys) {
        const row = select.get(key);
        if (row?.state === "running") {
          retry.push(key);
          continue;
        }
        // Stale outbox entries must not permanently occupy the bounded head of
        // the queue. Only a live claim can become deliverable later; missing,
        // incompatible, or otherwise superseded work is safe to discard.
        if (!row || !allowedKinds.has(row.kind)) {
          acknowledge.push(key);
          continue;
        }
        if (["succeeded", "imported", "dead_letter"].includes(row.state)) {
          acknowledge.push(key);
          continue;
        }
        if (
          ["pending", "retry_wait", "quota_wait"].includes(row.state) &&
          row.available_at <= now
        ) {
          if (!prioritizeKey(key)) {
            retry.push(key);
            continue;
          }
          acknowledge.push(key);
          continue;
        }
        if (row.last_error_code !== "FANOUT_RESOLUTION_PENDING") {
          acknowledge.push(key);
          continue;
        }
        if (!prioritizeKey(key)) {
          retry.push(key);
          continue;
        }
        const changed = wake.run(now, now, key, now);
        if (changed.changes !== 1) {
          retry.push(key);
          continue;
        }
        this.#appendEvent(
          key,
          null,
          null,
          "resolution_ready",
          { previousAvailableAt: row.available_at },
          now,
        );
        acknowledge.push(key);
      }
      return { acknowledge, retry };
    });
  }

  /** Persists exact fanout scheduling hints independently of process memory.
   * Existing creation order is retained on replay so a noisy producer cannot
   * starve an older hinted item by repeatedly touching it. */
  prioritizeFanoutWork(
    workKeys: readonly string[],
    kinds: readonly string[],
    now = Date.now(),
  ): number {
    if (workKeys.length > 500)
      throw new Error("Fanout priority batch must not exceed 500 work keys");
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Fanout priority kinds are required");
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Fanout priority timestamp must be nonnegative");
    const keys = [...new Set(workKeys.map((key) => Sha256Schema.parse(key)))];
    if (keys.length === 0) return 0;
    const placeholders = kinds.map(() => "?").join(",");
    return this.#immediate(() => {
      let priorityHintCount = CountRowSchema.parse(
        this.#database.prepare(COUNT_FANOUT_PRIORITY_HINTS_SQL).get(),
      ).count;
      const exists = this.#database.prepare(FIND_FANOUT_PRIORITY_HINT_SQL);
      const prioritize = this.#database.prepare(
        `INSERT INTO fanout_priority_hint(
           work_key, kind, state, available_at, created_at, updated_at
         )
         SELECT work_key, kind, state, available_at, ?, ? FROM work_item
          WHERE work_key = ? AND kind IN (${placeholders})
            AND state IN ('pending','retry_wait','quota_wait')
         ON CONFLICT(work_key) DO UPDATE SET
           kind = excluded.kind,
           state = excluded.state,
           available_at = excluded.available_at,
           updated_at = excluded.updated_at`,
      );
      let changed = 0;
      for (const key of keys) {
        const existing = exists.get(key);
        if (
          existing === undefined &&
          priorityHintCount >= MAX_DURABLE_FANOUT_PRIORITY_HINTS
        )
          continue;
        const result = prioritize.run(now, now, key, ...kinds);
        if (result.changes !== 1) continue;
        changed += 1;
        if (existing === undefined) priorityHintCount += 1;
      }
      return changed;
    });
  }

  /** Incrementally repairs pre-v23 ready resolution work into the durable
   * exact-priority queue. The partial index and global hint cap keep this from
   * becoming a historical-ledger scan or an unbounded startup transaction. */
  backfillFanoutPriorityWork(
    kinds: readonly string[],
    limit: number,
    now = Date.now(),
  ): number {
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Fanout priority kinds are required");
    const bounded = LedgerBatchLimitSchema.parse(limit);
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Fanout priority timestamp must be nonnegative");
    return this.#immediate(() => {
      const count = CountRowSchema.parse(
        this.#database.prepare(COUNT_FANOUT_PRIORITY_HINTS_SQL).get(),
      ).count;
      const capacity = Math.min(
        bounded,
        MAX_DURABLE_FANOUT_PRIORITY_HINTS - count,
      );
      if (capacity <= 0) return 0;
      // The priority index cannot seek by kind or readiness. Probe the range
      // index first so an empty compatibility poll never walks future work.
      // At most the globally capped hint rows precede an unhinted candidate.
      // Keep the original ordered insertion for priority fairness when ready.
      const candidate = this.#database
        .prepare<(number | string)[]>(
          `SELECT 1 FROM work_item AS work
             INDEXED BY work_item_fanout_resolution_ready
           WHERE work.kind IN (${kinds.map(() => "?").join(",")})
             AND work.state IN ('pending','retry_wait','quota_wait')
             AND work.last_error_code = 'FANOUT_RESOLUTION_PENDING'
             AND work.available_at <= ?
             AND NOT EXISTS(
               SELECT 1 FROM fanout_priority_hint AS hint
               WHERE hint.work_key = work.work_key
             )
           LIMIT 1`,
        )
        .get(...kinds, now);
      if (candidate === undefined) return 0;
      return this.#database
        .prepare<(number | string)[]>(
          `INSERT INTO fanout_priority_hint(
             work_key, kind, state, available_at, created_at, updated_at
           )
           SELECT work.work_key, work.kind, work.state, work.available_at, ?, ?
             FROM work_item AS work
                  INDEXED BY work_item_fanout_resolution_pending
            WHERE work.kind IN (${kinds.map(() => "?").join(",")})
              AND work.state IN ('pending','retry_wait','quota_wait')
              AND work.last_error_code = 'FANOUT_RESOLUTION_PENDING'
              AND work.available_at <= ?
              AND NOT EXISTS(
                SELECT 1 FROM fanout_priority_hint AS hint
                 WHERE hint.work_key = work.work_key
              )
            ORDER BY work.priority DESC, work.available_at,
                     work.created_at, work.work_key
            LIMIT ?`,
        )
        .run(now, now, ...kinds, now, capacity).changes;
    });
  }

  /** Pages only the unresolved fanout partial index so a one-time compatibility
   * pass can reconstruct durable resolution waiters retired by older runtimes.
   * The stable schedule tuple makes checkpoint replay safe without OFFSET. */
  listResolutionPendingAfter(
    cursor: null | ResolutionPendingCursor,
    kinds: readonly string[],
    limit: number,
    updatedBefore: number,
  ): ResolutionPendingPage {
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Resolution pending kinds are required");
    const bounded = LedgerBatchLimitSchema.parse(limit);
    const cutoff = LedgerTimestampSchema.parse(updatedBefore);
    const parsedCursor = cursor
      ? {
          availableAt: LedgerTimestampSchema.parse(cursor.availableAt),
          createdAt: LedgerTimestampSchema.parse(cursor.createdAt),
          priority: LedgerIntegerSchema.parse(cursor.priority),
          workKey: Sha256Schema.parse(cursor.workKey),
        }
      : null;
    const cursorPredicate = parsedCursor
      ? `AND (
           work_item.priority < ?
           OR (work_item.priority = ? AND work_item.available_at > ?)
           OR (work_item.priority = ? AND work_item.available_at = ?
               AND work_item.created_at > ?)
           OR (work_item.priority = ? AND work_item.available_at = ?
               AND work_item.created_at = ? AND work_item.work_key > ?)
         )`
      : "";
    const cursorParameters = parsedCursor
      ? [
          parsedCursor.priority,
          parsedCursor.priority,
          parsedCursor.availableAt,
          parsedCursor.priority,
          parsedCursor.availableAt,
          parsedCursor.createdAt,
          parsedCursor.priority,
          parsedCursor.availableAt,
          parsedCursor.createdAt,
          parsedCursor.workKey,
        ]
      : [];
    const rows = this.#database
      .prepare<(number | string)[], WorkRow>(
        `SELECT ${WORK_ROW_COLUMNS}
           FROM work_item INDEXED BY work_item_fanout_resolution_pending
          WHERE work_item.kind IN (${kinds.map(() => "?").join(",")})
            AND work_item.state IN ('pending','retry_wait','quota_wait')
            AND work_item.last_error_code = 'FANOUT_RESOLUTION_PENDING'
            AND work_item.updated_at < ?
            ${cursorPredicate}
          ORDER BY work_item.priority DESC, work_item.available_at,
                   work_item.created_at, work_item.work_key
          LIMIT ?`,
      )
      .all(...kinds, cutoff, ...cursorParameters, bounded + 1);
    const complete = rows.length <= bounded;
    const pageRows = rows.slice(0, bounded);
    const items = pageRows.map(rowToWork);
    const last = pageRows.at(-1);
    return {
      complete,
      cursor: last
        ? {
            availableAt: last.available_at,
            createdAt: last.created_at,
            priority: last.priority,
            workKey: Sha256Schema.parse(last.work_key),
          }
        : parsedCursor,
      items,
    };
  }

  listFanoutPriorityWorkKeys(
    kinds: readonly string[],
    limit: number,
    now = Date.now(),
  ): readonly string[] {
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Fanout priority kinds are required");
    const bounded = ReconciliationBatchLimitSchema.parse(limit);
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Fanout priority timestamp must be nonnegative");
    return this.#database
      .prepare<(number | string)[], { work_key: string }>(
        `SELECT hint.work_key
           FROM fanout_priority_hint AS hint
                INDEXED BY fanout_priority_hint_schedule
          WHERE hint.kind IN (${kinds.map(() => "?").join(",")})
            AND hint.state IN ('pending','retry_wait','quota_wait')
            AND hint.available_at <= ?
          ORDER BY hint.created_at, hint.work_key
          LIMIT ?`,
      )
      .all(...kinds, now, bounded)
      .map(({ work_key: workKeyValue }) => Sha256Schema.parse(workKeyValue));
  }

  acknowledgeFanoutPriorityWork(workKeys: readonly string[]): number {
    if (workKeys.length > 1_000)
      throw new Error(
        "Fanout priority acknowledgement cannot exceed 1000 keys",
      );
    const keys = [...new Set(workKeys.map((key) => Sha256Schema.parse(key)))];
    if (keys.length === 0) return 0;
    return this.#immediate(() => {
      const remove = this.#database.prepare(
        "DELETE FROM fanout_priority_hint WHERE work_key = ?",
      );
      let removed = 0;
      for (const key of keys) removed += remove.run(key).changes;
      return removed;
    });
  }

  indexFanoutDetailMaterial(input: {
    readonly detailFanoutWorkKey: string;
    readonly lineNfcHash: string;
    readonly promptMaterialHash: string;
    readonly sourceWorkKey: string;
    readonly now?: number;
  }): boolean {
    const detailFanoutWorkKey = Sha256Schema.parse(input.detailFanoutWorkKey);
    const lineNfcHash = Sha256Schema.parse(input.lineNfcHash);
    const promptMaterialHash = Sha256Schema.parse(input.promptMaterialHash);
    const sourceWorkKey = Sha256Schema.parse(input.sourceWorkKey);
    const now = LedgerTimestampSchema.parse(input.now ?? Date.now());
    return this.#immediate(() => {
      const definition = this.#database
        .prepare<
          [string, string],
          { detail_kind: string; source_kind: string; source_work_key: string }
        >(
          `SELECT detail.kind AS detail_kind, source.kind AS source_kind,
                  json_extract(detail.input_json, '$.source.workKey') AS source_work_key
           FROM work_item AS detail
           JOIN work_item AS source ON source.work_key = ?
           WHERE detail.work_key = ?`,
        )
        .get(sourceWorkKey, detailFanoutWorkKey);
      if (
        definition?.detail_kind !== "fanout-collected-detail" ||
        definition.source_kind !== collectionWorkKinds().poemDetail ||
        definition.source_work_key !== sourceWorkKey
      ) {
        throw new Error("FANOUT_DETAIL_MATERIAL_DEFINITION_INVALID");
      }
      const inserted = this.#database
        .prepare(
          `INSERT INTO fanout_detail_material(
             detail_fanout_work_key, source_work_key, prompt_material_hash,
             line_nfc_hash, created_at
           ) VALUES(?, ?, ?, ?, ?)
           ON CONFLICT(detail_fanout_work_key) DO NOTHING`,
        )
        .run(
          detailFanoutWorkKey,
          sourceWorkKey,
          promptMaterialHash,
          lineNfcHash,
          now,
        );
      if (inserted.changes === 1) return true;
      const existing = this.#database
        .prepare<
          [string],
          {
            line_nfc_hash: string;
            prompt_material_hash: string;
            source_work_key: string;
          }
        >(
          `SELECT source_work_key, prompt_material_hash, line_nfc_hash
           FROM fanout_detail_material WHERE detail_fanout_work_key = ?`,
        )
        .get(detailFanoutWorkKey);
      if (
        existing?.source_work_key !== sourceWorkKey ||
        existing.prompt_material_hash !== promptMaterialHash ||
        existing.line_nfc_hash !== lineNfcHash
      ) {
        throw new Error("FANOUT_DETAIL_MATERIAL_COLLISION");
      }
      return false;
    });
  }

  fanoutDetailMaterialHash(detailFanoutWorkKey: string): null | string {
    const detailWorkKey = Sha256Schema.parse(detailFanoutWorkKey);
    const row = this.#database
      .prepare<[string], { prompt_material_hash: string }>(
        `SELECT prompt_material_hash FROM fanout_detail_material
         WHERE detail_fanout_work_key = ?`,
      )
      .get(detailWorkKey);
    return row?.prompt_material_hash ?? null;
  }

  fanoutDetailWorkKeysForMaterial(
    promptMaterialHash: string,
  ): readonly string[] {
    const hash = Sha256Schema.parse(promptMaterialHash);
    return this.#database
      .prepare<[string], { detail_fanout_work_key: string }>(
        `SELECT detail_fanout_work_key FROM fanout_detail_material
         WHERE prompt_material_hash = ? ORDER BY detail_fanout_work_key`,
      )
      .all(hash)
      .map(({ detail_fanout_work_key }) => detail_fanout_work_key);
  }

  indexReusableEnrichment(input: {
    readonly fanoutWorkKey: string;
    readonly modelKey: string;
    readonly outputArtifactHash: string;
    readonly promptMaterialHash: string;
    readonly translationWorkKey: string;
    readonly now?: number;
  }): boolean {
    const fanoutWorkKey = Sha256Schema.parse(input.fanoutWorkKey);
    const outputArtifactHash = Sha256Schema.parse(input.outputArtifactHash);
    const promptMaterialHash = Sha256Schema.parse(input.promptMaterialHash);
    const translationWorkKey = Sha256Schema.parse(input.translationWorkKey);
    const modelKey = LedgerModelKeySchema.parse(input.modelKey);
    const now = LedgerTimestampSchema.parse(input.now ?? Date.now());
    return this.#immediate(() => {
      const translation = this.#database
        .prepare<
          string,
          {
            has_binding: number;
            has_derivation: number;
            output_artifact_hash: null | string;
            state: string;
          }
        >(
          `SELECT translation.state, translation.output_artifact_hash,
                  EXISTS(
                    SELECT 1 FROM canonical_translation_binding AS binding
                    WHERE binding.translation_work_key = translation.work_key
                  ) AS has_binding,
                  EXISTS(
                    SELECT 1 FROM publication_derivation AS derivation
                    WHERE derivation.translation_work_key = translation.work_key
                      AND derivation.approved_artifact_hash = translation.output_artifact_hash
                  ) AS has_derivation
           FROM work_item AS translation WHERE translation.work_key = ?`,
        )
        .get(translationWorkKey);
      if (
        translation?.output_artifact_hash !== outputArtifactHash ||
        (translation.state !== "succeeded" &&
          !(
            translation.state === "imported" &&
            translation.has_binding === 1 &&
            translation.has_derivation === 1
          ))
      )
        return false;
      const inserted = this.#database
        .prepare(
          `INSERT INTO fanout_reusable_enrichment(
             translation_work_key, fanout_work_key, model_key,
             prompt_material_hash, output_artifact_hash, created_at
           ) VALUES(?, ?, ?, ?, ?, ?)
           ON CONFLICT(translation_work_key) DO NOTHING`,
        )
        .run(
          translationWorkKey,
          fanoutWorkKey,
          modelKey,
          promptMaterialHash,
          outputArtifactHash,
          now,
        );
      if (inserted.changes === 1) return true;
      const existing = this.#database
        .prepare<
          [string],
          {
            fanout_work_key: string;
            model_key: string;
            output_artifact_hash: string;
            prompt_material_hash: string;
          }
        >(
          `SELECT fanout_work_key, model_key, prompt_material_hash,
                  output_artifact_hash
           FROM fanout_reusable_enrichment WHERE translation_work_key = ?`,
        )
        .get(translationWorkKey);
      if (
        existing?.fanout_work_key !== fanoutWorkKey ||
        existing.model_key !== modelKey ||
        existing.prompt_material_hash !== promptMaterialHash ||
        existing.output_artifact_hash !== outputArtifactHash
      ) {
        throw new Error("FANOUT_REUSABLE_ENRICHMENT_COLLISION");
      }
      return false;
    });
  }

  reusableEnrichmentState(
    modelKey: string,
    promptMaterialHash: string,
  ): "pending" | "succeeded" | "terminal" | null {
    const model = LedgerModelKeySchema.parse(modelKey);
    const hash = Sha256Schema.parse(promptMaterialHash);
    const rows = this.#database
      .prepare<[string, string], { fanout_state: WorkRow["state"] }>(
        `SELECT fanout.state AS fanout_state
         FROM fanout_reusable_enrichment AS reusable
         JOIN work_item AS translation
           ON translation.work_key = reusable.translation_work_key
          AND (
            translation.state = 'succeeded'
            OR (
              translation.state = 'imported'
              AND EXISTS(
                SELECT 1 FROM canonical_translation_binding AS binding
                WHERE binding.translation_work_key = translation.work_key
              )
              AND EXISTS(
                SELECT 1 FROM publication_derivation AS derivation
                WHERE derivation.translation_work_key = translation.work_key
                  AND derivation.approved_artifact_hash = translation.output_artifact_hash
              )
            )
          )
          AND translation.output_artifact_hash = reusable.output_artifact_hash
         JOIN work_item AS fanout ON fanout.work_key = reusable.fanout_work_key
         WHERE reusable.model_key = ? AND reusable.prompt_material_hash = ?`,
      )
      .all(model, hash);
    if (rows.some(({ fanout_state }) => fanout_state === "succeeded"))
      return "succeeded";
    if (
      rows.some(({ fanout_state }) =>
        ["pending", "retry_wait", "quota_wait", "running"].includes(
          fanout_state,
        ),
      )
    )
      return "pending";
    return rows.length > 0 ? "terminal" : null;
  }

  reusableEnrichmentForMaterial(
    modelKey: string,
    promptMaterialHash: string,
  ): {
    readonly fanoutState: WorkItem["state"];
    readonly fanoutWorkKey: string;
    readonly outputArtifactHash: string;
    readonly translationWorkKey: string;
  } | null {
    const model = LedgerModelKeySchema.parse(modelKey);
    const hash = Sha256Schema.parse(promptMaterialHash);
    const row = this.#database
      .prepare<
        [string, string],
        {
          fanout_state: WorkRow["state"];
          fanout_work_key: string;
          output_artifact_hash: string;
          translation_work_key: string;
        }
      >(
        `SELECT reusable.translation_work_key, reusable.fanout_work_key,
                reusable.output_artifact_hash, fanout.state AS fanout_state
         FROM fanout_reusable_enrichment AS reusable
         JOIN work_item AS translation
           ON translation.work_key = reusable.translation_work_key
          AND (
            translation.state = 'succeeded'
            OR (
              translation.state = 'imported'
              AND EXISTS(
                SELECT 1 FROM canonical_translation_binding AS binding
                WHERE binding.translation_work_key = translation.work_key
              )
              AND EXISTS(
                SELECT 1 FROM publication_derivation AS derivation
                WHERE derivation.translation_work_key = translation.work_key
                  AND derivation.approved_artifact_hash = translation.output_artifact_hash
              )
            )
          )
          AND translation.output_artifact_hash = reusable.output_artifact_hash
         JOIN work_item AS fanout ON fanout.work_key = reusable.fanout_work_key
         WHERE reusable.model_key = ? AND reusable.prompt_material_hash = ?
         ORDER BY CASE fanout.state
           WHEN 'succeeded' THEN 0
           WHEN 'running' THEN 1
           WHEN 'pending' THEN 2
           WHEN 'retry_wait' THEN 3
           WHEN 'quota_wait' THEN 4
           ELSE 5 END,
           reusable.translation_work_key
         LIMIT 1`,
      )
      .get(model, hash);
    return row
      ? {
          fanoutState: WorkStateSchema.parse(row.fanout_state),
          fanoutWorkKey: row.fanout_work_key,
          outputArtifactHash: row.output_artifact_hash,
          translationWorkKey: row.translation_work_key,
        }
      : null;
  }

  /** Makes exact, already-admitted local work immediately claimable without
   * changing its state or erasing the diagnostic that put it to sleep.
   *
   * This is intentionally key-addressed and bounded: callers may accelerate a
   * known dependency, but cannot sweep or reorder an unrelated queue. */
  expediteReadyWork(
    workKeys: readonly string[],
    kinds: readonly string[],
    priority: number,
    reason: string,
    now = Date.now(),
  ): number {
    if (workKeys.length > 500)
      throw new Error("Expedite batch must not exceed 500 work keys");
    if (kinds.length === 0 || kinds.some((kind) => kind.trim().length === 0))
      throw new Error("Expedite kinds are required");
    const parsedPriority = LedgerPrioritySchema.parse(priority);
    requireErrorCode(reason);
    if (!Number.isSafeInteger(now) || now < 0)
      throw new Error("Expedite timestamp must be a nonnegative integer");
    const keys = [...new Set(workKeys)];
    for (const key of keys) {
      if (!/^[a-f\d]{64}$/.test(key))
        throw new Error("Invalid expedite work key");
    }
    if (keys.length === 0) return 0;
    const allowedKinds = new Set(kinds);
    return this.#immediate(() => {
      const select = this.#database.prepare<
        [string],
        Pick<
          WorkRow,
          "available_at" | "kind" | "last_error_code" | "priority" | "state"
        >
      >(
        `SELECT kind, state, priority, available_at, last_error_code
         FROM work_item WHERE work_key = ?`,
      );
      const update = this.#database.prepare(
        `UPDATE work_item
         SET priority = max(priority, ?), available_at = min(available_at, ?),
             updated_at = ?
         WHERE work_key = ?
           AND state IN ('pending','retry_wait','quota_wait')
           AND (priority < ? OR available_at > ?)`,
      );
      let changed = 0;
      for (const key of keys) {
        const row = select.get(key);
        if (!row || !allowedKinds.has(row.kind)) continue;
        const result = update.run(
          parsedPriority,
          now,
          now,
          key,
          parsedPriority,
          now,
        );
        if (result.changes !== 1) continue;
        this.#appendEvent(
          key,
          null,
          null,
          "operator_prioritized",
          {
            previousAvailableAt: row.available_at,
            previousErrorCode: row.last_error_code,
            previousPriority: row.priority,
            reason,
          },
          now,
        );
        changed += 1;
      }
      return changed;
    });
  }

  requeueDeadLetters(
    kinds: readonly string[],
    errorCodes: readonly string[],
    now = Date.now(),
  ): number {
    if (kinds.length === 0 || errorCodes.length === 0)
      throw new Error("Requeue kinds and error codes are required");
    for (const code of errorCodes) requireErrorCode(code);
    return this.#immediate(() => {
      const rows = this.#database
        .prepare<string[], { work_key: string }>(
          `SELECT work_key FROM work_item
           WHERE state = 'dead_letter'
             AND kind IN (${kinds.map(() => "?").join(",")})
             AND last_error_code IN (${errorCodes.map(() => "?").join(",")})
           ORDER BY work_key`,
        )
        .all(...kinds, ...errorCodes);
      const update = this.#database.prepare(REQUEUE_DEAD_LETTER_SQL);
      for (const row of rows) {
        const changed = update.run(now, now, row.work_key);
        if (changed.changes !== 1)
          throw new Error("Dead-letter requeue lost its transaction");
        this.#appendEvent(
          row.work_key,
          null,
          null,
          "operator_requeued",
          { errorCodes, kinds },
          now,
        );
      }
      return rows.length;
    });
  }

  /** Reopens only completed Sol fanout whose exact immutable source artifact
   * was rejected by the legacy generic publication lane. This is deliberately
   * narrower than a general succeeded-work requeue: canonical rebinding can
   * repair these historical publications, while replaying arbitrary succeeded
   * work would violate terminal-work accounting. */
  reopenRejectedLegacySolFanout(
    workKeys: readonly string[],
    now = Date.now(),
  ): boolean {
    const keys = [...new Set(workKeys.map((key) => Sha256Schema.parse(key)))];
    if (keys.length === 0 || keys.length > 100)
      throw new Error("Legacy Sol fanout recovery requires 1 to 100 work keys");
    if (keys.length !== workKeys.length)
      throw new Error(
        "Legacy Sol fanout recovery contains duplicate work keys",
      );
    const updatedAt = LedgerTimestampSchema.parse(now);
    const allowedKinds = new Set([
      "fanout-succeeded-enrichment",
      "fanout-succeeded-sol",
    ]);

    return this.#immediate(() => {
      const select = this.#database.prepare(
        `SELECT fanout.state, fanout.kind, fanout.output_artifact_hash,
                json_extract(fanout.input_json, '$.modelKey') AS model_key,
                source.kind AS source_kind, source.state AS source_state,
                EXISTS(
                  SELECT 1 FROM work_item publication
                   WHERE publication.kind = 'corpus-publication-enrichment'
                     AND publication.state = 'dead_letter'
                     AND publication.last_error_code = 'CORPUS_IMPORT_REJECTED'
                     AND json_extract(publication.input_json, '$.sources[0].workKey') =
                         json_extract(fanout.input_json, '$.source.workKey')
                     AND json_extract(publication.input_json, '$.sources[0].artifactHash') =
                         json_extract(fanout.input_json, '$.source.artifactHash')
                ) AS rejected,
                EXISTS(
                  SELECT 1 FROM canonical_translation_binding binding
                   WHERE binding.translation_work_key = source.work_key
                ) AS bound,
                EXISTS(
                  SELECT 1 FROM publication_derivation derivation
                   WHERE derivation.translation_work_key = source.work_key
                ) AS derivation
           FROM work_item fanout
           LEFT JOIN work_item source
             ON source.work_key = json_extract(fanout.input_json, '$.source.workKey')
          WHERE fanout.work_key = ?`,
      );
      const rows = keys.map((key) => ({
        key,
        row: queryOptional(
          { operation: "ledger.legacySolFanoutRecovery" },
          () => select.get(key),
          LegacySolFanoutRecoveryRowSchema,
        ),
      }));
      if (
        rows.some(
          ({ row }) =>
            row?.state !== "succeeded" ||
            !allowedKinds.has(row.kind) ||
            row.modelKey !== "sol-5.6" ||
            row.outputArtifactHash === null ||
            row.sourceKind !== "poem-enrichment-sol" ||
            row.sourceState !== "succeeded" ||
            !row.rejected ||
            row.bound ||
            row.derivation,
        )
      )
        return false;

      const reopen = this.#database.prepare(
        `UPDATE work_item
            SET state = 'pending', available_at = ?, output_artifact_hash = NULL,
                last_error_code = 'FANOUT_LEGACY_REBIND_REQUIRED', updated_at = ?
          WHERE work_key = ? AND state = 'succeeded'
            AND output_artifact_hash = ?`,
      );
      for (const { key, row } of rows) {
        if (!row?.outputArtifactHash)
          throw new Error("Legacy Sol fanout recovery evidence disappeared");
        if (
          reopen.run(updatedAt, updatedAt, key, row.outputArtifactHash)
            .changes !== 1
        )
          throw new Error("Legacy Sol fanout recovery lost its transaction");
        this.#appendEvent(
          key,
          null,
          null,
          "operator_reopened",
          {
            previousArtifactHash: row.outputArtifactHash,
            reason: "legacy_sol_publication_rejected",
          },
          updatedAt,
        );
      }
      return true;
    });
  }

  listRecoverableDeadLetters(
    filter: DeadLetterCohortFilter,
    limit: number,
  ): readonly string[] {
    validateClaim("collector-recovery", 1, [filter.kind], {
      implementationVersion: filter.implementationVersion,
      schemaVersion: filter.schemaVersion,
    });
    if (filter.errorCodes.length === 0)
      throw new Error("Recovery error codes are required");
    for (const code of filter.errorCodes) requireErrorCode(code);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100)
      throw new Error("Recovery cohort limit must be between 1 and 100");
    return this.#database
      .prepare<(number | string)[], { work_key: string }>(
        `SELECT work_key FROM work_item
         WHERE state = 'dead_letter' AND kind = ?
           AND implementation_version = ? AND schema_version = ?
           AND last_error_code IN (${filter.errorCodes.map(() => "?").join(",")})
         ORDER BY priority DESC, created_at, work_key LIMIT ?`,
      )
      .all(
        filter.kind,
        filter.implementationVersion,
        filter.schemaVersion,
        ...filter.errorCodes,
        limit,
      )
      .map(({ work_key: workKeyValue }) => workKeyValue);
  }

  /** Atomically fences a scheduler reservation and releases only its exact
   * eligible dead letters into the existing worker queue. */
  reserveDeadLetterCohort(
    reservation: ReservedDeadLetterCohort,
    now = Date.now(),
  ): boolean {
    const stateKey = LedgerStateKeySchema.parse(reservation.schedulerStateKey);
    const serialized = SchedulerSerializedStateSchema.parse(
      reservation.schedulerStateSerialized,
    );
    const digest = Sha256Schema.parse(reservation.schedulerStateDigest);
    const expectedDigest = NullableSha256Schema.parse(
      reservation.expectedStateDigest,
    );
    const reservationId = LedgerStateKeySchema.parse(reservation.reservationId);
    const workKeys = [
      ...new Set(
        reservation.workKeys.map((workKeyValue) =>
          Sha256Schema.parse(workKeyValue),
        ),
      ),
    ];
    if (workKeys.length === 0 || workKeys.length > 100)
      throw new Error(
        "Reserved recovery cohort must contain 1 to 100 work keys",
      );
    if (workKeys.length !== reservation.workKeys.length)
      throw new Error("Reserved recovery cohort contains duplicate work keys");
    validateClaim("collector-recovery", 1, [reservation.kind], {
      implementationVersion: reservation.implementationVersion,
      schemaVersion: reservation.schemaVersion,
    });
    if (reservation.errorCodes.length === 0)
      throw new Error("Recovery error codes are required");
    for (const code of reservation.errorCodes) requireErrorCode(code);
    const updatedAt = LedgerTimestampSchema.parse(now);

    return this.#immediate(() => {
      const select = this.#database.prepare<
        [string],
        Pick<
          WorkRow,
          | "implementation_version"
          | "kind"
          | "last_error_code"
          | "schema_version"
          | "state"
        >
      >(
        `SELECT state, kind, implementation_version, schema_version,
                last_error_code
         FROM work_item WHERE work_key = ?`,
      );
      const allowedErrors = new Set(reservation.errorCodes);
      const rows = workKeys.map((workKeyValue) => ({
        row: select.get(workKeyValue),
        workKey: workKeyValue,
      }));
      if (
        rows.some(
          ({ row }) =>
            row?.state !== "dead_letter" ||
            row.kind !== reservation.kind ||
            row.implementation_version !== reservation.implementationVersion ||
            row.schema_version !== reservation.schemaVersion ||
            row.last_error_code === null ||
            !allowedErrors.has(row.last_error_code),
        )
      )
        return false;

      const stateChanged =
        expectedDigest === null
          ? this.#database
              .prepare(
                `INSERT INTO scheduler_state(
                   state_key, state_json, state_digest, updated_at
                 ) VALUES(?, ?, ?, ?)
                 ON CONFLICT(state_key) DO NOTHING`,
              )
              .run(stateKey, serialized, digest, updatedAt).changes
          : this.#database
              .prepare(
                `UPDATE scheduler_state
                 SET state_json = ?, state_digest = ?, updated_at = ?
                 WHERE state_key = ? AND state_digest = ?`,
              )
              .run(serialized, digest, updatedAt, stateKey, expectedDigest)
              .changes;
      if (stateChanged !== 1) return false;

      const release = this.#database.prepare(REQUEUE_DEAD_LETTER_SQL);
      for (const { row, workKey: reservedWorkKey } of rows) {
        if (!row?.last_error_code)
          throw new Error("Reserved recovery error code disappeared");
        if (release.run(updatedAt, updatedAt, reservedWorkKey).changes !== 1)
          throw new Error("Reserved recovery release lost its transaction");
        this.#appendEvent(
          reservedWorkKey,
          null,
          null,
          "operator_requeued",
          { previousErrorCode: row.last_error_code, reservationId },
          updatedAt,
        );
      }
      return true;
    });
  }

  markImported(
    key: string,
    expectedArtifactHash: string,
    now = Date.now(),
  ): ImportResult {
    if (!/^[a-f\d]{64}$/.test(expectedArtifactHash)) {
      throw new Error("Invalid artifact hash");
    }
    return this.#immediate(() => {
      const row = this.#database
        .prepare<
          [string],
          { output_artifact_hash: null | string; state: string }
        >(
          "SELECT state, output_artifact_hash FROM work_item WHERE work_key = ?",
        )
        .get(key);
      if (
        row?.output_artifact_hash !== expectedArtifactHash ||
        (row.state !== "succeeded" && row.state !== "imported")
      ) {
        throw new Error("Work is not the expected succeeded revision");
      }
      if (row.state === "imported") return "already_imported";

      const changed = this.#database
        .prepare(
          `UPDATE work_item SET state = 'imported', updated_at = ?
           WHERE work_key = ? AND state = 'succeeded' AND output_artifact_hash = ?`,
        )
        .run(now, key, expectedArtifactHash);
      if (changed.changes !== 1) {
        throw new Error("Work import transition lost its transaction");
      }
      this.#appendEvent(
        key,
        null,
        null,
        "imported",
        { artifactHash: expectedArtifactHash },
        now,
      );
      return "imported";
    });
  }

  recoverExpired(now = Date.now()): number {
    return this.#immediate(() => {
      const rows = queryMany(
        { operation: "ledger.recoverExpired" },
        () =>
          this.#database
            .prepare(
              `SELECT work_item.work_key AS workKey, work_item.lease_epoch AS leaseEpoch,
                  claimed.attempt_id AS attemptId,
                  CASE WHEN work_item.kind = 'poem-enrichment-sol' AND EXISTS (
                    SELECT 1 FROM sol_paid_usage_reservation AS reservation
                    WHERE reservation.work_key = work_item.work_key
                      AND reservation.attempt_id = claimed.attempt_id
                  ) THEN 1 ELSE 0 END AS reservedSolAttempt
           FROM work_item
           LEFT JOIN work_event AS claimed
             ON claimed.sequence = (
               SELECT MAX(candidate.sequence)
               FROM work_event AS candidate
               WHERE candidate.work_key = work_item.work_key
                 AND candidate.lease_epoch = work_item.lease_epoch
                 AND candidate.event_type = 'claimed'
             )
           WHERE work_item.state = 'running'
             AND work_item.lease_expires_at <= ?
           ORDER BY work_item.work_key`,
            )
            .all(now),
        ExpiredWorkRowSchema,
      );
      for (const row of rows) {
        const changed = this.#database
          .prepare(
            `UPDATE work_item SET state = 'pending', available_at = ?, lease_owner = NULL,
               lease_token = NULL, lease_expires_at = NULL,
               last_error_code = CASE
                 WHEN last_error_code = 'CODEX_OPERATION_OUTCOME_UNKNOWN' OR ? = 1
                   THEN 'CODEX_OPERATION_OUTCOME_UNKNOWN'
                 ELSE 'LEASE_EXPIRED'
               END,
               updated_at = ?
             WHERE work_key = ? AND state = 'running' AND lease_epoch = ? AND lease_expires_at <= ?`,
          )
          .run(
            now,
            row.reservedSolAttempt,
            now,
            row.workKey,
            row.leaseEpoch,
            now,
          );
        if (changed.changes !== 1) {
          throw new LostLeaseError("Expired lease recovery lost its fence");
        }
        this.#appendEvent(
          row.workKey,
          row.attemptId,
          row.leaseEpoch,
          "lease_expired",
          {},
          now,
        );
      }
      this.#database
        .prepare(
          "UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL, updated_at = ? WHERE lease_expires_at <= ?",
        )
        .run(now, now);
      return rows.length;
    });
  }

  claimOrigin(
    origin: string,
    now: number,
    leaseDurationMs: number,
  ): OriginClaimResult {
    requireOrigin(origin);
    requirePositiveDuration(leaseDurationMs);
    return this.#immediate(() => {
      this.#database
        .prepare(
          `INSERT INTO origin_gate(origin, updated_at) VALUES(?, ?)
           ON CONFLICT(origin) DO NOTHING`,
        )
        .run(origin, now);
      this.#database
        .prepare(
          `UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL, updated_at = ?
           WHERE origin = ? AND lease_expires_at <= ?`,
        )
        .run(now, origin, now);
      const gate = this.#database
        .prepare<
          [string],
          {
            active_token: null | string;
            cooldown_until: number;
            lease_epoch: number;
            lease_expires_at: null | number;
            next_allowed_at: number;
            stop_reason: null | string;
          }
        >(
          `SELECT active_token, cooldown_until, lease_epoch, lease_expires_at,
                  next_allowed_at, stop_reason
           FROM origin_gate WHERE origin = ?`,
        )
        .get(origin);
      if (!gate) throw new Error("Origin gate disappeared");
      if (gate.active_token) {
        return { retryAt: gate.lease_expires_at ?? now, state: "busy" };
      }
      if (gate.stop_reason) {
        const retryAt = Math.max(gate.next_allowed_at, gate.cooldown_until);
        if (!isRetryableOriginStopReason(gate.stop_reason)) {
          return { reason: gate.stop_reason, state: "stopped" };
        }
        if (retryAt > now) return { retryAt, state: "waiting" };
        const released = this.#database
          .prepare(RELEASE_RETRYABLE_ORIGIN_STOP_SQL)
          .run(now, origin, gate.stop_reason, now, now);
        if (released.changes !== 1) {
          return { reason: gate.stop_reason, state: "stopped" };
        }
      }
      const retryAt = Math.max(gate.next_allowed_at, gate.cooldown_until);
      if (retryAt > now) {
        return { retryAt, state: "waiting" };
      }
      const token = randomUUID();
      const epoch = gate.lease_epoch + 1;
      const changed = this.#database
        .prepare(
          `UPDATE origin_gate SET active_token = ?, lease_epoch = ?, lease_expires_at = ?, updated_at = ?
           WHERE origin = ? AND active_token IS NULL AND lease_epoch = ?`,
        )
        .run(
          token,
          epoch,
          now + leaseDurationMs,
          now,
          origin,
          gate.lease_epoch,
        );
      if (changed.changes !== 1) {
        throw new LostLeaseError("Origin claim lost its fence");
      }
      return { lease: { leaseEpoch: epoch, origin, token }, state: "claimed" };
    });
  }

  completeOrigin(
    lease: OriginLease,
    completedAt: number,
    minimumGapMs: number,
  ): void {
    if (minimumGapMs < 0) throw new Error("Origin gap cannot be negative");
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL,
           last_completed_at = ?, next_allowed_at = MAX(next_allowed_at, ?),
           cooldown_until = 0, consecutive_failures = 0, stop_reason = NULL,
           updated_at = ?
         WHERE origin = ? AND active_token = ? AND lease_epoch = ?
           AND lease_expires_at > ?`,
      )
      .run(
        completedAt,
        completedAt + minimumGapMs,
        completedAt,
        lease.origin,
        lease.token,
        lease.leaseEpoch,
        completedAt,
      );
    if (changed.changes !== 1)
      throw new LostLeaseError("Origin lease is no longer current");
  }

  releaseOrigin(lease: OriginLease, now = Date.now()): void {
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL,
           updated_at = ?
         WHERE origin = ? AND active_token = ? AND lease_epoch = ?
           AND lease_expires_at > ?`,
      )
      .run(now, lease.origin, lease.token, lease.leaseEpoch, now);
    if (changed.changes !== 1)
      throw new LostLeaseError("Origin lease is no longer current");
  }

  /** Durably gates an origin until a transient condition can be probed again,
   * without recording the condition as a source failure. */
  deferOrigin(
    lease: OriginLease,
    now: number,
    retryAt: number,
    reason: string,
  ): void {
    if (!Number.isSafeInteger(retryAt) || retryAt < now)
      throw new Error("Origin retryAt must be a future integer timestamp");
    if (!isRetryableOriginStopReason(reason))
      throw new Error("Origin deferral reason must be retryable");
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL,
           next_allowed_at = MAX(next_allowed_at, ?), stop_reason = ?, updated_at = ?
         WHERE origin = ? AND active_token = ? AND lease_epoch = ?
           AND lease_expires_at > ?`,
      )
      .run(
        retryAt,
        reason,
        now,
        lease.origin,
        lease.token,
        lease.leaseEpoch,
        now,
      );
    if (changed.changes !== 1)
      throw new LostLeaseError("Origin lease is no longer current");
  }

  failOrigin(
    lease: OriginLease,
    completedAt: number,
    minimumGapMs: number,
    policy: OriginFailurePolicy,
  ): OriginFailureResult {
    if (minimumGapMs < 0) throw new Error("Origin gap cannot be negative");
    requirePositiveDuration(policy.circuitBreakerAfter);
    requirePositiveDuration(policy.circuitBreakerCooldownMs);
    if (!Number.isSafeInteger(policy.retryAt) || policy.retryAt < completedAt) {
      throw new Error("Origin retryAt must be a future integer timestamp");
    }
    if (policy.stopReason?.trim().length === 0) {
      throw new Error("Origin stop reason cannot be empty");
    }
    return this.#immediate(() => {
      const gate = this.#database
        .prepare<
          [string, string, number, number],
          { consecutive_failures: number }
        >(
          `SELECT consecutive_failures FROM origin_gate
           WHERE origin = ? AND active_token = ? AND lease_epoch = ?
             AND lease_expires_at > ?`,
        )
        .get(lease.origin, lease.token, lease.leaseEpoch, completedAt);
      if (!gate) throw new LostLeaseError("Origin lease is no longer current");
      const consecutiveFailures = gate.consecutive_failures + 1;
      const circuitUntil =
        consecutiveFailures >= policy.circuitBreakerAfter
          ? completedAt + policy.circuitBreakerCooldownMs
          : completedAt;
      const nextAllowedAt = Math.max(
        completedAt + minimumGapMs,
        policy.retryAt,
        circuitUntil,
      );
      const changed = this.#database
        .prepare(
          `UPDATE origin_gate SET active_token = NULL, lease_expires_at = NULL,
             last_completed_at = ?, next_allowed_at = MAX(next_allowed_at, ?),
             cooldown_until = MAX(cooldown_until, ?), consecutive_failures = ?,
             stop_reason = ?, updated_at = ?
           WHERE origin = ? AND active_token = ? AND lease_epoch = ?
             AND lease_expires_at > ?`,
        )
        .run(
          completedAt,
          completedAt + minimumGapMs,
          nextAllowedAt,
          consecutiveFailures,
          policy.stopReason ?? null,
          completedAt,
          lease.origin,
          lease.token,
          lease.leaseEpoch,
          completedAt,
        );
      if (changed.changes !== 1) {
        throw new LostLeaseError("Origin lease is no longer current");
      }
      return {
        consecutiveFailures,
        nextAllowedAt,
        stopped: policy.stopReason !== undefined,
      };
    });
  }

  originConsecutiveFailures(origin: string): number {
    requireOrigin(origin);
    const row = this.#database
      .prepare<[string], { consecutive_failures: number }>(
        "SELECT consecutive_failures FROM origin_gate WHERE origin = ?",
      )
      .get(origin);
    return row?.consecutive_failures ?? 0;
  }

  originStopReason(origin: string): null | string {
    requireOrigin(origin);
    const row = this.#database
      .prepare<[string], { stop_reason: null | string }>(
        "SELECT stop_reason FROM origin_gate WHERE origin = ?",
      )
      .get(origin);
    return row?.stop_reason ?? null;
  }

  clearOriginStop(origin: string, now = Date.now()): boolean {
    requireOrigin(origin);
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET stop_reason = NULL, cooldown_until = 0,
           consecutive_failures = 0, next_allowed_at = MIN(next_allowed_at, ?),
           updated_at = ?
         WHERE origin = ? AND stop_reason IS NOT NULL AND active_token IS NULL`,
      )
      .run(now, now, origin);
    return changed.changes === 1;
  }

  clearOriginFailures(origin: string, now = Date.now()): boolean {
    requireOrigin(origin);
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET stop_reason = NULL, cooldown_until = 0,
           consecutive_failures = 0, next_allowed_at = MIN(next_allowed_at, ?),
           updated_at = ?
         WHERE origin = ? AND active_token IS NULL
           AND (stop_reason IS NOT NULL OR cooldown_until > 0 OR consecutive_failures > 0)`,
      )
      .run(now, now, origin);
    return changed.changes === 1;
  }

  renewOrigin(lease: OriginLease, now: number, leaseDurationMs: number): void {
    requirePositiveDuration(leaseDurationMs);
    const changed = this.#database
      .prepare(
        `UPDATE origin_gate SET lease_expires_at = ?, updated_at = ?
         WHERE origin = ? AND active_token = ? AND lease_epoch = ?
           AND lease_expires_at > ?`,
      )
      .run(
        now + leaseDurationMs,
        now,
        lease.origin,
        lease.token,
        lease.leaseEpoch,
        now,
      );
    if (changed.changes !== 1) {
      throw new LostLeaseError("Origin lease is no longer current");
    }
  }

  /** Reconstructs every materialized diagnostic aggregate under one writer
   * fence. This is an explicit repair operation: ordinary status reads remain
   * independent of corpus size and never perform the reconciliation scans. */
  rebuildStatusCounters(): void {
    this.#immediate(() => {
      this.#database.exec(`
        DELETE FROM ledger_state_count;
        DELETE FROM ledger_kind_state_count;
        DELETE FROM ledger_profile_state_count;
        DELETE FROM ledger_profile_availability_count;
        DELETE FROM ledger_error_count;
        DELETE FROM ledger_kind_error_count;
        DELETE FROM ledger_profile_error_count;
        DELETE FROM ledger_kind_success_clock;
        DELETE FROM ledger_profile_success_clock;
        DELETE FROM paid_operation_state_count;

        INSERT INTO ledger_state_count(state, item_count)
          SELECT state, COUNT(*) FROM work_item GROUP BY state;
        INSERT INTO ledger_kind_state_count(kind, state, item_count)
          SELECT kind, state, COUNT(*) FROM work_item GROUP BY kind, state;
        INSERT INTO ledger_profile_state_count(
          kind, implementation_version, schema_version, state, item_count
        )
          SELECT kind, implementation_version, schema_version, state, COUNT(*)
          FROM work_item
          GROUP BY kind, implementation_version, schema_version, state;
        INSERT INTO ledger_profile_availability_count(
          kind, implementation_version, schema_version, available_at, item_count
        )
          SELECT kind, implementation_version, schema_version, available_at, COUNT(*)
          FROM work_item
          WHERE state IN ('pending','retry_wait','quota_wait')
          GROUP BY kind, implementation_version, schema_version, available_at;
        INSERT INTO ledger_error_count(error_code, item_count)
          SELECT last_error_code, COUNT(*) FROM work_item
          WHERE last_error_code IS NOT NULL GROUP BY last_error_code;
        INSERT INTO ledger_kind_error_count(kind, error_code, item_count)
          SELECT kind, last_error_code, COUNT(*) FROM work_item
          WHERE last_error_code IS NOT NULL GROUP BY kind, last_error_code;
        INSERT INTO ledger_profile_error_count(
          kind, implementation_version, schema_version, error_code, item_count
        )
          SELECT kind, implementation_version, schema_version,
                 last_error_code, COUNT(*)
          FROM work_item WHERE last_error_code IS NOT NULL
          GROUP BY kind, implementation_version, schema_version, last_error_code;
        UPDATE ledger_status_clock SET
          last_success_at = (
            SELECT created_at FROM work_event
            WHERE event_type IN ('succeeded','imported')
            ORDER BY sequence DESC LIMIT 1
          ),
          last_failure_at = (
            SELECT created_at FROM work_event
            WHERE event_type IN ('retry_wait','quota_wait','dead_letter','lease_expired')
            ORDER BY sequence DESC LIMIT 1
          )
        WHERE singleton = 1;
        INSERT INTO ledger_kind_success_clock(kind, last_success_at)
          SELECT work_item.kind, MAX(work_event.created_at)
          FROM work_event
          INNER JOIN work_item ON work_item.work_key = work_event.work_key
          WHERE work_event.event_type IN ('succeeded','imported')
          GROUP BY work_item.kind;
        INSERT INTO ledger_profile_success_clock(
          kind, implementation_version, schema_version, last_success_at
        )
          SELECT work_item.kind, work_item.implementation_version,
                 work_item.schema_version, MAX(work_event.created_at)
          FROM work_event
          INNER JOIN work_item ON work_item.work_key = work_event.work_key
          WHERE work_event.event_type IN ('succeeded','imported')
          GROUP BY work_item.kind, work_item.implementation_version,
                   work_item.schema_version;
        INSERT INTO paid_operation_state_count(state, item_count)
          SELECT state, COUNT(*) FROM paid_operation_reconciliation GROUP BY state;
      `);
    });
  }

  status(now = Date.now()): LedgerStatus {
    const byState = emptyWorkStateCounts();
    const rows = this.#database
      .prepare<[], { count: number; state: string }>(
        "SELECT state, item_count AS count FROM ledger_state_count ORDER BY state",
      )
      .all();
    for (const row of rows)
      byState[WorkStateSchema.parse(row.state)] = row.count;
    // Most queued work is immediately available, so summing the historical
    // `available_at <= now` side grows with the ledger. Derive the same exact
    // value from the materialized ready-state totals and subtract only the
    // usually small future suffix, which is served by
    // ledger_profile_availability_time.
    const unavailable = this.#database
      .prepare<[number], { count: number }>(
        `SELECT COALESCE(SUM(item_count), 0) AS count
         FROM ledger_profile_availability_count WHERE available_at > ?`,
      )
      .get(now);
    const earliest = this.#database
      .prepare<[], { value: null | number }>(
        `SELECT MIN(available_at) AS value
         FROM ledger_profile_availability_count`,
      )
      .get();
    const errorRows = this.#database
      .prepare<[], { code: string; count: number }>(
        `SELECT error_code AS code, item_count AS count FROM ledger_error_count
         ORDER BY item_count DESC, error_code LIMIT 50`,
      )
      .all();
    const kindErrorRows = this.#database
      .prepare<[], { code: string; count: number; kind: string }>(
        `SELECT kind, error_code AS code, item_count AS count
         FROM ledger_kind_error_count
         ORDER BY kind, item_count DESC, error_code LIMIT 5001`,
      )
      .all();
    const failureEventRows = this.#database
      .prepare<[], { code: string; count: number }>(
        `WITH recent AS (
           SELECT event_type, payload_json FROM work_event
           ORDER BY sequence DESC LIMIT 10000
         )
         SELECT json_extract(payload_json, '$.errorCode') AS code,
                COUNT(*) AS count
         FROM recent
         WHERE event_type IN ('retry_wait','quota_wait','dead_letter')
           AND json_type(payload_json, '$.errorCode') = 'text'
         GROUP BY code ORDER BY count DESC, code LIMIT 50`,
      )
      .all();
    const kindRows = this.#database
      .prepare<[], { count: number; kind: string; state: string }>(
        `SELECT kind, state, item_count AS count
         FROM ledger_kind_state_count
         WHERE kind IN (
           SELECT DISTINCT kind FROM ledger_kind_state_count ORDER BY kind LIMIT 101
         )
         ORDER BY kind, state`,
      )
      .all();
    const kindSuccessRows = this.#database
      .prepare<[], { kind: string; last_success_at: number }>(
        `SELECT kind, last_success_at FROM ledger_kind_success_clock
         ORDER BY kind LIMIT 101`,
      )
      .all();
    const lastSuccessByKind = new Map(
      kindSuccessRows.map((row) => [row.kind, row.last_success_at]),
    );
    const progress = new Map<string, Record<WorkState, number>>();
    for (const row of kindRows) {
      const counts = progress.get(row.kind) ?? emptyWorkStateCounts();
      counts[WorkStateSchema.parse(row.state)] = row.count;
      progress.set(row.kind, counts);
    }
    const statusClock = this.#database
      .prepare<
        [],
        { last_failure_at: null | number; last_success_at: null | number }
      >(
        `SELECT last_success_at, last_failure_at FROM ledger_status_clock
         WHERE singleton = 1`,
      )
      .get();
    const origins = this.#database
      .prepare<
        [],
        {
          active: 0 | 1;
          consecutive_failures: number;
          cooldown_until: number;
          last_completed_at: null | number;
          next_allowed_at: number;
          origin: string;
          stop_reason: null | string;
        }
      >(
        `SELECT origin, active_token IS NOT NULL AS active, cooldown_until,
                consecutive_failures, last_completed_at, next_allowed_at,
                stop_reason
         FROM origin_gate ORDER BY origin LIMIT 101`,
      )
      .all();
    if (!unavailable || !earliest || !statusClock)
      throw new Error("Ledger status aggregates missing");
    const readyStateTotal =
      byState.pending + byState.retry_wait + byState.quota_wait;
    const ready = readyStateTotal - unavailable.count;
    if (ready < 0) throw new Error("Ledger status aggregates inconsistent");
    const kindProgress: LedgerStatus["kindProgress"][number][] = [];
    for (const [kind, counts] of progress) {
      if (kindProgress.length >= 100) break;
      const total = Object.values(counts).reduce(
        (sum, count) => sum + count,
        0,
      );
      kindProgress.push({
        byState: counts,
        completed: counts.succeeded + counts.imported,
        kind,
        lastSuccessAt: lastSuccessByKind.get(kind) ?? null,
        terminal: counts.succeeded + counts.imported + counts.dead_letter,
        total,
      });
    }
    return {
      affectedByErrorCode: errorRows,
      affectedByKindAndErrorCode: kindErrorRows.slice(0, 5_000),
      byState,
      earliestWorkAvailableAt: earliest.value,
      failureEventsByCode: failureEventRows,
      failureEventWindow: 10_000,
      kindProgress,
      lastFailureAt: statusClock.last_failure_at,
      lastSuccessAt: statusClock.last_success_at,
      origins: origins.slice(0, 100).map((row) => ({
        active: row.active === 1,
        consecutiveFailures: row.consecutive_failures,
        cooldownUntil: row.cooldown_until,
        lastCompletedAt: row.last_completed_at,
        nextAllowedAt: row.next_allowed_at,
        origin: row.origin,
        stopReason: row.stop_reason,
      })),
      ready,
      schemaVersion: this.#schemaVersion(),
      total: Object.values(byState).reduce((sum, count) => sum + count, 0),
      truncated:
        progress.size > 100 ||
        origins.length > 100 ||
        kindSuccessRows.length > 100 ||
        kindErrorRows.length > 5_000,
    };
  }

  doctor(
    now = Date.now(),
    integrityScope: HealthReport["integrityScope"] = "full",
  ): HealthReport {
    const integrityPragma =
      integrityScope === "full"
        ? "integrity_check"
        : "quick_check('sqlite_schema')";
    const integrity =
      NullableSqlitePragmaValueSchema.parse(
        this.#database.pragma(integrityPragma, { simple: true }),
      ) ?? "unknown";
    const journalMode =
      NullableSqlitePragmaValueSchema.parse(
        this.#database.pragma("journal_mode", { simple: true }),
      ) ?? "unknown";
    const stale = this.#database
      .prepare<[number], { count: number }>(
        "SELECT COUNT(*) AS count FROM work_item WHERE state = 'running' AND lease_expires_at <= ?",
      )
      .get(now);
    const originHealth = this.#database
      .prepare<[number], { cooling: null | number; stopped: null | number }>(
        `SELECT
           SUM(CASE WHEN stop_reason IS NOT NULL THEN 1 ELSE 0 END) AS stopped,
           SUM(CASE WHEN stop_reason IS NULL AND cooldown_until > ? THEN 1 ELSE 0 END) AS cooling
         FROM origin_gate`,
      )
      .get(now);
    if (!stale || !originHealth)
      throw new Error("Ledger health aggregates missing");
    return {
      coolingOrigins: originHealth.cooling ?? 0,
      integrity,
      integrityScope,
      journalMode,
      schemaVersion: this.#schemaVersion(),
      staleRunning: stale.count,
      stoppedOrigins: originHealth.stopped ?? 0,
    };
  }

  eventCount(key: string): number {
    const row = this.#database
      .prepare<[string], { count: number }>(
        "SELECT COUNT(*) AS count FROM work_event WHERE work_key = ?",
      )
      .get(key);
    if (!row) throw new Error("Event count aggregate missing");
    return row.count;
  }

  checkpointCount(key: string): number {
    const row = this.#database
      .prepare<[string], { count: number }>(
        "SELECT COUNT(*) AS count FROM checkpoint WHERE work_key = ?",
      )
      .get(key);
    if (!row) throw new Error("Checkpoint count aggregate missing");
    return row.count;
  }

  referencedArtifactHashes(): readonly string[] {
    const rows = this.#database
      .prepare<[], { hash: string }>(
        `SELECT output_artifact_hash AS hash FROM work_item
         WHERE output_artifact_hash IS NOT NULL
         UNION
         SELECT artifact_hash AS hash FROM checkpoint
         WHERE artifact_hash IS NOT NULL
         ORDER BY hash`,
      )
      .all();
    return rows.map(({ hash }) => hash);
  }

  latestCheckpoint(key: string, kind: string): null | StoredCheckpoint {
    if (kind.trim().length === 0)
      throw new Error("Checkpoint kind is required");
    const row = this.#database
      .prepare<
        [string, string],
        {
          artifact_hash: null | string;
          attempt_id: string;
          created_at: number;
          kind: string;
          lease_epoch: number;
          payload_json: string;
          sequence: number;
          work_key: string;
        }
      >(
        `SELECT sequence, work_key, attempt_id, lease_epoch, kind,
                payload_json, artifact_hash, created_at
         FROM checkpoint WHERE work_key = ? AND kind = ?
         ORDER BY sequence DESC LIMIT 1`,
      )
      .get(key, kind);
    if (!row) return null;
    return {
      artifactHash: row.artifact_hash,
      attemptId: row.attempt_id,
      createdAt: row.created_at,
      kind: row.kind,
      leaseEpoch: row.lease_epoch,
      payload: JsonRecordSchema.parse(JSON.parse(row.payload_json)),
      sequence: row.sequence,
      workKey: row.work_key,
    };
  }

  #poemThroughput(
    requirements: Required<ClaimRequirements>,
    now: number,
  ): SolPoemThroughput {
    const readWindow = (
      milestone: "generated" | "published",
    ): PoemMilestoneWindow => {
      const profile = [
        requirements.implementationVersion,
        requirements.schemaVersion,
        milestone,
      ] as const;
      return queryRequired(
        { operation: `ledger.poemThroughput.${milestone}` },
        () =>
          this.#database
            .prepare(
              `SELECT
                  (SELECT COUNT(*) FROM sol_poem_milestone
                    WHERE implementation_version = ? AND schema_version = ? AND milestone = ?
                      AND completed_at > ? AND completed_at <= ?) AS last_5m,
                  (SELECT COUNT(*) FROM sol_poem_milestone
                    WHERE implementation_version = ? AND schema_version = ? AND milestone = ?
                      AND completed_at > ? AND completed_at <= ?) AS last_15m,
                  (SELECT COUNT(*) FROM sol_poem_milestone
                    WHERE implementation_version = ? AND schema_version = ? AND milestone = ?
                      AND completed_at > ? AND completed_at <= ?) AS last_1h,
                  (SELECT completed_at FROM sol_poem_milestone
                    WHERE implementation_version = ? AND schema_version = ? AND milestone = ?
                      AND completed_at <= ?
                    ORDER BY completed_at DESC, work_key DESC LIMIT 1) AS last_at`,
            )
            .get(
              ...profile,
              now - 5 * 60_000,
              now,
              ...profile,
              now - 15 * 60_000,
              now,
              ...profile,
              now - 60 * 60_000,
              now,
              ...profile,
              now,
            ),
        SolMilestoneWindowRowSchema,
      );
    };
    const generated = readWindow("generated");
    const published = readWindow("published");
    const coverageRows = queryMany(
      { operation: "ledger.poemThroughput.coverage" },
      () =>
        this.#database
          .prepare(
            `SELECT cursor_sequence, high_watermark, completed_at
                 FROM sol_poem_milestone_backfill ORDER BY event_type`,
          )
          .all(),
      SolMilestoneBackfillRowSchema,
    );
    if (coverageRows.length !== 2)
      throw new Error("SOL_POEM_MILESTONE_COVERAGE_MISSING");
    const progress = this.profileProgress("poem-enrichment-sol", requirements);
    const availability = this.availability(
      ["poem-enrichment-sol"],
      now,
      requirements,
    );
    const queued =
      progress.byState.pending +
      progress.byState.retry_wait +
      progress.byState.quota_wait;
    const active = progress.byState.running;
    const generatedAwaitingPublication = progress.byState.succeeded;
    return {
      coverage: {
        backfillComplete: coverageRows.every(
          ({ completedAt }) => completedAt !== null,
        ),
        highWatermark: Math.max(
          ...coverageRows.map(({ highWatermark }) => highWatermark),
        ),
      },
      generated,
      published,
      remaining: {
        active,
        delayed: Math.max(0, queued - availability.ready),
        endToEndPublication: queued + active + generatedAwaitingPublication,
        generatedAwaitingPublication,
        generation: queued + active,
        ready: availability.ready,
        terminalDead: progress.byState.dead_letter,
      },
    };
  }

  #preserveStartedSolProfile(
    definition: WorkDefinition,
    now: number,
  ): WorkDefinition {
    if (
      definition.kind !== "poem-enrichment-sol" ||
      definition.implementationVersion !== "sol-word-gloss-v3"
    )
      return definition;
    const legacyDefinition = {
      ...definition,
      implementationVersion: "sol-word-gloss-v2",
    };
    const legacy = this.get(workKey(legacyDefinition));
    // Profile upgrades must not replay paid work or bypass ambiguous-operation
    // fences. This applies equally to baseline planning and local fanout.
    const preserve =
      legacy &&
      ((legacy.state === "dead_letter" &&
        (legacy.lastErrorCode === "CODEX_OPERATION_OUTCOME_UNKNOWN" ||
          legacy.lastErrorCode === "CODEX_OPERATION_UNRESOLVED_QUARANTINED")) ||
        ((legacy.state === "succeeded" || legacy.state === "imported") &&
          legacy.outputArtifactHash !== null) ||
        (legacy.attemptCount > 0 &&
          (legacy.state === "running" ||
            legacy.state === "pending" ||
            legacy.state === "retry_wait" ||
            legacy.state === "quota_wait")));
    if (!preserve) return definition;
    const duplicateKey = workKey(definition);
    const retired = this.#database
      .prepare(RETIRE_UNCLAIMED_SQL)
      .run(now, now, duplicateKey);
    if (retired.changes === 1)
      this.#appendEvent(
        duplicateKey,
        null,
        null,
        "version_retired",
        {
          reason: "SOL_EXISTING_IMMUTABLE_SOURCE_WORK_PRESERVED",
          replacementWorkKey: legacy.workKey,
        },
        now,
      );
    return legacyDefinition;
  }

  #seedMany(
    definitions: readonly WorkDefinition[],
    now: number,
    toleratePoemConflicts: boolean,
  ): PoemSeedBatchResult {
    const prepared = definitions.map((definition) => {
      const parsed = WorkDefinitionSchema.parse(definition);
      const inputJson = canonicalJson(parsed.input);
      if (inputHash(parsed.input) !== parsed.inputHash) {
        throw new Error(
          "Work definition inputHash does not match canonical input",
        );
      }
      return { inputJson, key: workKey(parsed), parsed };
    });
    if (prepared.length === 0) return { conflicts: [], results: [] };

    return this.#immediate(() => {
      const insert = this.#database.prepare(WORK_ITEM_INSERT_SQL);
      const select = this.#database.prepare<
        [string],
        {
          implementation_version: string;
          input_hash: string;
          input_json: string;
          kind: string;
          schema_version: string;
        }
      >(
        `SELECT kind, input_json, input_hash, schema_version,
                implementation_version
         FROM work_item WHERE work_key = ?`,
      );
      const raisePriority = this.#database.prepare(
        `UPDATE work_item SET priority = max(priority, ?), updated_at = ?
          WHERE work_key = ? AND state IN ('pending', 'retry_wait', 'quota_wait')`,
      );
      const event = this.#database.prepare(
        `INSERT INTO work_event(
          event_id, work_key, attempt_id, lease_epoch, event_type,
          payload_json, created_at
        ) VALUES(?, ?, NULL, NULL, 'seeded', '{}', ?)
        ON CONFLICT(event_id) DO NOTHING`,
      );
      const claimPoemIdentity = this.#database.prepare(
        `INSERT INTO poem_identity(
          source_name, poem_href, author_href, first_work_key, created_at
        ) VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(source_name, poem_href) DO NOTHING`,
      );
      const existingPoemIdentity = this.#database.prepare<
        [string, string],
        { author_href: string }
      >(
        `SELECT author_href FROM poem_identity
         WHERE source_name = ? AND poem_href = ?`,
      );
      const results: SeedResult[] = [];
      const conflicts: PoemIdentityConflictError[] = [];
      for (const preparedDefinition of prepared) {
        const parsed = this.#preserveStartedSolProfile(
          preparedDefinition.parsed,
          now,
        );
        const key = workKey(parsed);
        const { inputJson } = preparedDefinition;
        if (parsed.kind === collectionWorkKinds().poemDetail) {
          const identity = poemIdentity(parsed.input);
          try {
            claimPoemIdentity.run(
              currentSource().name,
              identity.poemHref,
              identity.authorHref,
              key,
              now,
            );
          } catch (error) {
            if (!isPoemIdentityConstraint(error)) throw error;
            const existing = existingPoemIdentity.get(
              currentSource().name,
              identity.poemHref,
            );
            const conflict = new PoemIdentityConflictError(
              identity.poemHref,
              identity.authorHref,
              existing?.author_href ?? null,
              { cause: error },
            );
            if (!toleratePoemConflicts) throw conflict;
            conflicts.push(conflict);
            continue;
          }
        }
        const result = insert.run(
          key,
          parsed.kind,
          inputJson,
          parsed.inputHash,
          parsed.schemaVersion,
          parsed.implementationVersion,
          parsed.priority,
          now,
          now,
          now,
        );
        if (result.changes === 0) {
          const existing = select.get(key);
          if (
            existing?.kind !== parsed.kind ||
            existing.input_hash !== parsed.inputHash ||
            existing.schema_version !== parsed.schemaVersion ||
            existing.implementation_version !== parsed.implementationVersion ||
            existing.input_json !== inputJson
          ) {
            throw new Error(`Deterministic work-key collision: ${key}`);
          }
          raisePriority.run(parsed.priority, now, key);
        } else {
          event.run(randomUUID(), key, now);
        }
        results.push({ inserted: result.changes === 1, workKey: key });
      }
      return { conflicts, results };
    });
  }

  #schemaVersion(): number {
    const row = this.#database
      .prepare<[], { version: number }>(
        "SELECT version FROM local_schema WHERE singleton = 1",
      )
      .get();
    if (!row) throw new Error("Local schema version missing");
    return row.version;
  }

  #assertLease(claim: WorkClaim, now: number): void {
    const row = this.#database
      .prepare(
        `SELECT 1 FROM work_item WHERE work_key = ? AND state = 'running'
         AND lease_token = ? AND lease_epoch = ? AND lease_expires_at > ?`,
      )
      .get(claim.work.workKey, claim.leaseToken, claim.leaseEpoch, now);
    if (!row) throw new LostLeaseError();
  }

  #transitionClaim(
    claim: WorkClaim,
    state: WorkState,
    now: number,
    artifactHash: null | string,
    errorCode: null | string,
    availableAt: number,
  ): void {
    this.#immediate(() => {
      const changed = this.#database
        .prepare(
          `UPDATE work_item SET state = ?, available_at = ?, output_artifact_hash = ?,
           last_error_code = ?, lease_owner = NULL, lease_token = NULL,
           lease_expires_at = NULL, updated_at = ?
         WHERE work_key = ? AND state = 'running' AND lease_token = ?
           AND lease_epoch = ? AND lease_expires_at > ?`,
        )
        .run(
          state,
          availableAt,
          artifactHash,
          errorCode,
          now,
          claim.work.workKey,
          claim.leaseToken,
          claim.leaseEpoch,
          now,
        );
      if (changed.changes !== 1) throw new LostLeaseError();
      this.#appendEvent(
        claim.work.workKey,
        claim.attemptId,
        claim.leaseEpoch,
        state,
        { artifactHash, errorCode },
        now,
      );
    });
  }

  #appendEvent(
    key: string,
    attemptId: null | string,
    epoch: null | number,
    type: string,
    payload: Readonly<Record<string, unknown>>,
    now: number,
  ): void {
    this.#database
      .prepare(
        `INSERT INTO work_event(event_id, work_key, attempt_id, lease_epoch, event_type, payload_json, created_at)
         VALUES(?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(event_id) DO NOTHING`,
      )
      .run(
        randomUUID(),
        key,
        attemptId,
        epoch,
        type,
        canonicalJson(payload),
        now,
      );
  }

  #immediate<T>(operation: () => T): T {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.inTransaction) {
        try {
          this.#database.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new AggregateError(
            [error, rollbackError],
            "Ledger operation and rollback both failed",
          );
        }
      }
      throw error;
    }
  }

  #readTransaction<T>(operation: () => T): T {
    this.#database.exec("BEGIN");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.inTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }
}

function rowToWork(row: WorkRow): WorkItem {
  return {
    attemptCount: row.attempt_count,
    availableAt: row.available_at,
    createdAt: row.created_at,
    implementationVersion: row.implementation_version,
    input: JsonRecordSchema.parse(JSON.parse(row.input_json)),
    inputHash: row.input_hash,
    kind: row.kind,
    lastErrorCode: row.last_error_code,
    leaseEpoch: row.lease_epoch,
    leaseExpiresAt: row.lease_expires_at,
    leaseOwner: row.lease_owner,
    leaseToken: row.lease_token,
    outputArtifactHash: row.output_artifact_hash,
    priority: row.priority,
    schemaVersion: row.schema_version,
    state: WorkStateSchema.parse(row.state),
    updatedAt: row.updated_at,
    workKey: row.work_key,
  };
}

function emptyWorkStateCounts(): Record<WorkState, number> {
  return {
    dead_letter: 0,
    imported: 0,
    pending: 0,
    quota_wait: 0,
    retry_wait: 0,
    running: 0,
    succeeded: 0,
  };
}

function validateClaim(
  owner: string,
  leaseDurationMs: number,
  kinds: readonly string[],
  requirements: StartedClaimRequirements,
): void {
  requirePositiveDuration(leaseDurationMs);
  if (requirements.minimumAttemptCount !== undefined)
    z.int().nonnegative().parse(requirements.minimumAttemptCount);
  if (owner.trim().length === 0) throw new Error("Lease owner is required");
  if (kinds.some((kind) => kind.trim().length === 0))
    throw new Error("Claim kinds cannot be empty");
  for (const [name, value] of [
    ["implementationVersion", requirements.implementationVersion],
    ["schemaVersion", requirements.schemaVersion],
  ] as const) {
    if (value?.trim().length === 0)
      throw new Error(`Claim ${name} cannot be empty`);
  }
}

class WorkClaimer {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  claim(options: {
    readonly appendEvent: (
      key: string,
      attemptId: null | string,
      epoch: null | number,
      type: string,
      payload: Readonly<Record<string, unknown>>,
      now: number,
    ) => void;
    readonly excludedErrorCodes: readonly string[];
    readonly includedWorkKeys?: readonly string[];
    readonly kinds: readonly string[];
    readonly leaseDurationMs: number;
    readonly lookup: (key: string) => null | WorkItem;
    readonly now: number;
    readonly owner: string;
    readonly requirements: StartedClaimRequirements;
  }): null | WorkClaim {
    return this.claimMany({ ...options, limit: 1 })[0] ?? null;
  }

  claimMany(options: {
    readonly appendEvent: (
      key: string,
      attemptId: null | string,
      epoch: null | number,
      type: string,
      payload: Readonly<Record<string, unknown>>,
      now: number,
    ) => void;
    readonly excludedErrorCodes: readonly string[];
    readonly includedWorkKeys?: readonly string[];
    readonly kinds: readonly string[];
    readonly leaseDurationMs: number;
    readonly limit: number;
    readonly lookup: (key: string) => null | WorkItem;
    readonly now: number;
    readonly owner: string;
    readonly requirements: StartedClaimRequirements;
  }): readonly WorkClaim[] {
    const kindFilter =
      options.kinds.length === 0
        ? ""
        : ` AND kind IN (${options.kinds.map(() => "?").join(",")})`;
    const implementationFilter =
      options.requirements.implementationVersion === undefined
        ? ""
        : " AND implementation_version = ?";
    const schemaFilter =
      options.requirements.schemaVersion === undefined
        ? ""
        : " AND schema_version = ?";
    const errorFilter =
      options.excludedErrorCodes.length === 0
        ? ""
        : ` AND (last_error_code IS NULL OR last_error_code NOT IN (${options.excludedErrorCodes.map(() => "?").join(",")}))`;
    const attemptFilter =
      options.requirements.minimumAttemptCount === undefined
        ? ""
        : " AND attempt_count >= ?";
    const workKeyFilter =
      options.includedWorkKeys === undefined
        ? ""
        : ` AND work_key IN (${options.includedWorkKeys.map(() => "?").join(",") || "NULL"})`;
    const claimIndex =
      options.kinds.length === 1 &&
      options.requirements.implementationVersion !== undefined &&
      options.requirements.schemaVersion !== undefined
        ? " INDEXED BY work_item_ready_priority"
        : "";
    const filterValues = [
      ...options.kinds,
      ...(options.requirements.implementationVersion === undefined
        ? []
        : [options.requirements.implementationVersion]),
      ...(options.requirements.schemaVersion === undefined
        ? []
        : [options.requirements.schemaVersion]),
      ...options.excludedErrorCodes,
      ...(options.requirements.minimumAttemptCount === undefined
        ? []
        : [options.requirements.minimumAttemptCount]),
      ...(options.includedWorkKeys ?? []),
    ];
    const rows = this.#database
      .prepare<(number | string)[], WorkRow>(
        `SELECT ${WORK_ROW_COLUMNS} FROM work_item${claimIndex}
       WHERE state IN ('pending','retry_wait','quota_wait') AND available_at <= ?
       ${kindFilter}${implementationFilter}${schemaFilter}${errorFilter}${attemptFilter}${workKeyFilter}
       ORDER BY priority DESC, created_at, work_key LIMIT ?`,
      )
      .all(options.now, ...filterValues, options.limit);
    const update = this.#database.prepare(
      `UPDATE work_item SET state = 'running', attempt_count = attempt_count + 1,
           lease_owner = ?, lease_token = ?, lease_epoch = ?, lease_expires_at = ?, updated_at = ?
       WHERE work_key = ? AND lease_epoch = ? AND state IN ('pending','retry_wait','quota_wait')`,
    );
    const claims: WorkClaim[] = [];
    for (const row of rows) {
      const token = randomUUID();
      const attemptId = randomUUID();
      const epoch = row.lease_epoch + 1;
      const changed = update.run(
        options.owner,
        token,
        epoch,
        options.now + options.leaseDurationMs,
        options.now,
        row.work_key,
        row.lease_epoch,
      );
      if (changed.changes !== 1) throw new LostLeaseError("Claim race lost");
      options.appendEvent(
        row.work_key,
        attemptId,
        epoch,
        "claimed",
        { owner: options.owner },
        options.now,
      );
      const work = options.lookup(row.work_key);
      if (!work) throw new Error("Claimed work disappeared");
      claims.push({ attemptId, leaseEpoch: epoch, leaseToken: token, work });
    }
    return claims;
  }
}

function collectAttemptIds(value: unknown, destination: Set<string>): void {
  if (typeof value === "string") {
    if (
      /^[\da-f]{8}-[\da-f]{4}-[1-8][\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/i.test(
        value,
      )
    )
      destination.add(value);
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectAttemptIds(item, destination);
    return;
  }
  if (value !== null && typeof value === "object")
    for (const item of Object.values(value))
      collectAttemptIds(item, destination);
}

function poemIdentity(input: Readonly<Record<string, unknown>>): {
  readonly authorHref: string;
  readonly poemHref: string;
} {
  const parsed = PoemIdentityInputSchema.parse(input);
  return { authorHref: parsed.authorHref, poemHref: parsed.poemHref };
}

function isPoemIdentityConstraint(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    error.code === "SQLITE_CONSTRAINT_TRIGGER" &&
    error.message.includes("POEM_DUPLICATE")
  );
}

function requirePositiveDuration(value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0)
    throw new Error("Lease duration must be a positive integer");
}

function requireErrorCode(value: string): void {
  if (!/^[A-Z][A-Z0-9_]{0,99}$/.test(value))
    throw new Error("Invalid error code");
}

function requireDeferredTimestamp(value: number, now: number): void {
  if (!Number.isSafeInteger(value) || value <= now) {
    throw new Error("Work retryAt must be later than now");
  }
}

function requireOrigin(origin: string): void {
  const parsed = new URL(origin);
  if (
    parsed.origin !== origin ||
    (parsed.protocol !== "http:" && parsed.protocol !== "https:")
  ) {
    throw new Error("Origin must be a canonical HTTP(S) origin");
  }
}

class LedgerIntegrityInspector {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  assertHealthy(): void {
    let result: string;
    try {
      // quick_check(N) limits the number of reported errors, not the number of
      // pages SQLite reads. Scoping the pragma to sqlite_schema keeps startup
      // proportional to schema size while still failing closed on an
      // unreadable header/schema. cell_size_check extends inexpensive B-tree
      // page validation to every page subsequently touched by normal work.
      this.#database.pragma("cell_size_check = ON");
      result = SqlitePragmaValueSchema.parse(
        this.#database.pragma("quick_check('sqlite_schema')", {
          simple: true,
        }),
      );
    } catch (error) {
      throw new LedgerIntegrityError("unreadable", { cause: error });
    }
    if (result !== "ok") throw new LedgerIntegrityError(result);
  }
}

export { CURRENT_SCHEMA_VERSION } from "./migrations.js";
