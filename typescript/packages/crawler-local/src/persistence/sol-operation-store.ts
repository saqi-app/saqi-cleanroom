import type Database from "better-sqlite3";
import { z } from "zod";

import {
  type SolOperationObservations,
  SolOperationObservationsSchema,
  SolReconciliationSchema,
  SolSessionCleanupPendingSchema,
  SolSessionCleanupSchema,
} from "./sol-operation-observation-schema.js";
import {
  type SolImportReceipt,
  SolImportReceiptSchema,
} from "./sol-operation-receipt-schema.js";
import { queryOptional, queryRequired } from "./sqlite-query.js";
import { canonicalJson } from "./work-key.js";

const HashSchema = z.string().regex(/^[a-f\d]{64}$/);
const TimestampSchema = z.int().nonnegative();
const EpochSchema = z.int().positive();
const SessionIdSchema = z.uuid();
const ImportCompleteSchema = z.strictObject({ enabled: z.literal(1) });
const DurableSynchronousSchema = z.literal([2, 3]);
const ForeignKeysEnabledSchema = z.literal(1);
const OperationKindSchema = z.enum(["generation", "review-1", "review-2"]);
const TerminalStateSchema = z.enum([
  "unknown",
  "known_success",
  "known_rejection",
]);
const AttemptStateSchema = z.enum([
  "intent",
  "unknown",
  "known_success",
  "known_rejection",
  "known_invalid",
]);
const ProfileSchema = z.strictObject({
  kind: OperationKindSchema,
  model: z.string().min(1).max(100),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
  provider: z.literal("sol"),
  reasoningEffort: z.string().min(1).max(100),
});
export const SolOperationClaimSchema = ProfileSchema.extend({
  attemptId: z.uuid(),
  inputHash: HashSchema,
  operationKey: HashSchema,
});
export type SolOperationClaim = z.infer<typeof SolOperationClaimSchema>;
const FenceSchema = z.strictObject({
  attemptId: z.uuid(),
  claimEpoch: EpochSchema,
  operationKey: HashSchema,
});
export type SolOperationFence = z.infer<typeof FenceSchema>;
function parseFence({
  attemptId,
  claimEpoch,
  operationKey,
}: SolOperationFence): SolOperationFence {
  return FenceSchema.parse({ attemptId, claimEpoch, operationKey });
}
const TerminalSchema = z
  .strictObject({
    exitCode: z.int().nullable(),
    finishedAt: TimestampSchema,
    signal: z.string().min(1).max(100).nullable(),
    state: TerminalStateSchema,
  })
  .superRefine((terminal, context) => {
    if (
      terminal.state === "known_success" &&
      (terminal.exitCode !== 0 || terminal.signal !== null)
    ) {
      context.addIssue({
        code: "custom",
        message: "Successful invocation requires a clean exit",
      });
    }
  });
export type SolInvocationTerminal = z.infer<typeof TerminalSchema>;
const CurrentSchema = SolOperationClaimSchema.extend({
  claimEpoch: EpochSchema,
  createdAt: TimestampSchema,
  exitCode: z.int().nullable(),
  finishedAt: TimestampSchema.nullable(),
  sessionId: SessionIdSchema.nullable(),
  sessionObservedAt: TimestampSchema.nullable(),
  signal: z.string().nullable(),
  state: AttemptStateSchema,
  turnStartedAt: TimestampSchema.nullable(),
  observations: z
    .string()
    .transform((value) =>
      SolOperationObservationsSchema.parse(JSON.parse(value)),
    ),
});
export type SolOperationCurrent = z.infer<typeof CurrentSchema>;
export interface SolOperationClaimResult {
  readonly claimed: boolean;
  readonly current: SolOperationCurrent;
}

export interface SolOperationClaimPort {
  assertImported(): SolImportReceipt;
  claim(raw: SolOperationClaim, now: number): SolOperationClaimResult;
  readCurrent(operationKey: string): SolOperationCurrent | undefined;
}

export interface SolAttemptObservationPort {
  markInvalid(fence: SolOperationFence): boolean;
  readAttempt(fence: SolOperationFence): SolOperationCurrent | undefined;
  recordCredentialObservation(
    fence: SolOperationFence,
    observation: NonNullable<SolOperationObservations["credential"]>,
  ): boolean;
  recordReconciliation(
    fence: SolOperationFence,
    observation: NonNullable<SolOperationObservations["reconciliation"]>,
  ): boolean;
  recordSession(
    fence: SolOperationFence,
    sessionId: string,
    observedAt: number,
  ): boolean;
  recordSessionCleanup(
    fence: SolOperationFence,
    observation: NonNullable<SolOperationObservations["cleanup"]>,
  ): boolean;
  recordSessionCleanupPending(
    fence: SolOperationFence,
    observation: NonNullable<SolOperationObservations["cleanupPending"]>,
  ): boolean;
  recordTerminal(
    fence: SolOperationFence,
    terminal: SolInvocationTerminal,
  ): boolean;
  recordTurnStarted(fence: SolOperationFence, observedAt: number): boolean;
}

const ATTEMPT_COLUMNS = `SELECT o.operation_key AS operationKey, o.kind, o.model,
  o.model_key AS modelKey, o.pipeline_version AS pipelineVersion, o.provider,
  o.reasoning_effort AS reasoningEffort, a.attempt_id AS attemptId,
  a.input_hash AS inputHash, a.claim_epoch AS claimEpoch, a.created_at AS createdAt,
  a.state, a.exit_code AS exitCode, a.signal, a.finished_at AS finishedAt,
  a.turn_started_at AS turnStartedAt, a.session_id AS sessionId,
  a.session_observed_at AS sessionObservedAt, a.observations_json AS observations`;
const READ_CURRENT = `${ATTEMPT_COLUMNS}
  FROM sol_operation o LEFT JOIN sol_invocation_attempt a
    ON a.operation_key = o.operation_key AND a.attempt_id = o.current_attempt_id
      AND a.claim_epoch = o.current_epoch
  WHERE o.operation_key = ?`;
const READ_FENCED_ATTEMPT = `${ATTEMPT_COLUMNS}
  FROM sol_operation o JOIN sol_invocation_attempt a ON a.operation_key = o.operation_key
  WHERE o.operation_key = ? AND a.attempt_id = ? AND a.claim_epoch = ?`;

/** Uses the caller's migrated ledger. No filesystem fallback or implicit schema creation. */
export class SolOperationStore
  implements SolOperationClaimPort, SolAttemptObservationPort
{
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
    DurableSynchronousSchema.parse(
      this.#database.pragma("synchronous", { simple: true }),
    );
    ForeignKeysEnabledSchema.parse(
      this.#database.pragma("foreign_keys", { simple: true }),
    );
  }

  readCurrent(operationKey: string): SolOperationCurrent | undefined {
    return queryOptional(
      { operation: "solOperation.current" },
      () =>
        this.#database
          .prepare(READ_CURRENT)
          .get(HashSchema.parse(operationKey)),
      CurrentSchema,
    );
  }

  /** Explicit readiness check; never creates a receipt or changes controls. */
  assertImported(): SolImportReceipt {
    queryRequired(
      { operation: "solOperation.importComplete" },
      () =>
        this.#database
          .prepare(
            "SELECT enabled FROM runtime_control WHERE control_key = 'sol_operation_import_complete'",
          )
          .get(),
      ImportCompleteSchema,
    );
    return queryRequired(
      { operation: "solOperation.importReceipt" },
      () =>
        this.#database
          .prepare(
            "SELECT source_digest AS sourceDigest, record_count AS records, source_bytes AS sourceBytes, imported_at AS importedAt FROM sol_operation_import_receipt WHERE singleton = 1",
          )
          .get(),
      SolImportReceiptSchema,
    );
  }

  /** Historical reads never authorize another invocation. */
  readAttempt(rawFence: SolOperationFence): SolOperationCurrent | undefined {
    const fence = parseFence(rawFence);
    return queryOptional(
      { operation: "solOperation.attempt" },
      () =>
        this.#database
          .prepare(READ_FENCED_ATTEMPT)
          .get(fence.operationKey, fence.attemptId, fence.claimEpoch),
      CurrentSchema,
    );
  }

  recordCredentialObservation(
    fence: SolOperationFence,
    raw: NonNullable<SolOperationObservations["credential"]>,
  ): boolean {
    const observation = SolOperationObservationsSchema.shape.credential
      .unwrap()
      .unwrap()
      .parse(raw);
    return this.#withFence(parseFence(fence), (current) => {
      if (
        observation.before.observedAt < current.createdAt ||
        (observation.after !== null &&
          observation.after.observedAt < observation.before.observedAt)
      )
        return false;
      const previous = current.observations.credential;
      if (previous !== null) {
        if (canonicalJson(previous) === canonicalJson(observation)) return true;
        if (
          previous.after !== null ||
          observation.after === null ||
          canonicalJson(previous.before) !== canonicalJson(observation.before)
        )
          return false;
      }
      return this.#saveObservations(current, {
        ...current.observations,
        credential: observation,
      });
    });
  }

  recordReconciliation(
    fence: SolOperationFence,
    raw: NonNullable<SolOperationObservations["reconciliation"]>,
  ): boolean {
    const observation = SolReconciliationSchema.parse(raw);
    return this.#withFence(parseFence(fence), (current) => {
      if (
        (current.state !== "intent" && current.state !== "unknown") ||
        observation.checkedAt < current.createdAt
      )
        return false;
      const previous = current.observations.reconciliation;
      if (previous !== null && observation.checkedAt < previous.checkedAt)
        return false;
      return this.#saveObservations(current, {
        ...current.observations,
        reconciliation: observation,
      });
    });
  }

  recordSessionCleanup(
    fence: SolOperationFence,
    raw: NonNullable<SolOperationObservations["cleanup"]>,
  ): boolean {
    const observation = SolSessionCleanupSchema.parse(raw);
    return this.#withCleanupAttempt(
      fence,
      observation.sessionId,
      observation.cleanedAt,
      (current) => {
        if (current.observations.cleanup !== null)
          return (
            canonicalJson(current.observations.cleanup) ===
            canonicalJson(observation)
          );
        if (
          observation.cleanedAt <
          (current.observations.cleanupPending?.observedAt ?? 0)
        )
          return false;
        return this.#saveObservations(current, {
          ...current.observations,
          cleanup: observation,
          cleanupPending: null,
        });
      },
    );
  }

  recordSessionCleanupPending(
    fence: SolOperationFence,
    raw: NonNullable<SolOperationObservations["cleanupPending"]>,
  ): boolean {
    const observation = SolSessionCleanupPendingSchema.parse(raw);
    return this.#withCleanupAttempt(
      fence,
      observation.sessionId,
      observation.observedAt,
      (current) => {
        if (
          current.observations.cleanup !== null ||
          observation.observedAt <
            (current.observations.cleanupPending?.observedAt ?? 0)
        )
          return false;
        return this.#saveObservations(current, {
          ...current.observations,
          cleanupPending: observation,
        });
      },
    );
  }

  claim(raw: SolOperationClaim, now: number): SolOperationClaimResult {
    if (this.#database.inTransaction)
      throw new Error("SOL_OPERATION_CLAIM_REQUIRES_TOP_LEVEL_TRANSACTION");
    const claim = SolOperationClaimSchema.parse(raw);
    TimestampSchema.parse(now);
    return this.#database
      .transaction(() => {
        this.assertImported();
        const prior = this.readCurrent(claim.operationKey);
        if (prior) {
          for (const key of ProfileSchema.keyof().options) {
            if (prior[key] !== claim[key])
              throw new Error("SOL_OPERATION_IDENTITY_MISMATCH");
          }
          if (
            prior.state !== "known_invalid" &&
            prior.state !== "known_rejection"
          ) {
            return { claimed: false, current: prior };
          }
        }
        const epoch = (prior?.claimEpoch ?? 0) + 1;
        EpochSchema.parse(epoch);
        this.#database
          .prepare(
            `INSERT INTO sol_invocation_attempt(
        attempt_id, operation_key, claim_epoch, input_hash, created_at, state
      ) VALUES (?, ?, ?, ?, ?, 'intent')`,
          )
          .run(
            claim.attemptId,
            claim.operationKey,
            epoch,
            claim.inputHash,
            now,
          );
        this.#database
          .prepare(
            `INSERT INTO sol_operation(
        operation_key, kind, model, model_key, pipeline_version, provider,
        reasoning_effort, current_attempt_id, current_epoch
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(operation_key) DO UPDATE SET
        current_attempt_id = excluded.current_attempt_id, current_epoch = excluded.current_epoch
      WHERE sol_operation.current_epoch = ?`,
          )
          .run(
            claim.operationKey,
            claim.kind,
            claim.model,
            claim.modelKey,
            claim.pipelineVersion,
            claim.provider,
            claim.reasoningEffort,
            claim.attemptId,
            epoch,
            prior?.claimEpoch ?? 0,
          );
        const current = this.readCurrent(claim.operationKey);
        if (
          current?.attemptId !== claim.attemptId ||
          current.claimEpoch !== epoch
        ) {
          throw new Error("SOL_OPERATION_CLAIM_LOST");
        }
        return { claimed: true, current };
      })
      .immediate();
  }

  recordTerminal(
    rawFence: SolOperationFence,
    rawTerminal: SolInvocationTerminal,
  ): boolean {
    const fence = parseFence(rawFence);
    const terminal = TerminalSchema.parse(rawTerminal);
    return this.#withFence(fence, (current) => {
      if (current.state !== "intent" && current.state !== "unknown")
        return false;
      if (terminal.finishedAt < current.createdAt)
        throw new Error("SOL_OPERATION_TIME_INVALID");
      if (
        terminal.state === "known_rejection" &&
        (current.state === "unknown" ||
          current.turnStartedAt !== null ||
          terminal.signal !== null ||
          terminal.exitCode === null)
      )
        return false;
      this.#database
        .prepare(
          `UPDATE sol_invocation_attempt
        SET state = ?, exit_code = ?, signal = ?, finished_at = ? WHERE attempt_id = ?`,
        )
        .run(
          terminal.state,
          terminal.exitCode,
          terminal.signal,
          terminal.finishedAt,
          fence.attemptId,
        );
      return true;
    });
  }

  markInvalid(fence: SolOperationFence): boolean {
    return this.#withFence(parseFence(fence), (current) => {
      if (current.state !== "known_success") return false;
      this.#database
        .prepare(
          "UPDATE sol_invocation_attempt SET state = 'known_invalid' WHERE attempt_id = ?",
        )
        .run(fence.attemptId);
      return true;
    });
  }

  recordTurnStarted(fence: SolOperationFence, observedAt: number): boolean {
    TimestampSchema.parse(observedAt);
    return this.#withFence(parseFence(fence), (current) => {
      if (current.state !== "intent" || observedAt < current.createdAt)
        return false;
      this.#database
        .prepare(
          `UPDATE sol_invocation_attempt SET turn_started_at = COALESCE(turn_started_at, ?)
        WHERE attempt_id = ?`,
        )
        .run(observedAt, fence.attemptId);
      return true;
    });
  }

  recordSession(
    fence: SolOperationFence,
    sessionId: string,
    observedAt: number,
  ): boolean {
    SessionIdSchema.parse(sessionId);
    TimestampSchema.parse(observedAt);
    return this.#withFence(parseFence(fence), (current) => {
      if (observedAt < current.createdAt) return false;
      if (current.sessionId !== null && current.sessionId !== sessionId) {
        throw new Error("SOL_OPERATION_SESSION_MISMATCH");
      }
      this.#database
        .prepare(
          `UPDATE sol_invocation_attempt SET session_id = ?,
        session_observed_at = COALESCE(session_observed_at, ?) WHERE attempt_id = ?`,
        )
        .run(sessionId, observedAt, fence.attemptId);
      return true;
    });
  }

  #withCleanupAttempt(
    rawFence: SolOperationFence,
    sessionId: string,
    observedAt: number,
    update: (current: SolOperationCurrent) => boolean,
  ): boolean {
    const fence = parseFence(rawFence);
    // Cleanup can finish after a rejected/invalid attempt has been superseded.
    // It is fenced to that immutable historical attempt, never its replacement.
    return this.#database
      .transaction(() => {
        const current = this.readAttempt(fence);
        if (
          current?.sessionId !== sessionId ||
          observedAt <
            Math.max(
              current.finishedAt ?? current.createdAt,
              current.sessionObservedAt ?? current.createdAt,
            ) ||
          current.state === "intent" ||
          current.state === "unknown"
        )
          return false;
        return update(current);
      })
      .immediate();
  }

  #saveObservations(
    current: SolOperationCurrent,
    raw: SolOperationObservations,
  ): boolean {
    const observations = SolOperationObservationsSchema.parse(raw);
    this.#database
      .prepare(
        "UPDATE sol_invocation_attempt SET observations_json = ? WHERE attempt_id = ? AND claim_epoch = ?",
      )
      .run(canonicalJson(observations), current.attemptId, current.claimEpoch);
    return true;
  }

  #withFence(
    fence: SolOperationFence,
    update: (current: SolOperationCurrent) => boolean,
  ): boolean {
    return this.#database
      .transaction(() => {
        const current = this.readCurrent(fence.operationKey);
        if (
          current?.attemptId !== fence.attemptId ||
          current.claimEpoch !== fence.claimEpoch
        )
          return false;
        return update(current);
      })
      .immediate();
  }
}
