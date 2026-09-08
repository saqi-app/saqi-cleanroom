import { lstat } from "node:fs/promises";
import { join, resolve } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

import { acquireRunLock, readRunLock } from "../runtime/run-lock.js";
import {
  type SolImportedAttempt,
  SolImportedAttemptSchema,
  type SolImportReceipt,
  SolImportReceiptSchema,
  type SolLegacyScan,
} from "./sol-operation-import-schema.js";
import { SolOperationLegacyReader } from "./sol-operation-legacy-reader.js";
import { queryOptional, queryRequired } from "./sqlite-query.js";
import { canonicalJson } from "./work-key.js";

const HashSchema = z.string().regex(/^[a-f\d]{64}$/);
const StoppedControlsSchema = z.strictObject({
  serviceEnabled: z.literal(0),
  paused: z.literal(1),
  paidPaused: z.literal(1),
  pauseImported: z.literal(1),
  serviceImported: z.literal(1),
});
const EmptySchema = z.strictObject({ count: z.literal(0) });
const ImportedSchema = z.strictObject({ enabled: z.literal(1) });
const VersionSchema = z.strictObject({
  version: z.literal(36),
});
const READ_RECEIPT = `SELECT source_digest AS sourceDigest, record_count AS records,
  source_bytes AS sourceBytes, imported_at AS importedAt FROM sol_operation_import_receipt WHERE singleton = 1`;
export type SolOperationImportOptions = {
  readonly stateDirectory: string;
} & (
  | { readonly apply: true; readonly expectedDigest: string }
  | { readonly apply?: false }
);
export type SolOperationImportResult =
  | {
      readonly mode: "already_imported" | "applied";
      readonly receipt: SolImportReceipt;
    }
  | {
      readonly mode: "dry_run";
      readonly sourceDigest: string;
      readonly records: number;
      readonly sourceBytes: number;
      readonly quarantine: SolLegacyScan["quarantine"];
      readonly legacyIntentNormalizations: number;
      readonly legacyObservations: SolLegacyScan["legacyObservations"];
    };

interface ImportPort {
  apply(
    reader: SolOperationLegacyReader,
    expectedDigest: string,
  ): Promise<SolImportReceipt>;
  assertStopped(): 36;
  readReceipt(): SolImportReceipt | undefined;
}

/** Offline only: all legacy runners must already be stopped, including standalone run-enrichment. */
export async function importLegacySolOperations(
  options: SolOperationImportOptions,
): Promise<SolOperationImportResult> {
  const root = resolve(options.stateDirectory);
  const directory = await lstat(root);
  if (!directory.isDirectory() || directory.isSymbolicLink())
    throw new Error("SOL_IMPORT_UNSAFE_ROOT");
  const ledgerPath = join(root, "ledger.sqlite3");
  const ledgerFile = await lstat(ledgerPath);
  if (!ledgerFile.isFile() || ledgerFile.isSymbolicLink())
    throw new Error("SOL_IMPORT_UNSAFE_LEDGER");
  const database = new Database(ledgerPath, {
    fileMustExist: true,
    readonly: options.apply !== true,
  });
  try {
    const repository = new SolOperationImportRepository(database);
    const receipt = repository.readReceipt();
    if (receipt !== undefined) return { mode: "already_imported", receipt };
    repository.assertStopped();
    const lockPath = join(root, "RUN.lock");
    const legacyLock = await lstat(lockPath).catch((error: unknown) => {
      if (error instanceof Error && "code" in error && error.code === "ENOENT")
        return null;
      throw error;
    });
    if (legacyLock !== null)
      throw new Error("RUNTIME_OWNER_LEGACY_LOCK_PRESENT");
    if ((await readRunLock(lockPath)) !== null)
      throw new Error("SOL_IMPORT_REQUIRES_STOPPED_OWNER");
    const reader = new SolOperationLegacyReader(join(root, "sol-attempts"));
    if (options.apply !== true) {
      const scan = await reader.scan();
      return {
        mode: "dry_run",
        sourceDigest: scan.sourceDigest,
        sourceBytes: scan.sourceBytes,
        records: scan.records.length,
        quarantine: scan.quarantine,
        legacyIntentNormalizations: scan.legacyIntentNormalizations,
        legacyObservations: scan.legacyObservations,
      };
    }
    const expectedDigest = HashSchema.parse(options.expectedDigest);
    const lock = await acquireRunLock(lockPath, expectedDigest, new Date(), {
      recoverStale: false,
    });
    try {
      return {
        mode: "applied",
        receipt: await repository.apply(reader, expectedDigest),
      };
    } finally {
      await lock.release();
    }
  } finally {
    database.close();
  }
}

class SolOperationImportRepository implements ImportPort {
  readonly #database: Database.Database;
  constructor(database: Database.Database) {
    this.#database = database;
  }

  readReceipt(): SolImportReceipt | undefined {
    const marker = queryOptional(
      { operation: "solImport.marker" },
      () =>
        this.#database
          .prepare(
            "SELECT enabled FROM runtime_control WHERE control_key = 'sol_operation_import_complete'",
          )
          .get(),
      ImportedSchema,
    );
    const receipt = queryOptional(
      { operation: "solImport.receipt" },
      () => this.#database.prepare(READ_RECEIPT).get(),
      SolImportReceiptSchema,
    );
    if ((marker === undefined) !== (receipt === undefined))
      throw new Error("SOL_IMPORT_RECEIPT_INCONSISTENT");
    return receipt;
  }

  assertStopped(): 36 {
    const { version } = queryRequired(
      { operation: "solImport.schema" },
      () =>
        this.#database
          .prepare("SELECT version FROM local_schema WHERE singleton = 1")
          .get(),
      VersionSchema,
    );
    queryRequired(
      { operation: "solImport.controls" },
      () =>
        this.#database
          .prepare(
            `SELECT
      (SELECT enabled FROM runtime_control WHERE control_key = 'service_enabled') AS serviceEnabled,
      (SELECT enabled FROM runtime_control WHERE control_key = 'global_paused') AS paused,
      (SELECT enabled FROM runtime_control WHERE control_key = 'paid_work_paused') AS paidPaused,
      (SELECT enabled FROM runtime_control WHERE control_key = 'legacy_pause_imported') AS pauseImported,
      (SELECT enabled FROM runtime_control WHERE control_key = 'legacy_service_imported') AS serviceImported`,
          )
          .get(),
      StoppedControlsSchema,
    );
    queryRequired(
      { operation: "solImport.running" },
      () =>
        this.#database
          .prepare(
            "SELECT COUNT(*) AS count FROM work_item WHERE kind = 'poem-enrichment-sol' AND state = 'running'",
          )
          .get(),
      EmptySchema,
    );
    return version;
  }

  async apply(
    reader: SolOperationLegacyReader,
    expectedDigest: string,
  ): Promise<SolImportReceipt> {
    this.#database.pragma("foreign_keys = ON");
    this.#database.pragma("synchronous = FULL");
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      this.assertStopped();
      const prior = this.readReceipt();
      if (prior !== undefined) throw new Error("SOL_IMPORT_ALREADY_COMMITTED");
      queryRequired(
        { operation: "solImport.empty" },
        () =>
          this.#database
            .prepare(
              "SELECT (SELECT COUNT(*) FROM sol_operation) + (SELECT COUNT(*) FROM sol_invocation_attempt) AS count",
            )
            .get(),
        EmptySchema,
      );
      const scan = await reader.scan();
      if (scan.sourceDigest !== expectedDigest)
        throw new Error("SOL_IMPORT_SOURCE_CHANGED");
      this.#insert(scan);
      const receipt = SolImportReceiptSchema.parse({
        sourceDigest: scan.sourceDigest,
        records: scan.records.length,
        sourceBytes: scan.sourceBytes,
        importedAt: Date.now(),
      });
      this.#database
        .prepare(
          `INSERT INTO sol_operation_import_receipt VALUES(1, ?, ?, ?, ?)`,
        )
        .run(
          receipt.sourceDigest,
          receipt.records,
          receipt.sourceBytes,
          receipt.importedAt,
        );
      this.#database
        .prepare(
          "INSERT INTO runtime_control VALUES('sol_operation_import_complete', 1)",
        )
        .run();
      this.#database.exec("COMMIT");
      return queryRequired(
        { operation: "solImport.readback" },
        () => this.#database.prepare(READ_RECEIPT).get(),
        SolImportReceiptSchema,
      );
    } catch (error) {
      if (this.#database.inTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }

  #insert(scan: SolLegacyScan): void {
    const operation = this.#database.prepare(`INSERT INTO sol_operation(
      operation_key, kind, model, model_key, pipeline_version, provider, reasoning_effort,
      current_attempt_id, current_epoch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)`);
    const attempt = this.#database.prepare(`INSERT INTO sol_invocation_attempt(
      attempt_id, operation_key, claim_epoch, input_hash, created_at, state, exit_code, signal,
      finished_at, turn_started_at, session_id, session_observed_at, observations_json)
      VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const raw of scan.records) {
      const row: SolImportedAttempt = SolImportedAttemptSchema.parse(raw);
      operation.run(
        row.operationKey,
        row.kind,
        row.model,
        row.modelKey,
        row.pipelineVersion,
        row.provider,
        row.reasoningEffort,
        row.attemptId,
      );
      attempt.run(
        row.attemptId,
        row.operationKey,
        row.inputHash,
        row.createdAt,
        row.state,
        row.exitCode,
        row.signal,
        row.finishedAt,
        row.turnStartedAt,
        row.sessionId,
        row.sessionObservedAt,
        canonicalJson(row.observations),
      );
    }
  }
}
