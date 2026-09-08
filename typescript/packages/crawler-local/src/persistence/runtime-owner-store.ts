import type Database from "better-sqlite3";
import { z } from "zod";

import { CURRENT_SCHEMA_VERSION } from "./baseline-schema.js";
import {
  RuntimeOwnerKindSchema,
  type RuntimeOwnerRecord,
  RuntimeOwnerRecordSchema,
  type RuntimeOwnerRow,
  RuntimeOwnerRowSchema,
} from "./runtime-owner-schema.js";

const VersionSchema = z.strictObject({
  version: z.literal(CURRENT_SCHEMA_VERSION),
});
export interface RuntimeOwnerFence {
  readonly epoch: number;
  readonly record: RuntimeOwnerRecord;
}
interface RuntimeOwnerPort {
  claim(
    record: RuntimeOwnerRecord,
    kind: "maintenance" | "supervisor",
    recoverDead: boolean,
  ): RuntimeOwnerFence;
  read(): null | RuntimeOwnerFence;
  release(fence: RuntimeOwnerFence, requireDead?: boolean): boolean;
  withCurrentOwner(
    pid: number,
    runId: string,
    action: () => undefined,
  ): boolean;
}
export class RuntimeOwnerBusyError extends Error {
  readonly record: RuntimeOwnerRecord;
  constructor(record: RuntimeOwnerRecord) {
    super(`Runtime owner already held by pid ${String(record.pid)}`);
    this.name = "RuntimeOwnerBusyError";
    this.record = record;
  }
}

export class RuntimeOwnerStore implements RuntimeOwnerPort {
  readonly #database: Database.Database;
  readonly #definitelyDead: (pid: number) => boolean;
  constructor(
    database: Database.Database,
    definitelyDead: (pid: number) => boolean = processDefinitelyDead,
  ) {
    this.#database = database;
    this.#definitelyDead = definitelyDead;
  }
  read(): null | RuntimeOwnerFence {
    const row = this.#row();
    return row.held === 0 ? null : rowFence(row);
  }
  claim(
    record: RuntimeOwnerRecord,
    kind: "maintenance" | "supervisor",
    recoverDead: boolean,
  ): RuntimeOwnerFence {
    const parsed = RuntimeOwnerRecordSchema.parse(record);
    RuntimeOwnerKindSchema.parse(kind);
    return this.#database
      .transaction(() => {
        const row = this.#row();
        if (row.held === 1) {
          const existing = rowFence(row);
          if (
            kind === "maintenance" ||
            !recoverDead ||
            !this.#definitelyDead(existing.record.pid)
          )
            throw new RuntimeOwnerBusyError(existing.record);
        }
        const epoch = row.epoch + 1;
        if (!Number.isSafeInteger(epoch))
          throw new Error("RUNTIME_OWNER_EPOCH_EXHAUSTED");
        const changed = this.#database
          .prepare(
            `UPDATE runtime_owner
        SET epoch=?, held=1, owner_kind=?, pid=?, run_id=?, config_digest=?, started_at=?
        WHERE singleton=1 AND epoch=?`,
          )
          .run(
            epoch,
            kind,
            parsed.pid,
            parsed.runId,
            parsed.configDigest,
            parsed.startedAt,
            row.epoch,
          );
        if (changed.changes !== 1)
          throw new Error("RUNTIME_OWNER_CLAIM_FENCE_LOST");
        return { epoch, record: parsed };
      })
      .immediate();
  }
  release(fence: RuntimeOwnerFence, requireDead = false): boolean {
    return this.#database
      .transaction(() => {
        const current = this.read();
        if (
          current?.epoch !== fence.epoch ||
          current.record.pid !== fence.record.pid ||
          current.record.runId !== fence.record.runId
        )
          return false;
        if (requireDead && !this.#definitelyDead(current.record.pid))
          return false;
        const changed = this.#database
          .prepare(
            "UPDATE runtime_owner SET held=0 WHERE singleton=1 AND held=1 AND epoch=? AND pid=? AND run_id=?",
          )
          .run(fence.epoch, fence.record.pid, fence.record.runId);
        return changed.changes === 1;
      })
      .immediate();
  }
  withCurrentOwner(
    pid: number,
    runId: string,
    action: () => undefined,
  ): boolean {
    return this.#database
      .transaction(() => {
        const current = this.read();
        if (current?.record.pid !== pid || current.record.runId !== runId)
          return false;
        action();
        return true;
      })
      .immediate();
  }
  #row(): RuntimeOwnerRow {
    VersionSchema.parse(
      this.#database
        .prepare("SELECT version FROM local_schema WHERE singleton=1")
        .get(),
    );
    return RuntimeOwnerRowSchema.parse(
      this.#database
        .prepare(
          "SELECT singleton, epoch, held, owner_kind, pid, run_id, config_digest, started_at FROM runtime_owner WHERE singleton=1",
        )
        .get(),
    );
  }
}
function rowFence(row: RuntimeOwnerRow): RuntimeOwnerFence {
  return {
    epoch: row.epoch,
    record: RuntimeOwnerRecordSchema.parse({
      configDigest: row.config_digest,
      pid: row.pid,
      runId: row.run_id,
      schemaVersion: 1,
      startedAt: row.started_at,
    }),
  };
}
function processDefinitelyDead(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    return error instanceof Error && "code" in error && error.code === "ESRCH";
  }
}
