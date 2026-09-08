// eslint-disable-next-line @sarj/prefer-node-fs-promises -- Legacy sentinel updates must finish within the synchronous SQLite pause-control transition.
import { rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

const PauseStateSchema = z
  .strictObject({
    global_paused: z.literal([0, 1]),
    paid_work_paused: z.literal([0, 1]),
  })
  .transform((row) => ({
    paused: row.global_paused === 1,
    paidWorkPaused: row.paid_work_paused === 1,
  }));
const READ_PAUSES = `SELECT
  (SELECT enabled FROM runtime_control WHERE control_key = 'global_paused') AS global_paused,
  (SELECT enabled FROM runtime_control WHERE control_key = 'paid_work_paused') AS paid_work_paused
  FROM runtime_control WHERE control_key = 'legacy_pause_imported'`;

/** SQLite is authoritative after the first transactional legacy import. */
export interface PauseControlPort {
  read(): { paused: boolean; paidWorkPaused: boolean };
  set(
    kind: "global" | "paid",
    paused: boolean,
  ): { paused: boolean; paidWorkPaused: boolean };
}

export class PauseControls implements PauseControlPort {
  readonly #existingOnly: boolean;
  readonly database: Database.Database;
  readonly root: string;

  constructor(database: Database.Database, root: string, existingOnly = false) {
    this.database = database;
    this.root = root;
    this.#existingOnly = existingOnly;
  }

  /** Operator controls need no source credentials and never initialize a ledger. */
  static withExisting<T>(
    root: string,
    operation: (controls: PauseControlPort) => T,
  ): T {
    const database = new Database(join(root, "ledger.sqlite3"), {
      fileMustExist: true,
    });
    try {
      database.pragma("busy_timeout = 5000");
      database.pragma("synchronous = FULL");
      PauseStateSchema.parse(
        database.prepare(`${READ_PAUSES} AND enabled = 1`).get(),
      );
      return operation(new PauseControls(database, root, true));
    } finally {
      database.close();
    }
  }

  read() {
    if (this.#existingOnly) {
      return PauseStateSchema.parse(
        this.database.prepare(`${READ_PAUSES} AND enabled = 1`).get(),
      );
    }
    const row = this.database.prepare(READ_PAUSES).get();
    if (row !== undefined) return PauseStateSchema.parse(row);
    return this.database
      .transaction(() => {
        const insert = this.database.prepare(
          "INSERT OR IGNORE INTO runtime_control(control_key, enabled) VALUES(?, ?)",
        );
        insert.run(
          "global_paused",
          Number(
            statSync(join(this.root, "PAUSED"), { throwIfNoEntry: false }) !==
              undefined,
          ),
        );
        insert.run(
          "paid_work_paused",
          Number(
            statSync(join(this.root, "PAID_WORK_PAUSED"), {
              throwIfNoEntry: false,
            }) !== undefined,
          ),
        );
        insert.run("legacy_pause_imported", 1);
        return PauseStateSchema.parse(this.database.prepare(READ_PAUSES).get());
      })
      .immediate();
  }

  set(kind: "global" | "paid", paused: boolean) {
    this.read();
    const result = this.database
      .transaction(() => {
        this.database
          .prepare(
            "UPDATE runtime_control SET enabled = ? WHERE control_key = ?",
          )
          .run(
            Number(paused),
            kind === "global" ? "global_paused" : "paid_work_paused",
          );
        return this.read();
      })
      .immediate();
    // Rollback projections must never veto a committed operator control.
    const path = join(
      this.root,
      kind === "global" ? "PAUSED" : "PAID_WORK_PAUSED",
    );
    try {
      if (paused)
        writeFileSync(path, `${new Date().toISOString()}\n`, { mode: 0o600 });
      else rmSync(path, { force: true });
    } catch {
      process.emitWarning(
        "SQLite pause saved; legacy rollback mirror unavailable",
        {
          code: "SAQI_PAUSE_MIRROR_UNAVAILABLE",
        },
      );
    }
    return result;
  }
}
