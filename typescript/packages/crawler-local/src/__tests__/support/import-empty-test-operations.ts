import { mkdirSync } from "node:fs";
import { join } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

import { Ledger } from "../../persistence/ledger.js";
import { importLegacySolOperations } from "../../persistence/sol-operation-import.js";

const ControlsSchema = z.array(z.object({ key: z.string(), enabled: z.int() }));
const CONTROL_KEYS =
  "'service_enabled','global_paused','paid_work_paused','legacy_pause_imported','legacy_service_imported'";

/** Explicit successful-runtime fixture setup, never used by production startup. */
export async function importEmptyTestOperations(root: string): Promise<void> {
  const path = join(root, "ledger.sqlite3");
  Ledger.initialize(path).close();
  mkdirSync(join(root, "sol-attempts", "operation-index"), { recursive: true });
  const db = new Database(path);
  const previous = ControlsSchema.parse(
    db
      .prepare(
        `SELECT control_key AS key, enabled FROM runtime_control WHERE control_key IN (${CONTROL_KEYS})`,
      )
      .all(),
  );
  try {
    db.exec(`INSERT INTO runtime_control VALUES('service_enabled', 0)
      ON CONFLICT(control_key) DO UPDATE SET enabled = 0;
      INSERT INTO runtime_control VALUES('global_paused', 1), ('paid_work_paused', 1),
        ('legacy_pause_imported', 1), ('legacy_service_imported', 1)
      ON CONFLICT(control_key) DO UPDATE SET enabled = 1;`);
    const plan = await importLegacySolOperations({ stateDirectory: root });
    if (plan.mode === "dry_run") {
      if (plan.records !== 0)
        throw new Error("Fixture requires empty legacy operations");
      await importLegacySolOperations({
        stateDirectory: root,
        apply: true,
        expectedDigest: plan.sourceDigest,
      });
    }
  } finally {
    db.transaction(() => {
      db.exec(
        `DELETE FROM runtime_control WHERE control_key IN (${CONTROL_KEYS})`,
      );
      const insert = db.prepare("INSERT INTO runtime_control VALUES(?, ?)");
      for (const row of previous) insert.run(row.key, row.enabled);
    }).immediate();
    db.close();
  }
}
