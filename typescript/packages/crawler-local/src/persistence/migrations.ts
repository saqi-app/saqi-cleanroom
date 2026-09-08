import { currentSource } from "@saqi/source-adapter";
import type Database from "better-sqlite3";
import { z } from "zod";

import {
  CURRENT_SCHEMA_VERSION,
  FRESH_BASELINE_SQL,
  UPGRADE_V35_TO_V36_SQL,
} from "./baseline-schema.js";

interface LedgerMigrationPort {
  assertConfiguredSourceIdentity(): void;
  migrate(): number;
}

const READ_LOCAL_SCHEMA_VERSION_SQL =
  "SELECT version FROM local_schema WHERE singleton = 1";
const READ_USER_SCHEMA_OBJECTS =
  "SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'";
const LocalSchemaVersionRowSchema = z.strictObject({
  version: z.int().nonnegative(),
});
const ExistingSchemaObjectsSchema = z.array(
  z.strictObject({ name: z.string() }),
);
const LocalSourceIdentityRowSchema = z.strictObject({
  source_name: z.string(),
  source_origin: z.string(),
});
const UpgradeAdmissionSchema = z.strictObject({
  held: z.literal(0),
  running: z.literal(0),
});

export { CURRENT_SCHEMA_VERSION } from "./baseline-schema.js";

/** Initializes the one public baseline or upgrades the final private schema.
 * Older ledgers must first be upgraded with the archived private binary. */
export class LedgerMigrator implements LedgerMigrationPort {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  migrate(): number {
    const objects = ExistingSchemaObjectsSchema.parse(
      this.#database.prepare(READ_USER_SCHEMA_OBJECTS).all(),
    );
    if (objects.length === 0) return this.#initializeFresh();
    if (!objects.some(({ name }) => name === "local_schema"))
      throw new Error("RUNTIME_OWNER_UNVERSIONED_NONEMPTY_LEDGER");

    const version = this.#readVersion();
    if (version === CURRENT_SCHEMA_VERSION) {
      this.assertConfiguredSourceIdentity();
      this.#assertPublicSchema();
      return version;
    }
    if (version < 35)
      throw new Error("PUBLIC_BASELINE_REQUIRES_ARCHIVED_SCHEMA35_UPGRADE");
    if (version > CURRENT_SCHEMA_VERSION)
      throw new Error(
        `Ledger schema ${String(version)} is newer than supported schema ${String(CURRENT_SCHEMA_VERSION)}`,
      );
    return this.#upgradeVersion35();
  }

  assertConfiguredSourceIdentity(): void {
    const version = this.#readVersion();
    if (version < 35)
      throw new Error("PUBLIC_BASELINE_REQUIRES_ARCHIVED_SCHEMA35_UPGRADE");
    const configured = currentSource();
    const persisted = LocalSourceIdentityRowSchema.parse(
      this.#database
        .prepare(
          `SELECT source_name, source_origin
             FROM local_source_identity WHERE singleton = 1`,
        )
        .get(),
    );
    if (
      persisted.source_name !== configured.name ||
      persisted.source_origin !== configured.origin
    )
      throw new Error("LOCAL_SOURCE_IDENTITY_MISMATCH");
  }

  #initializeFresh(): number {
    return this.#database
      .transaction(() => {
        if (
          this.#database.prepare(READ_USER_SCHEMA_OBJECTS).get() !== undefined
        )
          throw new Error("PUBLIC_BASELINE_FRESH_BOOTSTRAP_CHANGED");
        this.#database.exec(FRESH_BASELINE_SQL);
        this.#database
          .prepare("INSERT INTO local_schema(singleton, version) VALUES(1, ?)")
          .run(CURRENT_SCHEMA_VERSION);
        this.#database.exec(`
          INSERT INTO ledger_status_clock(singleton) VALUES(1);
          INSERT INTO source_author_metadata_revision(singleton, revision)
            VALUES(1, 0);
          INSERT INTO sol_poem_milestone_backfill(
            event_type, cursor_sequence, high_watermark, completed_at
          ) VALUES('succeeded', 0, 0, NULL), ('imported', 0, 0, NULL);
          INSERT INTO runtime_owner(singleton, epoch, held) VALUES(1, 0, 0);
        `);
        const source = currentSource();
        this.#database
          .prepare(
            `INSERT INTO local_source_identity(
               singleton, source_name, source_origin
             ) VALUES(1, ?, ?)`,
          )
          .run(source.name, source.origin);
        this.assertConfiguredSourceIdentity();
        this.#assertPublicSchema();
        return CURRENT_SCHEMA_VERSION;
      })
      .immediate();
  }

  #upgradeVersion35(): number {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const lockedVersion = this.#readVersion();
      if (lockedVersion === CURRENT_SCHEMA_VERSION) {
        this.#database.exec("COMMIT");
        this.assertConfiguredSourceIdentity();
        this.#assertPublicSchema();
        return CURRENT_SCHEMA_VERSION;
      }
      if (lockedVersion !== 35)
        throw new Error("PUBLIC_BASELINE_VERSION_FENCE");
      this.assertConfiguredSourceIdentity();
      UpgradeAdmissionSchema.parse(
        this.#database
          .prepare(
            `SELECT
               (SELECT held FROM runtime_owner WHERE singleton = 1) AS held,
               (SELECT COUNT(*) FROM work_item WHERE state = 'running') AS running`,
          )
          .get(),
      );
      this.#database.exec(UPGRADE_V35_TO_V36_SQL);
      if (this.#readVersion() !== CURRENT_SCHEMA_VERSION)
        throw new Error("PUBLIC_BASELINE_VERSION_FENCE");
      this.#assertPublicSchema();
      this.#database.exec("COMMIT");
      return CURRENT_SCHEMA_VERSION;
    } catch (error) {
      this.#database.exec("ROLLBACK");
      throw error;
    }
  }

  #readVersion(): number {
    return LocalSchemaVersionRowSchema.parse(
      this.#database.prepare(READ_LOCAL_SCHEMA_VERSION_SQL).get(),
    ).version;
  }

  #assertPublicSchema(): void {
    const names = new Set(
      ExistingSchemaObjectsSchema.parse(
        this.#database.prepare(READ_USER_SCHEMA_OBJECTS).all(),
      ).map(({ name }) => name),
    );
    for (const name of [
      "local_source_identity",
      "poem_identity",
      "runtime_owner",
      "sol_operation",
      "source_author_metadata",
      "work_item",
      "work_item_poem_definition_lookup",
    ])
      if (!names.has(name)) throw new Error(`PUBLIC_BASELINE_MISSING_${name}`);
    if (names.has("retired_scheduler_state"))
      throw new Error("PUBLIC_BASELINE_RETIRED_STATE_PRESENT");
  }
}
