import { currentSource } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { expect, test } from "vitest";
import { z } from "zod";

import {
  CURRENT_SCHEMA_VERSION,
  FRESH_BASELINE_SQL,
} from "../persistence/baseline-schema.js";
import { LedgerMigrator } from "../persistence/migrations.js";

test("fresh initialization installs the complete source-neutral v36 baseline", () => {
  const database = new Database(":memory:");
  const migrator = new LedgerMigrator(database);
  expect(migrator.migrate()).toBe(CURRENT_SCHEMA_VERSION);
  expect(migrator.migrate()).toBe(CURRENT_SCHEMA_VERSION);
  expect(database.prepare("SELECT total_changes()").pluck().get()).toBe(7);
  expect(database.prepare("PRAGMA integrity_check").pluck().get()).toBe("ok");
  expect(database.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
  expect(
    database
      .prepare("SELECT version FROM local_schema WHERE singleton=1")
      .pluck()
      .get(),
  ).toBe(36);
  expect(
    database
      .prepare(
        `SELECT name FROM sqlite_schema
         WHERE name IN ('retired_scheduler_state','work_item_poem_definition_unique')`,
      )
      .all(),
  ).toEqual([]);
  const schemaText = database
    .prepare("SELECT group_concat(sql, '') FROM sqlite_schema")
    .pluck()
    .get();
  expect(schemaText).toEqual(expect.any(String));
  expect(schemaText).not.toMatch(/\/poem|source\.invalid|agy|claude/u);
  database.close();
});

test("the fresh baseline is fully transactional", () => {
  const database = new Database(":memory:");
  expect(() =>
    database.transaction(() => {
      database.exec(FRESH_BASELINE_SQL);
      throw new Error("injected failure");
    })(),
  ).toThrow("injected failure");
  expect(
    database
      .prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'")
      .all(),
  ).toEqual([]);
  database.close();
});

test("v35 upgrade preserves retained rows and converges on the fresh object manifest", () => {
  const upgraded = new Database(":memory:");
  initializeVersion35Fixture(upgraded);
  const before = retainedRows(upgraded);
  const countsBefore = retainedTableCounts(upgraded);
  expect(new LedgerMigrator(upgraded).migrate()).toBe(36);
  expect(retainedRows(upgraded)).toEqual(before);
  expect(retainedTableCounts(upgraded)).toEqual(countsBefore);
  expect(upgraded.prepare("PRAGMA integrity_check").pluck().get()).toBe("ok");
  expect(upgraded.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
  expect(
    upgraded
      .prepare(
        "SELECT name FROM sqlite_schema WHERE name='retired_scheduler_state'",
      )
      .get(),
  ).toBeUndefined();

  const fresh = new Database(":memory:");
  new LedgerMigrator(fresh).migrate();
  expect(schemaManifest(upgraded)).toEqual(schemaManifest(fresh));
  fresh.close();
  upgraded.close();
});

test.each([
  ["held owner", { held: true, running: false }],
  ["running work", { held: false, running: true }],
] as const)(
  "v35 upgrade refuses %s without partial writes",
  (_name, options) => {
    const database = new Database(":memory:");
    initializeVersion35Fixture(database, options);
    const before = schemaManifest(database);
    expect(() => new LedgerMigrator(database).migrate()).toThrow();
    expect(schemaManifest(database)).toEqual(before);
    expect(
      database.prepare("SELECT version FROM local_schema").pluck().get(),
    ).toBe(35);
    database.close();
  },
);

test("a compatibility DDL failure rolls back prior schema changes", () => {
  const database = new Database(":memory:");
  initializeVersion35Fixture(database);
  database.exec("DROP TRIGGER poem_identity_reject_delete");
  expect(() => new LedgerMigrator(database).migrate()).toThrow();
  expect(
    database
      .prepare(
        "SELECT name FROM sqlite_schema WHERE name='poem_work_requires_registered_identity_insert'",
      )
      .pluck()
      .get(),
  ).toBe("poem_work_requires_registered_identity_insert");
  expect(
    database.prepare("SELECT version FROM local_schema").pluck().get(),
  ).toBe(35);
  database.close();
});

test.each([0, 1, 34])("schema %i fails closed", (version) => {
  const database = new Database(":memory:");
  database.exec(
    `CREATE TABLE local_schema(singleton INTEGER PRIMARY KEY, version INTEGER NOT NULL) STRICT;
     INSERT INTO local_schema VALUES(1, ${String(version)});`,
  );
  expect(() => new LedgerMigrator(database).migrate()).toThrow(
    "PUBLIC_BASELINE_REQUIRES_ARCHIVED_SCHEMA35_UPGRADE",
  );
  expect(
    database.prepare("SELECT version FROM local_schema").pluck().get(),
  ).toBe(version);
  database.close();
});

test("unversioned nonempty and future schemas fail closed", () => {
  const unversioned = new Database(":memory:");
  unversioned.exec("CREATE TABLE existing(value TEXT) STRICT");
  expect(() => new LedgerMigrator(unversioned).migrate()).toThrow(
    "RUNTIME_OWNER_UNVERSIONED_NONEMPTY_LEDGER",
  );
  expect(
    unversioned.prepare("SELECT name FROM sqlite_schema").pluck().all(),
  ).toContain("existing");
  unversioned.close();

  const future = new Database(":memory:");
  future.exec(
    "CREATE TABLE local_schema(singleton INTEGER PRIMARY KEY, version INTEGER NOT NULL) STRICT; INSERT INTO local_schema VALUES(1, 37)",
  );
  expect(() => new LedgerMigrator(future).migrate()).toThrow(
    "newer than supported",
  );
  future.close();
});

function initializeVersion35Fixture(
  database: Database.Database,
  options: { readonly held?: boolean; readonly running?: boolean } = {},
): void {
  database.exec(
    FRESH_BASELINE_SQL.replace("CHECK(version = 36)", "CHECK(version >= 0)"),
  );
  const source = currentSource();
  database.prepare("INSERT INTO local_schema VALUES(1, 35)").run();
  database
    .prepare("INSERT INTO local_source_identity VALUES(1, ?, ?)")
    .run(source.name, source.origin);
  database.exec(`
    INSERT INTO ledger_status_clock(singleton) VALUES(1);
    INSERT INTO source_author_metadata_revision VALUES(1, 0);
    INSERT INTO sol_poem_milestone_backfill VALUES('succeeded',0,0,NULL),('imported',0,0,NULL);
    INSERT INTO runtime_owner(singleton,epoch,held,owner_kind,pid,run_id,config_digest,started_at)
      VALUES(1,${options.held ? "1,1,'supervisor',123,'00000000-0000-4000-8000-000000000001','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','2026-01-01T00:00:00.000Z'" : "0,0,NULL,NULL,NULL,NULL,NULL"});
  `);
  database.exec(`
    DROP TRIGGER poem_identity_reject_conflicting_insert;
    DROP TRIGGER poem_identity_requires_configured_source_insert;
    DROP TRIGGER poem_identity_reject_identity_update;
    DROP TRIGGER poem_identity_reject_delete;
    DROP TRIGGER poem_work_requires_registered_identity_insert;
    DROP TRIGGER poem_work_reject_duplicate_definition_insert;
    DROP TRIGGER poem_work_requires_registered_identity_update;
    DROP INDEX work_item_poem_definition_lookup;
    DROP TABLE poem_identity;
    CREATE TABLE poem_identity(
      source_name TEXT NOT NULL, poem_href TEXT NOT NULL,
      poem_numeric_id TEXT, author_href TEXT NOT NULL,
      first_work_key TEXT NOT NULL, created_at INTEGER NOT NULL,
      PRIMARY KEY(source_name, poem_href)
    ) STRICT;
    CREATE UNIQUE INDEX work_item_poem_definition_unique
      ON work_item(implementation_version,schema_version,input_hash)
      WHERE kind='private_poem_detail';
    CREATE TRIGGER poem_identity_reject_conflicting_insert BEFORE INSERT ON poem_identity BEGIN SELECT 1; END;
    CREATE TRIGGER poem_identity_reject_identity_update BEFORE UPDATE ON poem_identity BEGIN SELECT 1; END;
    CREATE TRIGGER poem_identity_reject_delete BEFORE DELETE ON poem_identity BEGIN SELECT 1; END;
    CREATE TRIGGER poem_work_requires_registered_identity_insert BEFORE INSERT ON work_item BEGIN SELECT 1; END;
    CREATE TRIGGER poem_work_reject_duplicate_definition_insert BEFORE INSERT ON work_item BEGIN SELECT 1; END;
    CREATE TRIGGER poem_work_requires_registered_identity_update BEFORE UPDATE ON work_item BEGIN SELECT 1; END;
    DROP TRIGGER source_author_metadata_requires_configured_source_insert;
    DROP TRIGGER source_author_metadata_requires_configured_source_update;
    DROP TRIGGER source_author_metadata_revision_insert;
    DROP TRIGGER source_author_metadata_revision_update;
    DROP TABLE source_author_metadata;
    CREATE TABLE source_author_metadata(
      source_name TEXT NOT NULL, author_href TEXT NOT NULL,
      author_name_arabic TEXT NOT NULL, refresh_generation TEXT NOT NULL,
      observed_at INTEGER NOT NULL, PRIMARY KEY(source_name,author_href)
    ) STRICT;
    CREATE TRIGGER source_author_metadata_revision_insert AFTER INSERT ON source_author_metadata BEGIN SELECT 1; END;
    CREATE TRIGGER source_author_metadata_revision_update AFTER UPDATE ON source_author_metadata BEGIN SELECT 1; END;
    CREATE TABLE retired_scheduler_state(state_key TEXT PRIMARY KEY, state_json TEXT NOT NULL) STRICT;
    CREATE TRIGGER retired_scheduler_state_reject_update BEFORE UPDATE ON retired_scheduler_state BEGIN SELECT 1; END;
    CREATE TRIGGER retired_scheduler_state_reject_delete BEFORE DELETE ON retired_scheduler_state BEGIN SELECT 1; END;
  `);
  database
    .prepare(
      "INSERT INTO poem_identity(source_name,poem_href,poem_numeric_id,author_href,first_work_key,created_at) VALUES(?,?,?,?,?,?)",
    )
    .run(
      source.name,
      `${source.origin}/work/42`,
      "42",
      `${source.origin}/writer/a`,
      "a".repeat(64),
      1,
    );
  database
    .prepare("INSERT INTO source_author_metadata VALUES(?,?,?,?,?)")
    .run(source.name, `${source.origin}/writer/a`, "شاعر", "generation", 1);
  database
    .prepare("INSERT INTO retired_scheduler_state VALUES('old','{}')")
    .run();
  database
    .prepare(
      "INSERT INTO scheduler_state(state_key,state_json,state_digest,updated_at) VALUES('retained','{}',?,1)",
    )
    .run("b".repeat(64));
  if (options.running)
    database
      .prepare(
        `INSERT INTO work_item(
           work_key,kind,input_json,input_hash,schema_version,
           implementation_version,state,available_at,lease_owner,lease_token,
           lease_epoch,lease_expires_at,created_at,updated_at
         ) VALUES(?,?,?,?,?,?,'running',1,'owner','token',1,2,1,1)`,
      )
      .run("c".repeat(64), "test", "{}", "d".repeat(64), "v1", "v1");
}

function retainedRows(database: Database.Database): unknown {
  return {
    poem: database
      .prepare(
        "SELECT source_name,poem_href,author_href,first_work_key,created_at FROM poem_identity",
      )
      .all(),
    scheduler: database.prepare("SELECT * FROM scheduler_state").all(),
    source: database.prepare("SELECT * FROM source_author_metadata").all(),
  };
}

function schemaManifest(database: Database.Database): unknown {
  return database
    .prepare(
      `SELECT type,name,tbl_name FROM sqlite_schema
       WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name,tbl_name`,
    )
    .all();
}

function retainedTableCounts(
  database: Database.Database,
): Readonly<Record<string, number>> {
  const tables = z.array(z.string()).parse(
    database
      .prepare(
        `SELECT name FROM sqlite_schema
       WHERE type='table' AND name NOT GLOB 'sqlite_*'
         AND name <> 'retired_scheduler_state'
       ORDER BY name`,
      )
      .pluck()
      .all(),
  );
  return Object.fromEntries(
    tables.map((name) => {
      if (!/^[a-z_]+$/u.test(name))
        throw new Error("Unsafe fixture table name");
      return [
        name,
        z
          .number()
          .int()
          .nonnegative()
          .parse(
            database.prepare(`SELECT COUNT(*) FROM ${name}`).pluck().get(),
          ),
      ];
    }),
  );
}
