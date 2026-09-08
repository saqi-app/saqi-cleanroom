import type {
  D1Database,
  D1PreparedStatement,
  D1Result,
} from "@cloudflare/workers-types";
import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";

import { D1SourceLineageMaintenanceRepository } from "../source-lineage-maintenance";

const IDS = {
  bare: "00000000-0000-4000-8000-000000000001",
  coherent: "00000000-0000-4000-8000-000000000002",
  partial: "00000000-0000-4000-8000-000000000003",
  unrelated: "00000000-0000-4000-8000-000000000004",
} as const;

describe("D1 source lineage maintenance repository", () => {
  const databases: Database.Database[] = [];
  afterEach(() => {
    for (const database of databases) database.close();
    databases.length = 0;
  });

  it("selects only eligible configured-source gaps, including partial and unrelated lineage", async () => {
    const sqlite = new Database(":memory:");
    databases.push(sqlite);
    sqlite.exec(`
      CREATE TABLE author (id TEXT PRIMARY KEY);
      CREATE TABLE poem (
        id TEXT PRIMARY KEY, author_id TEXT, slug TEXT,
        active_source_revision_id TEXT
      );
      CREATE TABLE source_author_identity (
        id TEXT PRIMARY KEY, source_name TEXT, canonical_author_id TEXT
      );
      CREATE TABLE source_poem_identity (
        id TEXT PRIMARY KEY, source_name TEXT, external_id TEXT, source_author_id TEXT,
        canonical_poem_id TEXT, tombstoned_at INTEGER
      );
      CREATE TABLE poem_source_revision (id TEXT PRIMARY KEY, source_poem_id TEXT);
      CREATE TABLE poem_source_pointer (source_poem_id TEXT, revision_id TEXT);
      CREATE TABLE source_lineage_conflict (
        poem_id TEXT PRIMARY KEY, error_code TEXT, first_seen_at INTEGER,
        last_seen_at INTEGER, attempt_count INTEGER, lease_epoch INTEGER,
        resolved_at INTEGER
      );
      CREATE TABLE source_lineage_maintenance_job (
        singleton INTEGER PRIMARY KEY, state TEXT, cursor_poem_id TEXT,
        lease_owner TEXT, lease_epoch INTEGER, lease_expires_at INTEGER,
        lease_token TEXT, pass INTEGER, scanned_total INTEGER,
        adopted_total INTEGER, last_error_code TEXT, updated_at INTEGER
      );
      INSERT INTO author VALUES ('author');
      INSERT INTO source_author_identity VALUES
        ('author-current', 'configured', 'author'),
        ('author-other', 'other-source', 'author');
      INSERT INTO poem_source_revision VALUES
        ('revision-current', 'poem-current'),
        ('revision-other', 'poem-other');
      INSERT INTO poem_source_pointer VALUES
        ('poem-current', 'revision-current'),
        ('poem-other', 'revision-other');
      INSERT INTO source_lineage_maintenance_job VALUES
        (1, 'active', NULL, 'test', 1, 100,
         '00000000-0000-4000-8000-000000000010', 0, 0, 0, NULL, 0);
    `);
    const insertPoem = sqlite.prepare(
      "INSERT INTO poem VALUES (?, 'author', ?, ?)"
    );
    insertPoem.run(IDS.bare, "work-1", null);
    insertPoem.run(IDS.coherent, "work-2", "revision-current");
    insertPoem.run(IDS.partial, "work-3", null);
    insertPoem.run(IDS.unrelated, "work-4", "revision-other");
    insertPoem.run("00000000-0000-4000-8000-000000000005", "other-5", null);
    insertPoem.run("00000000-0000-4000-8000-000000000006", "work-bad", null);
    const insertIdentity = sqlite.prepare(
      "INSERT INTO source_poem_identity VALUES (?, ?, ?, ?, ?, NULL)"
    );
    insertIdentity.run(
      "poem-current",
      "configured",
      "2",
      "author-current",
      IDS.coherent
    );
    insertIdentity.run(
      "poem-partial",
      "configured",
      "3",
      "author-current",
      IDS.partial
    );
    insertIdentity.run(
      "poem-other",
      "other-source",
      "4",
      "author-other",
      IDS.unrelated
    );
    const repository = new D1SourceLineageMaintenanceRepository(
      d1Database(sqlite),
      {
        sourceName: "configured",
        sourceProfile: DEFAULT_SOURCE_ADAPTER_PROFILE,
      }
    );

    await expect(
      repository.poemIds({
        cursorPoemId: null,
        leaseEpoch: 1,
        leaseToken: "00000000-0000-4000-8000-000000000010",
        pass: 0,
      })
    ).resolves.toEqual([IDS.bare, IDS.partial, IDS.unrelated]);
    await expect(repository.remaining()).resolves.toBe(3);
    const lease = {
      cursorPoemId: null,
      leaseEpoch: 1,
      leaseToken: "00000000-0000-4000-8000-000000000010",
      pass: 0,
    };
    const conflict = [{ code: "SOURCE_LINEAGE_CONFLICT", poemId: IDS.partial }];
    await repository.quarantine(lease, conflict, 10);
    await repository.quarantine(lease, conflict, 11);
    expect(
      sqlite
        .prepare(
          "SELECT COUNT(*) AS count, attempt_count FROM source_lineage_conflict"
        )
        .get()
    ).toEqual({ attempt_count: 2, count: 1 });
    await expect(repository.status()).resolves.toMatchObject({
      conflictTotal: 1,
    });
  });
});

function d1Database(database: Database.Database): D1Database {
  function unexpected(): never {
    throw new Error("Unexpected database operation");
  }
  const prepare = (query: string): D1PreparedStatement => statement(query, []);
  const statement = (
    query: string,
    values: readonly unknown[]
  ): D1PreparedStatement => ({
    all: async <T = unknown>() =>
      z.custom<D1Result<T>>().parse({
        meta: meta(),
        results: database.prepare(query).all(...values),
        success: true,
      }),
    bind: (...nextValues) => statement(query, nextValues),
    first: async <T = Record<string, unknown>>(column?: string) => {
      const value: unknown = database.prepare(query).get(...values);
      if (value === undefined) return null;
      const record = z.record(z.string(), z.unknown()).parse(value);
      return z.custom<T>().parse(column ? record[column] : record);
    },
    raw: unexpected,
    run: async <T = Record<string, unknown>>() => {
      const result = database.prepare(query).run(...values);
      return z.custom<D1Result<T>>().parse({
        meta: { ...meta(), changes: result.changes },
        results: [],
        success: true,
      });
    },
  });
  return {
    batch: async <T = unknown>(statements: D1PreparedStatement[]) =>
      z
        .custom<D1Result<T>[]>()
        .parse(await Promise.all(statements.map((value) => value.run<T>()))),
    dump: unexpected,
    exec: unexpected,
    prepare,
    withSession: unexpected,
  };
}

function meta() {
  return {
    changed_db: false,
    changes: 0,
    duration: 0,
    last_row_id: 0,
    rows_read: 0,
    rows_written: 0,
    size_after: 0,
  };
}
