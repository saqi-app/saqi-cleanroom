import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { SAQI_PRODUCTION_DATABASE_ID } from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";

const DIRECTORY = fileURLToPath(
  new URL("../../../../app/migrations/", import.meta.url),
);
const MIGRATION_LEDGER = `
  CREATE TABLE d1_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  ) STRICT;
`;
const PRODUCTION_IDENTITY_SOURCES = {
  app: new URL("../../../../app/wrangler.jsonc", import.meta.url),
  sitePreview: new URL("../../../../site/wrangler.jsonc", import.meta.url),
  siteProduction: new URL(
    "../../../../site/wrangler.production.jsonc",
    import.meta.url,
  ),
} as const;

describe("production migration compatibility", () => {
  const databases: Database.Database[] = [];
  afterEach(() => {
    for (const database of databases) database.close();
    databases.length = 0;
  });

  const open = (): Database.Database => {
    const database = new Database(":memory:");
    database.pragma("foreign_keys = ON");
    database.exec(MIGRATION_LEDGER);
    databases.push(database);
    return database;
  };

  it("keeps every deployment binding and SQL sentinel on one database", () => {
    const configuredIds = Object.fromEntries(
      Object.entries(PRODUCTION_IDENTITY_SOURCES).map(([name, url]) => [
        name,
        extractOnlyDatabaseId(readFileSync(url, "utf8")),
      ]),
    );
    expect(configuredIds).toEqual({
      app: SAQI_PRODUCTION_DATABASE_ID,
      sitePreview: SAQI_PRODUCTION_DATABASE_ID,
      siteProduction: SAQI_PRODUCTION_DATABASE_ID,
    });
    expect(
      extractMigrationDatabaseId(
        readFileSync(
          join(DIRECTORY, "0037_retire_legacy_translation_tasks.sql"),
          "utf8",
        ),
      ),
    ).toBe(SAQI_PRODUCTION_DATABASE_ID);
  });

  it("creates the current schema from a fresh bootstrap and replays as a no-op", () => {
    const database = open();
    const first = applyPending(database, migrationFiles());
    expect(first).toEqual(["0037_retire_legacy_translation_tasks.sql"]);
    expectCorpusRevisionSchema(database);
    expectModelPublicationGuards(database);
    expectLegacySolPublicationPrecedence(database);
    expectLegacyPointerGuards(database);
    expectCanonicalDataGuards(database);
    expectModelProfileRegistry(database);
    expectProductionDeploymentIdentity(database);
    expectSlugIndexesReduced(database);
    expectSlugUniqueness(database);
    expect(
      database.prepare("SELECT * FROM source_lineage_maintenance_job").get(),
    ).toMatchObject({
      adopted_total: 0,
      cursor_poem_id: null,
      lease_epoch: 0,
      pass: 0,
      scanned_total: 0,
      singleton: 1,
      state: "idle",
    });
    expect(
      database
        .prepare(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'source_lineage_conflict'",
        )
        .get(),
    ).toEqual({ name: "source_lineage_conflict" });
    const before = schemaSnapshot(database);
    expect(applyPending(database, migrationFiles())).toEqual([]);
    expect(schemaSnapshot(database)).toEqual(before);
  });

  it("skips the baseline for the current deployed receipt without touching data", () => {
    const database = open();
    applyPending(database, migrationFiles());
    recordApplied(database, "0036_atomic_model_publication_pointer_upsert.sql");
    insertProductionRow(database);
    database.exec(`
      INSERT INTO task (id, type, status, created_at)
      VALUES ('historical', 'translate-poem', 'completed', 1);
      UPDATE poem SET translation = '{"content":["Preserved translation"]}';
    `);
    const before = {
      schema: schemaSnapshot(database),
      poems: database.prepare("SELECT * FROM poem").all(),
      tasks: database.prepare("SELECT * FROM task").all(),
      receipts: database
        .prepare("SELECT * FROM d1_migrations ORDER BY id")
        .all(),
    };

    expect(applyPending(database, migrationFiles())).toEqual([]);
    expect({
      schema: schemaSnapshot(database),
      poems: database.prepare("SELECT * FROM poem").all(),
      tasks: database.prepare("SELECT * FROM task").all(),
      receipts: database
        .prepare("SELECT * FROM d1_migrations ORDER BY id")
        .all(),
    }).toEqual(before);
    expect(database.pragma("foreign_key_check")).toEqual([]);
  });

  it("initializes the writer timestamp at install time rather than schema generation time", () => {
    const database = open();
    database.function("unixepoch", () => 1_700_000_000);
    applyPending(database, migrationFiles());

    expect(
      database
        .prepare("SELECT updated_at FROM scraper_writer_control")
        .pluck()
        .get(),
    ).toBe(1_700_000_000);
  });

  it.each([
    "0019_remove_legacy_favorites_tts.sql",
    "0021_materialized_public_catalog.sql",
    "0036_atomic_model_publication_pointer_upsert.sql",
  ])(
    "rejects an existing %s catalog without manufacturing a baseline receipt",
    (receipt) => {
      const database = open();
      database.exec(
        "CREATE TABLE author (id TEXT PRIMARY KEY); INSERT INTO author VALUES ('preserved');",
      );
      recordApplied(database, receipt);
      const before = schemaSnapshot(database);

      expect(() => applyPending(database, migrationFiles())).toThrow(
        /table author already exists/u,
      );
      expect(schemaSnapshot(database)).toEqual(before);
      expect(database.prepare("SELECT id FROM author").all()).toEqual([
        { id: "preserved" },
      ]);
      expect(database.prepare("SELECT name FROM d1_migrations").all()).toEqual([
        { name: receipt },
      ]);
    },
  );

  it("rejects noncanonical hashes, malformed source envelopes, and alias rewrites", () => {
    const database = open();
    applyPending(database, migrationFiles());
    database
      .prepare(
        "INSERT INTO author(id, slug, name_arabic) VALUES ('author-guard', 'author-guard', 'شاعر')",
      )
      .run();

    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_bundle (
            id, schema_version, manifest_hash, expected_record_count, status,
            writer_epoch, created_at
          ) VALUES ('bad-hash', 1, ?, 1, 'open', 1, 1)`,
        )
        .run("A".repeat(64)),
    ).toThrow(/CRAWL_IMPORT_BUNDLE_HASH_INVALID/u);

    database
      .prepare(
        `INSERT INTO crawl_import_bundle (
          id, schema_version, manifest_hash, expected_record_count, status,
          writer_epoch, created_at
        ) VALUES ('shape-guard', 1, ?, 1, 'open', 1, 1)`,
      )
      .run("a".repeat(64));
    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_record (
            bundle_id, ordinal, record_hash, source_name, source_author_id,
            source_author_url, author_name_arabic, canonical_author_id,
            source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
            content_arabic, content_hash, observed_at
          ) VALUES (
            'shape-guard', 0, ?, 'source', 'author-source', 'https://a.test',
            'شاعر', 'author-guard', 'poem-source', 'https://p.test', NULL,
            'قصيدة', '{"content":[]}', ?, 1
          )`,
        )
        .run("b".repeat(64), "c".repeat(64)),
    ).toThrow(/CRAWL_IMPORT_RECORD_DOCUMENT_INVALID/u);
    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_record (
            bundle_id, ordinal, record_hash, source_name, source_author_id,
            source_author_url, author_name_arabic, canonical_author_id,
            source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
            content_arabic, content_hash, observed_at
          ) VALUES (
            'shape-guard', 0, ?, 'source', 'author-source', 'https://a.test',
            'شاعر', 'author-guard', 'poem-source', 'https://p.test', NULL,
            'قصيدة', ?, ?, 1
          )`,
        )
        .run(
          "b".repeat(64),
          JSON.stringify({ content: ["x".repeat(2_000_001)] }),
          "c".repeat(64),
        ),
    ).toThrow(/CRAWL_IMPORT_RECORD_DOCUMENT_INVALID/u);

    database
      .prepare(
        `INSERT INTO source_author_identity (
          id, source_name, external_id, canonical_url, name_arabic,
          canonical_author_id, first_observed_at, last_observed_at
        ) VALUES (
          'source-author-guard', 'source', 'author-source', 'https://a.test',
          'شاعر', 'author-guard', 1, 1
        )`,
      )
      .run();
    expect(() =>
      database
        .prepare(
          `INSERT INTO source_author_alias (
            source_author_id, alias_url, first_observed_at, last_observed_at
          ) VALUES ('source-author-guard', 'https://alias.test', 2, 1)`,
        )
        .run(),
    ).toThrow(/SOURCE_AUTHOR_ALIAS_INVALID/u);
    database
      .prepare(
        `INSERT INTO source_author_alias (
          source_author_id, alias_url, first_observed_at, last_observed_at
        ) VALUES ('source-author-guard', 'https://alias.test', 1, 1)`,
      )
      .run();
    expect(() =>
      database
        .prepare(
          `UPDATE source_author_alias SET alias_url = 'https://rewrite.test'
           WHERE source_author_id = 'source-author-guard'`,
        )
        .run(),
    ).toThrow(/SOURCE_AUTHOR_ALIAS_IMMUTABLE/u);
    expect(() =>
      database
        .prepare(
          `DELETE FROM source_author_alias
           WHERE source_author_id = 'source-author-guard'`,
        )
        .run(),
    ).toThrow(/SOURCE_AUTHOR_ALIAS_IMMUTABLE/u);
  });

  it("enforces title-aware revision-v2 envelopes without rewriting v1", () => {
    const database = open();
    applyPending(database, migrationFiles());
    const hash = "a".repeat(64);
    database
      .prepare(
        `INSERT INTO crawl_import_bundle (
          id, schema_version, manifest_hash, expected_record_count, status,
          writer_epoch, created_at
        ) VALUES ('v2-bundle', 2, ?, 0, 'open', 1, 1)`,
      )
      .run(hash);
    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_record (
            bundle_id, ordinal, record_hash, source_name, source_author_id,
            source_author_url, author_name_arabic, canonical_author_id,
            source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
            content_arabic, content_hash, observed_at
          ) VALUES (
            'v2-bundle', 0, ?, 'primary-source', 'author', 'https://source.invalid/a',
            'شاعر', 'missing-author', '42', 'https://example.com/p', NULL,
            'العنوان', '{"content":["بيت"]}', ?, 1
          )`,
        )
        .run(hash, hash),
    ).toThrow();
    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_bundle (
            id, schema_version, manifest_hash, expected_record_count, status,
            writer_epoch, created_at
          ) VALUES ('future', 3, ?, 0, 'open', 1, 1)`,
        )
        .run("b".repeat(64)),
    ).toThrow(/CRAWL_IMPORT_SCHEMA_UNSUPPORTED/u);
  });

  it("fences the writer root and keeps profile attribution immutable", () => {
    const database = open();
    applyPending(database, migrationFiles());

    expect(() =>
      database
        .prepare(
          `UPDATE scraper_writer_control
           SET writer_epoch = 3, writer_id = 'skipped', updated_at = 1
           WHERE singleton = 1`,
        )
        .run(),
    ).toThrow(/SCRAPER_WRITER_CONTROL_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE scraper_writer_control
           SET writer_epoch = 2, writer_id = 'writer-2',
               updated_at = updated_at + 1
           WHERE singleton = 1`,
        )
        .run(),
    ).not.toThrow();
    expect(() =>
      database
        .prepare(
          `UPDATE scraper_writer_control
           SET writer_id = 'rewritten', updated_at = updated_at + 1
           WHERE singleton = 1`,
        )
        .run(),
    ).toThrow(/SCRAPER_WRITER_CONTROL_UPDATE_INVALID/u);
    expect(() =>
      database.prepare("DELETE FROM scraper_writer_control").run(),
    ).toThrow(/SCRAPER_WRITER_CONTROL_IMMUTABLE/u);

    expect(() =>
      database
        .prepare(
          "UPDATE enrichment_profile SET reasoning_effort = 'low' WHERE profile_key = 'sol-5.6/source-v2'",
        )
        .run(),
    ).toThrow(/ENRICHMENT_PROFILE_IMMUTABLE/u);

    insertProductionRow(database);
    database
      .prepare(
        `INSERT INTO poem_legacy_payload_attribution (
          poem_id, legacy_field, source_payload_hash, attribution_key,
          attributed_at
        ) VALUES (?, 'translation', ?, 'legacy-claude-1-or-2', 1)`,
      )
      .run("00000000-0000-4000-8000-000000000002", "a".repeat(64));
    expect(
      database
        .prepare(
          `SELECT hash_algorithm FROM poem_legacy_payload_attribution
           WHERE poem_id = ?`,
        )
        .pluck()
        .get("00000000-0000-4000-8000-000000000002"),
    ).toBe("sha256-utf8-exact-v1");
    expect(() =>
      database
        .prepare(
          `UPDATE poem_legacy_payload_attribution
           SET source_payload_hash = ? WHERE poem_id = ?`,
        )
        .run("b".repeat(64), "00000000-0000-4000-8000-000000000002"),
    ).toThrow(/LEGACY_PAYLOAD_ATTRIBUTION_IMMUTABLE/u);
    expect(() =>
      database
        .prepare(
          `INSERT INTO poem_legacy_payload_attribution (
            poem_id, legacy_field, source_payload_hash, attribution_key,
            attributed_at
          ) VALUES (?, 'translation_gemini', ?, 'legacy-claude-1-or-2', 1)`,
        )
        .run("00000000-0000-4000-8000-000000000002", "c".repeat(64)),
    ).toThrow(/LEGACY_PAYLOAD_ATTRIBUTION_INVALID/u);
  });
});

function extractOnlyDatabaseId(source: string): string {
  const ids = source
    .matchAll(/"database_id"\s*:\s*"([^"]+)"/gu)
    .map((match) => match[1])
    .toArray();
  expect(ids).toHaveLength(1);
  return ids[0] ?? "";
}

function extractMigrationDatabaseId(source: string): string {
  const match = /VALUES\s*\(\s*'production'\s*,\s*'([^']+)'\s*\)/u.exec(source);
  expect(match).not.toBeNull();
  return match?.[1] ?? "";
}

function migrationFiles(): readonly string[] {
  return readdirSync(DIRECTORY)
    .filter((name) => /^\d{4}_.+\.sql$/u.test(name))
    .toSorted();
}

function applyPending(
  database: Database.Database,
  names: readonly string[],
): readonly string[] {
  const applied: string[] = [];
  const run = database.transaction((name: string) => {
    const exists = database
      .prepare("SELECT 1 FROM d1_migrations WHERE name = ?")
      .get(name);
    if (exists) return false;
    database.exec(readFileSync(join(DIRECTORY, name), "utf8"));
    recordApplied(database, name);
    return true;
  });
  for (const name of names) if (run(name)) applied.push(name);
  return applied;
}

function recordApplied(database: Database.Database, name: string): void {
  database.prepare("INSERT INTO d1_migrations(name) VALUES (?)").run(name);
}

function insertProductionRow(database: Database.Database): void {
  database
    .prepare(
      `INSERT INTO author(id, slug, name_arabic)
       VALUES ('00000000-0000-4000-8000-000000000001', 'author', 'شاعر')`,
    )
    .run();
  database
    .prepare(
      `INSERT INTO poem(
         id, author_id, slug, verses, name_arabic, content_arabic
       ) VALUES (?, ?, 'poem42', 1, 'قصيدة', '{"content":["صدر","عجز"]}')`,
    )
    .run(
      "00000000-0000-4000-8000-000000000002",
      "00000000-0000-4000-8000-000000000001",
    );
}

function expectCorpusRevisionSchema(database: Database.Database): void {
  const tables = database
    .prepare(
      "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
    )
    .pluck()
    .all() as string[];
  expect(tables).toEqual(
    expect.arrayContaining([
      "crawl_import_bundle",
      "crawl_import_record",
      "crawl_import_receipt",
      "ai_model",
      "ai_vendor",
      "enrichment_artifact",
      "enrichment_profile",
      "enrichment_validation",
      "inference_backend",
      "legacy_model_attribution",
      "model_enrichment_artifact",
      "model_enrichment_artifact_profile",
      "model_enrichment_validation",
      "model_publication_receipt",
      "poem_legacy_payload_attribution",
      "poem_publication_pointer",
      "poem_model_publication_pointer",
      "poem_source_pointer",
      "poem_source_revision",
      "scraper_writer_control",
      "source_author_identity",
      "source_admission_clock",
      "source_poem_identity",
      "source_revision_fingerprint",
    ]),
  );
  const poemColumns = database.prepare("PRAGMA table_info(poem)").all() as {
    name: string;
  }[];
  expect(poemColumns.map(({ name }) => name)).toEqual(
    expect.arrayContaining([
      "active_enrichment_artifact_id",
      "active_source_revision_id",
      "insights_sol",
      "translation_sol",
    ]),
  );
  const enrichmentColumns = database
    .prepare("PRAGMA table_info(model_enrichment_artifact)")
    .all() as { name: string }[];
  expect(enrichmentColumns.map(({ name }) => name)).toContain("model_key");
  expect(
    database
      .prepare(
        "SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1",
      )
      .pluck()
      .get(),
  ).toBe(1);
}

function expectModelPublicationGuards(database: Database.Database): void {
  const triggerNames = database
    .prepare(
      `SELECT name FROM sqlite_schema
       WHERE type = 'trigger' AND tbl_name = 'poem_model_publication_pointer'
       ORDER BY name`,
    )
    .pluck()
    .all() as string[];
  expect(triggerNames).toEqual(
    expect.arrayContaining([
      "poem_model_publication_pointer_delete_forbidden",
      "poem_model_publication_pointer_insert_relationship",
      "poem_model_publication_pointer_insert_version",
      "poem_model_publication_pointer_insert_writer",
      "poem_model_publication_pointer_update_guard",
    ]),
  );
}

function expectLegacySolPublicationPrecedence(
  database: Database.Database,
): void {
  const triggerNames = database
    .prepare(
      `SELECT name FROM sqlite_schema
       WHERE type = 'trigger'
         AND name IN (
           'legacy_sol_model_pointer_create_only',
           'legacy_sol_publication_receipt_create_only'
         )
       ORDER BY name`,
    )
    .pluck()
    .all();
  expect(triggerNames).toEqual([
    "legacy_sol_model_pointer_create_only",
    "legacy_sol_publication_receipt_create_only",
  ]);
}

function expectLegacyPointerGuards(database: Database.Database): void {
  const triggerNames = database
    .prepare(
      `SELECT name FROM sqlite_schema
       WHERE type = 'trigger'
         AND tbl_name IN ('poem_source_pointer', 'poem_publication_pointer')
       ORDER BY name`,
    )
    .pluck()
    .all() as string[];
  expect(triggerNames).toEqual(
    expect.arrayContaining([
      "poem_publication_pointer_delete_forbidden",
      "poem_publication_pointer_insert_guard",
      "poem_publication_pointer_update_guard",
      "poem_source_pointer_delete_forbidden",
      "poem_source_pointer_insert_guard",
      "poem_source_pointer_update_guard",
    ]),
  );
}

function expectCanonicalDataGuards(database: Database.Database): void {
  const triggerNames = database
    .prepare(
      `SELECT name FROM sqlite_schema
       WHERE type = 'trigger' ORDER BY name`,
    )
    .pluck()
    .all() as string[];
  expect(triggerNames).toEqual(
    expect.arrayContaining([
      "crawl_import_bundle_canonical_hash_insert",
      "crawl_import_record_document_insert",
      "poem_source_revision_document_insert",
      "enrichment_artifact_document_insert",
      "enrichment_validation_document_insert",
      "model_enrichment_artifact_document_insert",
      "model_enrichment_validation_document_insert",
      "crawl_import_receipt_canonical_hash_insert",
      "source_author_alias_insert_guard",
      "source_author_alias_update_guard",
      "source_author_alias_delete_forbidden",
    ]),
  );
}

function expectProductionDeploymentIdentity(database: Database.Database): void {
  expect(
    database
      .prepare(
        `SELECT scope, database_id
           FROM production_deployment_identity
          ORDER BY scope`,
      )
      .all(),
  ).toEqual([
    {
      database_id: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
      scope: "production",
    },
  ]);
  expect(() =>
    database
      .prepare(
        `INSERT INTO production_deployment_identity (scope, database_id)
         VALUES ('preview', '11111111-1111-4111-8111-111111111111')`,
      )
      .run(),
  ).toThrow();
}

function expectModelProfileRegistry(database: Database.Database): void {
  const tables = database
    .prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")
    .pluck()
    .all() as string[];
  expect(tables).toEqual(
    expect.arrayContaining([
      "ai_model",
      "ai_vendor",
      "enrichment_profile",
      "inference_backend",
      "legacy_model_attribution",
      "model_enrichment_artifact_profile",
      "poem_legacy_payload_attribution",
    ]),
  );
  expect(
    database.prepare("SELECT count(*) FROM enrichment_profile").pluck().get(),
  ).toBe(6);
  expect(
    database
      .prepare(
        "SELECT count(*) FROM enrichment_profile WHERE output_schema_version = 2",
      )
      .pluck()
      .get(),
  ).toBe(4);
  expect(
    database
      .prepare(
        `SELECT display_name FROM legacy_model_attribution
         WHERE attribution_key = 'legacy-claude-1-or-2' -- gitleaks:allow: public legacy attribution label
        `,
      )
      .pluck()
      .get(),
  ).toBe("Claude 1 or 2");
  expect(
    database
      .prepare(
        `SELECT certainty, display_name FROM legacy_model_attribution
         WHERE attribution_key = 'legacy-gemini-unknown'`,
      )
      .get(),
  ).toEqual({
    certainty: "unknown",
    display_name: "Gemini (legacy model unknown)",
  });
  const triggers = database
    .prepare("SELECT name FROM sqlite_schema WHERE type = 'trigger'")
    .pluck()
    .all() as string[];
  expect(triggers).toEqual(
    expect.arrayContaining([
      "model_enrichment_artifact_profile_bind",
      "model_enrichment_artifact_profile_required",
      "model_enrichment_word_gloss_v2_shape",
      "model_publication_profile_insert_guard",
      "model_publication_profile_update_guard",
      "scraper_writer_control_delete_forbidden",
      "scraper_writer_control_update_guard",
    ]),
  );
}

function expectSlugIndexesReduced(database: Database.Database): void {
  const authorIndexes = indexNames(database, "author");
  const poemIndexes = indexNames(database, "poem");
  expect(authorIndexes).not.toContain("idx_author_slug");
  expect(poemIndexes).not.toContain("idx_poem_slug");
  expect(authorIndexes).toContain("sqlite_autoindex_author_2");
  expect(poemIndexes).toContain("sqlite_autoindex_poem_2");

  const authorPlan = queryPlan(
    database,
    "SELECT id FROM author WHERE slug = ?",
  );
  const poemPlan = queryPlan(database, "SELECT id FROM poem WHERE slug = ?");
  expect(authorPlan).toContain("sqlite_autoindex_author_2");
  expect(poemPlan).toContain("sqlite_autoindex_poem_2");
}

function expectSlugUniqueness(database: Database.Database): void {
  database
    .prepare("INSERT INTO author(id, slug, name_arabic) VALUES (?, ?, ?)")
    .run("unique-author-1", "unique-author", "شاعر");
  expect(() =>
    database
      .prepare("INSERT INTO author(id, slug, name_arabic) VALUES (?, ?, ?)")
      .run("unique-author-2", "unique-author", "شاعر ثان"),
  ).toThrow(/UNIQUE constraint failed: author\.slug/u);

  database
    .prepare(
      `INSERT INTO poem(id, author_id, slug, verses, name_arabic, content_arabic)
       VALUES (?, ?, ?, ?, ?, ?)`,
    )
    .run(
      "unique-poem-1",
      "unique-author-1",
      "unique-poem",
      1,
      "قصيدة",
      '{"content":["صدر","عجز"]}',
    );
  expect(() =>
    database
      .prepare(
        `INSERT INTO poem(id, author_id, slug, verses, name_arabic, content_arabic)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        "unique-poem-2",
        "unique-author-1",
        "unique-poem",
        1,
        "قصيدة ثانية",
        '{"content":["صدر","عجز"]}',
      ),
  ).toThrow(/UNIQUE constraint failed: poem\.slug/u);
}

function indexNames(database: Database.Database, tableName: string): string[] {
  return database
    .prepare("SELECT name FROM pragma_index_list(?) ORDER BY name")
    .pluck()
    .all(tableName) as string[];
}

function queryPlan(database: Database.Database, query: string): string {
  return (
    database.prepare(`EXPLAIN QUERY PLAN ${query}`).all("slug") as {
      detail: string;
    }[]
  )
    .map(({ detail }) => detail)
    .join("\n");
}

function schemaSnapshot(database: Database.Database): string {
  return JSON.stringify(
    database
      .prepare(
        `SELECT type, name, tbl_name, sql FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`,
      )
      .all(),
  );
}
