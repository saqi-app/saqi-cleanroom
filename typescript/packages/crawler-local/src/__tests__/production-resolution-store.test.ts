import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import Database from "better-sqlite3";
import { afterEach, describe, expect, expectTypeOf, it } from "vitest";

import { collectionWorkKinds } from "../collection/collection-scheduler";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import {
  exportProductionResolution,
  ProductionResolutionStore,
} from "../persistence/production-resolution-store";
import type { WorkItem } from "../persistence/schema";
import { SqliteQueryValidationError } from "../persistence/sqlite-query";
import {
  canonicalJson,
  inputHash,
  sha256,
  workKey as calculateWorkKey,
} from "../persistence/work-key";

const ROOTS: string[] = [];
const OBSERVED_AT = "2026-08-27T00:00:00.000Z";

afterEach(() => {
  for (const root of ROOTS) rmSync(root, { force: true, recursive: true });
  ROOTS.length = 0;
});

describe("production resolution store", () => {
  it("exports indexed identity and independent model pointers, then replays exactly", async () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 42);
    fixture.database
      .prepare("INSERT INTO poem_publication_pointer VALUES (?, ?)")
      .run(poemId(42), 3);
    fixture.database
      .prepare("INSERT INTO poem_model_publication_pointer VALUES (?, ?, ?)")
      .run(poemId(42), "historical-model", 4);
    fixture.database.close();

    const first = await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    expectTypeOf(first.poemCount).toEqualTypeOf<number>();
    expectTypeOf(first.writerEpoch).toEqualTypeOf<number>();
    expect(first).toMatchObject({
      modelPointerCount: 1,
      poemCount: 1,
      promotion: "created",
      replayed: false,
      writerEpoch: 7,
    });
    expect(statSync(fixture.output).mode & 0o777).toBe(0o600);
    const store = await ProductionResolutionStore.open(fixture.output);
    await expect(
      store.resolve(sourceWork(42), detail(42)),
    ).resolves.toMatchObject({
      mapping: {
        poemId: poemId(42),
        sourceAuthorSlug: "poet",
        sourcePoemId: "42",
      },
      observedAt: OBSERVED_AT,
      writerEpoch: 7,
    });
    await expect(
      store.resolvePublication(poemId(42), "historical-model", false),
    ).resolves.toEqual({ expectedPointerVersion: 4, writerEpoch: 7 });
    await expect(
      store.resolvePublication(poemId(42), "sol-5.6", true),
    ).resolves.toEqual({
      expectedPointerVersion: 3,
      writerEpoch: 7,
    });
    await expect(
      store.resolvePublication(poemId(42), "missing-model", false),
    ).resolves.toEqual({
      expectedPointerVersion: null,
      writerEpoch: 7,
    });
    await expect(
      store.resolvePublication(
        poemId(42),
        "historical-model",
        false,
        "revision-42",
      ),
    ).resolves.toEqual({ expectedPointerVersion: 4, writerEpoch: 7 });
    await expect(
      store.resolvePublication(
        poemId(42),
        "historical-model",
        false,
        "revision-stale",
      ),
    ).resolves.toBeNull();
    await expect(
      store.resolve(sourceWork(42, "wrong"), detail(42, "wrong")),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_AUTHOR_MISMATCH");
    store.close();

    const replay = await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    expect(replay).toMatchObject({ promotion: "replayed", replayed: true });
    expect(replay.manifestSha256).toBe(first.manifestSha256);
  });

  it("atomically hot reloads a new generation and detects corruption", async () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 1);
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    const live = await ProductionResolutionStore.open(fixture.output);
    const liveReport = await live.report();
    const firstManifest = liveReport.manifestSha256;
    const changedSource = new Database(fixture.source);
    changedSource.prepare("UPDATE author SET name_arabic = 'مختلف'").run();
    changedSource.close();
    const refreshed = await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    expect(refreshed.manifestSha256).not.toBe(firstManifest);
    const liveResolution = await live.resolve(sourceWork(1), detail(1));
    expect(liveResolution?.mapping.authorNameArabic).toBe("مختلف");
    live.close();
    const corrupt = new Database(fixture.output);
    corrupt
      .prepare("UPDATE poem_resolution SET author_name_arabic = 'فاسد'")
      .run();
    corrupt.close();
    await expect(
      ProductionResolutionStore.open(fixture.output),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_MANIFEST_MISMATCH");
  });

  it("rechecks bounded snapshot freshness for long-running resolvers", async () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 5);
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    let now = Date.parse(OBSERVED_AT) + 1_000;
    const store = await ProductionResolutionStore.open(fixture.output, {
      maximumAgeMs: 60_000,
      now: () => now,
    });
    await expect(
      store.resolve(sourceWork(5), detail(5)),
    ).resolves.not.toBeNull();
    now = Date.parse(OBSERVED_AT) + 60_001;
    await expect(store.resolve(sourceWork(5), detail(5))).rejects.toThrow(
      "PRODUCTION_RESOLUTION_STALE",
    );
    store.close();
  });

  it("streams a production-scale fixture without materializing the catalog", async () => {
    const fixture = createFixture();
    const poemInsert = fixture.database.prepare(
      "INSERT INTO poem VALUES (?, 'author-1', ?)",
    );
    const sourceInsert = fixture.database.prepare(
      `INSERT INTO source_poem_identity VALUES (
        ?, 'source', ?, 'source-author-1', ?, ?, 1, 1, NULL
      )`,
    );
    const revisionInsert = fixture.database.prepare(
      "INSERT INTO poem_source_revision VALUES (?, ?)",
    );
    const pointerInsert = fixture.database.prepare(
      "INSERT INTO poem_source_pointer VALUES (?, ?)",
    );
    const insert = fixture.database.transaction(() => {
      for (let index = 1; index <= 25_000; index += 1) {
        const numericId = String(index);
        const canonicalPoemId = poemId(index);
        poemInsert.run(canonicalPoemId, `work-${numericId}`);
        sourceInsert.run(
          `source-poem-${numericId}`,
          numericId,
          `https://source.invalid/works/${numericId}`,
          canonicalPoemId,
        );
        revisionInsert.run(`revision-${numericId}`, `source-poem-${numericId}`);
        pointerInsert.run(`source-poem-${numericId}`, `revision-${numericId}`);
      }
    });
    insert();
    fixture.database.close();
    const report = await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    expect(report.poemCount).toBe(25_000);
    const store = await ProductionResolutionStore.open(fixture.output);
    const resolution = await store.resolve(sourceWork(25_000), detail(25_000));
    expect(resolution?.mapping.poemId).toBe(poemId(25_000));
    store.close();
    expect(statSync(fixture.output).size).toBeLessThan(20 * 1024 * 1024);
  });

  it("uses authoritative source identities instead of canonical slugs", async () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 42, 999);
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    const store = await ProductionResolutionStore.open(fixture.output);
    const resolution = await store.resolve(sourceWork(42), detail(42));
    expect(resolution?.mapping.poemId).toBe(poemId(42));
    store.close();
  });

  it("onboards an unknown scraped identity deterministically", async () => {
    const fixture = createFixture();
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    const store = await ProductionResolutionStore.open(fixture.output);
    const newPoetResolution = await store.resolve(
      sourceWork(73, "new-poet", "شاعر جديد"),
      detail(73, "new-poet", "شاعر جديد"),
    );
    const mapping = newPoetResolution?.mapping;
    expect(mapping).toEqual({
      authorId: sha256("author\u{1F}source\u{1F}new-poet"),
      authorNameArabic: "شاعر جديد",
      canonicalPoemId: null,
      poemId: sha256("poem\u{1F}source\u{1F}73"),
      sourceAuthorSlug: "new-poet",
      sourcePoemId: "73",
    });
    await expect(
      store.resolve(sourceWork(73, "new-poet"), detail(73, "new-poet")),
    ).resolves.toBeNull();
    const contextualResolution = await store.resolve(
      sourceWork(74, "context-poet"),
      {
        ...detail(74, "context-poet"),
        sourceContext: {
          authorNameArabic: "شاعر السياق",
          refreshGeneration: "generation-2",
        },
      },
    );
    expect(contextualResolution?.mapping.authorNameArabic).toBe("شاعر السياق");
    store.close();
  });

  it("rejects syntactically valid source ownership disagreement", async () => {
    const fixture = createFixture();
    fixture.database.exec(`
      INSERT INTO author VALUES ('author-2', 'شاعر آخر', 'other');
      INSERT INTO poem VALUES ('wrong-poem', 'author-2', 'work-77');
      INSERT INTO source_poem_identity VALUES (
        'source-poem-77', 'source', '77', 'source-author-1',
        'https://source.invalid/works/77', 'wrong-poem', 1, 1, NULL
      );
    `);
    fixture.database.close();
    await expect(
      exportProductionResolution({
        database: fixture.source,
        observedAt: OBSERVED_AT,
        output: fixture.output,
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_SOURCE_OWNERSHIP_MISMATCH");
  });

  it("exposes a deterministic CLI contract", () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 9);
    fixture.database.close();
    const cli = resolve(import.meta.dirname, "../cli.ts");
    const stdout = execFileSync(
      process.execPath,
      [
        "--import",
        "tsx",
        cli,
        "export-resolution",
        "--database",
        fixture.source,
        "--output",
        fixture.output,
        "--observed-at",
        OBSERVED_AT,
      ],
      { encoding: "utf8" },
    );
    expect(JSON.parse(stdout)).toMatchObject({
      command: "export-resolution",
      result: { poemCount: 1, promotion: "created" },
    });
    expect(readFileSync(fixture.output).length).toBeGreaterThan(0);
  });

  it("rejects malformed stored rows without retaining their values", async () => {
    const fixture = createFixture();
    insertPoem(fixture.database, 11);
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    const secret = `private-${"x".repeat(10_001)}`;
    const corrupt = new Database(fixture.output);
    corrupt
      .prepare("UPDATE poem_resolution SET author_name_arabic = ?")
      .run(secret);
    corrupt.close();

    const error = await ProductionResolutionStore.open(fixture.output).catch(
      (reason: unknown) => reason,
    );
    if (!(error instanceof SqliteQueryValidationError)) throw error;
    expect(error.operation).toBe("productionResolution.manifestPoems");
    expect(error.cardinality).toBe("stream");
    expect(error.rowIndex).toBe(0);
    expect(error.paths).toEqual([["author_name_arabic"]]);
    expect(error.message).not.toContain(secret);
    expect("cause" in error).toBe(false);
  });

  it("reports a missing required metadata row as a sanitized cardinality failure", async () => {
    const fixture = createFixture();
    fixture.database.close();
    await exportProductionResolution({
      database: fixture.source,
      observedAt: OBSERVED_AT,
      output: fixture.output,
    });
    const corrupt = new Database(fixture.output);
    corrupt.prepare("DELETE FROM resolution_meta").run();
    corrupt.close();

    const error = await ProductionResolutionStore.open(fixture.output).catch(
      (reason: unknown) => reason,
    );
    if (!(error instanceof SqliteQueryValidationError)) throw error;
    expect(error).toMatchObject({
      cardinality: "required",
      operation: "productionResolution.meta",
      paths: [[]],
      rowIndex: null,
    });
  });
});

function createFixture() {
  const root = mkdtempSync(join(tmpdir(), "production-resolution-"));
  ROOTS.push(root);
  const source = join(root, "d1.sqlite");
  const database = new Database(source);
  database.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE author (
      id TEXT PRIMARY KEY,
      name_arabic TEXT NOT NULL,
      slug TEXT NOT NULL UNIQUE
    );
    CREATE TABLE poem (
      id TEXT PRIMARY KEY,
      author_id TEXT REFERENCES author(id),
      slug TEXT NOT NULL UNIQUE
    );
    CREATE TABLE scraper_writer_control (
      singleton INTEGER PRIMARY KEY,
      writer_epoch INTEGER NOT NULL
    );
    CREATE TABLE poem_publication_pointer (
      poem_id TEXT PRIMARY KEY REFERENCES poem(id),
      pointer_version INTEGER NOT NULL
    );
    CREATE TABLE poem_model_publication_pointer (
      poem_id TEXT NOT NULL REFERENCES poem(id),
      model_key TEXT NOT NULL,
      pointer_version INTEGER NOT NULL,
      PRIMARY KEY (poem_id, model_key)
    );
    CREATE TABLE source_author_identity (
      id TEXT PRIMARY KEY,
      source_name TEXT NOT NULL,
      external_id TEXT NOT NULL,
      canonical_url TEXT NOT NULL,
      canonical_author_id TEXT NOT NULL REFERENCES author(id)
    );
    CREATE TABLE source_poem_identity (
      id TEXT PRIMARY KEY,
      source_name TEXT NOT NULL,
      external_id TEXT NOT NULL,
      source_author_id TEXT NOT NULL REFERENCES source_author_identity(id),
      canonical_url TEXT NOT NULL,
      canonical_poem_id TEXT REFERENCES poem(id),
      first_observed_at INTEGER NOT NULL,
      last_observed_at INTEGER NOT NULL,
      tombstoned_at INTEGER
    );
    CREATE TABLE poem_source_revision (
      id TEXT PRIMARY KEY,
      source_poem_id TEXT NOT NULL REFERENCES source_poem_identity(id)
    );
    CREATE TABLE poem_source_pointer (
      source_poem_id TEXT PRIMARY KEY REFERENCES source_poem_identity(id),
      revision_id TEXT NOT NULL REFERENCES poem_source_revision(id)
    );
    CREATE INDEX source_author_canonical
      ON source_author_identity(canonical_author_id);
    CREATE INDEX source_poem_canonical
      ON source_poem_identity(canonical_poem_id);
    CREATE INDEX source_poem_author
      ON source_poem_identity(source_author_id, external_id);
    INSERT INTO author VALUES ('author-1', 'شاعر', 'poet');
    INSERT INTO source_author_identity VALUES (
      'source-author-1', 'source', 'poet',
      'https://source.invalid/writers/poet', 'author-1'
    );
    INSERT INTO scraper_writer_control VALUES (1, 7);
  `);
  return { database, output: join(root, "resolution.sqlite"), root, source };
}

function insertPoem(
  database: Database.Database,
  numericId: number,
  canonicalSlugNumericId = numericId,
): void {
  database
    .prepare("INSERT INTO poem VALUES (?, 'author-1', ?)")
    .run(poemId(numericId), `work-${String(canonicalSlugNumericId)}`);
  database
    .prepare(
      `INSERT INTO source_poem_identity VALUES (
          ?, 'source', ?, 'source-author-1', ?, ?, 1, 1, NULL
        )`,
    )
    .run(
      `source-poem-${String(numericId)}`,
      String(numericId),
      `https://source.invalid/works/${String(numericId)}`,
      poemId(numericId),
    );
  database
    .prepare("INSERT INTO poem_source_revision VALUES (?, ?)")
    .run(`revision-${String(numericId)}`, `source-poem-${String(numericId)}`);
  database
    .prepare("INSERT INTO poem_source_pointer VALUES (?, ?)")
    .run(`source-poem-${String(numericId)}`, `revision-${String(numericId)}`);
}

function poemId(numericId: number): string {
  return `poem-${String(numericId).padStart(8, "0")}`;
}

function sourceWork(
  numericId: number,
  authorSlug = "poet",
  authorNameArabic?: string,
): WorkItem {
  const input = {
    authorHref: `https://source.invalid/writers/${authorSlug}`,
    ...(authorNameArabic === undefined ? {} : { authorNameArabic }),
    poemHref: `https://source.invalid/works/${String(numericId)}`,
  };
  const definition = {
    implementationVersion: collectorImplementationVersion(),
    input,
    inputHash: inputHash(input),
    kind: collectionWorkKinds().poemDetail,
    priority: 0,
    schemaVersion: collectorSchemaVersion(),
  };
  return {
    ...definition,
    attemptCount: 1,
    availableAt: 1,
    createdAt: 1,
    lastErrorCode: null,
    leaseEpoch: 1,
    leaseExpiresAt: null,
    leaseOwner: null,
    leaseToken: null,
    outputArtifactHash: "a".repeat(64),
    state: "succeeded",
    updatedAt: 1,
    workKey: calculateWorkKey(definition),
  };
}

function detail(
  numericId: number,
  authorSlug = "poet",
  authorNameArabic?: string,
) {
  const work = sourceWork(numericId, authorSlug, authorNameArabic);
  const source = {
    author: {
      canonicalId: `source:author:${authorSlug}`,
      href: `https://source.invalid/writers/${authorSlug}`,
      path: `/writers/${authorSlug}`,
      slug: authorSlug,
    },
    canonicalId: `source:poem:${String(numericId)}`,
    href: `https://source.invalid/works/${String(numericId)}`,
    lines: ["صدر", "عجز"],
    numericId: String(numericId),
    slug: `work-${String(numericId)}`,
    structure: "classical",
    title: "قصيدة",
    verses: 1,
  };
  return {
    artifactSchemaVersion: 1,
    collectedBy: collectorImplementationVersion(),
    source,
    sourceHash: sha256(canonicalJson(source)),
    workKey: work.workKey,
  };
}
