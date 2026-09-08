import {
  lstatSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";

import { exportProductionBaseline } from "../persistence/baseline-export";
import { Ledger } from "../persistence/ledger";
import {
  planProductionBaselineFiles,
  ProductionBaselineAuthorRowSchema,
  ProductionBaselinePoemRowSchema,
  readNdjson,
} from "../persistence/production-baseline-planner";

const AUTHOR_ID = "00000000-0000-4000-8000-000000000001";
const ROOTS: string[] = [];

afterEach(() => {
  for (const root of ROOTS) rmSync(root, { force: true, recursive: true });
  ROOTS.length = 0;
});

describe("production baseline export", () => {
  it("exports lean deterministic rows accepted by the baseline consumer", async () => {
    const fixture = createFixture();
    fixture.database.exec(
      "ALTER TABLE poem ADD COLUMN active_source_revision_id TEXT",
    );
    insertAuthor(fixture.database);
    insertPoem(fixture.database, 4, AUTHOR_ID, null, null, null);
    const sourceBoundId = "a".repeat(64);
    fixture.database
      .prepare(
        `UPDATE poem SET id = ?, slug = ?, active_source_revision_id = ?
         WHERE slug = 'work-4'`,
      )
      .run(sourceBoundId, `source-${sourceBoundId}`, "b".repeat(64));
    insertPoem(fixture.database, 1, null, null, null, null);
    insertPoem(fixture.database, 2, AUTHOR_ID, "legacy", null, "legacy");
    fixture.database
      .prepare("UPDATE poem SET content_arabic = ? WHERE slug = 'work-2'")
      .run(JSON.stringify({ content: ["صدر\u{2029}عجز"] }));
    insertPoem(
      fixture.database,
      3,
      "00000000-0000-4000-8000-000000000999",
      null,
      null,
      null,
    );
    insertPoem(fixture.database, 67, AUTHOR_ID, null, null, null);
    fixture.database
      .prepare("UPDATE poem SET content_arabic = ? WHERE slug = 'work-67'")
      .run(JSON.stringify({ content: ["بيت\u{0007} غير آمن"] }));
    for (const [index, content, emptyTitle] of [
      [76_154, [], false],
      [80_293, ["أَنا الَّذي نَظَرَ الأَعمى إِلى أَدَبي"], true],
      [91_494, ["", "أول سطر عربي صالح"], true],
      [78_927, [""], true],
    ] as const) {
      insertPoem(fixture.database, index, AUTHOR_ID, null, null, null);
      fixture.database
        .prepare(
          "UPDATE poem SET name_arabic = ?, content_arabic = ? WHERE slug = ?",
        )
        .run(
          emptyTitle ? "" : `قصيدة ${String(index)}`,
          JSON.stringify({ content }),
          `work-${String(index)}`,
        );
    }
    fixture.database.close();

    const first = await exportProductionBaseline(fixture.paths);
    expect(first).toMatchObject({
      authors: { rows: 1 },
      poems: {
        eligibleRows: 4,
        ineligible: expect.arrayContaining([
          {
            canonicalSourceId: "source:poem:1",
            poemId: "00000000-0000-4000-8000-000000000001",
            reason: "missing_arabic_author",
            slug: "work-1",
          },
          {
            canonicalSourceId: "source:poem:3",
            poemId: "00000000-0000-4000-8000-000000000003",
            reason: "missing_arabic_author",
            slug: "work-3",
          },
          {
            canonicalSourceId: "source:poem:67",
            poemId: "00000000-0000-4000-8000-000000000067",
            reason: "unsafe_source_text",
            slug: "work-67",
          },
          {
            canonicalSourceId: "source:poem:76154",
            poemId: "00000000-0000-4000-8000-000000076154",
            reason: "missing_arabic_content",
            slug: "work-76154",
          },
          {
            canonicalSourceId: "source:poem:78927",
            poemId: "00000000-0000-4000-8000-000000078927",
            reason: "missing_arabic_title_and_content",
            slug: "work-78927",
          },
        ]),
        orphans: 2,
        rows: 9,
      },
      promotion: "created",
      replayed: false,
      schemaId: "saqi.production-baseline-export",
      schemaVersion: 1,
    });
    const authors = await collect(
      fixture.paths.authorsOutput,
      ProductionBaselineAuthorRowSchema,
    );
    const poems = await collect(
      fixture.paths.poemsOutput,
      ProductionBaselinePoemRowSchema,
    );
    expect(authors).toEqual([
      { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
    ]);
    expect(poems.slice(0, 3)).toEqual([
      expect.objectContaining({
        author_id: null,
        insights_missing: true,
        slug: "work-1",
        translation_missing: true,
      }),
      expect.objectContaining({
        author_id: AUTHOR_ID,
        content_arabic: { content: ["صدر\u{2029}عجز"] },
        insights_missing: false,
        slug: "work-2",
        translation_missing: false,
      }),
      expect.objectContaining({
        author_id: "00000000-0000-4000-8000-000000000999",
        slug: "work-3",
      }),
    ]);
    expect(poems.find(({ slug }) => slug === "work-80293")?.name_arabic).toBe(
      "أَنا الَّذي نَظَرَ الأَعمى إِلى أَدَبي",
    );
    expect(poems.find(({ slug }) => slug === "work-91494")?.name_arabic).toBe(
      "أول سطر عربي صالح",
    );
    expect(poems.find(({ slug }) => slug === "work-78927")).toMatchObject({
      ineligible_reason: "missing_arabic_title_and_content",
      name_arabic: "",
    });
    expect(poems.find(({ slug }) => slug === "work-67")).toMatchObject({
      ineligible_reason: "unsafe_source_text",
    });
    expect(poems.find(({ id }) => id === sourceBoundId)).toMatchObject({
      active_source_revision_id: "b".repeat(64),
      slug: `source-${sourceBoundId}`,
    });
    const serialized = readFileSync(fixture.paths.poemsOutput, "utf8");
    expect(serialized).not.toContain("legacy");
    expect(lstatSync(fixture.paths.poemsOutput).mode & 0o777).toBe(0o600);

    const ledger = Ledger.open(":memory:");
    const planned = await planProductionBaselineFiles({
      authorsPath: fixture.paths.authorsOutput,
      ledger,
      poemsPath: fixture.paths.poemsOutput,
    });
    expect(planned.canonicalPoemIds.size).toBe(9);
    expect(planned.report).toMatchObject({
      eligiblePoems: 4,
      ineligiblePoems: 5,
      poems: 9,
      seededEnrichmentWork: 4,
      seededSolWork: 4,
    });
    const plannedReplay = await planProductionBaselineFiles({
      authorsPath: fixture.paths.authorsOutput,
      ledger,
      poemsPath: fixture.paths.poemsOutput,
    });
    expect(plannedReplay.report).toMatchObject({
      duplicateEnrichmentWork: 4,
      duplicateSolWork: 4,
      seededSolWork: 0,
    });
    expect(plannedReplay.report.planHash).toBe(planned.report.planHash);
    ledger.close();

    const modifiedAt = statSync(fixture.paths.poemsOutput).mtimeMs;
    const replay = await exportProductionBaseline(fixture.paths);
    expect(replay.promotion).toBe("replayed");
    expect(replay.replayed).toBe(true);
    expect(replay.authors.sha256).toBe(first.authors.sha256);
    expect(replay.poems.sha256).toBe(first.poems.sha256);
    expect(statSync(fixture.paths.poemsOutput).mtimeMs).toBe(modifiedAt);
    expect(temporaryFiles(fixture.root)).toEqual([]);

    unlinkSync(fixture.paths.poemsOutput);
    const resumed = await exportProductionBaseline(fixture.paths);
    expect(resumed.promotion).toBe("resumed");
    expect(resumed.replayed).toBe(true);
    expect(resumed.poems.sha256).toBe(first.poems.sha256);
    expect(temporaryFiles(fixture.root)).toEqual([]);
  });

  it("exports current per-model publication provenance for independent backfills", async () => {
    const fixture = createFixture();
    const sourceRevisionId = "c".repeat(64);
    insertAuthor(fixture.database);
    insertPoem(fixture.database, 1, AUTHOR_ID, "legacy", null, "legacy");
    fixture.database.exec(`
      ALTER TABLE poem ADD COLUMN active_source_revision_id TEXT;
      CREATE TABLE model_enrichment_artifact(
        id TEXT PRIMARY KEY,
        prompt_version TEXT NOT NULL
      );
      CREATE TABLE poem_model_publication_pointer(
        poem_id TEXT NOT NULL,
        model_key TEXT NOT NULL,
        source_revision_id TEXT NOT NULL,
        enrichment_artifact_id TEXT NOT NULL,
        PRIMARY KEY(poem_id, model_key)
      );
    `);
    fixture.database
      .prepare("UPDATE poem SET active_source_revision_id = ? WHERE slug = ?")
      .run(sourceRevisionId, "work-1");
    fixture.database
      .prepare(
        "INSERT INTO model_enrichment_artifact(id, prompt_version) VALUES (?, ?)",
      )
      .run("artifact-1", "sol-enrichment-v1");
    fixture.database
      .prepare(
        `INSERT INTO poem_model_publication_pointer(
          poem_id, model_key, source_revision_id, enrichment_artifact_id
        ) VALUES (?, ?, ?, ?)`,
      )
      .run(
        "00000000-0000-4000-8000-000000000001",
        "sol-5.6",
        sourceRevisionId,
        "artifact-1",
      );
    fixture.database.close();

    await exportProductionBaseline(fixture.paths);
    const [poem] = await collect(
      fixture.paths.poemsOutput,
      ProductionBaselinePoemRowSchema,
    );
    expect(poem).toMatchObject({
      active_source_revision_id: sourceRevisionId,
      model_publications: [
        {
          model_key: "sol-5.6",
          prompt_version: "sol-enrichment-v1",
          source_revision_id: sourceRevisionId,
        },
      ],
    });

    const ledger = Ledger.open(":memory:");
    const planned = await planProductionBaselineFiles({
      authorsPath: fixture.paths.authorsOutput,
      ledger,
      poemsPath: fixture.paths.poemsOutput,
    });
    expect(planned.report).toMatchObject({
      currentProfileWork: 1,
      missingProfileWork: 0,
      seededEnrichmentWork: 0,
    });
    expect(ledger.status().total).toBe(0);
    ledger.close();
  });

  it("supports the pre-model-pointer schema while preserving source revision identity", async () => {
    const fixture = createFixture();
    const sourceRevisionId = "d".repeat(64);
    insertAuthor(fixture.database);
    insertPoem(fixture.database, 1, AUTHOR_ID, null, null, null);
    fixture.database.exec(
      "ALTER TABLE poem ADD COLUMN active_source_revision_id TEXT",
    );
    fixture.database
      .prepare("UPDATE poem SET active_source_revision_id = ? WHERE slug = ?")
      .run(sourceRevisionId, "work-1");
    fixture.database.close();

    await exportProductionBaseline(fixture.paths);
    const [poem] = await collect(
      fixture.paths.poemsOutput,
      ProductionBaselinePoemRowSchema,
    );
    expect(poem).toMatchObject({
      active_source_revision_id: sourceRevisionId,
      model_publications: [],
    });
  });

  it("preserves prior outputs on invalid schema and pre-promotion failure", async () => {
    const invalid = createFixture(false);
    writeFileSync(invalid.paths.authorsOutput, "prior-authors\n", {
      mode: 0o600,
    });
    writeFileSync(invalid.paths.poemsOutput, "prior-poems\n", { mode: 0o600 });
    invalid.database.close();
    await expect(exportProductionBaseline(invalid.paths)).rejects.toThrow(
      "BASELINE_EXPORT_COLUMN_MISSING:poem.insights",
    );
    expect(readFileSync(invalid.paths.authorsOutput, "utf8")).toBe(
      "prior-authors\n",
    );
    expect(readFileSync(invalid.paths.poemsOutput, "utf8")).toBe(
      "prior-poems\n",
    );

    const crash = createFixture();
    insertAuthor(crash.database);
    insertPoem(crash.database, 1, AUTHOR_ID, null, null, null);
    crash.database.close();
    await expect(
      exportProductionBaseline({
        ...crash.paths,
        beforePromote: () => {
          throw new Error("SIMULATED_CRASH");
        },
      }),
    ).rejects.toThrow("SIMULATED_CRASH");
    expect(() => statSync(crash.paths.authorsOutput)).toThrow();
    expect(() => statSync(crash.paths.poemsOutput)).toThrow();
    expect(temporaryFiles(crash.root)).toEqual([]);
  });

  it("refuses symlink and differing-output overwrite surprises", async () => {
    const fixture = createFixture();
    insertAuthor(fixture.database);
    fixture.database.close();
    writeFileSync(fixture.paths.authorsOutput, "different\n", { mode: 0o600 });
    await expect(exportProductionBaseline(fixture.paths)).rejects.toThrow(
      "BASELINE_EXPORT_OUTPUT_EXISTS_DIFFERENT",
    );
    expect(readFileSync(fixture.paths.authorsOutput, "utf8")).toBe(
      "different\n",
    );

    const symlink = createFixture();
    symlink.database.close();
    const target = join(symlink.root, "target.ndjson");
    writeFileSync(target, "safe\n", { mode: 0o600 });
    symlinkSync(target, symlink.paths.authorsOutput);
    await expect(exportProductionBaseline(symlink.paths)).rejects.toThrow(
      "BASELINE_EXPORT_OUTPUT_UNSAFE",
    );
    expect(readFileSync(target, "utf8")).toBe("safe\n");
  });

  it(
    "streams 100,064 rows in stable primary-key order",
    { timeout: 120_000 },
    async () => {
      const fixture = createFixture();
      insertAuthor(fixture.database);
      const insert = fixture.database.prepare(
        `INSERT INTO poem(
          id, author_id, name_arabic, content_arabic, slug,
          translation, translation_gemini, insights
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL)`,
      );
      const transaction = fixture.database.transaction(() => {
        for (let index = 100_064; index > 0; index -= 1) {
          const value = String(index).padStart(12, "0");
          const ineligible = index === 78_927;
          insert.run(
            `00000000-0000-4000-8000-${value}`,
            AUTHOR_ID,
            ineligible ? "" : `قصيدة ${value}`,
            JSON.stringify({ content: [ineligible ? "" : `بيت ${value}`] }),
            `work-${String(index)}`,
          );
        }
      });
      transaction();
      fixture.database.close();
      const report = await exportProductionBaseline(fixture.paths);
      expect(report.poems).toMatchObject({
        eligibleRows: 100_063,
        ineligible: [
          {
            canonicalSourceId: "source:poem:78927",
            poemId: "00000000-0000-4000-8000-000000078927",
            reason: "missing_arabic_title_and_content",
            slug: "work-78927",
          },
        ],
        orphans: 0,
        rows: 100_064,
      });
      const lines = readFileSync(fixture.paths.poemsOutput, "utf8")
        .trimEnd()
        .split("\n");
      expect(JSON.parse(lines[0]!) as unknown).toMatchObject({
        slug: "work-1",
      });
      expect(JSON.parse(lines.at(-1)!) as unknown).toMatchObject({
        slug: "work-100064",
      });
    },
  );
});

function createFixture(valid = true) {
  const root = mkdtempSync(join(tmpdir(), "saqi-baseline-export-"));
  ROOTS.push(root);
  const databasePath = join(root, "snapshot.sqlite3");
  const database = new Database(databasePath);
  database.exec(`
    CREATE TABLE author(
      id TEXT PRIMARY KEY,
      name_arabic TEXT NOT NULL,
      slug TEXT NOT NULL
    );
    CREATE TABLE poem(
      id TEXT PRIMARY KEY,
      author_id TEXT,
      name_arabic TEXT NOT NULL,
      content_arabic TEXT NOT NULL,
      slug TEXT NOT NULL,
      translation TEXT,
      translation_gemini TEXT
      ${valid ? ", insights TEXT" : ""}
    );
  `);
  return {
    database,
    paths: {
      authorsOutput: join(root, "authors.ndjson"),
      database: databasePath,
      poemsOutput: join(root, "poems.ndjson"),
    },
    root,
  };
}

function insertAuthor(database: Database.Database): void {
  database
    .prepare("INSERT INTO author(id, name_arabic, slug) VALUES (?, ?, ?)")
    .run(AUTHOR_ID, "المتنبي", "mutanabi");
}

function insertPoem(
  database: Database.Database,
  index: number,
  authorId: null | string,
  translation: null | string,
  translationGemini: null | string,
  insights: null | string,
): void {
  const suffix = String(index).padStart(12, "0");
  database
    .prepare(
      `INSERT INTO poem(
        id, author_id, name_arabic, content_arabic, slug,
        translation, translation_gemini, insights
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      `00000000-0000-4000-8000-${suffix}`,
      authorId,
      `قصيدة ${String(index)}`,
      JSON.stringify({ content: [`بيت ${String(index)}`] }),
      `work-${String(index)}`,
      translation,
      translationGemini,
      insights,
    );
}

async function collect<T>(
  path: string,
  schema: { parse(value: unknown): T },
): Promise<T[]> {
  const values: T[] = [];
  for await (const value of readNdjson(path)) values.push(schema.parse(value));
  return values;
}

function temporaryFiles(root: string): string[] {
  return readdirSync(root).filter((name) => name.endsWith(".tmp"));
}
