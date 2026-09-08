import {
  APPROVED_ENRICHMENT_PROFILES,
  approvedEnrichmentValidations,
  READABLE_ENRICHMENT_PROFILES,
  type ReadableEnrichmentProfile,
  sitemapShardForId,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { D1PoemStore } from "../poem-store";
import { AUTHOR_TABLE as author, POEM_TABLE as poem } from "../schema";

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS author (
  id TEXT PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  name_arabic TEXT NOT NULL,
  sort_name_arabic TEXT NOT NULL DEFAULT '',
  name TEXT,
  status TEXT NOT NULL DEFAULT 'init',
  poem_count INTEGER DEFAULT 0,
  gemini_translation_count INTEGER DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0,
  public_poem_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS poem (
  id TEXT PRIMARY KEY,
  author_id TEXT REFERENCES author(id),
  slug TEXT UNIQUE NOT NULL,
  verses INTEGER NOT NULL,
  name_arabic TEXT NOT NULL,
  sort_name_arabic TEXT NOT NULL DEFAULT '',
  name_english TEXT,
  content_arabic TEXT NOT NULL,
  translation TEXT,
  translation_gemini TEXT,
  insights TEXT,
  english_name_original_translation TEXT,
  poem_title_first_line TEXT,
  hidden INTEGER NOT NULL DEFAULT 0,
  publishable INTEGER NOT NULL DEFAULT 0,
  has_english INTEGER NOT NULL DEFAULT 0,
  has_insights INTEGER NOT NULL DEFAULT 0,
  sitemap_shard INTEGER NOT NULL DEFAULT 0,
  active_source_revision_id TEXT,
  active_enrichment_artifact_id TEXT,
  translation_sol TEXT,
  insights_sol TEXT
);

CREATE TABLE IF NOT EXISTS task (
  id TEXT PRIMARY KEY,
  work_key TEXT,
  status TEXT NOT NULL
);

CREATE TABLE model_enrichment_artifact (
  id TEXT PRIMARY KEY,
  source_revision_id TEXT NOT NULL,
  task_key TEXT NOT NULL,
  variant INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  model_key TEXT NOT NULL,
  reasoning_effort TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE model_enrichment_validation (
  id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL,
  validator_key TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  outcome TEXT NOT NULL,
  highest_severity TEXT NOT NULL,
  report_hash TEXT NOT NULL,
  report TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE poem_model_publication_pointer (
  poem_id TEXT NOT NULL,
  model_key TEXT NOT NULL,
  source_revision_id TEXT NOT NULL,
  enrichment_artifact_id TEXT NOT NULL,
  pointer_version INTEGER NOT NULL,
  writer_epoch INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (poem_id, model_key)
);
`;

describe("D1PoemStore", () => {
  let sqlite: InstanceType<typeof Database>;
  let db: ReturnType<typeof drizzle>;
  let store: D1PoemStore;
  let testAuthorId: string;
  let testPoemId: string;

  beforeEach(() => {
    sqlite = new Database(":memory:");
    sqlite.exec(SCHEMA_SQL);

    db = drizzle(sqlite);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument -- better-sqlite3 and D1 use compatible Drizzle APIs but distinct driver types.
    store = new D1PoemStore(db as any);

    testAuthorId = crypto.randomUUID();
    testPoemId = crypto.randomUUID();

    db.insert(author)
      .values({
        id: testAuthorId,
        slug: `test-author-${testAuthorId}`,
        nameArabic: "مؤلف اختبار",
        status: "init",
      })
      .run();

    db.insert(poem)
      .values({
        id: testPoemId,
        authorId: testAuthorId,
        slug: `test-poem-${testPoemId}`,
        verses: 4,
        nameArabic: "قصيدة اختبار",
        contentArabic: JSON.stringify({ content: ["سطر 1", "سطر 2"] }),
      })
      .run();
  });

  afterEach(() => {
    sqlite.close();
  });

  it("reads the author slug using the exact poem identifier", async () => {
    await expect(
      store.getAuthorSlugForPoem("missing' OR 1=1 --"),
    ).resolves.toBeNull();
    await expect(store.getAuthorSlugForPoem(testPoemId)).resolves.toBe(
      `test-author-${testAuthorId}`,
    );
  });

  it.each([null, undefined, {}, { author_slug: null }, { author_slug: "" }])(
    "returns no publication route for %j",
    async (row) => {
      vi.spyOn(db, "get").mockReturnValueOnce(row);
      await expect(store.getAuthorSlugForPoem("poem-1")).resolves.toBeNull();
    },
  );

  it.each([{ author_slug: 7 }, { author_slug: false }])(
    "rejects malformed publication route row %j",
    async (row) => {
      vi.spyOn(db, "get").mockReturnValueOnce(row);
      await expect(store.getAuthorSlugForPoem("poem-1")).rejects.toMatchObject({
        name: "ZodError",
      });
    },
  );

  describe("getById", () => {
    it("returns a loaded poem by id", async () => {
      const result = await store.getById(testPoemId);

      expect(result.id).toBe(testPoemId);
      expect(result.nameArabic).toBe("قصيدة اختبار");
      expect(result.linesArabic.length).toBeGreaterThanOrEqual(0);
    });

    it("throws error for non-existent poem", async () => {
      await expect(store.getById("non-existent")).rejects.toThrow(
        "Poem not found",
      );
    });

    it("loads every current validated model and suppresses stale legacy Sol", async () => {
      db.update(poem)
        .set({
          activeSourceRevisionId: "revision-current",
          translationSol: { content: ["Legacy Sol"] },
        })
        .where(eq(poem.id, testPoemId))
        .run();
      for (const [index, profile] of APPROVED_ENRICHMENT_PROFILES.entries()) {
        seedModelEnrichment(
          sqlite,
          testPoemId,
          "revision-current",
          profile,
          index,
        );
      }

      const result = await store.getById(testPoemId);

      expect(result.linesEnglishSol).toBeUndefined();
      expect(result.modelEnrichments?.map(({ modelKey }) => modelKey)).toEqual(
        APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey),
      );
      expect(result.modelEnrichments?.map(({ lines }) => lines[0])).toEqual(
        APPROVED_ENRICHMENT_PROFILES.map(
          (_profile, index) => `Translation ${String(index)}`,
        ),
      );
    });

    it("never exposes a normalized model for a stale source revision", async () => {
      db.update(poem)
        .set({
          activeSourceRevisionId: "revision-new",
          translationSol: { content: ["Stale legacy Sol"] },
        })
        .where(eq(poem.id, testPoemId))
        .run();
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-old",
        APPROVED_ENRICHMENT_PROFILES[0],
        0,
      );

      const result = await store.getById(testPoemId);

      expect("modelEnrichments" in result).toBe(false);
      expect("linesEnglishSol" in result).toBe(false);
    });

    it("rejects a model track when any stored review rejects it", async () => {
      db.update(poem)
        .set({ activeSourceRevisionId: "revision-current" })
        .where(eq(poem.id, testPoemId))
        .run();
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-current",
        APPROVED_ENRICHMENT_PROFILES[0],
        0,
      );
      sqlite
        .prepare(
          `INSERT INTO model_enrichment_validation VALUES
           ('rejected-extra', 'artifact-0', 'extra-review', 'v1', 3,
            'fail', 'major', ?, ?, 2)`,
        )
        .run(
          "c".repeat(64),
          JSON.stringify({
            fidelityScore: 0,
            findings: [],
            insightScore: 0,
            verdict: "fail",
          }),
        );

      const result = await store.getById(testPoemId);

      expect("modelEnrichments" in result).toBe(false);
    });

    it("isolates malformed normalized JSON without failing the poem read", async () => {
      db.update(poem)
        .set({ activeSourceRevisionId: "revision-current" })
        .where(eq(poem.id, testPoemId))
        .run();
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-current",
        APPROVED_ENRICHMENT_PROFILES[0],
        0,
      );
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-current",
        READABLE_ENRICHMENT_PROFILES[2],
        1,
      );
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-current",
        READABLE_ENRICHMENT_PROFILES[4],
        2,
      );
      sqlite
        .prepare(
          "UPDATE model_enrichment_artifact SET payload = ? WHERE id = ?",
        )
        .run("{invalid", "artifact-1");
      sqlite
        .prepare(
          "UPDATE model_enrichment_validation SET report = ? WHERE id = ?",
        )
        .run("{invalid", "validation-0-1");

      const result = await store.getById(testPoemId);

      expect(result.modelEnrichments?.map(({ modelKey }) => modelKey)).toEqual([
        READABLE_ENRICHMENT_PROFILES[4].modelKey,
      ]);
    });

    it("falls back to legacy Sol only when normalized provenance tables are unavailable", async () => {
      sqlite.exec(`
        DROP TABLE poem_model_publication_pointer;
        DROP TABLE model_enrichment_validation;
        DROP TABLE model_enrichment_artifact;
      `);
      db.update(poem)
        .set({ translationSol: { content: ["Legacy Sol"] } })
        .where(eq(poem.id, testPoemId))
        .run();

      const result = await store.getById(testPoemId);

      expect(result.linesEnglishSol).toEqual(["Legacy Sol"]);
      expect(result.modelEnrichments).toBeUndefined();
    });

    it("fails closed without crashing on a partial normalized schema", async () => {
      sqlite.exec(`
        DROP TABLE model_enrichment_validation;
        CREATE TABLE model_enrichment_validation (
          id TEXT PRIMARY KEY,
          artifact_id TEXT NOT NULL
        );
      `);
      db.update(poem)
        .set({
          activeSourceRevisionId: "revision-current",
          translationSol: { content: ["Unverified legacy Sol"] },
        })
        .where(eq(poem.id, testPoemId))
        .run();
      seedModelEnrichment(
        sqlite,
        testPoemId,
        "revision-current",
        APPROVED_ENRICHMENT_PROFILES[0],
        0,
        false,
      );

      const result = await store.getById(testPoemId);

      expect({
        linesEnglishSol: result.linesEnglishSol,
        modelEnrichments: result.modelEnrichments,
      }).toEqual({ linesEnglishSol: undefined, modelEnrichments: undefined });
    });
  });

  describe("getByIds", () => {
    it("returns map of poems by ids", async () => {
      const result = await store.getByIds([testPoemId]);

      expect(result.size).toBe(1);
      expect(result.get(testPoemId)?.nameArabic).toBe("قصيدة اختبار");
    });

    it("returns empty map for empty array", async () => {
      const result = await store.getByIds([]);
      expect(result.size).toBe(0);
    });

    it("ignores non-existent ids", async () => {
      const result = await store.getByIds([testPoemId, "non-existent"]);
      expect(result.size).toBe(1);
    });

    it("preserves first-requested order and de-duplicates ids", async () => {
      const secondPoemId = crypto.randomUUID();
      db.insert(poem)
        .values({
          id: secondPoemId,
          authorId: testAuthorId,
          slug: `second-poem-${secondPoemId}`,
          verses: 2,
          nameArabic: "قصيدة ثانية",
          contentArabic: JSON.stringify({ content: ["سطر"] }),
        })
        .run();

      const result = await store.getByIds([
        secondPoemId,
        testPoemId,
        secondPoemId,
      ]);

      expect(result.keys().toArray()).toEqual([secondPoemId, testPoemId]);
    });
  });

  describe("poemsForAuthor", () => {
    it("returns poems for author sorted by name", async () => {
      const secondPoemId = crypto.randomUUID();
      db.insert(poem)
        .values({
          id: secondPoemId,
          authorId: testAuthorId,
          slug: `second-poem-${secondPoemId}`,
          verses: 2,
          nameArabic: "أقصيدة ثانية",
          contentArabic: JSON.stringify({ content: ["سطر"] }),
        })
        .run();

      const result = await store.poemsForAuthor(testAuthorId);

      expect(result.length).toBe(2);
    });

    it("returns empty array for author with no poems", async () => {
      const result = await store.poemsForAuthor("non-existent");
      expect(result).toEqual([]);
    });
  });

  describe("untranslatedPoemIdsForAuthor", () => {
    it("rejects malformed raw database result rows", async () => {
      const all = vi
        .spyOn(db, "all")
        .mockResolvedValue([{ failed: "0", id: null, total: 1 }]);

      await expect(
        store.untranslatedPoemIdsForAuthor(testAuthorId, 10),
      ).rejects.toThrow();
      all.mockRestore();
    });

    it("includes blank translations and excludes valid translations", async () => {
      db.update(poem)
        .set({
          translationGemini: { content: [" ".repeat(3)] },
        })
        .where(eq(poem.id, testPoemId))
        .run();

      const blank = await store.untranslatedPoemIdsForAuthor(testAuthorId, 10);
      expect(blank).toEqual({ failed: 0, ids: [testPoemId], total: 1 });

      db.update(poem)
        .set({ translationGemini: { content: ["Translation"] } })
        .where(eq(poem.id, testPoemId))
        .run();
      const translated = await store.untranslatedPoemIdsForAuthor(
        testAuthorId,
        10,
      );
      expect(translated).toEqual({ failed: 0, ids: [], total: 0 });
    });

    it("counts failed translation work without dispatching it again", async () => {
      sqlite
        .prepare(
          "INSERT INTO task (id, work_key, status) VALUES (?, ?, 'failed')",
        )
        .run(crypto.randomUUID(), `translate-poem:${testPoemId}`);

      const result = await store.untranslatedPoemIdsForAuthor(testAuthorId, 10);

      expect(result).toEqual({ failed: 1, ids: [], total: 0 });
    });
  });

  describe("getArabicCopy", () => {
    it("returns poem copy with Arabic content", async () => {
      const result = await store.getArabicCopy(testPoemId);

      expect(result.id).toBe(testPoemId);
      expect(result.title).toBe("قصيدة اختبار");
      expect(Array.isArray(result.lines)).toBe(true);
    });

    it("throws error for non-existent poem", async () => {
      await expect(store.getArabicCopy("non-existent")).rejects.toThrow(
        "Poem not found",
      );
    });
  });

  describe("getArabicCopies", () => {
    it("returns map of poem copies", async () => {
      const result = await store.getArabicCopies([testPoemId]);

      expect(result.size).toBe(1);
      expect(Array.isArray(result.get(testPoemId)?.lines)).toBe(true);
    });

    it("returns empty map for empty array", async () => {
      const result = await store.getArabicCopies([]);
      expect(result.size).toBe(0);
    });
  });

  describe("upsert", () => {
    it("inserts new poem", async () => {
      const newSlug = `new-poem-${String(Date.now())}`;
      const result = await store.upsert({
        authorId: testAuthorId,
        slug: newSlug,
        verses: 3,
        name: "قصيدة جديدة",
        content: ["سطر جديد"],
      });

      expect(result.slug).toBe(newSlug);
      expect(result.nameArabic).toBe("قصيدة جديدة");
      expect(
        sqlite
          .prepare("SELECT sitemap_shard FROM poem WHERE id = ?")
          .pluck()
          .get(result.id),
      ).toBe(sitemapShardForId(result.id));
    });

    it("rejects legacy updates to revision-managed poems", async () => {
      sqlite
        .prepare("UPDATE poem SET active_source_revision_id = ? WHERE id = ?")
        .run("revision-1", testPoemId);

      await expect(
        store.upsert({
          authorId: testAuthorId,
          slug: `test-poem-${testPoemId}`,
          verses: 1,
          name: "محاولة قديمة",
          content: ["يجب ألا يحل محل المراجعة"],
        }),
      ).rejects.toThrow("REVISION_MANAGED_POEM_REJECTS_LEGACY_UPSERT");

      expect(
        sqlite
          .prepare("SELECT name_arabic FROM poem WHERE id = ?")
          .pluck()
          .get(testPoemId),
      ).toBe("قصيدة اختبار");
    });

    it("updates existing poem on conflict", async () => {
      const existingSlug = `test-poem-${testPoemId}`;
      sqlite
        .prepare(
          `UPDATE poem SET
            translation = '{"content":["Old"]}',
            translation_gemini = '{"content":["Old"]}',
            insights = '{"summary":"Old"}',
            name_english = 'Old',
            poem_title_first_line = 'Old',
            english_name_original_translation = 'Old',
            has_english = 1,
            has_insights = 1
           WHERE id = ?`,
        )
        .run(testPoemId);
      const result = await store.upsert({
        authorId: testAuthorId,
        slug: existingSlug,
        verses: 5,
        name: "عنوان محدث",
        content: ["محتوى محدث"],
      });

      expect(result).toMatchObject({ nameArabic: "عنوان محدث", verses: 1 });
      const loaded = await store.getById(result.id);
      expect(loaded.linesArabic).toEqual(["محتوى محدث"]);
      const generated = sqlite
        .prepare(
          `SELECT translation, translation_gemini, insights, name_english,
                  poem_title_first_line, english_name_original_translation,
                  has_english, has_insights
             FROM poem WHERE id = ?`,
        )
        .get(testPoemId);
      expect(generated).toEqual({
        english_name_original_translation: null,
        has_english: 0,
        has_insights: 0,
        insights: null,
        name_english: null,
        poem_title_first_line: null,
        translation: null,
        translation_gemini: null,
      });
    });

    it("preserves generated artifacts when a scrape is idempotent", async () => {
      const existingSlug = `test-poem-${testPoemId}`;
      sqlite
        .prepare(
          `UPDATE poem SET
            content_arabic = '{"content":["سطر 1","سطر 2"]}',
            translation = '{"content":["Same"]}',
            has_english = 1
           WHERE id = ?`,
        )
        .run(testPoemId);
      await store.upsert({
        authorId: testAuthorId,
        slug: existingSlug,
        verses: 99,
        name: "قصيدة اختبار",
        content: ["سطر 1", "سطر 2"],
      });
      expect(
        sqlite
          .prepare("SELECT translation FROM poem WHERE id = ?")
          .pluck()
          .get(testPoemId),
      ).toBe('{"content":["Same"]}');
    });
  });

  describe("poemsToScrape", () => {
    it("returns slugs that do not exist in database", async () => {
      const existingSlug = `test-poem-${testPoemId}`;
      const newSlug = "new-slug";

      const result = await store.poemsToScrape([existingSlug, newSlug], 10);

      expect(result.total).toBe(1);
      expect(result.slugs.size).toBe(1);
      expect(result.slugs.has(newSlug)).toBe(true);
      expect(result.slugs.has(existingSlug)).toBe(false);
    });

    it("returns empty set for empty array", async () => {
      const result = await store.poemsToScrape([], 10);
      expect(result).toEqual({ slugs: new Set(), total: 0 });
    });

    it("selects a full author corpus with one JSON-backed SQL query", async () => {
      const existingSlug = `test-poem-${testPoemId}`;
      const newSlugs = Array.from(
        { length: 1200 },
        (_, index) => `new-slug-${String(index)}`,
      );

      const result = await store.poemsToScrape(
        [existingSlug, ...newSlugs, newSlugs[0] ?? ""],
        10,
      );

      expect(result.total).toBe(1200);
      expect(result.slugs.size).toBe(10);
      expect(result.slugs.has(existingSlug)).toBe(false);
      expect(result.slugs.has("new-slug-0")).toBe(true);
      expect(result.slugs.has("new-slug-9")).toBe(true);
      expect(result.slugs.has("new-slug-10")).toBe(false);
    });
  });

  describe("getAdjacentPoems", () => {
    it("returns prev and next poem ids", async () => {
      const poem1Id = crypto.randomUUID();
      const poem2Id = crypto.randomUUID();
      const poem3Id = crypto.randomUUID();

      db.insert(poem)
        .values({
          id: poem1Id,
          authorId: testAuthorId,
          slug: `poem-1-${poem1Id}`,
          verses: 1,
          nameArabic: "أ قصيدة",
          contentArabic: JSON.stringify({ content: [] }),
          nameEnglish: "A poem",
        })
        .run();

      db.insert(poem)
        .values({
          id: poem2Id,
          authorId: testAuthorId,
          slug: `poem-2-${poem2Id}`,
          verses: 1,
          nameArabic: "ب قصيدة",
          contentArabic: JSON.stringify({ content: [] }),
          nameEnglish: "B poem",
        })
        .run();

      db.insert(poem)
        .values({
          id: poem3Id,
          authorId: testAuthorId,
          slug: `poem-3-${poem3Id}`,
          verses: 1,
          nameArabic: "ت قصيدة",
          contentArabic: JSON.stringify({ content: [] }),
          nameEnglish: "C poem",
        })
        .run();

      const result = await store.getAdjacentPoems(poem2Id, testAuthorId);
      const resultBySlug = await store.getAdjacentPoemsBySlug(
        poem2Id,
        `test-author-${testAuthorId}`,
      );

      expect(result.prev).toBe(poem1Id);
      expect(result.next).toBe(poem3Id);
      expect(resultBySlug).toEqual(result);
    });

    it("returns null for prev when first poem", async () => {
      const poem1Id = crypto.randomUUID();
      const authorId = crypto.randomUUID();

      db.insert(author)
        .values({
          id: authorId,
          slug: `adjacent-author-${authorId}`,
          nameArabic: "مؤلف",
          status: "init",
        })
        .run();

      db.insert(poem)
        .values({
          id: poem1Id,
          authorId,
          slug: `single-poem-${poem1Id}`,
          verses: 1,
          nameArabic: "قصيدة وحيدة",
          contentArabic: JSON.stringify({ content: [] }),
        })
        .run();

      const result = await store.getAdjacentPoems(poem1Id, authorId);

      expect(result).toMatchObject({ prev: null, next: null });
    });
  });
});

function seedModelEnrichment(
  database: InstanceType<typeof Database>,
  poemId: string,
  sourceRevisionId: string,
  profile: ReadableEnrichmentProfile,
  ordinal: number,
  seedValidations = true,
): void {
  const artifactId = `artifact-${String(ordinal)}`;
  const payload = {
    insights: {
      culturalSignificance: `Cultural significance ${String(ordinal)}`,
      historicalContext: `Historical context ${String(ordinal)}`,
      literaryDevices: ["Metaphor"],
      notableLines: [{ explanation: "Explanation", line: "سطر 1" }],
      summary: `Summary ${String(ordinal)}`,
      themes: ["Memory"],
    },
    translation: { lines: [`Translation ${String(ordinal)}`, "Second line"] },
  };
  database
    .prepare(
      `INSERT INTO model_enrichment_artifact (
         id, source_revision_id, task_key, variant, schema_version,
         prompt_version, model, model_key, reasoning_effort, payload_hash,
         payload, created_at
       ) VALUES (?, ?, ?, 0, 1, ?, ?, ?, ?, ?, ?, 1)`,
    )
    .run(
      artifactId,
      sourceRevisionId,
      `task-${String(ordinal)}`,
      profile.promptVersion,
      profile.model,
      profile.modelKey,
      profile.reasoningEffort,
      String(ordinal).padStart(64, "a"),
      JSON.stringify(payload),
    );
  database
    .prepare(
      `INSERT INTO poem_model_publication_pointer VALUES
       (?, ?, ?, ?, 1, 1, 1)`,
    )
    .run(poemId, profile.modelKey, sourceRevisionId, artifactId);
  const review = JSON.stringify({
    fidelityScore: 99,
    findings: [],
    insightScore: 99,
    verdict: "pass",
  });
  if (!seedValidations) return;
  for (const validation of approvedEnrichmentValidations(profile).all) {
    database
      .prepare(
        `INSERT INTO model_enrichment_validation VALUES
         (?, ?, ?, ?, ?, 'pass', 'none', ?, ?, 1)`,
      )
      .run(
        `validation-${String(ordinal)}-${String(validation.attempt)}`,
        artifactId,
        validation.validatorKey,
        validation.validatorVersion,
        validation.attempt,
        String(validation.attempt).padStart(64, "b"),
        review,
      );
  }
}
