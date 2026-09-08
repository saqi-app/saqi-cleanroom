import assert from "node:assert/strict";
import { hash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import test from "node:test";

import {
  APPROVED_ENRICHMENT_MODEL,
  APPROVED_ENRICHMENT_PROMPT_VERSION,
  APPROVED_ENRICHMENT_REASONING_EFFORT,
  APPROVED_ENRICHMENT_VALIDATIONS,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";

import {
  AUTHOR_PAGE_SIZE,
  type CatalogDatabase,
  CatalogRepository,
} from "../src/lib/catalog";

class TestStatement {
  readonly #database: Database.Database;
  readonly #query: string;
  readonly #values: unknown[];

  constructor(
    database: Database.Database,
    query: string,
    values: unknown[] = [],
  ) {
    this.#database = database;
    this.#query = query;
    this.#values = values;
  }

  bind(...values: unknown[]) {
    return new TestStatement(this.#database, this.#query, values);
  }

  async all() {
    const statement = this.#database.prepare(this.#query);
    const parameters = Object.fromEntries(
      this.#values.map((value, index) => [String(index + 1), value]),
    );
    return {
      results:
        this.#values.length === 0 ? statement.all() : statement.all(parameters),
    };
  }
}

function catalogRepository(database: Database.Database): CatalogRepository {
  const adapter: CatalogDatabase = {
    async batch(statements) {
      return Promise.all(statements.map((statement) => statement.all()));
    },
    prepare(query) {
      return new TestStatement(database, query);
    },
  };
  return new CatalogRepository(adapter);
}

function createDatabase() {
  const database = new Database(":memory:");
  const migrations = new URL("../../app/migrations/", import.meta.url);
  for (const fileName of readdirSync(migrations)
    .filter((name) => name.endsWith(".sql"))
    .toSorted()) {
    database.exec(readFileSync(new URL(fileName, migrations), "utf8"));
  }
  return database;
}

void test("legacy attribution requires the exact stored payload hash", async () => {
  const sqlite = createDatabase();
  try {
    const translation = '{"content":["A verse"]}';
    const payloadHash = hash("sha256", translation, "hex");
    sqlite.exec(`
      INSERT INTO author (id, slug, name_arabic, name, hidden)
      VALUES ('a-attributed', 'attributed-poet', 'شاعر', 'Poet', 0);
    `);
    sqlite
      .prepare(
        `INSERT INTO poem
          (id, author_id, slug, verses, name_arabic, content_arabic,
           translation, hidden)
         VALUES ('p-attributed', 'a-attributed', 'attributed', 1, 'قصيدة',
           '{"content":["بيت"]}', ?, 0)`,
      )
      .run(translation);
    sqlite
      .prepare(
        `INSERT INTO poem_legacy_payload_attribution (
           poem_id, legacy_field, source_payload_hash, attribution_key,
           attributed_at
         ) VALUES (
           'p-attributed', 'translation', ?, 'legacy-claude-1-or-2', 1
         )`,
      )
      .run(payloadHash);
    const database = catalogRepository(sqlite);
    const attributed = await database.getPoemPage(
      "attributed-poet",
      "p-attributed",
    );
    assert.ok(attributed);
    assert.equal(attributed.poem.linesEnglishModel, "Claude 1 or 2");
    assert.equal(attributed.poem.linesEnglishModelVendor, "anthropic");
    assert.equal(
      attributed.poem.linesEnglishAttributionCertainty,
      "inferred_range",
    );

    sqlite
      .prepare("UPDATE poem SET translation = ? WHERE id = 'p-attributed'")
      .run('{"content": ["A verse"]}');
    const changed = await database.getPoemPage(
      "attributed-poet",
      "p-attributed",
    );
    assert.ok(changed);
    assert.equal(changed.poem.linesEnglishModel, undefined);
    assert.equal(changed.poem.linesEnglishModelVendor, undefined);
    const summary = await database.getAuthorPage("attributed-poet");
    assert.deepEqual(
      summary?.poems[0]?.translationModels.map(
        ({ key, provider, attributionCertainty }) => ({
          key,
          provider,
          attributionCertainty,
        }),
      ),
      [
        {
          key: "legacy",
          provider: "anthropic",
          attributionCertainty: "inferred_range",
        },
      ],
      "list fallback must not acquire exact attribution from a mismatched payload hash",
    );
    const exactPayload = JSON.stringify({
      content: ["A verse"],
      model: "claude-opus-5",
    });
    sqlite
      .prepare("UPDATE poem SET translation = ? WHERE id = 'p-attributed'")
      .run(exactPayload);
    sqlite
      .prepare(
        `INSERT INTO poem_legacy_payload_attribution
      (poem_id, legacy_field, source_payload_hash, attribution_key, attributed_at)
      VALUES ('p-attributed', 'translation', ?, 'legacy-claude-1-or-2', 2)`,
      )
      .run(hash("sha256", exactPayload, "hex"));
    const exact = await database.getPoemPage("attributed-poet", "p-attributed");
    assert.equal(exact?.poem.linesEnglishModel, "claude-opus-5");
    assert.equal(exact.poem.linesEnglishAttributionCertainty, undefined);
    const exactSummary = await database.getAuthorPage("attributed-poet");
    assert.equal(
      exactSummary?.poems[0]?.translationModels[0]?.model,
      "Claude Opus 5",
    );
  } finally {
    sqlite.close();
  }
});

void test("catalog SQL excludes hidden, empty, and malformed content", async (t) => {
  const sqlite = createDatabase();
  try {
    sqlite.exec(`
      INSERT INTO author (id, slug, name_arabic, name, hidden) VALUES
        ('a-good', 'good-poet', 'شاعر', 'Poet', 0),
        ('a-empty', 'empty-poet', 'فارغ', 'Empty', 0),
        ('a-percent', 'poet%20legacy', 'قديم', 'Legacy', 0),
        ('a-malformed', 'bad/poet', '   ', 'Bad', 0),
        ('a-hidden', 'hidden-poet', 'خفي', 'Hidden', 1);
      INSERT INTO poem (id, author_id, slug, verses, name_arabic, content_arabic, translation, hidden) VALUES
        ('p-valid', 'a-good', 'valid', 99, 'قصيدة', '{"content":["سطر أول","سطر ثان"]}', '{"content":["Line one","Line two"]}', 0),
        ('p-invalid-json', 'a-good', 'invalid-json', 1, 'سيئة', '{', NULL, 0),
        ('p-blank', 'a-good', 'blank', 1, 'فارغة', '{"content":["   "]}', NULL, 0),
        ('p-hidden', 'a-good', 'hidden', 1, 'خفية', '{"content":["بيت"]}', NULL, 1),
        ('p-percent-author', 'a-percent', 'percent-author', 1, 'قديمة', '{"content":["بيت"]}', NULL, 0),
        ('p-malformed-author', 'a-malformed', 'bad-author', 1, 'قصيدة', '{"content":["بيت"]}', NULL, 0),
        ('p-hidden-author', 'a-hidden', 'hidden-author', 1, 'خفية', '{"content":["بيت"]}', NULL, 0);
    `);
    const database = catalogRepository(sqlite);

    const authors = await database.listAuthors();
    assert.deepEqual(
      authors.map(({ author, poemCount }) => [author.id, poemCount]),
      [["a-good", 1]],
    );
    const authorPage = await database.getAuthorPage("good-poet");
    assert.ok(authorPage);
    assert.equal(authorPage.poems.length, 1);
    assert.equal(authorPage.poems[0]?.hasEnglish, true);
    assert.equal(authorPage.poems[0]?.hasInsights, false);
    assert.equal(
      authorPage.poems[0]?.verses,
      1,
      "verse count comes from content",
    );
    sqlite
      .prepare("UPDATE poem SET translation = ? WHERE id = 'p-valid'")
      .run(JSON.stringify({ content: [" ".repeat(3)] }));
    sqlite.prepare("UPDATE poem SET insights = ? WHERE id = 'p-valid'").run(
      JSON.stringify({
        summary: "A concise reading.",
        themes: ["Memory"],
        historicalContext: "A historical setting.",
        literaryDevices: ["Metaphor"],
        culturalSignificance: "A cultural note.",
        notableLines: [{ line: "سطر أول", explanation: "A notable image." }],
      }),
    );
    const pageWithBlankTranslation = await database.getAuthorPage("good-poet");
    assert.equal(pageWithBlankTranslation?.poems[0]?.hasEnglish, false);
    assert.equal(pageWithBlankTranslation.poems[0]?.hasInsights, true);
    assert.equal(await database.getAuthorPage("empty-poet"), undefined);

    const poemPage = await database.getPoemPage("good-poet", "p-valid");
    assert.ok(poemPage);
    assert.equal(poemPage.author.id, "a-good");
    assert.equal(poemPage.poem.authorId, poemPage.author.id);
    assert.deepEqual(poemPage.poem.linesArabic, ["سطر أول", "سطر ثان"]);
    assert.equal(poemPage.poem.insights?.summary, "A concise reading.");
    assert.equal(poemPage.poem.insightsTrack, "legacy");
    sqlite
      .prepare(
        "UPDATE poem SET translation_sol = ?, insights_sol = ? WHERE id = 'p-valid'",
      )
      .run(
        JSON.stringify({ content: ["Stale line one", "Stale line two"] }),
        JSON.stringify({
          summary: "A stale reading.",
          themes: ["Memory"],
          historicalContext: "A grounded setting.",
          literaryDevices: ["Metaphor"],
          culturalSignificance: "A cultural note.",
          notableLines: [{ line: "سطر أول", explanation: "A notable image." }],
        }),
      );
    const solPoemPage = await database.getPoemPage("good-poet", "p-valid");
    assert.equal(solPoemPage?.poem.linesEnglishSol, undefined);
    assert.equal(solPoemPage?.poem.insights?.summary, "A concise reading.");

    const artifactHash = "a".repeat(64);
    const normalizedPayload = JSON.stringify({
      schemaId: "saqi.poem-enrichment-output",
      schemaVersion: 2,
      translation: { lines: ["Sol line one", "Sol line two"] },
      wordGlosses: {
        tokenizerVersion: "saqi-orthographic-v1",
        lines: [
          {
            lineIndex: 0,
            segments: [
              { kind: "word", meaning: "line", surface: "سطر", tokenIndex: 0 },
              { kind: "text", surface: " " },
              { kind: "word", meaning: "first", surface: "أول", tokenIndex: 1 },
            ],
          },
          {
            lineIndex: 1,
            segments: [
              { kind: "word", meaning: "line", surface: "سطر", tokenIndex: 0 },
              { kind: "text", surface: " " },
              {
                kind: "word",
                meaning: "second",
                surface: "ثان",
                tokenIndex: 1,
              },
            ],
          },
        ],
      },
    });
    sqlite
      .prepare(
        `INSERT INTO crawl_import_bundle
          (id, schema_version, manifest_hash, expected_record_count, status,
           writer_epoch, created_at)
         VALUES ('bundle-sol', 1, ?, 1, 'open', 1, 1)`,
      )
      .run(artifactHash);
    sqlite
      .prepare(
        `INSERT INTO crawl_import_record
          (bundle_id, ordinal, record_hash, source_name, source_author_id,
           source_author_url, author_name_arabic, canonical_author_id,
           source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
           content_arabic, content_hash, observed_at)
         VALUES ('bundle-sol', 0, ?, 'source', 'author-1',
           'https://example.test/author', 'شاعر', 'a-good', 'poem-1',
           'https://example.test/poem', 'p-valid', 'قصيدة',
           '{"content":["سطر أول","سطر ثان"]}', ?, 1)`,
      )
      .run(artifactHash, artifactHash);
    sqlite.exec(`
      INSERT INTO source_author_identity VALUES
        ('source-author-1', 'source', 'author-1', 'https://example.test/author',
         'شاعر', 'a-good', 1, 1);
      INSERT INTO source_poem_identity VALUES
        ('source-poem-1', 'source', 'poem-1', 'source-author-1',
         'https://example.test/poem', 'p-valid', 1, 1, NULL);
    `);
    sqlite
      .prepare(
        `INSERT INTO poem_source_revision VALUES
          ('revision-1', 'source-poem-1', 1, ?, 'قصيدة',
           '{"content":["سطر أول","سطر ثان"]}', 1, 1,
           'bundle-sol', 0)`,
      )
      .run(artifactHash);
    sqlite
      .prepare(
        `INSERT INTO model_enrichment_artifact (
           id, source_revision_id, task_key, variant, schema_version,
           prompt_version, model, model_key, reasoning_effort, payload_hash,
           payload, created_at
         ) VALUES
          ('artifact-1', 'revision-1', 'enrich:revision-1', 0, 2, ?,
           ?, 'sol-5.6', ?, ?, ?, 1)`,
      )
      .run(
        APPROVED_ENRICHMENT_PROMPT_VERSION,
        APPROVED_ENRICHMENT_MODEL,
        APPROVED_ENRICHMENT_REASONING_EFFORT,
        artifactHash,
        normalizedPayload,
      );
    sqlite
      .prepare(
        `INSERT INTO model_enrichment_validation VALUES
          ('validation-1', 'artifact-1', 'reviewer-1', 'v1', 0, 'pass',
           'none', ?, '{}', 1)`,
      )
      .run(artifactHash);
    sqlite.exec(`
      UPDATE poem SET active_source_revision_id = 'revision-1'
      WHERE id = 'p-valid';
      INSERT INTO poem_model_publication_pointer VALUES
        ('p-valid', 'sol-5.6', 'revision-1', 'artifact-1', 1, 1, 1);
    `);
    const publishedSolPage = await database.getPoemPage("good-poet", "p-valid");
    const availableModels = async () => {
      const page = await database.getAuthorPage("good-poet");
      return page?.poems
        .find(({ id }) => id === "p-valid")
        ?.translationModels.filter(({ key }) => key === "sol-5.6");
    };
    assert.deepEqual(
      await availableModels(),
      [],
      "unapproved validation does not advertise a model",
    );
    assert.equal(
      publishedSolPage?.poem.linesEnglishSol,
      undefined,
      "an arbitrary passing validator must not expose Sol output",
    );
    const [fidelity, grounding] = APPROVED_ENRICHMENT_VALIDATIONS;
    assert.ok(fidelity);
    assert.ok(grounding);
    sqlite
      .prepare(
        `INSERT INTO model_enrichment_validation VALUES
          ('validation-fidelity', 'artifact-1', ?, ?, ?, 'pass',
           'none', ?, '{}', 2)`,
      )
      .run(
        fidelity.validatorKey,
        fidelity.validatorVersion,
        fidelity.attempt,
        artifactHash,
      );
    const singlyReviewedSolPage = await database.getPoemPage(
      "good-poet",
      "p-valid",
    );
    assert.deepEqual(
      await availableModels(),
      [],
      "both required validators must accept the publication",
    );
    assert.equal(
      singlyReviewedSolPage?.poem.linesEnglishSol,
      undefined,
      "one approved validator must not expose Sol output",
    );
    sqlite
      .prepare(
        `INSERT INTO model_enrichment_validation VALUES
          ('validation-grounding', 'artifact-1', ?, ?, ?, 'pass',
           'none', ?, '{}', 3)`,
      )
      .run(
        grounding.validatorKey,
        grounding.validatorVersion,
        grounding.attempt,
        artifactHash,
      );
    const exactlyReviewedSolPage = await database.getPoemPage(
      "good-poet",
      "p-valid",
    );
    assert.ok(exactlyReviewedSolPage);
    assert.deepEqual(
      await availableModels(),
      [{ key: "sol-5.6", model: "Sol 5.6", provider: "openai" }],
      "normal v2 publications expose compact model metadata",
    );
    assert.equal(
      exactlyReviewedSolPage.poem.linesEnglishSol,
      undefined,
      "normalized publication must not expose the stale legacy projection",
    );
    const [modelEnrichment] =
      exactlyReviewedSolPage.poem.modelEnrichments ?? [];
    assert.ok(modelEnrichment);
    assert.equal(modelEnrichment.model, "gpt-5.6-sol");
    assert.equal(modelEnrichment.displayName, "Sol 5.6");
    assert.equal(modelEnrichment.vendorKey, "openai");
    assert.equal(modelEnrichment.backendKey, "openai-codex-cli");
    assert.equal(modelEnrichment.backendName, "Codex CLI");
    assert.equal(modelEnrichment.profileKey, "sol-5.6/word-gloss-v3/source-v1");
    assert.equal(modelEnrichment.reasoningEffort, "medium");
    assert.equal(
      modelEnrichment.wordGlosses?.lines[0]?.segments[0]?.surface,
      "سطر",
    );
    sqlite.exec("DROP TABLE model_enrichment_artifact_profile");
    const preRegistryFallback = await database.getPoemPage(
      "good-poet",
      "p-valid",
    );
    assert.equal(
      preRegistryFallback?.poem.modelEnrichments?.[0]?.modelKey,
      "sol-5.6",
      "current normalized tracks remain readable before registry migration",
    );
    sqlite
      .prepare(
        `INSERT INTO model_enrichment_validation VALUES
          ('obsolete-rejection', 'artifact-1', 'obsolete-review', 'v0', 99,
           'fail', 'critical', ?, '{}', 4)`,
      )
      .run(artifactHash);
    const stillPublished = await database.getPoemPage("good-poet", "p-valid");
    assert.equal(
      stillPublished?.poem.modelEnrichments?.[0]?.modelKey,
      "sol-5.6",
      "an unrelated obsolete failure must not poison the exact active policy",
    );
    assert.equal(
      await database.getPoemPage("wrong-poet", "p-valid"),
      undefined,
    );
    assert.equal(
      await database.getPoemPage("good-poet", "p-invalid-json"),
      undefined,
    );
    const sitemapPoems = await database.listSitemapPoems(1);
    assert.equal(sitemapPoems.length, 1);
    assert.equal(sitemapPoems[0]?.author.id, sitemapPoems[0]?.poem.authorId);
    sqlite.exec("DROP TRIGGER model_enrichment_artifact_immutable_update");
    for (const lines of [[], ["", " "], "not an array"]) {
      await t.test(
        `unusable publication lines: ${JSON.stringify(lines)}`,
        async () => {
          sqlite
            .prepare(
              "UPDATE model_enrichment_artifact SET payload = ? WHERE id = 'artifact-1'",
            )
            .run(JSON.stringify({ translation: { lines } }));
          assert.deepEqual(
            await availableModels(),
            [],
            "missing usable lines do not advertise a publication",
          );
        },
      );
    }
    sqlite
      .prepare(
        "UPDATE model_enrichment_artifact SET payload = ? WHERE id = 'artifact-1'",
      )
      .run(normalizedPayload);
    sqlite.exec(
      "UPDATE model_enrichment_artifact SET reasoning_effort = 'low' WHERE id = 'artifact-1'",
    );
    assert.deepEqual(
      await availableModels(),
      [],
      "mismatched profile settings do not advertise a publication",
    );
    sqlite.exec(
      "UPDATE model_enrichment_artifact SET reasoning_effort = 'high' WHERE id = 'artifact-1'",
    );
    sqlite.exec(`
      UPDATE model_enrichment_artifact SET prompt_version = 'sol-word-gloss-v2'
        WHERE id = 'artifact-1';
      INSERT INTO model_enrichment_validation
        SELECT id || '-v2', artifact_id, validator_key, 'sol-word-gloss-v2', attempt,
          outcome, highest_severity, report_hash, report, created_at
        FROM model_enrichment_validation WHERE id IN ('validation-fidelity', 'validation-grounding');
    `);
    assert.deepEqual(
      await availableModels(),
      [{ key: "sol-5.6", model: "Sol 5.6", provider: "openai" }],
      "readable v2 high publications remain visible after the v3 medium upgrade",
    );
    sqlite.exec(`
      UPDATE model_enrichment_artifact SET prompt_version = 'sol-enrichment-v1', schema_version = 1,
        payload = '{"translation":{"lines":["First","Second"]},"insights":{"summary":"Reading","themes":["Memory"],"historicalContext":"History","literaryDevices":["Metaphor"],"culturalSignificance":"Culture","notableLines":[{"line":"First","explanation":"Meaning"}]}}'
        WHERE id = 'artifact-1';
      INSERT INTO model_enrichment_validation
        SELECT id || '-legacy', artifact_id, validator_key, 'sol-enrichment-v1', attempt,
          outcome, highest_severity, report_hash, report, created_at
        FROM model_enrichment_validation WHERE id IN ('validation-fidelity', 'validation-grounding');
    `);
    assert.deepEqual(
      await availableModels(),
      [{ key: "sol-5.6", model: "Sol 5.6", provider: "openai" }],
      "readable v1 publications retain the same model identity without duplicate recipe badges",
    );
    sqlite.exec(
      "UPDATE poem SET active_source_revision_id = NULL WHERE id = 'p-valid'",
    );
    assert.deepEqual(
      await availableModels(),
      [],
      "stale publication revisions do not advertise a model",
    );
  } finally {
    sqlite.close();
  }
});

void test("catalog retries legacy queries when normalized model tables are absent", async () => {
  const sqlite = createDatabase();
  try {
    sqlite.exec(`
      DROP TABLE poem_model_publication_pointer;
      DROP TABLE model_enrichment_validation;
      DROP TABLE model_enrichment_artifact;
      INSERT INTO author (id, slug, name_arabic, name, hidden)
      VALUES ('a-legacy', 'legacy-poet', 'شاعر', 'Legacy Poet', 0);
      INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, content_arabic,
         translation, insights, hidden)
      VALUES
        ('p-legacy', 'a-legacy', 'legacy-poem', 1, 'قصيدة',
         '{"content":["بيت"]}', '{"content":["A verse"]}',
         '{"summary":"Reading","themes":["Memory"],
           "historicalContext":"History","literaryDevices":["Metaphor"],
           "culturalSignificance":"Culture","notableLines":[
             {"line":"بيت","explanation":"Image"}]}', 0);
    `);
    const database = catalogRepository(sqlite);

    const authorPage = await database.getAuthorPage("legacy-poet");
    assert.ok(authorPage);
    assert.equal(authorPage.poems[0]?.hasEnglish, true);
    assert.equal(authorPage.poems[0]?.hasInsights, true);

    const poemPage = await database.getPoemPage("legacy-poet", "p-legacy");
    assert.ok(poemPage);
    assert.deepEqual(poemPage.poem.linesEnglish, ["A verse"]);
    assert.equal(poemPage.poem.insights?.summary, "Reading");
    assert.deepEqual(poemPage.poem.modelEnrichments, undefined);
  } finally {
    sqlite.close();
  }
});

void test("catalog rejects generated refusals and overlong translation tracks", async () => {
  const sqlite = createDatabase();
  try {
    sqlite.exec(`
      DROP TRIGGER poem_generated_title_guard_before_insert;
      DROP TRIGGER poem_generated_title_guard_before_update;
    `);
    sqlite.exec(`
      INSERT INTO author (id, slug, name_arabic) VALUES ('a-quality', 'quality-poet', 'شاعر');
      INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, name_english, poem_title_first_line, content_arabic, translation)
      VALUES
        ('p-refusal', 'a-quality', 'refusal', 1, 'قصيدة أولى',
         'Unfortunately I am unable to translate this poem',
         'Valid legacy title',
         '{"content":["سطر"]}',
         '{"content":["Unfortunately I am unable to translate this poem"]}'),
        ('p-preamble', 'a-quality', 'preamble', 1, 'قصيدة تمهيدية',
         'Here is the English translation of the Arabic poem title: Will I Ever Say One Day',
         NULL,
         '{"content":["سطر"]}',
         '{"content":["A line"]}'),
        ('p-multiline', 'a-quality', 'multiline', 1, 'قصيدة متعددة',
         'A plausible title\n\nقصيدة متعددة',
         NULL,
         '{"content":["سطر"]}',
         '{"content":["A line"]}'),
        ('p-first-person', 'a-quality', 'first-person', 1, 'لي صديق',
         'I Have a Friend Who Keeps My Secrets',
         NULL,
         '{"content":["سطر"]}',
         '{"content":["A line"]}'),
        ('p-overlong', 'a-quality', 'overlong', 1, 'قصيدة ثانية', NULL, NULL,
         '{"content":["سطر"]}',
         '{"content":["Line one","Hallucinated extra line"]}'),
        ('p-blank-pair', 'a-quality', 'blank-pair', 2, 'قصيدة ثالثة', NULL, NULL,
         '{"content":[" ","","سطر أول","سطر ثان"]}',
         '{"content":[" ","","Line one","Line two"]}');
      UPDATE poem SET insights = '{"summary":"Only a summary"}' WHERE id = 'p-overlong';
    `);
    const database = catalogRepository(sqlite);

    const authorPage = await database.getAuthorPage("quality-poet");
    assert.ok(authorPage);
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-refusal")?.nameEnglish,
      "Valid legacy title",
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-overlong")?.hasInsights,
      false,
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-overlong")?.hasEnglish,
      false,
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-blank-pair")?.hasEnglish,
      true,
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-preamble")?.nameEnglish,
      undefined,
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-multiline")?.nameEnglish,
      undefined,
    );
    assert.equal(
      authorPage.poems.find((poem) => poem.id === "p-first-person")
        ?.nameEnglish,
      "I Have a Friend Who Keeps My Secrets",
    );

    const refusal = await database.getPoemPage("quality-poet", "p-refusal");
    assert.ok(refusal);
    assert.equal(refusal.poem.nameEnglish, "Valid legacy title");
    assert.equal(refusal.poem.linesEnglish, undefined);

    const overlong = await database.getPoemPage("quality-poet", "p-overlong");
    assert.ok(overlong);
    assert.equal(overlong.poem.linesEnglish, undefined);
    assert.equal(overlong.poem.insights, undefined);

    const normalized = await database.getPoemPage(
      "quality-poet",
      "p-blank-pair",
    );
    assert.ok(normalized);
    assert.deepEqual(normalized.poem.linesArabic, ["سطر أول", "سطر ثان"]);
    assert.deepEqual(normalized.poem.linesEnglish, ["Line one", "Line two"]);
    assert.equal(normalized.poem.verses, 1);

    for (const model of [" ", "x".repeat(101), `bad\u{202e}model`]) {
      sqlite
        .prepare(
          "UPDATE poem SET translation_gemini = ? WHERE id = 'p-blank-pair'",
        )
        .run(JSON.stringify({ content: ["One", "Two"], model }));
      const page = await database.getPoemPage("quality-poet", "p-blank-pair");
      assert.deepEqual(page?.poem.linesEnglishGemini, ["One", "Two"]);
      assert.equal(page.poem.linesEnglishGeminiModel, undefined);
    }

    sqlite
      .prepare("UPDATE poem SET name_english = ? WHERE id = 'p-blank-pair'")
      .run(`Spoof\u{202e}title`);
    const unsafeTitlePage = await database.getPoemPage(
      "quality-poet",
      "p-blank-pair",
    );
    assert.ok(unsafeTitlePage);
    assert.equal(unsafeTitlePage.poem.nameEnglish, undefined);

    sqlite
      .prepare(
        "UPDATE poem SET translation_gemini = ? WHERE id = 'p-blank-pair'",
      )
      .run(JSON.stringify({ content: [`One\u{202e}`, "Two"] }));
    const unsafeTranslationPage = await database.getPoemPage(
      "quality-poet",
      "p-blank-pair",
    );
    assert.ok(unsafeTranslationPage);
    assert.equal(unsafeTranslationPage.poem.linesEnglishGemini, undefined);

    sqlite
      .prepare("UPDATE poem SET content_arabic = ? WHERE id = 'p-blank-pair'")
      .run(JSON.stringify({ content: [`سطر\u{0001}`] }));
    assert.equal(
      await database.getPoemPage("quality-poet", "p-blank-pair"),
      undefined,
    );
  } finally {
    sqlite.close();
  }
});

void test("database rejects generated title preambles at write time", () => {
  const sqlite = createDatabase();
  try {
    sqlite.exec(`
      INSERT INTO author (id, slug, name_arabic)
      VALUES ('a-title-guard', 'title-guard', 'شاعر');
      INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, name_english, content_arabic)
      VALUES
        ('p-title-guard', 'a-title-guard', 'title-guard', 1, 'قصيدة',
         'I Have a Friend Who Keeps My Secrets', '{"content":["سطر"]}');
    `);

    assert.throws(
      () =>
        sqlite
          .prepare("UPDATE poem SET name_english = ? WHERE id = ?")
          .run(
            "Here is the English translation of the Arabic poem title: A Title",
            "p-title-guard",
          ),
      /invalid generated poem title/u,
    );
    assert.throws(
      () =>
        sqlite
          .prepare("UPDATE poem SET name_english = ? WHERE id = ?")
          .run(
            "I will not provide translations without proper context.",
            "p-title-guard",
          ),
      /invalid generated poem title/u,
    );
    assert.equal(
      sqlite
        .prepare("SELECT name_english FROM poem WHERE id = ?")
        .pluck()
        .get("p-title-guard"),
      "I Have a Friend Who Keeps My Secrets",
    );
  } finally {
    sqlite.close();
  }
});

void test("author pages bound result size and reject pages beyond the catalog", async () => {
  const sqlite = createDatabase();
  try {
    sqlite
      .prepare("INSERT INTO author (id, slug, name_arabic) VALUES (?, ?, ?)")
      .run("a-many", "many-poems", "شاعر غزير");
    const insertPoem = sqlite.prepare(
      `INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, content_arabic)
       VALUES (?, 'a-many', ?, 1, ?, '{"content":["بيت"]}')`,
    );
    sqlite.transaction(() => {
      for (let index = 0; index < AUTHOR_PAGE_SIZE + 5; index += 1) {
        const suffix = String(index).padStart(4, "0");
        insertPoem.run(`p-${suffix}`, `poem-${suffix}`, `قصيدة ${suffix}`);
      }
    })();
    const database = catalogRepository(sqlite);

    const first = await database.getAuthorPage("many-poems");
    const second = await database.getAuthorPage("many-poems", 2);
    assert.ok(first);
    assert.ok(second);
    assert.equal(first.poemCount, AUTHOR_PAGE_SIZE + 5);
    assert.equal(first.pageCount, 2);
    assert.equal(first.poems.length, AUTHOR_PAGE_SIZE);
    assert.equal(second.poems.length, 5);
    assert.equal(
      new Set([...first.poems, ...second.poems].map(({ id }) => id)).size,
      AUTHOR_PAGE_SIZE + 5,
    );
    assert.equal(await database.getAuthorPage("many-poems", 3), undefined);
    assert.equal(await database.getAuthorPage("many-poems", 0), undefined);
    assert.equal(
      await database.getAuthorPage("many-poems", Number.MAX_SAFE_INTEGER),
      undefined,
    );
  } finally {
    sqlite.close();
  }
});

void test("sitemap partitioning is stable and rejects invalid shards", async () => {
  const sqlite = createDatabase();
  try {
    const database = catalogRepository(sqlite);
    sqlite
      .prepare("INSERT INTO author (id, slug, name_arabic) VALUES (?, ?, ?)")
      .run("a-sitemap", "sitemap-poet", "شاعر");
    const insertPoem = sqlite.prepare(
      `INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, content_arabic, sitemap_shard)
       VALUES (?, 'a-sitemap', ?, 1, 'قصيدة', '{"content":["بيت"]}', ?)`,
    );
    const hexadecimalPrefixes = [
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "a",
      "b",
      "c",
      "d",
      "e",
      "f",
    ];
    for (const [shard, prefix] of hexadecimalPrefixes.entries()) {
      insertPoem.run(`${prefix}-poem`, `${prefix}-poem`, shard);
    }
    for (let shard = 1; shard <= 16; shard += 1) {
      const sitemapPoems = await database.listSitemapPoems(shard);
      assert.deepEqual(
        sitemapPoems.map(({ poem }) => poem.id),
        [`${(shard - 1).toString(16)}-poem`],
      );
    }
    assert.deepEqual(await database.listSitemapPoems(0), []);
    assert.deepEqual(await database.listSitemapPoems(17), []);
  } finally {
    sqlite.close();
  }
});

for (const authorCount of [0, 1, 199, 200, 201]) {
  void test(`author index lists all ${String(authorCount)} publishable authors`, async () => {
    const sqlite = createDatabase();
    try {
      const insertAuthor = sqlite.prepare(
        "INSERT INTO author (id, slug, name_arabic) VALUES (?, ?, ?)",
      );
      const insertPoem = sqlite.prepare(
        `INSERT INTO poem
          (id, author_id, slug, verses, name_arabic, content_arabic)
         VALUES (?, ?, ?, 1, ?, '{"content":["بيت"]}')`,
      );
      sqlite.transaction(() => {
        for (let index = 0; index < authorCount; index += 1) {
          const suffix = String(index).padStart(3, "0");
          insertAuthor.run(`a-${suffix}`, `poet-${suffix}`, `شاعر ${suffix}`);
          insertPoem.run(
            `p-${suffix}`,
            `a-${suffix}`,
            `poem-${suffix}`,
            `قصيدة ${suffix}`,
          );
        }
        insertAuthor.run("a-zero", "zero-poems", "شاعر بلا قصائد");
      })();
      const database = catalogRepository(sqlite);
      const page = await database.getAuthorIndex();
      assert.ok(page);
      assert.equal(page.authors.length, authorCount);
      const ids = page.authors.map(({ author }) => author.id);
      assert.equal(ids.length, authorCount);
      assert.equal(new Set(ids).size, authorCount);
      assert.ok(!ids.includes("a-zero"), "authors with zero poems stay hidden");
    } finally {
      sqlite.close();
    }
  });
}

void test("publishability triggers maintain public counts idempotently", () => {
  const database = createDatabase();
  try {
    database.exec(`
      INSERT INTO author (id, slug, name_arabic, hidden)
      VALUES ('a-trigger', 'trigger', 'اختبار', 0);
      INSERT INTO poem (id, author_id, slug, verses, name_arabic, content_arabic, hidden)
      VALUES ('p-trigger', 'a-trigger', 'trigger-poem', 1, 'اختبار', '{"content":[]}', 0);
    `);
    const count = () =>
      database
        .prepare(
          "SELECT public_poem_count AS count FROM author WHERE id = 'a-trigger'",
        )
        .get() as { count: number };
    assert.equal(count().count, 0);
    database
      .prepare("UPDATE poem SET content_arabic = ? WHERE id = 'p-trigger'")
      .run(JSON.stringify({ content: [1, "بيت"] }));
    assert.equal(count().count, 0);
    database
      .prepare("UPDATE poem SET content_arabic = ? WHERE id = 'p-trigger'")
      .run(JSON.stringify({ content: [{ line: "بيت" }] }));
    assert.equal(count().count, 0);
    database
      .prepare("UPDATE poem SET content_arabic = ? WHERE id = 'p-trigger'")
      .run(JSON.stringify({ content: ["بيت"] }));
    assert.equal(count().count, 1);
    database.prepare("UPDATE poem SET hidden = 1 WHERE id = 'p-trigger'").run();
    assert.equal(count().count, 0);
    database.prepare("UPDATE poem SET hidden = 0 WHERE id = 'p-trigger'").run();
    assert.equal(count().count, 1);
    database.prepare("DELETE FROM poem WHERE id = 'p-trigger'").run();
    assert.equal(count().count, 0);
  } finally {
    database.close();
  }
});

void test("stable poem pagination index replaces translation-derived ordering", () => {
  const database = createDatabase();
  try {
    const indexes = database
      .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
      .all() as { name: string }[];
    const names = new Set(indexes.map(({ name }) => name));
    assert.ok(names.has("idx_poem_public_author_title"));
    assert.ok(!names.has("idx_poem_public_author_order"));
  } finally {
    database.close();
  }
});

void test("catalogs use normalized Arabic alphabetical order", async () => {
  const sqlite = createDatabase();
  try {
    const insertAuthor = sqlite.prepare(
      "INSERT INTO author (id, slug, name_arabic) VALUES (?, ?, ?)",
    );
    const insertPoem = sqlite.prepare(
      `INSERT INTO poem
        (id, author_id, slug, verses, name_arabic, content_arabic)
       VALUES (?, ?, ?, 1, ?, '{"content":["بيت"]}')`,
    );
    const names = ["آمنة", "بدر", "أحمد", "إبراهيم"];
    sqlite.transaction(() => {
      for (const [index, name] of names.entries()) {
        insertAuthor.run(`a-${String(index)}`, `author-${String(index)}`, name);
        insertPoem.run(
          `p-${String(index)}`,
          `a-${String(index)}`,
          `poem-${String(index)}`,
          name,
        );
      }
      for (const [index, name] of names.entries()) {
        insertPoem.run(
          `ps-${String(index)}`,
          "a-0",
          `sorted-poem-${String(index)}`,
          name,
        );
      }
    })();

    const authors = await catalogRepository(sqlite).listAuthors();
    assert.deepEqual(
      authors.map(({ author }) => author.nameArabic),
      ["إبراهيم", "أحمد", "آمنة", "بدر"],
    );
    const poems = await catalogRepository(sqlite).getAuthorPage("author-0");
    assert.deepEqual(
      poems?.poems.map(({ nameArabic }) => nameArabic),
      ["إبراهيم", "أحمد", "آمنة", "آمنة", "بدر"],
    );
  } finally {
    sqlite.close();
  }
});
