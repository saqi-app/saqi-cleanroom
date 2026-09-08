import { hash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  APPROVED_ENRICHMENT_PROMPT_VERSION,
  APPROVED_ENRICHMENT_REASONING_EFFORT,
  APPROVED_ENRICHMENT_VALIDATIONS,
  enrichmentPublicationActionHashBody,
  publicationIntentIdBody,
  sitemapShardForId,
  sourceAdmissionIdBody,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CorpusImportCoordinator } from "../corpus-import-coordinator";
import {
  CorpusRevisionConflictError,
  D1CorpusRevisionStore,
  LostWriterEpochError,
  type StageRecordInput,
} from "../corpus-revision-store";

const MIGRATIONS_DIRECTORY = fileURLToPath(
  new URL("../../../../app/migrations/", import.meta.url),
);
const MIGRATION_FILES = readdirSync(MIGRATIONS_DIRECTORY)
  .filter((fileName) => fileName.endsWith(".sql"))
  .toSorted();
const HASH_C = "c".repeat(64);
const SOURCE_BASE_URL = "https://source.invalid/";
const SOURCE_NAME = "primary-source";
const PASS_REVIEW = {
  fidelityScore: 96,
  findings: [],
  insightScore: 94,
  verdict: "pass" as const,
};

function seedFixtureModelProfiles(database: InstanceType<typeof Database>) {
  database.exec(`
    INSERT INTO ai_vendor (vendor_key, display_name, created_at)
      VALUES ('fixture-vendor', 'Fixture Vendor', 0);
    INSERT INTO inference_backend (backend_key, display_name, created_at)
      VALUES ('fixture-backend', 'Fixture Backend', 0);
    INSERT INTO ai_model (
      model_key, vendor_key, family_key, version_label, display_name, created_at
    ) VALUES
      ('fixture-model-a', 'fixture-vendor', 'fixture', 'a', 'Fixture Model A', 0),
      ('fixture-model-b', 'fixture-vendor', 'fixture', 'b', 'Fixture Model B', 0);
    INSERT INTO enrichment_profile (
      profile_key, public_track_key, model_key, backend_key, runtime_model_id,
      prompt_version, reasoning_effort, input_schema_version,
      output_schema_version, created_at
    ) VALUES
      ('fixture-model-a/source-v1', 'fixture-model-a', 'fixture-model-a',
       'fixture-backend', 'fixture-model-a', 'fixture-a-v1', 'high', 1, 1, 0),
      ('fixture-model-b/source-v1', 'fixture-model-b', 'fixture-model-b',
       'fixture-backend', 'fixture-model-b', 'fixture-b-v1', 'high', 1, 1, 0);
  `);
}

describe("D1CorpusRevisionStore", () => {
  let database: InstanceType<typeof Database>;
  let db: ReturnType<typeof drizzle>;
  let store: D1CorpusRevisionStore;

  beforeEach(() => {
    database = new Database(":memory:");
    database.pragma("foreign_keys = ON");
    for (const fileName of MIGRATION_FILES) {
      database.exec(
        readFileSync(`${MIGRATIONS_DIRECTORY}/${fileName}`, "utf8"),
      );
    }
    database
      .prepare(
        "INSERT INTO author (id, slug, name_arabic, status) VALUES (?, ?, ?, ?)",
      )
      .run("author-1", "author-1", "شاعر", "init");
    database
      .prepare(
        "INSERT INTO poem (id, author_id, slug, verses, name_arabic, content_arabic) VALUES (?, ?, ?, ?, ?, ?)",
      )
      .run("poem-1", "author-1", "poem-1", 1, "قديم", '{"content":["قديم"]}');
    db = drizzle(database);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument -- the better-sqlite3 and D1 Drizzle APIs are compatible for store tests.
    store = new D1CorpusRevisionStore(db as any, {
      sourceBaseUrl: SOURCE_BASE_URL,
      sourceName: SOURCE_NAME,
      sourceProfile: DEFAULT_SOURCE_ADAPTER_PROFILE,
    });
  });

  afterEach(() => database.close());

  it("reads the current writer epoch from the singleton", async () => {
    await expect(store.currentWriterEpoch()).resolves.toBe(1);
  });

  it.each([null, undefined, {}, { writer_epoch: 0 }])(
    "retains missing writer classification for %j",
    async (row) => {
      vi.spyOn(db, "get").mockReturnValueOnce(row);
      await expect(store.currentWriterEpoch()).rejects.toBeInstanceOf(
        LostWriterEpochError,
      );
    },
  );

  it.each(["7", -1, 1.5])(
    "rejects malformed writer epoch %j without casting",
    async (writerEpoch) => {
      vi.spyOn(db, "get").mockReturnValueOnce({ writer_epoch: writerEpoch });
      await expect(store.currentWriterEpoch()).rejects.toMatchObject({
        name: "ZodError",
      });
    },
  );

  it.each([null, 42, {}, "invalid-id"])(
    "rejects malformed lineage row identifiers before indexing: %j",
    async (poemId) => {
      vi.spyOn(db, "all").mockReturnValueOnce([{ poem_id: poemId }]);
      const admit = vi.spyOn(store, "admitSource");
      await expect(
        store.adoptLegacySourceLineage({
          poemIds: ["22222222-2222-4222-8222-222222222222"],
        }),
      ).rejects.toMatchObject({ name: "ZodError" });
      expect(admit).not.toHaveBeenCalled();
    },
  );

  it("stages, seals, plans, and promotes the same source record idempotently", async () => {
    await stageSealedBundle(store);
    await expect(store.stageRecord(stagedRecord())).resolves.toBeUndefined();
    const firstPlan = await store.planPromotion("bundle-1", 1);

    expect(firstPlan.items).toMatchObject([
      {
        ordinal: 0,
        revisionAction: "insert",
        pointerAction: "create",
        expectedPointerVersion: null,
      },
    ]);
    const item = firstPlan.items[0];
    expect(item).toBeDefined();

    await expect(store.promoteRecord(firstPlan, item)).resolves.toMatchObject({
      pointerVersion: 1,
      revisionState: "inserted",
    });
    await expect(store.promoteRecord(firstPlan, item)).resolves.toMatchObject({
      pointerVersion: 1,
      revisionState: "reused",
    });
    const counts = {
      advancedPointers: 1,
      insertedRevisions: 1,
      reusedRevisions: 0,
      unchangedPointers: 0,
    };
    await store.recordImportReceipt(firstPlan, counts);
    await store.recordImportReceipt(firstPlan, counts);
    await expect(
      store.recordImportReceipt(firstPlan, {
        ...counts,
        insertedRevisions: 0,
        reusedRevisions: 1,
      }),
    ).rejects.toBeInstanceOf(CorpusRevisionConflictError);
    expect(scalar(database, "SELECT count(*) FROM poem_source_revision")).toBe(
      1,
    );
    expect(scalar(database, "SELECT count(*) FROM poem_source_pointer")).toBe(
      1,
    );
    expect(
      database
        .prepare("SELECT content_arabic FROM poem WHERE id = 'poem-1'")
        .pluck()
        .get(),
    ).toBe('{"content":["بيت جديد"]}');

    const replayPlan = await store.planPromotion("bundle-1", 1);
    expect(replayPlan).toEqual(firstPlan);
  });

  it("backfills exact active-source fingerprints in bounded resumable pages", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);
    // Reproduce a revision that predates migration 0034. Current promotion
    // correctly fingerprints new rows, while the production backfill must
    // cover the immutable historical population created before that contract.
    database.exec(`
      DROP TRIGGER source_revision_fingerprint_immutable_delete;
      DELETE FROM source_revision_fingerprint;
    `);
    expect(
      scalar(database, "SELECT count(*) FROM source_revision_fingerprint"),
    ).toBe(0);

    const first = await store.backfillActiveSourceFingerprints({ limit: 1 });
    expect(first).toEqual({
      complete: true,
      existing: 0,
      inserted: 1,
      nextCursor: {
        createdAt: expect.any(Number),
        sourceRevisionId: item.revisionId,
      },
      scanned: 1,
    });
    expect(
      database
        .prepare(
          `SELECT algorithm, line_nfc_hash, prompt_material_hash
             FROM source_revision_fingerprint
            WHERE source_revision_id = ?`,
        )
        .get(item.revisionId),
    ).toEqual({
      algorithm: "sha256-canonical-nfc-v1",
      line_nfc_hash: hash("sha256", sourceLineNfcHashBody(["بيت جديد"]), "hex"),
      prompt_material_hash: hash(
        "sha256",
        sourcePromptMaterialHashBody({
          authorArabic: "شاعر",
          linesArabic: ["بيت جديد"],
          titleArabic: "قصيدة جديدة",
        }),
        "hex",
      ),
    });

    await expect(
      store.backfillActiveSourceFingerprints({
        cursor: first.nextCursor ?? undefined,
        limit: 1,
      }),
    ).resolves.toEqual({
      complete: true,
      existing: 0,
      inserted: 0,
      nextCursor: null,
      scanned: 0,
    });
    expect(
      scalar(database, "SELECT count(*) FROM source_revision_fingerprint"),
    ).toBe(1);
  });

  it("adopts legacy source lineage without replacing its canonical UUID", async () => {
    const authorId = "11111111-1111-4111-8111-111111111111";
    const poemId = "22222222-2222-4222-8222-222222222222";
    insertLegacyPoem(database, { authorId, poemId });

    const first = await store.adoptLegacySourceLineage({ poemIds: [poemId] });
    const replay = await store.adoptLegacySourceLineage({ poemIds: [poemId] });
    const sourceContentSha256 = canonicalHash({
      content: ["ذاك ظبي", "تحير الحسن"],
      titleArabic: "ذاك ظبي تحير الحسن في",
    });
    const sourceRevisionId = hash(
      "sha256",
      `revision\u{1F}${SOURCE_NAME}\u{1F}68242\u{1F}2\u{1F}${sourceContentSha256}`,
      "hex",
    );

    expect(first).toEqual({
      adopted: 1,
      conflicts: [],
      scanned: 1,
      unchanged: 0,
    });
    expect(replay).toEqual({
      ...first,
      adopted: 0,
      unchanged: 1,
    });
    expect(
      database
        .prepare(
          `SELECT source.external_id, source.canonical_poem_id,
                  pointer.revision_id, poem.active_source_revision_id
             FROM source_poem_identity source
             JOIN poem_source_pointer pointer ON pointer.source_poem_id = source.id
             JOIN poem ON poem.id = source.canonical_poem_id`,
        )
        .get(),
    ).toMatchObject({
      active_source_revision_id: sourceRevisionId,
      canonical_poem_id: poemId,
      external_id: "68242",
      revision_id: sourceRevisionId,
    });
    expect(
      scalar(
        database,
        "SELECT count(*) FROM poem WHERE id <> 'poem-1' AND slug LIKE 'source-%'",
      ),
    ).toBe(0);
    expect(
      scalar(database, "SELECT count(*) FROM source_revision_fingerprint"),
    ).toBe(1);
    expect(database.pragma("foreign_key_check")).toEqual([]);
  });

  it("adopts only an explicit bounded set of completed canonical UUIDs", async () => {
    const selectedPoemId = "22222222-2222-4222-8222-222222222222";
    const deferredPoemId = "44444444-4444-4444-8444-444444444444";
    insertLegacyPoem(database, {
      authorId: "11111111-1111-4111-8111-111111111111",
      poemId: selectedPoemId,
    });
    insertLegacyPoem(database, {
      authorId: "33333333-3333-4333-8333-333333333333",
      poemId: deferredPoemId,
      sourcePoemId: "68243",
    });

    await expect(
      store.adoptLegacySourceLineage({ poemIds: [selectedPoemId] }),
    ).resolves.toEqual({
      adopted: 1,
      conflicts: [],
      scanned: 1,
      unchanged: 0,
    });
    expect(
      database
        .prepare(
          "SELECT canonical_poem_id FROM source_poem_identity ORDER BY canonical_poem_id",
        )
        .pluck()
        .all(),
    ).toEqual([selectedPoemId]);
    await expect(
      store.adoptLegacySourceLineage({ poemIds: [deferredPoemId] }),
    ).resolves.toMatchObject({ adopted: 1 });
    await expect(
      store.adoptLegacySourceLineage({
        poemIds: ["66666666-6666-4666-8666-666666666666"],
      }),
    ).resolves.toEqual({
      adopted: 0,
      conflicts: [
        {
          code: "LEGACY_SOURCE_LINEAGE_TARGET_UNRESOLVED",
          poemId: "66666666-6666-4666-8666-666666666666",
        },
      ],
      scanned: 1,
      unchanged: 0,
    });
  });

  it("isolates one exact conflict while adopting and replaying nine peers", async () => {
    const poemIds = Array.from(
      { length: 10 },
      (_, index) =>
        `20000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
    );
    for (const [index, poemId] of poemIds.entries()) {
      insertLegacyPoem(database, {
        authorId: `10000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
        poemId,
        sourcePoemId: String(70_000 + index),
      });
    }
    const conflictPoemId = poemIds.at(-1);
    if (!conflictPoemId) throw new Error("conflict poem missing");
    database
      .prepare(
        `INSERT INTO source_author_identity (
          id, source_name, external_id, canonical_url, name_arabic,
          canonical_author_id, first_observed_at, last_observed_at
        ) VALUES (?, 'primary-source', ?, ?, 'شاعر', 'author-1', 1, 1)`,
      )
      .run(
        `${SOURCE_NAME}\u{1F}poet-Test-70009`,
        "poet-Test-70009",
        "https://source.invalid/writers/poet-Test-70009",
      );

    const first = await store.adoptLegacySourceLineage({ poemIds });
    const replay = await store.adoptLegacySourceLineage({ poemIds });

    expect(first).toEqual({
      adopted: 9,
      conflicts: [
        {
          code: "LEGACY_SOURCE_AUTHOR_OWNERSHIP_CONFLICT",
          poemId: conflictPoemId,
        },
      ],
      scanned: 10,
      unchanged: 0,
    });
    expect(replay).toEqual({
      adopted: 0,
      conflicts: first.conflicts,
      scanned: 10,
      unchanged: 9,
    });
    expect(scalar(database, "SELECT count(*) FROM source_poem_identity")).toBe(
      9,
    );
  });

  it("does not convert writer epoch or infrastructure failures into row conflicts", async () => {
    const poemId = "22222222-2222-4222-8222-222222222222";
    insertLegacyPoem(database, {
      authorId: "11111111-1111-4111-8111-111111111111",
      poemId,
    });
    const admit = vi.spyOn(store, "admitSource");
    const epoch = new LostWriterEpochError();
    admit.mockRejectedValueOnce(epoch);
    await expect(
      store.adoptLegacySourceLineage({ poemIds: [poemId] }),
    ).rejects.toBe(epoch);

    const infrastructure = new Error("D1_UNAVAILABLE");
    admit.mockRejectedValueOnce(infrastructure);
    await expect(
      store.adoptLegacySourceLineage({ poemIds: [poemId] }),
    ).rejects.toBe(infrastructure);
    admit.mockRestore();
  });

  it("rejects conflicting legacy source-author ownership", async () => {
    insertLegacyPoem(database, {
      authorId: "11111111-1111-4111-8111-111111111111",
      poemId: "22222222-2222-4222-8222-222222222222",
    });
    database
      .prepare(
        `INSERT INTO source_author_identity (
          id, source_name, external_id, canonical_url, name_arabic,
          canonical_author_id, first_observed_at, last_observed_at
        ) VALUES (?, 'primary-source', 'poet-Muslim-ibn-al-Walid', ?, 'شاعر',
                  'author-1', 1, 1)`,
      )
      .run(
        `${SOURCE_NAME}\u{1F}poet-Muslim-ibn-al-Walid`,
        "https://source.invalid/writers/poet-Muslim-ibn-al-Walid",
      );
    await expect(
      store.adoptLegacySourceLineage({
        poemIds: ["22222222-2222-4222-8222-222222222222"],
      }),
    ).resolves.toEqual({
      adopted: 0,
      conflicts: [
        {
          code: "LEGACY_SOURCE_AUTHOR_OWNERSHIP_CONFLICT",
          poemId: "22222222-2222-4222-8222-222222222222",
        },
      ],
      scanned: 1,
      unchanged: 0,
    });
  });

  it("admits an exact source once and returns a stable production binding", async () => {
    insertAdmissionAuthor(database);
    const input = exactAdmissionInput();

    const first = await store.admitSource(input);
    const replay = await store.admitSource(input);

    expect(first.state).toBe("admitted");
    expect(replay).toEqual({ ...first, state: "unchanged" });
    expect(first.binding).toMatchObject({
      authorId: "author-1",
      authorNameArabic: "شاعر",
      externalPoemId: "101681",
      lineNfcHash: input.lineNfcHash,
      promptMaterialHash: hash(
        "sha256",
        sourcePromptMaterialHashBody({
          authorArabic: "شاعر",
          linesArabic: input.linesArabic,
          titleArabic: input.titleArabic,
        }),
        "hex",
      ),
      sourceRevisionId: input.sourceRevisionId,
    });
    expect(
      scalar(database, "SELECT count(*) FROM source_revision_fingerprint"),
    ).toBe(1);
    expect(
      scalar(database, "SELECT count(*) FROM source_admission_clock"),
    ).toBe(1);
  });

  it("rejects admissions outside the configured source boundary", async () => {
    insertAdmissionAuthor(database);
    const input = exactAdmissionInput();

    await expect(
      store.admitSource({ ...input, sourceName: "other-source" }),
    ).rejects.toThrow("SOURCE_CONFIGURATION_MISMATCH");
    await expect(
      store.admitSource({
        ...input,
        sourcePoemUrl: "https://other.invalid/works/101681",
      }),
    ).rejects.toThrow("SOURCE_CONFIGURATION_MISMATCH");
    await expect(
      store.admitSource({
        ...input,
        sourceAuthorUrl: "https://source.invalid/writers/poet-almaarri?view=1",
      }),
    ).rejects.toThrow("SOURCE_CONFIGURATION_MISMATCH");
    expect(
      scalar(database, "SELECT count(*) FROM source_admission_clock"),
    ).toBe(0);
  });

  it("atomically publishes a bound artifact and replays its receipt after writer rotation", async () => {
    insertAdmissionAuthor(database);
    const admitted = await store.admitSource(exactAdmissionInput());
    const payload = {
      schemaId: "saqi.poem-enrichment-output" as const,
      schemaVersion: 2 as const,
      translation: { lines: ["Verse"] },
      wordGlosses: {
        lines: [{ lineIndex: 0, segments: [] }],
        tokenizerVersion: "saqi-orthographic-v1" as const,
      },
    };
    const artifact = {
      id: "bound-artifact-1",
      model: "gpt-5.6-sol" as const,
      modelKey: "sol-5.6" as const,
      payload,
      payloadHash: canonicalHash(payload),
      promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
      reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
      schemaVersion: 2,
      sourceRevisionId: admitted.binding.sourceRevisionId,
      taskKey: "bound-task-1",
      variant: 0,
    };
    const validations = APPROVED_ENRICHMENT_VALIDATIONS.map(
      ({ attempt, validatorKey, validatorVersion }, index) => ({
        artifactId: artifact.id,
        attempt,
        highestSeverity: "none" as const,
        id: `bound-validation-${String(index + 1)}`,
        outcome: "pass" as const,
        report: PASS_REVIEW,
        reportHash: canonicalHash(PASS_REVIEW),
        validatorKey,
        validatorVersion,
      }),
    );
    const publicationIntentId = hash(
      "sha256",
      publicationIntentIdBody({
        artifactId: artifact.id,
        bindingId: admitted.binding.bindingId,
        modelKey: artifact.modelKey,
        promptVersion: artifact.promptVersion,
      }),
      "hex",
    );
    const body = {
      artifact,
      binding: admitted.binding,
      publicationIntentId,
      validations,
    };
    const input = {
      ...body,
      actionHash: hash(
        "sha256",
        enrichmentPublicationActionHashBody(body),
        "hex",
      ),
    };
    const coordinator = new CorpusImportCoordinator(store);
    await store.putEnrichmentArtifact(artifact);
    for (const validation of validations) {
      await store.putEnrichmentValidation(validation);
    }

    expect(database.pragma("foreign_key_check")).toEqual([]);
    expect(
      database
        .prepare(
          `SELECT
            EXISTS(SELECT 1 FROM poem WHERE id = ?) AS poem_found,
            EXISTS(SELECT 1 FROM enrichment_profile WHERE public_track_key = ?) AS model_found,
            EXISTS(SELECT 1 FROM poem_source_revision WHERE id = ?) AS revision_found,
            EXISTS(SELECT 1 FROM model_enrichment_artifact WHERE id = ?) AS artifact_found`,
        )
        .get(
          admitted.binding.poemId,
          artifact.modelKey,
          admitted.binding.sourceRevisionId,
          artifact.id,
        ),
    ).toEqual({
      artifact_found: 1,
      model_found: 1,
      poem_found: 1,
      revision_found: 1,
    });

    const receipt = await coordinator.publishBoundEnrichment(input);
    const previousPayload = {
      ...payload,
      translation: { lines: ["Earlier verse"] },
    };
    const previousArtifact = {
      ...artifact,
      payload: previousPayload,
      payloadHash: canonicalHash(previousPayload),
      id: "late-v2-artifact",
      taskKey: "late-v2-task",
      variant: 1,
      promptVersion: "sol-word-gloss-v2" as const,
      reasoningEffort: "high" as const,
    };
    const previousValidations = validations.map((validation) => ({
      ...validation,
      id: `late-${validation.id}`,
      artifactId: previousArtifact.id,
      validatorVersion: previousArtifact.promptVersion,
    }));
    const previousBody = {
      artifact: previousArtifact,
      binding: admitted.binding,
      publicationIntentId: hash(
        "sha256",
        publicationIntentIdBody({
          artifactId: previousArtifact.id,
          bindingId: admitted.binding.bindingId,
          modelKey: previousArtifact.modelKey,
          promptVersion: previousArtifact.promptVersion,
        }),
        "hex",
      ),
      validations: previousValidations,
    };
    await expect(
      coordinator.publishBoundEnrichment({
        ...previousBody,
        actionHash: hash(
          "sha256",
          enrichmentPublicationActionHashBody(previousBody),
          "hex",
        ),
      }),
    ).rejects.toThrow("LEGACY_SOL_PUBLICATION_SUPERSEDED");
    await expect(
      store.publishEnrichment({
        poemId: admitted.binding.poemId,
        artifactId: previousArtifact.id,
        expectedPointerVersion: 1,
        writerEpoch: 1,
        requiredValidations: previousValidations.map(
          ({ attempt, validatorKey, validatorVersion }) => ({
            attempt,
            validatorKey,
            validatorVersion,
          }),
        ),
      }),
    ).rejects.toThrow("LEGACY_SOL_PUBLICATION_SUPERSEDED");
    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
      SET enrichment_artifact_id = ?, pointer_version = pointer_version + 1
      WHERE poem_id = ? AND model_key = 'sol-5.6'`,
        )
        .run(previousArtifact.id, admitted.binding.poemId),
    ).toThrow(/LEGACY_SOL_PUBLICATION_SUPERSEDED/u);
    expect(() =>
      database
        .prepare(
          `INSERT INTO model_publication_receipt (
        intent_id, action_hash, poem_id, model_key, prompt_version,
        source_revision_id, enrichment_artifact_id, expected_pointer_version,
        pointer_version, writer_epoch, outcome, committed_at
      ) SELECT ?, ?, poem_id, model_key, 'sol-word-gloss-v2',
        source_revision_id, ?, pointer_version, pointer_version + 1,
        writer_epoch, 'published', committed_at
      FROM model_publication_receipt WHERE intent_id = ?`,
        )
        .run(
          previousBody.publicationIntentId,
          "d".repeat(64),
          previousArtifact.id,
          publicationIntentId,
        ),
    ).toThrow(/LEGACY_SOL_PUBLICATION_SUPERSEDED/u);
    expect(
      database
        .prepare(
          "SELECT enrichment_artifact_id FROM poem_model_publication_pointer WHERE poem_id = ?",
        )
        .get(admitted.binding.poemId),
    ).toEqual({ enrichment_artifact_id: artifact.id });
    expect(
      database
        .prepare(
          "SELECT count(*) AS count FROM model_publication_receipt WHERE intent_id = ?",
        )
        .get(previousBody.publicationIntentId),
    ).toEqual({ count: 0 });
    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    await expect(store.admitSource(exactAdmissionInput())).resolves.toEqual({
      ...admitted,
      state: "unchanged",
    });
    const replay = await coordinator.publishBoundEnrichment(input);

    expect(receipt).toMatchObject({
      actionHash: input.actionHash,
      artifactId: artifact.id,
      outcome: "published",
      poemId: admitted.binding.poemId,
      pointerVersion: 1,
      publicationIntentId,
      writerEpoch: 1,
    });
    expect(replay).toEqual(receipt);
    expect(
      scalar(database, "SELECT count(*) FROM model_publication_receipt"),
    ).toBe(1);
    expect(
      scalar(database, "SELECT count(*) FROM poem_model_publication_pointer"),
    ).toBe(1);
  });

  it("atomically finalizes a bundle when its immutable receipt is inserted", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(plan, plan.items[0]);

    database
      .prepare(
        `INSERT INTO crawl_import_receipt (
          bundle_id, plan_hash, writer_epoch, inserted_revisions,
          reused_revisions, advanced_pointers, unchanged_pointers, created_at
        ) VALUES (?, ?, 1, 1, 0, 1, 0, 1)`,
      )
      .run(plan.bundleId, plan.planHash);

    expect(
      database
        .prepare(
          "SELECT status, promoted_at FROM crawl_import_bundle WHERE id = ?",
        )
        .get(plan.bundleId),
    ).toMatchObject({ status: "promoted" });
    expect(
      database
        .prepare(
          "SELECT promoted_at IS NOT NULL FROM crawl_import_bundle WHERE id = ?",
        )
        .pluck()
        .get(plan.bundleId),
    ).toBe(1);

    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    await expect(
      store.createOrReuseBundle({
        expectedRecordCount: 1,
        id: plan.bundleId,
        manifestHash: manifestHash([stagedRecord()]),
        schemaVersion: 1,
        writerEpoch: 2,
      }),
    ).rejects.toThrow("IMPORT_BUNDLE_ID_CONFLICT");
    expect(
      database
        .prepare("SELECT status FROM crawl_import_bundle WHERE id = ?")
        .pluck()
        .get(plan.bundleId),
    ).toBe("promoted");
  });

  it("rejects a stale writer receipt before any partial finalization", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(plan, plan.items[0]);
    await store.advanceWriterEpoch(1, 2, "replacement-writer");

    expect(() =>
      database
        .prepare(
          `INSERT INTO crawl_import_receipt (
            bundle_id, plan_hash, writer_epoch, inserted_revisions,
            reused_revisions, advanced_pointers, unchanged_pointers, created_at
          ) VALUES (?, ?, 1, 1, 0, 1, 0, 1)`,
        )
        .run(plan.bundleId, plan.planHash),
    ).toThrow(/CRAWL_IMPORT_RECEIPT_BUNDLE_INVALID/u);
    expect(
      database
        .prepare(
          "SELECT count(*) FROM crawl_import_receipt WHERE bundle_id = ?",
        )
        .pluck()
        .get(plan.bundleId),
    ).toBe(0);
    expect(
      database
        .prepare("SELECT status FROM crawl_import_bundle WHERE id = ?")
        .pluck()
        .get(plan.bundleId),
    ).toBe("sealed");
  });

  it("rejects conflicting replay into an occupied bundle ordinal", async () => {
    const staged = stagedRecord();
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 1,
      manifestHash: manifestHash([staged]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(staged);

    await expect(
      store.stageRecord(stagedRecord({ titleArabic: "عنوان متعارض" })),
    ).rejects.toBeInstanceOf(CorpusRevisionConflictError);
  });

  it("advances a v2 source pointer for a title-only correction", async () => {
    const original = stagedRecord({
      contentArabic: { content: ["بيت جديد"], titleArabic: "قصيدة جديدة" },
    });
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 2,
      manifestHash: manifestHash([original]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(original);
    await store.sealBundle("bundle-1", rootHash([original]));
    const first = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(first, first.items[0]);

    const corrected = stagedRecord({
      bundleId: "bundle-2",
      titleArabic: "قصيدة مصححة",
      contentArabic: { content: ["بيت جديد"], titleArabic: "قصيدة مصححة" },
      observedAt: new Date("2026-08-25T12:01:00Z"),
    });
    await store.createOrReuseBundle({
      id: "bundle-2",
      schemaVersion: 2,
      manifestHash: manifestHash([corrected]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(corrected);
    await store.sealBundle("bundle-2", rootHash([corrected]));
    const second = await store.planPromotion("bundle-2", 1);

    expect(second.items[0]).toMatchObject({
      pointerAction: "advance",
      revisionAction: "insert",
    });
    expect(second.items[0]?.revisionId).not.toBe(first.items[0]?.revisionId);
    await store.promoteRecord(second, second.items[0]);
    expect(scalar(database, "SELECT count(*) FROM poem_source_revision")).toBe(
      2,
    );
  });

  it("rejects v2 content whose embedded title disagrees", async () => {
    const staged = stagedRecord({
      contentArabic: { content: ["بيت جديد"], titleArabic: "عنوان آخر" },
    });
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 2,
      manifestHash: manifestHash([staged]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await expect(store.stageRecord(staged)).rejects.toThrow(
      "CONTENT_SCHEMA_MISMATCH",
    );
  });

  it("fences a stale writer epoch", async () => {
    await stageSealedBundle(store);
    const stalePlan = await store.planPromotion("bundle-1", 1);
    const staleItem = stalePlan.items[0];

    await store.advanceWriterEpoch(1, 2, "local-crawler");
    await store.advanceWriterEpoch(1, 2, "local-crawler");
    await expect(
      store.promoteRecord(stalePlan, staleItem),
    ).rejects.toBeInstanceOf(LostWriterEpochError);
  });

  it("refences unfinished sealed work and regenerates its plan after epoch rollover", async () => {
    await stageSealedBundle(store);
    const stalePlan = await store.planPromotion("bundle-1", 1);
    const staged = stagedRecord();
    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 1,
      manifestHash: manifestHash([staged]),
      expectedRecordCount: 1,
      writerEpoch: 2,
    });

    const adopted = await store.planPromotion("bundle-1", 2);
    expect(adopted.writerEpoch).toBe(2);
    expect(adopted.planHash).not.toBe(stalePlan.planHash);
    await expect(
      store.promoteRecord(adopted, adopted.items[0]),
    ).resolves.toMatchObject({ state: "promoted" });
  });

  it("refences an unchanged pointer to the new writer epoch", async () => {
    await stageSealedBundle(store);
    const initial = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(initial, initial.items[0]);
    await store.advanceWriterEpoch(1, 2, "replacement-writer");

    const replay = stagedRecord({
      bundleId: "bundle-2",
      observedAt: new Date("2026-08-26T12:00:00Z"),
    });
    await store.createOrReuseBundle({
      id: "bundle-2",
      schemaVersion: 1,
      manifestHash: manifestHash([replay]),
      expectedRecordCount: 1,
      writerEpoch: 2,
    });
    await store.stageRecord(replay);
    await store.sealBundle("bundle-2", rootHash([replay]));
    const adopted = await store.planPromotion("bundle-2", 2);
    expect(adopted.items[0]?.pointerAction).toBe("unchanged");
    await expect(
      store.promoteRecord(adopted, adopted.items[0]),
    ).resolves.toMatchObject({ state: "already_current" });
    expect(
      database
        .prepare("SELECT writer_epoch FROM poem_source_pointer")
        .pluck()
        .get(),
    ).toBe(2);
  });

  it("keeps a same-epoch unchanged pointer stable and repairs its projection", async () => {
    await stageSealedBundle(store);
    const initial = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(initial, initial.items[0]);
    database
      .prepare(
        `UPDATE poem
         SET name_arabic = 'عنوان تالف', content_arabic = '{"content":["تالف"]}'
         WHERE id = 'poem-1'`,
      )
      .run();

    const replay = stagedRecord({
      bundleId: "bundle-same-epoch",
      observedAt: new Date("2026-08-26T13:00:00Z"),
    });
    await store.createOrReuseBundle({
      id: replay.bundleId,
      schemaVersion: 1,
      manifestHash: manifestHash([replay]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(replay);
    await store.sealBundle(replay.bundleId, rootHash([replay]));
    const replayPlan = await store.planPromotion(replay.bundleId, 1);

    expect(replayPlan.items[0]?.pointerAction).toBe("unchanged");
    await store.promoteRecord(replayPlan, replayPlan.items[0]);
    expect(
      database
        .prepare(
          "SELECT pointer_version FROM poem_source_pointer WHERE source_poem_id = ?",
        )
        .pluck()
        .get(replayPlan.items[0]?.sourcePoemKey),
    ).toBe(1);
    expect(
      database
        .prepare(
          "SELECT name_arabic, content_arabic FROM poem WHERE id = 'poem-1'",
        )
        .get(),
    ).toMatchObject({
      content_arabic: '{"content":["بيت جديد"]}',
      name_arabic: "قصيدة جديدة",
    });
  });

  it("recomputes canonical staging hashes instead of trusting the caller", async () => {
    const staged = stagedRecord();
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 1,
      manifestHash: manifestHash([staged]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await expect(
      store.stageRecord({ ...staged, contentHash: HASH_C }),
    ).rejects.toThrow("CONTENT_HASH_MISMATCH");
    await expect(
      store.stageRecord({ ...staged, recordHash: HASH_C }),
    ).rejects.toThrow("IMPORT_RECORD_HASH_MISMATCH");
  });

  it("rejects fractional observations and delimiter-bearing identities", async () => {
    await expect(
      store.stageRecord(
        stagedRecord({ observedAt: new Date("2026-08-25T12:00:00.123Z") }),
      ),
    ).rejects.toThrow("whole-second");
    await expect(
      store.stageRecord(stagedRecord({ sourcePoemId: "one\u{1F}two" })),
    ).rejects.toThrow("safe identity");
  });

  it("creates a missing canonical author deterministically", async () => {
    const discovered = stagedRecord({
      authorNameArabic: "شاعر جديد",
      canonicalAuthorId: "author-new",
      canonicalPoemId: null,
    });
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 1,
      manifestHash: manifestHash([discovered]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(discovered);
    await store.sealBundle("bundle-1", rootHash([discovered]));
    const plan = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(plan, plan.items[0]);
    expect(
      database
        .prepare("SELECT name_arabic, slug FROM author WHERE id = ?")
        .get("author-new"),
    ).toMatchObject({
      name_arabic: "شاعر جديد",
      slug: expect.stringMatching(/^source-[\da-f]{64}$/),
    });
  });

  it("creates a deterministic poem only beneath the declared existing author", async () => {
    const discovered = stagedRecord({ canonicalPoemId: null });
    await store.createOrReuseBundle({
      id: "bundle-1",
      schemaVersion: 1,
      manifestHash: manifestHash([discovered]),
      expectedRecordCount: 1,
      writerEpoch: 1,
    });
    await store.stageRecord(discovered);
    await store.sealBundle("bundle-1", rootHash([discovered]));
    const plan = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(plan, plan.items[0]);

    const created = database
      .prepare("SELECT author_id, slug, sitemap_shard FROM poem WHERE id = ?")
      .get(plan.items[0].canonicalPoemId) as
      { author_id: string; sitemap_shard: number; slug: string } | undefined;
    expect(created?.author_id).toBe("author-1");
    expect(created?.slug).toBe(`source-${plan.items[0].canonicalPoemId}`);
    expect(created?.sitemap_shard).toBe(
      sitemapShardForId(plan.items[0].canonicalPoemId),
    );
  });

  it("rejects source ownership conflicts and immutable history mutation", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    await store.promoteRecord(plan, plan.items[0]);

    expect(() =>
      database
        .prepare(
          "UPDATE source_poem_identity SET canonical_url = ? WHERE external_id = ?",
        )
        .run("https://source.invalid/works/999", "101680"),
    ).toThrow(/SOURCE_POEM_OWNERSHIP_IMMUTABLE/);
    expect(() =>
      database.prepare("DELETE FROM poem_source_revision").run(),
    ).toThrow(/POEM_SOURCE_REVISION_IMMUTABLE/);
  });

  it("publishes only a current, validated enrichment and replays safely", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);
    const payload = {
      schemaId: "saqi.poem-enrichment-output" as const,
      schemaVersion: 2 as const,
      translation: { lines: ["A new verse"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            segments: [
              {
                kind: "word" as const,
                meaning: "new",
                surface: "بيت",
                tokenIndex: 0,
              },
              { kind: "text" as const, surface: " " },
              {
                kind: "word" as const,
                meaning: "verse",
                surface: "جديد",
                tokenIndex: 1,
              },
            ],
          },
        ],
        tokenizerVersion: "saqi-orthographic-v1" as const,
      },
    };
    await store.putEnrichmentArtifact({
      id: "artifact-1",
      sourceRevisionId: item.revisionId,
      taskKey: `enrich:${item.revisionId}`,
      variant: 0,
      schemaVersion: 2,
      promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
      model: "gpt-5.6-sol",
      reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
      payloadHash: canonicalHash(payload),
      payload,
    });
    const conflictingPayload = {
      ...payload,
      translation: { lines: ["A conflicting replay"] },
    };
    await expect(
      store.putEnrichmentArtifact({
        id: "artifact-1",
        sourceRevisionId: item.revisionId,
        taskKey: `enrich:${item.revisionId}`,
        variant: 0,
        schemaVersion: 2,
        promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
        model: "gpt-5.6-sol",
        reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
        payloadHash: canonicalHash(conflictingPayload),
        payload: conflictingPayload,
      }),
    ).rejects.toThrow("ENRICHMENT_ARTIFACT_ID_CONFLICT");
    await store.putEnrichmentValidation({
      id: "validation-1",
      artifactId: "artifact-1",
      validatorKey: "sol-review-1",
      validatorVersion: "v1",
      attempt: 0,
      outcome: "pass",
      highestSeverity: "none",
      reportHash: canonicalHash(PASS_REVIEW),
      report: PASS_REVIEW,
    });
    await store.putEnrichmentValidation({
      id: "validation-2",
      artifactId: "artifact-1",
      validatorKey: "sol-review-2",
      validatorVersion: "v1",
      attempt: 0,
      outcome: "pass",
      highestSeverity: "none",
      reportHash: canonicalHash(PASS_REVIEW),
      report: PASS_REVIEW,
    });
    await expect(
      store.putEnrichmentValidation({
        id: "validation-1",
        artifactId: "artifact-1",
        validatorKey: "sol-review-1",
        validatorVersion: "v1",
        attempt: 0,
        outcome: "pass",
        highestSeverity: "none",
        reportHash: canonicalHash({ ...PASS_REVIEW, insightScore: 95 }),
        report: { ...PASS_REVIEW, insightScore: 95 },
      }),
    ).rejects.toThrow("VALIDATION_ID_CONFLICT");
    const input = {
      poemId: "poem-1",
      artifactId: "artifact-1",
      expectedPointerVersion: null,
      writerEpoch: 1,
      requiredValidations: [
        { validatorKey: "sol-review-1", validatorVersion: "v1", attempt: 0 },
        { validatorKey: "sol-review-2", validatorVersion: "v1", attempt: 0 },
      ],
    };

    await expect(store.publishEnrichment(input)).resolves.toEqual({
      authorSlug: "author-1",
      poemId: "poem-1",
      pointerVersion: 1,
      state: "published",
    });
    await expect(store.publishEnrichment(input)).resolves.toEqual({
      authorSlug: "author-1",
      poemId: "poem-1",
      pointerVersion: 1,
      state: "already_current",
    });
    await store.promoteRecord(plan, item);
    expect(
      scalar(database, "SELECT count(*) FROM poem_model_publication_pointer"),
    ).toBe(1);
    expect(
      database
        .prepare("SELECT active_enrichment_artifact_id FROM poem WHERE id = ?")
        .pluck()
        .get("poem-1"),
    ).toBeNull();
    expect(
      database
        .prepare("SELECT translation_sol FROM poem WHERE id = ?")
        .pluck()
        .get("poem-1"),
    ).toBeNull();
    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    await expect(
      store.publishEnrichment({
        ...input,
        expectedPointerVersion: 1,
        writerEpoch: 2,
      }),
    ).resolves.toEqual({
      authorSlug: "author-1",
      poemId: "poem-1",
      pointerVersion: 2,
      state: "published",
    });
    expect(
      database
        .prepare(
          "SELECT writer_epoch FROM poem_model_publication_pointer WHERE poem_id = ? AND model_key = 'sol-5.6'",
        )
        .pluck()
        .get("poem-1"),
    ).toBe(2);
  });

  it("stores identical canonical payloads independently for every model", async () => {
    seedFixtureModelProfiles(database);
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);
    const insert = database.prepare(`
      INSERT INTO model_enrichment_artifact (
        id, source_revision_id, task_key, variant, schema_version,
        prompt_version, model, model_key, reasoning_effort, payload_hash,
        payload, created_at
      ) VALUES (?, ?, 'same-task', 0, 1, ?, ?, ?, ?, ?, '{}', 1)
    `);
    expect(() =>
      insert.run(
        "same-sol",
        item.revisionId,
        "sol-enrichment-v1",
        "gpt-5.6-sol",
        "sol-5.6",
        "high",
        HASH_C,
      ),
    ).not.toThrow();
    expect(() =>
      insert.run(
        "same-fixture-a",
        item.revisionId,
        "fixture-a-v1",
        "fixture-model-a",
        "fixture-model-a",
        "high",
        HASH_C,
      ),
    ).not.toThrow();
    expect(() =>
      insert.run(
        "same-fixture-b",
        item.revisionId,
        "fixture-b-v1",
        "fixture-model-b",
        "fixture-model-b",
        "high",
        HASH_C,
      ),
    ).not.toThrow();
    expect(
      scalar(database, "SELECT count(*) FROM model_enrichment_artifact"),
    ).toBe(3);
    expect(
      database
        .prepare(
          `SELECT profile_key FROM model_enrichment_artifact_profile
           ORDER BY profile_key`,
        )
        .pluck()
        .all(),
    ).toEqual([
      "fixture-model-a/source-v1",
      "fixture-model-b/source-v1",
      "sol-5.6/source-v1",
    ]);
    expect(() =>
      insert.run(
        "same-sol-again",
        item.revisionId,
        "sol-enrichment-v1",
        "gpt-5.6-sol",
        "sol-5.6",
        "high",
        HASH_C,
      ),
    ).toThrow();
    expect(() =>
      database
        .prepare(
          `INSERT INTO model_enrichment_artifact (
            id, source_revision_id, task_key, variant, schema_version,
            prompt_version, model, model_key, reasoning_effort, payload_hash,
            payload, created_at
          ) VALUES (?, ?, 'unknown-profile', 0, 1, 'unknown-prompt',
            'gpt-5.6-sol', 'sol-5.6', 'high', ?, '{}', 1)`,
        )
        .run("unknown-profile", item.revisionId, "d".repeat(64)),
    ).toThrow(/MODEL_ENRICHMENT_PROFILE_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE model_enrichment_artifact_profile
           SET profile_key = 'sol-5.6/source-v2'
           WHERE artifact_id = 'same-fixture-a'`,
        )
        .run(),
    ).toThrow(/ARTIFACT_PROFILE_IMMUTABLE/u);
  });

  it("uses legacy Sol only to create an empty pointer and never downgrades v2", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);

    const legacyPayload = {
      insights: {
        culturalSignificance: "culture",
        historicalContext: "history",
        literaryDevices: ["device"],
        notableLines: [{ explanation: "meaning", line: "بيت جديد" }],
        summary: "summary",
        themes: ["theme"],
      },
      translation: { lines: ["A new verse"] },
    };
    await store.putEnrichmentArtifact({
      id: "legacy-sol-artifact",
      sourceRevisionId: item.revisionId,
      taskKey: `legacy:${item.revisionId}`,
      variant: 0,
      schemaVersion: 1,
      promptVersion: "sol-enrichment-v1",
      model: "gpt-5.6-sol",
      reasoningEffort: "high",
      payloadHash: canonicalHash(legacyPayload),
      payload: legacyPayload,
    });
    for (const [attempt, validatorKey] of [
      [1, "sol-fidelity-review"],
      [2, "sol-grounding-review"],
    ] as const) {
      await store.putEnrichmentValidation({
        id: `legacy-validation-${String(attempt)}`,
        artifactId: "legacy-sol-artifact",
        validatorKey,
        validatorVersion: "sol-enrichment-v1",
        attempt,
        outcome: "pass",
        highestSeverity: "none",
        reportHash: canonicalHash(PASS_REVIEW),
        report: PASS_REVIEW,
      });
    }
    const legacyPublication = {
      poemId: "poem-1",
      artifactId: "legacy-sol-artifact",
      expectedPointerVersion: null,
      writerEpoch: 1,
      requiredValidations: [
        {
          attempt: 1,
          validatorKey: "sol-fidelity-review",
          validatorVersion: "sol-enrichment-v1",
        },
        {
          attempt: 2,
          validatorKey: "sol-grounding-review",
          validatorVersion: "sol-enrichment-v1",
        },
      ],
    };
    await expect(store.publishEnrichment(legacyPublication)).resolves.toEqual({
      authorSlug: "author-1",
      poemId: "poem-1",
      pointerVersion: 1,
      state: "published",
    });

    const currentPayload = {
      schemaId: "saqi.poem-enrichment-output" as const,
      schemaVersion: 2 as const,
      translation: { lines: ["A better new verse"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            segments: [
              {
                kind: "word" as const,
                meaning: "verse",
                surface: "بيت",
                tokenIndex: 0,
              },
            ],
          },
        ],
        tokenizerVersion: "saqi-orthographic-v1" as const,
      },
    };
    await store.putEnrichmentArtifact({
      id: "current-sol-artifact",
      sourceRevisionId: item.revisionId,
      taskKey: `current:${item.revisionId}`,
      variant: 0,
      schemaVersion: 2,
      promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
      model: "gpt-5.6-sol",
      reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
      payloadHash: canonicalHash(currentPayload),
      payload: currentPayload,
    });
    for (const [attempt, validatorKey] of [
      [1, "sol-fidelity-review"],
      [2, "sol-grounding-review"],
    ] as const) {
      await store.putEnrichmentValidation({
        id: `current-validation-${String(attempt)}`,
        artifactId: "current-sol-artifact",
        validatorKey,
        validatorVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
        attempt,
        outcome: "pass",
        highestSeverity: "none",
        reportHash: canonicalHash(PASS_REVIEW),
        report: PASS_REVIEW,
      });
    }
    database
      .prepare(
        `INSERT INTO model_publication_receipt (
           intent_id, action_hash, poem_id, model_key, prompt_version,
           source_revision_id, enrichment_artifact_id,
           expected_pointer_version, pointer_version, writer_epoch,
           outcome, committed_at
         ) VALUES (?, ?, 'poem-1', 'sol-5.6', ?, ?, ?, 1, 2, 1,
           'published', unixepoch())`,
      )
      .run(
        "d".repeat(64),
        "e".repeat(64),
        APPROVED_ENRICHMENT_PROMPT_VERSION,
        item.revisionId,
        "current-sol-artifact",
      );
    await expect(
      store.publishEnrichment({
        ...legacyPublication,
        artifactId: "current-sol-artifact",
        expectedPointerVersion: 1,
        requiredValidations: legacyPublication.requiredValidations.map(
          (validation) => ({
            ...validation,
            validatorVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
          }),
        ),
      }),
    ).resolves.toMatchObject({ pointerVersion: 2, state: "already_current" });

    await expect(
      store.publishEnrichment({
        ...legacyPublication,
        expectedPointerVersion: 2,
      }),
    ).rejects.toThrow("LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER");
    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
           SET enrichment_artifact_id = 'legacy-sol-artifact',
               pointer_version = pointer_version + 1
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).toThrow(/LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER/u);
    expect(() =>
      database
        .prepare(
          `INSERT INTO model_publication_receipt (
             intent_id, action_hash, poem_id, model_key, prompt_version,
             source_revision_id, enrichment_artifact_id,
             expected_pointer_version, pointer_version, writer_epoch,
             outcome, committed_at
           ) VALUES (
             'legacy-downgrade-intent', ?, 'poem-1', 'sol-5.6',
             'sol-enrichment-v1', ?, 'legacy-sol-artifact', 2, 3, 1,
             'published', 1
           )`,
        )
        .run("e".repeat(64), item.revisionId),
    ).toThrow(/LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER/u);
    expect(
      database
        .prepare(
          `SELECT enrichment_artifact_id, pointer_version
           FROM poem_model_publication_pointer
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .get(),
    ).toEqual({
      enrichment_artifact_id: "current-sol-artifact",
      pointer_version: 2,
    });
  });

  it("rejects incoherent, stale, and non-monotonic model publication writes", async () => {
    seedFixtureModelProfiles(database);
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);
    database
      .prepare(
        `INSERT INTO model_enrichment_artifact (
          id, source_revision_id, task_key, variant, schema_version,
          prompt_version, model, model_key, reasoning_effort, payload_hash,
          payload, created_at
        ) VALUES (?, ?, ?, 0, 1, ?, ?, ?, ?, ?, '{}', 1)`,
      )
      .run(
        "guard-sol",
        item.revisionId,
        "guard-sol-task",
        "sol-enrichment-v1",
        "gpt-5.6-sol",
        "sol-5.6",
        "high",
        HASH_C,
      );
    database
      .prepare(
        `INSERT INTO model_enrichment_artifact (
          id, source_revision_id, task_key, variant, schema_version,
          prompt_version, model, model_key, reasoning_effort, payload_hash,
          payload, created_at
        ) VALUES (?, ?, ?, 0, 1, ?, ?, ?, ?, ?, '{}', 1)`,
      )
      .run(
        "guard-fixture",
        item.revisionId,
        "guard-fixture-task",
        "fixture-a-v1",
        "fixture-model-a",
        "fixture-model-a",
        "high",
        HASH_C,
      );

    const insertPointer = database.prepare(`
      INSERT INTO poem_model_publication_pointer (
        poem_id, model_key, source_revision_id, enrichment_artifact_id,
        pointer_version, writer_epoch, updated_at
      ) VALUES ('poem-1', ?, ?, ?, ?, ?, 1)
    `);
    expect(() =>
      insertPointer.run("fixture-model-a", item.revisionId, "guard-sol", 1, 1),
    ).toThrow(/MODEL_PUBLICATION_(?:PROFILE|RELATIONSHIP)_INVALID/u);
    database
      .prepare(
        `INSERT INTO poem (
          id, author_id, slug, verses, name_arabic, content_arabic,
          active_source_revision_id
        ) VALUES (
          'poem-2', 'author-1', 'poem-2', 1, 'ثانية', '{"content":["بيت"]}', ?
        )`,
      )
      .run(item.revisionId);
    expect(() =>
      database
        .prepare(
          `INSERT INTO poem_model_publication_pointer (
            poem_id, model_key, source_revision_id, enrichment_artifact_id,
            pointer_version, writer_epoch, updated_at
          ) VALUES ('poem-2', 'sol-5.6', ?, 'guard-sol', 1, 1, 1)`,
        )
        .run(item.revisionId),
    ).toThrow(/MODEL_PUBLICATION_RELATIONSHIP_INVALID/u);
    expect(() =>
      insertPointer.run("sol-5.6", item.revisionId, "guard-sol", 2, 1),
    ).toThrow(/MODEL_PUBLICATION_POINTER_VERSION_INVALID/u);
    expect(() =>
      insertPointer.run("sol-5.6", item.revisionId, "guard-sol", 1, 2),
    ).toThrow(/MODEL_PUBLICATION_WRITER_EPOCH_STALE/u);
    expect(() =>
      insertPointer.run("sol-5.6", item.revisionId, "guard-sol", 1, 1),
    ).not.toThrow();

    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
           SET pointer_version = pointer_version
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).toThrow(/MODEL_PUBLICATION_(?:PROFILE|UPDATE)_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
           SET enrichment_artifact_id = 'guard-fixture',
               pointer_version = pointer_version + 1
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).toThrow(/MODEL_PUBLICATION_(?:PROFILE|UPDATE)_INVALID/u);
    expect(() =>
      database
        .prepare(
          `DELETE FROM poem_model_publication_pointer
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).toThrow(/MODEL_PUBLICATION_POINTER_DELETE_FORBIDDEN/u);

    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 1,
               updated_at = updated_at + 1
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).toThrow(/MODEL_PUBLICATION_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE poem_model_publication_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 2,
               updated_at = updated_at + 1
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .run(),
    ).not.toThrow();
    expect(
      database
        .prepare(
          `SELECT pointer_version, writer_epoch
           FROM poem_model_publication_pointer
           WHERE poem_id = 'poem-1' AND model_key = 'sol-5.6'`,
        )
        .get(),
    ).toMatchObject({ pointer_version: 2, writer_epoch: 2 });
  });

  it("fences raw SQL writes to legacy source and publication pointers", async () => {
    await stageSealedBundle(store);
    const plan = await store.planPromotion("bundle-1", 1);
    const item = plan.items[0];
    await store.promoteRecord(plan, item);

    expect(() =>
      database
        .prepare(
          `UPDATE poem_source_pointer
           SET pointer_version = pointer_version
           WHERE source_poem_id = ?`,
        )
        .run(item.sourcePoemKey),
    ).toThrow(/SOURCE_POINTER_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare("DELETE FROM poem_source_pointer WHERE source_poem_id = ?")
        .run(item.sourcePoemKey),
    ).toThrow(/SOURCE_POINTER_DELETE_FORBIDDEN/u);

    const insertPublication = database.prepare(
      `INSERT INTO poem_publication_pointer (
        poem_id, source_revision_id, enrichment_artifact_id,
        pointer_version, writer_epoch, updated_at
      ) VALUES ('poem-1', ?, NULL, ?, ?, 1)`,
    );
    expect(() => insertPublication.run(item.revisionId, 2, 1)).toThrow(
      /PUBLICATION_POINTER_INSERT_INVALID/u,
    );
    expect(() => insertPublication.run(item.revisionId, 1, 2)).toThrow(
      /PUBLICATION_POINTER_INSERT_INVALID/u,
    );
    expect(() => insertPublication.run(item.revisionId, 1, 1)).not.toThrow();
    expect(() =>
      database
        .prepare(
          `UPDATE poem_publication_pointer
           SET pointer_version = pointer_version
           WHERE poem_id = 'poem-1'`,
        )
        .run(),
    ).toThrow(/PUBLICATION_POINTER_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare(
          "DELETE FROM poem_publication_pointer WHERE poem_id = 'poem-1'",
        )
        .run(),
    ).toThrow(/PUBLICATION_POINTER_DELETE_FORBIDDEN/u);

    await store.advanceWriterEpoch(1, 2, "replacement-writer");
    expect(() =>
      database
        .prepare(
          `UPDATE poem_source_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 1,
               updated_at = updated_at + 1
           WHERE source_poem_id = ?`,
        )
        .run(item.sourcePoemKey),
    ).toThrow(/SOURCE_POINTER_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE poem_source_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 2,
               updated_at = updated_at + 1
           WHERE source_poem_id = ?`,
        )
        .run(item.sourcePoemKey),
    ).not.toThrow();
    expect(() =>
      database
        .prepare(
          `UPDATE poem_publication_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 1,
               updated_at = updated_at + 1
           WHERE poem_id = 'poem-1'`,
        )
        .run(),
    ).toThrow(/PUBLICATION_POINTER_UPDATE_INVALID/u);
    expect(() =>
      database
        .prepare(
          `UPDATE poem_publication_pointer
           SET pointer_version = pointer_version + 1, writer_epoch = 2,
               updated_at = updated_at + 1
           WHERE poem_id = 'poem-1'`,
        )
        .run(),
    ).not.toThrow();
  });

  it("freezes a dry-run plan and requires its hash before resumable promotion", async () => {
    const coordinator = new CorpusImportCoordinator(store);
    const staged = stagedRecord();
    const plan = await coordinator.stageAndPlan({
      bundle: {
        id: "bundle-1",
        schemaVersion: 1,
        manifestHash: manifestHash([staged]),
        expectedRecordCount: 1,
        writerEpoch: 1,
      },
      records: [staged],
      rootHash: rootHash([staged]),
    });

    await expect(coordinator.promote("bundle-1", 1, HASH_C)).rejects.toThrow(
      "PROMOTION_PLAN_NOT_CONFIRMED",
    );
    const first = await coordinator.promote("bundle-1", 1, plan.planHash);
    const replay = await coordinator.promote("bundle-1", 1, plan.planHash);
    expect(replay).toEqual(first);
    expect(scalar(database, "SELECT count(*) FROM crawl_import_receipt")).toBe(
      1,
    );
  });
});

async function stageSealedBundle(store: D1CorpusRevisionStore): Promise<void> {
  const staged = stagedRecord();
  await store.createOrReuseBundle({
    id: "bundle-1",
    schemaVersion: 1,
    manifestHash: manifestHash([staged]),
    expectedRecordCount: 1,
    writerEpoch: 1,
  });
  await store.createOrReuseBundle({
    id: "bundle-1",
    schemaVersion: 1,
    manifestHash: manifestHash([staged]),
    expectedRecordCount: 1,
    writerEpoch: 1,
  });
  await store.stageRecord(staged);
  await store.stageRecord(staged);
  await store.sealBundle("bundle-1", rootHash([staged]));
  await store.sealBundle("bundle-1", rootHash([staged]));
}

function stagedRecord(
  overrides: Partial<StageRecordInput> = {},
): StageRecordInput {
  const body = {
    bundleId: "bundle-1",
    ordinal: 0,
    sourceName: SOURCE_NAME,
    sourceAuthorId: "495",
    sourceAuthorUrl: "https://source.invalid/writers/poet-almaarri",
    authorNameArabic: "أبو العلاء المعري",
    canonicalAuthorId: "author-1",
    sourcePoemId: "101680",
    sourcePoemUrl: "https://source.invalid/works/101680",
    canonicalPoemId: "poem-1",
    titleArabic: "قصيدة جديدة",
    contentArabic: { content: ["بيت جديد"] },
    observedAt: new Date("2026-08-25T12:00:00Z"),
    ...overrides,
  };
  const contentHash = canonicalHash(body.contentArabic);
  const recordBody = {
    authorNameArabic: body.authorNameArabic,
    canonicalAuthorId: body.canonicalAuthorId,
    canonicalPoemId: body.canonicalPoemId,
    contentArabic: body.contentArabic,
    contentHash,
    observedAt: body.observedAt.toISOString(),
    ordinal: body.ordinal,
    sourceAuthorId: body.sourceAuthorId,
    sourceAuthorUrl: body.sourceAuthorUrl,
    sourceName: body.sourceName,
    sourcePoemId: body.sourcePoemId,
    sourcePoemUrl: body.sourcePoemUrl,
    titleArabic: body.titleArabic,
  };
  return { ...body, contentHash, recordHash: canonicalHash(recordBody) };
}

function exactAdmissionInput() {
  const linesArabic = ["ا\u{654}"];
  const titleArabic = "قصيدة";
  const sourceContentSha256 = canonicalHash({
    content: linesArabic,
    titleArabic,
  });
  const sourceRevisionId = hash(
    "sha256",
    `revision\u{1F}${SOURCE_NAME}\u{1F}101681\u{1F}2\u{1F}${sourceContentSha256}`,
    "hex",
  );
  const body = {
    externalPoemId: "101681",
    lineNfcHash: hash("sha256", sourceLineNfcHashBody(linesArabic), "hex"),
    linesArabic,
    sourceAuthorId: "495",
    sourceAuthorUrl: "https://source.invalid/writers/poet-almaarri",
    sourceContentSha256,
    sourceName: SOURCE_NAME,
    sourcePoemUrl: "https://source.invalid/works/101681",
    sourceRevisionId,
    titleArabic,
  };
  return {
    ...body,
    admissionId: hash("sha256", sourceAdmissionIdBody(body), "hex"),
  };
}

function insertAdmissionAuthor(database: InstanceType<typeof Database>): void {
  database
    .prepare(
      `INSERT INTO source_author_identity (
        id, source_name, external_id, canonical_url, name_arabic,
        canonical_author_id, first_observed_at, last_observed_at
      ) VALUES (?, 'primary-source', '495', ?, 'شاعر', 'author-1', 1, 1)`,
    )
    .run(
      `${SOURCE_NAME}\u{1F}495`,
      "https://source.invalid/writers/poet-almaarri",
    );
}

function insertLegacyPoem(
  database: InstanceType<typeof Database>,
  input: {
    readonly authorId: string;
    readonly poemId: string;
    readonly sourcePoemId?: string;
  },
): void {
  const sourcePoemId = input.sourcePoemId ?? "68242";
  const authorSlug =
    sourcePoemId === "68242"
      ? "poet-Muslim-ibn-al-Walid"
      : `poet-Test-${sourcePoemId}`;
  database
    .prepare(
      "INSERT INTO author (id, slug, name_arabic, status) VALUES (?, ?, ?, ?)",
    )
    .run(input.authorId, authorSlug, "صريع الغواني", "completed-scrape");
  database
    .prepare(
      `INSERT INTO poem (
        id, author_id, slug, verses, name_arabic, content_arabic
      ) VALUES (?, ?, ?, 2, ?, ?)`,
    )
    .run(
      input.poemId,
      input.authorId,
      `work-${sourcePoemId}`,
      "ذاك ظبي تحير الحسن في",
      JSON.stringify({ content: ["ذاك ظبي", "تحير الحسن"] }),
    );
}

function manifestHash(records: StageRecordInput[]): string {
  return canonicalHash({
    recordHashes: records.map(({ recordHash }) => recordHash),
  });
}

function rootHash(records: StageRecordInput[]): string {
  return canonicalHash(records.map(({ recordHash }) => recordHash));
}

function canonicalHash(value: unknown): string {
  return hash("sha256", stableJson(value), "hex");
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.entries(value)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => `${JSON.stringify(key)}:${stableJson(nested)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function scalar(
  database: InstanceType<typeof Database>,
  statement: string,
): number {
  return database.prepare(statement).pluck().get() as number;
}
