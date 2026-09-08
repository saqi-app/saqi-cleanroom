import {
  AnyPoemEnrichmentOutputSchema,
  canonicalPoemBindingIdBody,
  type CanonicalPoemBindingV1,
  CORPUS_REVISION_SCHEMA_VERSION,
  type CorpusContentArabic,
  CorpusContentArabicSchema,
  enrichmentPublicationActionHashBody,
  type EnrichmentPublicationReceiptV1,
  EnrichmentPublicationReceiptV1Schema,
  type EnrichmentPublicationV2Request,
  LEGACY_CORPUS_REVISION_SCHEMA_VERSION,
  PoemEnrichmentReviewSchema,
  publicationEnrichmentProfile,
  publicationIntentIdBody,
  reviewsAcceptEnrichment,
  sitemapShardForId,
  sourceAdmissionIdBody,
  SourceAdmissionV2ItemSchema,
  type SourceAdmissionV2Request,
  sourceLineNfcHashBody,
  SourceNameSchema,
  SourceOriginSchema,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import {
  renderSourcePath,
  type SourceAdapterProfileV1,
  SourceAdapterProfileV1Schema,
  sourcePathValue,
} from "@saqi/source-adapter";
import { and, eq, sql } from "drizzle-orm";
import { z } from "zod";

import type { Database } from "./db-types";
import {
  AUTHOR_TABLE as author,
  CRAWL_IMPORT_BUNDLE_TABLE as bundle,
  CRAWL_IMPORT_RECEIPT_TABLE as receipt,
  CRAWL_IMPORT_RECORD_TABLE as record,
  ENRICHMENT_ARTIFACT_TABLE as artifact,
  ENRICHMENT_VALIDATION_TABLE as validation,
  MODEL_PUBLICATION_RECEIPT_TABLE as publicationReceipt,
  POEM_SOURCE_REVISION_TABLE as revision,
  POEM_TABLE as poem,
  SCRAPER_WRITER_CONTROL_TABLE as writerControl,
  SOURCE_ADMISSION_CLOCK_TABLE as admissionClock,
  SOURCE_REVISION_FINGERPRINT_TABLE as sourceFingerprint,
} from "./schema";

const SINGLETON = 1;
export interface StageBundleInput {
  expectedRecordCount: number;
  id: string;
  manifestHash: string;
  schemaVersion: number;
  writerEpoch: number;
}

export interface StageRecordInput {
  authorNameArabic: string;
  bundleId: string;
  canonicalAuthorId: string;
  canonicalPoemId: null | string;
  contentArabic: CorpusContentArabic;
  contentHash: string;
  observedAt: Date;
  ordinal: number;
  recordHash: string;
  sourceAuthorId: string;
  sourceAuthorUrl: string;
  sourceName: string;
  sourcePoemId: string;
  sourcePoemUrl: string;
  titleArabic: string;
}

export interface PromotionPlanItem {
  canonicalPoemId: string;
  expectedPointerVersion: null | number;
  ordinal: number;
  pointerAction: "advance" | "create" | "unchanged";
  recordHash: string;
  revisionAction: "insert" | "reuse";
  revisionId: string;
  sourcePoemKey: string;
}

export interface PromotionPlan {
  bundleId: string;
  items: PromotionPlanItem[];
  planHash: string;
  writerEpoch: number;
}

const PointerActionSchema = z.enum(["advance", "create", "unchanged"]);
const RevisionActionSchema = z.enum(["insert", "reuse"]);

const PromotionPlanSchema = z.object({
  bundleId: z.string().min(1),
  items: z.array(
    z.object({
      canonicalPoemId: z.string().min(1),
      expectedPointerVersion: z.number().int().positive().nullable(),
      ordinal: z.number().int().nonnegative(),
      pointerAction: PointerActionSchema,
      recordHash: z.string().regex(/^[\da-f]{64}$/),
      revisionAction: RevisionActionSchema,
      revisionId: z.string().min(1),
      sourcePoemKey: z.string().min(1),
    }),
  ),
  planHash: z.string().regex(/^[\da-f]{64}$/),
  writerEpoch: z.number().int().positive(),
});

const LegacySourceLineageRowSchema = z.strictObject({
  author_id: z.uuid(),
  author_name_arabic: z.string().trim().min(1).max(512),
  author_slug: z.string().trim().min(1).max(512),
  content_arabic: z.string().max(2_000_000),
  poem_id: z.uuid(),
  poem_slug: z.string().trim().min(1).max(512),
  title_arabic: z.string().max(512),
});

const SourceFingerprintBackfillRowSchema = z
  .strictObject({
    author_name_arabic: z.string().trim().min(1).max(512),
    content_arabic: z.string(),
    created_at: z.int().nonnegative(),
    source_revision_id: z.string().regex(/^[\da-f]{64}$/),
    title_arabic: z.string().max(512),
  })
  .transform(
    ({
      author_name_arabic: authorNameArabic,
      content_arabic: contentArabic,
      created_at: createdAt,
      source_revision_id: sourceRevisionId,
      title_arabic: titleArabic,
    }) => ({
      authorNameArabic,
      contentArabic,
      createdAt,
      sourceRevisionId,
      titleArabic,
    }),
  );

const SourceAuthorRowSchema = z
  .strictObject({
    canonical_author_id: z.string().trim().min(1).max(512),
    canonical_name_arabic: z.string().trim().min(1).max(512),
    canonical_url: z.url(),
  })
  .transform(
    ({
      canonical_author_id: canonicalAuthorId,
      canonical_name_arabic: canonicalNameArabic,
      canonical_url: canonicalUrl,
    }) => ({ canonicalAuthorId, canonicalNameArabic, canonicalUrl }),
  );

const KnownSourceRowSchema = z
  .strictObject({
    canonical_poem_id: z.string().trim().min(1).max(512).nullable(),
    canonical_url: z.url(),
    current_revision_id: z
      .string()
      .regex(/^[\da-f]{64}$/)
      .nullable(),
    line_nfc_hash: z
      .string()
      .regex(/^[\da-f]{64}$/)
      .nullable(),
    pointer_version: z.int().positive().nullable(),
    prompt_material_hash: z
      .string()
      .regex(/^[\da-f]{64}$/)
      .nullable(),
    source_author_id: z.string().min(1),
    tombstoned_at: z.int().nonnegative().nullable(),
  })
  .transform(
    ({
      canonical_poem_id: canonicalPoemId,
      canonical_url: canonicalUrl,
      current_revision_id: currentRevisionId,
      line_nfc_hash: lineNfcHash,
      pointer_version: pointerVersion,
      prompt_material_hash: promptMaterialHash,
      source_author_id: sourceAuthorId,
      tombstoned_at: tombstonedAt,
    }) => ({
      canonicalPoemId,
      canonicalUrl,
      currentRevisionId,
      lineNfcHash,
      pointerVersion,
      promptMaterialHash,
      sourceAuthorId,
      tombstonedAt,
    }),
  );

const FoundRowSchema = z.strictObject({ found: z.literal(1) });

const PublicationPointerRowSchema = z
  .strictObject({
    author_slug: z.string().trim().min(1).max(512),
    enrichment_artifact_id: z.string().min(1).nullable(),
    poem_id: z.string().min(1),
    pointer_version: z.int().positive(),
    source_revision_id: z.string().min(1),
    writer_epoch: z.int().positive(),
  })
  .transform(
    ({
      author_slug: authorSlug,
      enrichment_artifact_id: enrichmentArtifactId,
      poem_id: poemId,
      pointer_version: pointerVersion,
      source_revision_id: sourceRevisionId,
      writer_epoch: writerEpoch,
    }) => ({
      authorSlug,
      enrichmentArtifactId,
      poemId,
      pointerVersion,
      sourceRevisionId,
      writerEpoch,
    }),
  );

const BoundPublicationRowSchema = z
  .strictObject({
    author_id: z.string().min(1),
    external_id: z.string().min(1),
    line_nfc_hash: z.string().regex(/^[\da-f]{64}$/),
    model_key: z.string().min(1),
    poem_id: z.string().min(1),
    prompt_material_hash: z.string().regex(/^[\da-f]{64}$/),
    source_name: z.string().min(1),
    source_pointer_version: z.int().positive(),
    source_revision_id: z.string().min(1),
    writer_epoch: z.int().positive(),
  })
  .transform(
    ({
      author_id: authorId,
      external_id: externalId,
      line_nfc_hash: lineNfcHash,
      model_key: modelKey,
      poem_id: poemId,
      prompt_material_hash: promptMaterialHash,
      source_name: sourceName,
      source_pointer_version: sourcePointerVersion,
      source_revision_id: sourceRevisionId,
      writer_epoch: writerEpoch,
    }) => ({
      authorId,
      externalId,
      lineNfcHash,
      modelKey,
      poemId,
      promptMaterialHash,
      sourceName,
      sourcePointerVersion,
      sourceRevisionId,
      writerEpoch,
    }),
  );

const ModelPointerRowSchema = z
  .strictObject({
    enrichment_artifact_id: z.string().min(1),
    pointer_version: z.int().positive(),
    source_revision_id: z.string().min(1),
    writer_epoch: z.int().positive(),
  })
  .transform(
    ({
      enrichment_artifact_id: enrichmentArtifactId,
      pointer_version: pointerVersion,
      source_revision_id: sourceRevisionId,
      writer_epoch: writerEpoch,
    }) => ({
      enrichmentArtifactId,
      pointerVersion,
      sourceRevisionId,
      writerEpoch,
    }),
  );

const SourcePointerRowSchema = z
  .strictObject({
    pointer_version: z.int().positive(),
    revision_id: z.string().min(1),
    writer_epoch: z.int().positive(),
  })
  .transform(
    ({
      pointer_version: pointerVersion,
      revision_id: revisionId,
      writer_epoch: writerEpoch,
    }) => ({ pointerVersion, revisionId, writerEpoch }),
  );

const ProjectedRevisionRowSchema = z
  .strictObject({ active_source_revision_id: z.string().min(1) })
  .transform(({ active_source_revision_id: activeSourceRevisionId }) => ({
    activeSourceRevisionId,
  }));

const SourceAuthorOwnershipRowSchema = z
  .strictObject({
    canonical_author_id: z.string().min(1).nullable(),
    canonical_url: z.url(),
    external_id: z.string().min(1),
    source_name: z.string().min(1),
  })
  .transform(
    ({
      canonical_author_id: canonicalAuthorId,
      canonical_url: canonicalUrl,
      external_id: externalId,
      source_name: sourceName,
    }) => ({ canonicalAuthorId, canonicalUrl, externalId, sourceName }),
  );

const SourcePoemOwnershipRowSchema = z
  .strictObject({
    canonical_poem_id: z.string().min(1).nullable(),
    canonical_url: z.url(),
    source_author_id: z.string().min(1),
  })
  .transform(
    ({
      canonical_poem_id: canonicalPoemId,
      canonical_url: canonicalUrl,
      source_author_id: sourceAuthorId,
    }) => ({ canonicalPoemId, canonicalUrl, sourceAuthorId }),
  );

export interface PromoteRecordResult {
  pointerVersion: number;
  revisionId: string;
  revisionState: "inserted" | "reused";
  state: "already_current" | "promoted";
}

export interface EnrichmentArtifactInput {
  id: string;
  model: string;
  modelKey?: string;
  payload: Record<string, unknown>;
  payloadHash: string;
  promptVersion: string;
  reasoningEffort: string;
  schemaVersion: number;
  sourceRevisionId: string;
  taskKey: string;
  variant: number;
}

export interface EnrichmentValidationInput {
  artifactId: string;
  attempt: number;
  highestSeverity: "critical" | "major" | "minor" | "none";
  id: string;
  outcome: "fail" | "pass";
  report: Record<string, unknown>;
  reportHash: string;
  validatorKey: string;
  validatorVersion: string;
}

export interface PublishEnrichmentInput {
  artifactId: string;
  expectedPointerVersion: null | number;
  poemId: string;
  requiredValidations: {
    validatorKey: string;
    validatorVersion: string;
    attempt: number;
  }[];
  writerEpoch: number;
}

export interface PublicationResult {
  authorSlug: string;
  poemId: string;
  pointerVersion: number;
  state: "already_current" | "published";
}

export type BoundEnrichmentPublication =
  EnrichmentPublicationV2Request["items"][number];
export type SourceAdmission = SourceAdmissionV2Request["items"][number];
export interface SourceAdmissionResult {
  binding: CanonicalPoemBindingV1;
  state: "admitted" | "unchanged";
}

export interface SourceFingerprintBackfillCursor {
  readonly createdAt: number;
  readonly sourceRevisionId: string;
}

export interface SourceFingerprintBackfillResult {
  readonly complete: boolean;
  readonly existing: number;
  readonly inserted: number;
  readonly nextCursor: null | SourceFingerprintBackfillCursor;
  readonly scanned: number;
}

export interface LegacySourceLineageAdoptionResult {
  readonly adopted: number;
  readonly conflicts: readonly {
    readonly code: string;
    readonly poemId: string;
  }[];
  readonly scanned: number;
  readonly unchanged: number;
}

export interface CorpusRevisionStore {
  admitSource(input: SourceAdmission): Promise<SourceAdmissionResult>;
  adoptLegacySourceLineage(input: {
    readonly poemIds: readonly string[];
  }): Promise<LegacySourceLineageAdoptionResult>;
  advanceWriterEpoch(
    expectedEpoch: number,
    nextEpoch: number,
    writerId: string,
  ): Promise<void>;
  backfillActiveSourceFingerprints(input?: {
    readonly cursor?: SourceFingerprintBackfillCursor;
    readonly limit?: number;
  }): Promise<SourceFingerprintBackfillResult>;
  createOrReuseBundle(input: StageBundleInput): Promise<void>;
  currentWriterEpoch(): Promise<number>;
  planPromotion(bundleId: string, writerEpoch: number): Promise<PromotionPlan>;
  promoteRecord(
    plan: PromotionPlan,
    item: PromotionPlanItem,
  ): Promise<PromoteRecordResult>;
  publishBoundEnrichment(
    input: BoundEnrichmentPublication,
  ): Promise<EnrichmentPublicationReceiptV1>;
  publishEnrichment(input: PublishEnrichmentInput): Promise<PublicationResult>;
  putEnrichmentArtifact(input: EnrichmentArtifactInput): Promise<void>;
  putEnrichmentValidation(input: EnrichmentValidationInput): Promise<void>;
  recordImportReceipt(
    plan: PromotionPlan,
    counts: Omit<ImportCounts, "bundleId">,
  ): Promise<void>;
  sealBundle(bundleId: string, rootHash: string): Promise<void>;
  stageRecord(input: StageRecordInput): Promise<void>;
}

export interface ImportCounts {
  advancedPointers: number;
  bundleId: string;
  insertedRevisions: number;
  reusedRevisions: number;
  unchangedPointers: number;
}

const PlanningRowSchema = z
  .strictObject({
    canonical_poem_id: z.string().min(1).nullable(),
    content_hash: z.string().regex(/^[\da-f]{64}$/),
    current_revision_id: z
      .string()
      .regex(/^[\da-f]{64}$/)
      .nullable(),
    existing_revision_id: z
      .string()
      .regex(/^[\da-f]{64}$/)
      .nullable(),
    identity_canonical_poem_id: z.string().min(1).nullable(),
    ordinal: z.int().nonnegative(),
    pointer_version: z.int().positive().nullable(),
    record_hash: z.string().regex(/^[\da-f]{64}$/),
    schema_version: z.int().positive(),
    source_name: z.string().trim().min(1).max(128),
    source_poem_id: z.string().trim().min(1).max(512),
  })
  .transform(
    ({
      canonical_poem_id: canonicalPoemId,
      content_hash: contentHash,
      current_revision_id: currentRevisionId,
      existing_revision_id: existingRevisionId,
      identity_canonical_poem_id: identityCanonicalPoemId,
      ordinal,
      pointer_version: pointerVersion,
      record_hash: recordHash,
      schema_version: schemaVersion,
      source_name: sourceName,
      source_poem_id: sourcePoemId,
    }) => ({
      canonicalPoemId,
      contentHash,
      currentRevisionId,
      existingRevisionId,
      identityCanonicalPoemId,
      ordinal,
      pointerVersion,
      recordHash,
      schemaVersion,
      sourceName,
      sourcePoemId,
    }),
  );

const LegacySourceLineagePoemIdsSchema = z.array(z.uuid()).min(1).max(10);
const SourceFingerprintBackfillRowsSchema = z.array(
  SourceFingerprintBackfillRowSchema,
);
const SourceAuthorRowsSchema = z.array(SourceAuthorRowSchema).max(1);
const KnownSourceRowsSchema = z.array(KnownSourceRowSchema).max(1);
const PlanningRowsSchema = z.array(PlanningRowSchema);

const CurrentWriterRowSchema = z
  .object({ writer_epoch: z.unknown().optional() })
  .nullish();
const CurrentWriterEpochSchema = z.number().int().positive();

/**
 * Persistence for the revision pipeline. It deliberately never calls the legacy
 * slug upsert: source identity, immutable history, and the legacy read projection
 * are reconciled with independent uniqueness and compare-and-swap guards.
 */
export class D1CorpusRevisionStore implements CorpusRevisionStore {
  readonly #db: Database;
  readonly #sourceBaseUrl: URL;
  readonly #sourceName: string;
  readonly #sourceProfile: SourceAdapterProfileV1;

  constructor(
    db: Database,
    options: {
      readonly sourceBaseUrl: string;
      readonly sourceName: string;
      readonly sourceProfile: SourceAdapterProfileV1;
    },
  ) {
    this.#db = db;
    this.#sourceBaseUrl = new URL(
      SourceOriginSchema.parse(options.sourceBaseUrl),
    );
    this.#sourceName = SourceNameSchema.parse(options.sourceName);
    this.#sourceProfile = SourceAdapterProfileV1Schema.parse(
      options.sourceProfile,
    );
  }

  async currentWriterEpoch(): Promise<number> {
    const row = CurrentWriterRowSchema.parse(
      await this.#db.get(sql`
      SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
    `),
    );
    if (!row?.writer_epoch) throw new LostWriterEpochError();
    return CurrentWriterEpochSchema.parse(row.writer_epoch);
  }

  async adoptLegacySourceLineage(input: {
    readonly poemIds: readonly string[];
  }): Promise<LegacySourceLineageAdoptionResult> {
    const poemIds = LegacySourceLineagePoemIdsSchema.parse(input.poemIds);
    if (new Set(poemIds).size !== poemIds.length) {
      throw new Error("LEGACY_SOURCE_LINEAGE_TARGET_DUPLICATE");
    }
    const rows = LegacySourceLineageRowSchema.array()
      .parse(
        await this.#db.all(sql`
      SELECT poem.id AS poem_id, poem.slug AS poem_slug,
        poem.name_arabic AS title_arabic,
        poem.content_arabic AS content_arabic,
        author.id AS author_id, author.slug AS author_slug,
        author.name_arabic AS author_name_arabic
      FROM json_each(${JSON.stringify(poemIds)}) requested
      JOIN poem ON poem.id = requested.value
      JOIN author ON author.id = poem.author_id
      ORDER BY CAST(requested.key AS INTEGER)
    `),
      )
      .filter((row) => {
        const externalId = sourcePathValue(
          this.#sourceProfile.routes.poemSlug,
          "id",
          row.poem_slug,
        );
        return externalId !== undefined && /^[1-9]\d*$/u.test(externalId);
      });
    const rowsByPoemId = new Map(
      rows.map((row) => [row.poem_id, row] as const),
    );
    let adopted = 0;
    const conflicts: { code: string; poemId: string }[] = [];
    let unchanged = 0;
    for (const poemId of poemIds) {
      const value = rowsByPoemId.get(poemId);
      if (!value) {
        conflicts.push({
          code: "LEGACY_SOURCE_LINEAGE_TARGET_UNRESOLVED",
          poemId,
        });
        continue;
      }
      try {
        // eslint-disable-next-line no-await-in-loop -- Serialize bounded D1 promotions to preserve writer fencing and conflict attribution.
        const state = await this.#adoptLegacySourceLineageRow(value);
        if (state === "admitted") adopted += 1;
        else unchanged += 1;
      } catch (error) {
        if (!(error instanceof CorpusRevisionConflictError)) throw error;
        conflicts.push({ code: error.message, poemId });
      }
    }
    return {
      adopted,
      conflicts,
      scanned: poemIds.length,
      unchanged,
    };
  }

  async backfillActiveSourceFingerprints(
    input: {
      readonly cursor?: SourceFingerprintBackfillCursor;
      readonly limit?: number;
    } = {},
  ): Promise<SourceFingerprintBackfillResult> {
    const limit = input.limit ?? 20;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 20) {
      throw new Error("SOURCE_FINGERPRINT_BACKFILL_LIMIT_INVALID");
    }
    const cursor = input.cursor ?? { createdAt: -1, sourceRevisionId: "" };
    if (
      !Number.isSafeInteger(cursor.createdAt) ||
      cursor.createdAt < -1 ||
      (cursor.sourceRevisionId !== "" &&
        !/^[a-f\d]{64}$/.test(cursor.sourceRevisionId))
    ) {
      throw new Error("SOURCE_FINGERPRINT_BACKFILL_CURSOR_INVALID");
    }
    const rows = SourceFingerprintBackfillRowsSchema.parse(
      await this.#db.all(sql`
        SELECT author.name_arabic AS author_name_arabic,
          revision.content_arabic, revision.created_at,
          revision.id AS source_revision_id, revision.title_arabic
        FROM poem_source_revision revision
        JOIN source_poem_identity source_poem
          ON source_poem.id = revision.source_poem_id
         AND source_poem.source_name = ${this.#sourceName}
         AND source_poem.tombstoned_at IS NULL
        JOIN poem
          ON poem.id = source_poem.canonical_poem_id
         AND poem.active_source_revision_id = revision.id
        JOIN source_author_identity source_author
          ON source_author.id = source_poem.source_author_id
         AND source_author.source_name = ${this.#sourceName}
         AND source_author.canonical_author_id = poem.author_id
        JOIN author ON author.id = poem.author_id
        LEFT JOIN source_revision_fingerprint fingerprint
          ON fingerprint.source_revision_id = revision.id
        WHERE fingerprint.source_revision_id IS NULL
          AND (
            revision.created_at > ${cursor.createdAt}
            OR (
              revision.created_at = ${cursor.createdAt}
              AND revision.id > ${cursor.sourceRevisionId}
            )
          )
        ORDER BY revision.created_at, revision.id
        LIMIT ${limit + 1}
      `),
    );
    const selected = rows.slice(0, limit);
    let inserted = 0;
    let existing = 0;
    for (const row of selected) {
      const content = CorpusContentArabicSchema.parse(
        JSON.parse(row.contentArabic),
      );
      // eslint-disable-next-line no-await-in-loop -- Compute each bounded row before its sequential insert and verification.
      const lineNfcHash = await sha256(sourceLineNfcHashBody(content.content));
      // eslint-disable-next-line no-await-in-loop -- Compute each bounded row before its sequential insert and verification.
      const promptMaterialHash = await sha256(
        sourcePromptMaterialHashBody({
          authorArabic: row.authorNameArabic,
          linesArabic: content.content,
          titleArabic: row.titleArabic,
        }),
      );
      // eslint-disable-next-line no-await-in-loop -- Insert and verify each row before advancing the durable cursor.
      const created = await this.#db
        .insert(sourceFingerprint)
        .values({
          algorithm: "sha256-canonical-nfc-v1",
          createdAt: sql`(unixepoch())`,
          lineNfcHash,
          promptMaterialHash,
          sourceRevisionId: row.sourceRevisionId,
        })
        .onConflictDoNothing()
        .returning({ sourceRevisionId: sourceFingerprint.sourceRevisionId });
      if (created.length === 1) inserted += 1;
      else existing += 1;
      // eslint-disable-next-line no-await-in-loop -- Read back this insert before advancing the durable cursor.
      const [stored] = await this.#db
        .select()
        .from(sourceFingerprint)
        .where(eq(sourceFingerprint.sourceRevisionId, row.sourceRevisionId))
        .limit(1);
      if (
        stored?.algorithm !== "sha256-canonical-nfc-v1" ||
        stored.lineNfcHash !== lineNfcHash ||
        stored.promptMaterialHash !== promptMaterialHash
      ) {
        throw new CorpusRevisionConflictError("SOURCE_FINGERPRINT_CONFLICT");
      }
    }
    const last = selected.at(-1);
    return {
      complete: rows.length <= limit,
      existing,
      inserted,
      nextCursor: last
        ? {
            createdAt: last.createdAt,
            sourceRevisionId: last.sourceRevisionId,
          }
        : null,
      scanned: selected.length,
    };
  }

  async admitSource(
    input: SourceAdmission,
    canonicalPoemId: null | string = null,
  ): Promise<SourceAdmissionResult> {
    assertConfiguredSourceAdmission(
      input,
      this.#sourceName,
      this.#sourceBaseUrl,
    );
    assertHash(input.admissionId, "admissionId");
    const { admissionId: _admissionId, ...admissionBody } = input;
    if (
      (await sha256(sourceAdmissionIdBody(admissionBody))) !== input.admissionId
    ) {
      throw new CorpusRevisionConflictError("SOURCE_ADMISSION_ID_MISMATCH");
    }
    const contentArabic = {
      content: input.linesArabic,
      titleArabic: input.titleArabic,
    };
    if ((await hashCanonical(contentArabic)) !== input.sourceContentSha256) {
      throw new CorpusRevisionConflictError("SOURCE_CONTENT_HASH_MISMATCH");
    }
    if (
      (await sha256(sourceLineNfcHashBody(input.linesArabic))) !==
      input.lineNfcHash
    ) {
      throw new CorpusRevisionConflictError("SOURCE_FINGERPRINT_MISMATCH");
    }
    const expectedRevisionId = await revisionIdentity(
      input.sourceName,
      input.externalPoemId,
      CORPUS_REVISION_SCHEMA_VERSION,
      input.sourceContentSha256,
    );
    if (expectedRevisionId !== input.sourceRevisionId) {
      throw new CorpusRevisionConflictError("SOURCE_REVISION_ID_MISMATCH");
    }
    const [sourceAuthor] = SourceAuthorRowsSchema.parse(
      await this.#db.all(sql`
        SELECT source.canonical_author_id, source.canonical_url,
          author.name_arabic AS canonical_name_arabic
        FROM source_author_identity source
        JOIN author ON author.id = source.canonical_author_id
        WHERE source.source_name = ${input.sourceName}
          AND source.external_id = ${input.sourceAuthorId}
      `),
    );
    if (!sourceAuthor) {
      throw new CorpusRevisionConflictError("AUTHOR_NOT_FOUND");
    }
    if (sourceAuthor.canonicalUrl !== input.sourceAuthorUrl) {
      throw new CorpusRevisionConflictError("SOURCE_AUTHOR_IDENTITY_CONFLICT");
    }
    const promptMaterialHash = await sha256(
      sourcePromptMaterialHashBody({
        authorArabic: sourceAuthor.canonicalNameArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    );
    const [knownSource] = KnownSourceRowsSchema.parse(
      await this.#db.all(sql`
        SELECT source.canonical_poem_id, source.canonical_url,
          pointer.revision_id AS current_revision_id, source.source_author_id,
          source.tombstoned_at, pointer.pointer_version,
          fingerprint.line_nfc_hash, fingerprint.prompt_material_hash
        FROM source_poem_identity source
        LEFT JOIN poem_source_pointer pointer ON pointer.source_poem_id = source.id
        LEFT JOIN source_revision_fingerprint fingerprint
          ON fingerprint.source_revision_id = pointer.revision_id
        WHERE source.source_name = ${input.sourceName}
          AND source.external_id = ${input.externalPoemId}
      `),
    );
    if (knownSource?.tombstonedAt !== null && knownSource !== undefined) {
      throw new CorpusRevisionConflictError("SOURCE_TOMBSTONED");
    }

    await this.#db
      .insert(admissionClock)
      .values({ admissionId: input.admissionId, issuedAt: sql`(unixepoch())` })
      .onConflictDoNothing();
    const [clock] = await this.#db
      .select()
      .from(admissionClock)
      .where(eq(admissionClock.admissionId, input.admissionId))
      .limit(1);
    if (!clock) {
      throw new CorpusRevisionConflictError("SOURCE_ADMISSION_CLOCK_MISSING");
    }
    if (knownSource?.currentRevisionId === input.sourceRevisionId) {
      if (
        !knownSource.canonicalPoemId ||
        (canonicalPoemId !== null &&
          knownSource.canonicalPoemId !== canonicalPoemId) ||
        !knownSource.pointerVersion ||
        knownSource.canonicalUrl !== input.sourcePoemUrl ||
        knownSource.sourceAuthorId !==
          sourceIdentity(input.sourceName, input.sourceAuthorId) ||
        knownSource.lineNfcHash !== input.lineNfcHash ||
        knownSource.promptMaterialHash !== promptMaterialHash
      ) {
        throw new CorpusRevisionConflictError("SOURCE_IDENTITY_CONFLICT");
      }
      return {
        binding: await canonicalBinding(
          input,
          knownSource.canonicalPoemId,
          knownSource.pointerVersion,
          clock.issuedAt,
          sourceAuthor.canonicalAuthorId,
          sourceAuthor.canonicalNameArabic,
          promptMaterialHash,
        ),
        state: "unchanged",
      };
    }
    const [writer] = await this.#db
      .select({ writerEpoch: writerControl.writerEpoch })
      .from(writerControl)
      .where(eq(writerControl.singleton, SINGLETON))
      .limit(1);
    if (!writer) throw new LostWriterEpochError();

    const bundleId = `source-admission-${input.admissionId}`;
    const baseRecord = {
      authorNameArabic: sourceAuthor.canonicalNameArabic,
      bundleId,
      canonicalAuthorId: sourceAuthor.canonicalAuthorId,
      canonicalPoemId,
      contentArabic,
      contentHash: input.sourceContentSha256,
      observedAt: clock.issuedAt,
      ordinal: 0,
      sourceAuthorId: input.sourceAuthorId,
      sourceAuthorUrl: input.sourceAuthorUrl,
      sourceName: input.sourceName,
      sourcePoemId: input.externalPoemId,
      sourcePoemUrl: input.sourcePoemUrl,
      titleArabic: input.titleArabic,
    } satisfies Omit<StageRecordInput, "recordHash">;
    const staged = {
      ...baseRecord,
      recordHash: await hashCanonical(recordHashBody(baseRecord)),
    };
    const recordHashes = [staged.recordHash];
    await this.createOrReuseBundle({
      expectedRecordCount: 1,
      id: bundleId,
      manifestHash: await hashCanonical({ recordHashes }),
      schemaVersion: CORPUS_REVISION_SCHEMA_VERSION,
      writerEpoch: writer.writerEpoch,
    });
    await this.stageRecord(staged);
    await this.sealBundle(bundleId, await hashCanonical(recordHashes));
    const plan = await this.planPromotion(bundleId, writer.writerEpoch);
    const item = plan.items[0];
    if (item?.revisionId !== input.sourceRevisionId) {
      throw new CorpusRevisionConflictError("SOURCE_ADMISSION_PLAN_MISMATCH");
    }
    const promoted = await this.promoteRecord(plan, item);
    await this.recordImportReceipt(plan, {
      advancedPointers: item.pointerAction === "unchanged" ? 0 : 1,
      insertedRevisions: item.revisionAction === "insert" ? 1 : 0,
      reusedRevisions: item.revisionAction === "reuse" ? 1 : 0,
      unchangedPointers: item.pointerAction === "unchanged" ? 1 : 0,
    });

    return {
      binding: await canonicalBinding(
        input,
        item.canonicalPoemId,
        promoted.pointerVersion,
        clock.issuedAt,
        sourceAuthor.canonicalAuthorId,
        sourceAuthor.canonicalNameArabic,
        promptMaterialHash,
      ),
      state: "admitted",
    };
  }

  async advanceWriterEpoch(
    expectedEpoch: number,
    nextEpoch: number,
    writerId: string,
  ): Promise<void> {
    assertPositiveInteger(expectedEpoch, "expectedEpoch");
    assertPositiveInteger(nextEpoch, "nextEpoch");
    if (nextEpoch !== expectedEpoch + 1 || writerId.length === 0) {
      throw new TypeError(
        "Writer epoch must advance exactly once with an owner",
      );
    }
    await this.#db.run(sql`
      UPDATE scraper_writer_control
      SET writer_epoch = ${nextEpoch}, writer_id = ${writerId},
          updated_at = unixepoch()
      WHERE singleton = 1 AND writer_epoch = ${expectedEpoch}
    `);
    const [stored] = await this.#db
      .select({
        writerEpoch: writerControl.writerEpoch,
        writerId: writerControl.writerId,
      })
      .from(writerControl)
      .where(eq(writerControl.singleton, SINGLETON))
      .limit(1);
    if (stored?.writerEpoch !== nextEpoch || stored.writerId !== writerId) {
      throw new LostWriterEpochError();
    }
  }

  async createOrReuseBundle(input: StageBundleInput): Promise<void> {
    assertHash(input.manifestHash, "manifestHash");
    assertNonnegativeInteger(input.expectedRecordCount, "expectedRecordCount");
    assertPositiveInteger(input.schemaVersion, "schemaVersion");
    if (
      input.schemaVersion !== LEGACY_CORPUS_REVISION_SCHEMA_VERSION &&
      input.schemaVersion !== CORPUS_REVISION_SCHEMA_VERSION
    ) {
      throw new CorpusRevisionConflictError("IMPORT_SCHEMA_UNSUPPORTED");
    }
    assertPositiveInteger(input.writerEpoch, "writerEpoch");
    await this.#assertWriterEpoch(input.writerEpoch);

    await this.#db
      .insert(bundle)
      .values({
        ...input,
        createdAt: sql`(unixepoch())`,
      })
      .onConflictDoNothing();

    // A crashed writer's open/sealed bundle may be adopted by the current
    // epoch. Its immutable staged rows remain intact, while any plan frozen
    // against the old epoch is discarded and deterministically regenerated.
    await this.#db.run(sql`
      UPDATE crawl_import_bundle
      SET writer_epoch = ${input.writerEpoch}, plan_hash = NULL,
          promotion_plan = NULL
      WHERE id = ${input.id}
        AND status IN ('open', 'sealed')
        AND writer_epoch <> ${input.writerEpoch}
        AND schema_version = ${input.schemaVersion}
        AND manifest_hash = ${input.manifestHash}
        AND expected_record_count = ${input.expectedRecordCount}
        AND ${input.writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
    `);

    const [stored] = await this.#db
      .select()
      .from(bundle)
      .where(eq(bundle.id, input.id))
      .limit(1);
    if (
      stored?.schemaVersion !== input.schemaVersion ||
      stored.manifestHash !== input.manifestHash ||
      stored.expectedRecordCount !== input.expectedRecordCount ||
      stored.writerEpoch !== input.writerEpoch
    ) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_ID_CONFLICT");
    }
  }

  async stageRecord(input: StageRecordInput): Promise<void> {
    assertNonnegativeInteger(input.ordinal, "ordinal");
    assertHash(input.recordHash, "recordHash");
    assertHash(input.contentHash, "contentHash");
    assertWholeSecond(input.observedAt, "observedAt");
    assertIdentityPart(input.sourceName, "sourceName");
    assertIdentityPart(input.sourceAuthorId, "sourceAuthorId");
    assertIdentityPart(input.sourcePoemId, "sourcePoemId");
    const [parentBundle] = await this.#db
      .select({ schemaVersion: bundle.schemaVersion })
      .from(bundle)
      .where(eq(bundle.id, input.bundleId))
      .limit(1);
    if (!parentBundle) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_MISSING");
    }
    assertContentEnvelope(
      input.contentArabic,
      input.titleArabic,
      parentBundle.schemaVersion,
    );
    if ((await hashCanonical(input.contentArabic)) !== input.contentHash) {
      throw new CorpusRevisionConflictError("CONTENT_HASH_MISMATCH");
    }
    if ((await hashCanonical(recordHashBody(input))) !== input.recordHash) {
      throw new CorpusRevisionConflictError("IMPORT_RECORD_HASH_MISMATCH");
    }

    // The staging table intentionally has a foreign key to the canonical
    // author. Materialize a deterministic shell before the immutable record,
    // then verify the same ownership again during promotion.
    await this.#ensureCanonicalAuthor(input);
    await this.#db.insert(record).values(input).onConflictDoNothing();
    const [stored] = await this.#db
      .select()
      .from(record)
      .where(
        and(
          eq(record.bundleId, input.bundleId),
          eq(record.ordinal, input.ordinal),
        ),
      )
      .limit(1);
    if (
      !stored ||
      stableJson(stageReplayBody(stored)) !== stableJson(stageReplayBody(input))
    ) {
      throw new CorpusRevisionConflictError("IMPORT_RECORD_SLOT_CONFLICT");
    }
  }

  async sealBundle(bundleId: string, rootHash: string): Promise<void> {
    assertHash(rootHash, "rootHash");
    const [storedBundleBeforeSeal] = await this.#db
      .select()
      .from(bundle)
      .where(eq(bundle.id, bundleId))
      .limit(1);
    if (!storedBundleBeforeSeal) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_MISSING");
    }
    await this.#assertWriterEpoch(storedBundleBeforeSeal.writerEpoch);
    const stagedRows = await this.#db
      .select({ ordinal: record.ordinal, recordHash: record.recordHash })
      .from(record)
      .where(eq(record.bundleId, bundleId));
    stagedRows.sort((left, right) => left.ordinal - right.ordinal);
    const recordHashes = stagedRows.map(({ recordHash }) => recordHash);
    const computedRootHash = await hashCanonical(recordHashes);
    const computedManifestHash = await hashCanonical({ recordHashes });
    if (
      computedRootHash !== rootHash ||
      computedManifestHash !== storedBundleBeforeSeal.manifestHash
    ) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_HASH_MISMATCH");
    }
    await this.#db.run(sql`
      UPDATE crawl_import_bundle
      SET status = 'sealed', root_hash = ${rootHash}, sealed_at = unixepoch()
      WHERE id = ${bundleId}
        AND status = 'open'
        AND expected_record_count = (
          SELECT count(*) FROM crawl_import_record
          WHERE bundle_id = ${bundleId}
        )
        AND writer_epoch = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
    `);

    const [stored] = await this.#db
      .select()
      .from(bundle)
      .where(eq(bundle.id, bundleId))
      .limit(1);
    if (!stored) throw new CorpusRevisionConflictError("IMPORT_BUNDLE_MISSING");
    if (stored.status === "open") {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_NOT_SEALABLE");
    }
    if (stored.rootHash !== rootHash) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_ROOT_CONFLICT");
    }
  }

  async planPromotion(
    bundleId: string,
    writerEpoch: number,
  ): Promise<PromotionPlan> {
    assertPositiveInteger(writerEpoch, "writerEpoch");
    await this.#assertWriterEpoch(writerEpoch);
    const [storedBundle] = await this.#db
      .select()
      .from(bundle)
      .where(eq(bundle.id, bundleId))
      .limit(1);
    if (!storedBundle || storedBundle.status === "open") {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_NOT_SEALED");
    }
    if (storedBundle.writerEpoch !== writerEpoch) {
      throw new LostWriterEpochError();
    }
    if (storedBundle.promotionPlan !== null) {
      return parseStoredPlan(storedBundle.promotionPlan, storedBundle.planHash);
    }

    const rows = PlanningRowsSchema.parse(
      await this.#db.all(sql`
        SELECT
          staged.ordinal,
          staged.record_hash,
          staged.source_name,
          staged.source_poem_id,
          staged.canonical_poem_id,
          identities.canonical_poem_id AS identity_canonical_poem_id,
          bundles.schema_version,
          staged.content_hash,
          existing.id AS existing_revision_id,
          pointers.revision_id AS current_revision_id,
          pointers.pointer_version
        FROM crawl_import_record staged
        JOIN crawl_import_bundle bundles ON bundles.id = staged.bundle_id
        LEFT JOIN source_poem_identity identities
          ON identities.source_name = staged.source_name
          AND identities.external_id = staged.source_poem_id
        LEFT JOIN poem_source_revision existing
          ON existing.source_poem_id = identities.id
          AND existing.schema_version = bundles.schema_version
          AND existing.content_hash = staged.content_hash
        LEFT JOIN poem_source_pointer pointers
          ON pointers.source_poem_id = identities.id
        WHERE staged.bundle_id = ${bundleId}
        ORDER BY staged.ordinal
      `),
    );
    if (rows.length !== storedBundle.expectedRecordCount) {
      throw new CorpusRevisionConflictError("IMPORT_BUNDLE_COUNT_CHANGED");
    }

    const items = await Promise.all(
      rows.map(async (row) => {
        if (
          row.canonicalPoemId !== null &&
          row.identityCanonicalPoemId !== null &&
          row.canonicalPoemId !== row.identityCanonicalPoemId
        ) {
          throw new CorpusRevisionConflictError(
            "SOURCE_POEM_CANONICAL_CONFLICT",
          );
        }
        const revisionId =
          row.existingRevisionId ??
          (await revisionIdentity(
            row.sourceName,
            row.sourcePoemId,
            row.schemaVersion,
            row.contentHash,
          ));
        return {
          ordinal: row.ordinal,
          recordHash: row.recordHash,
          sourcePoemKey: sourceIdentity(row.sourceName, row.sourcePoemId),
          canonicalPoemId:
            row.identityCanonicalPoemId ??
            row.canonicalPoemId ??
            (await canonicalPoemIdentity(row.sourceName, row.sourcePoemId)),
          revisionAction: row.existingRevisionId ? "reuse" : "insert",
          pointerAction:
            row.currentRevisionId === revisionId
              ? "unchanged"
              : row.pointerVersion === null
                ? "create"
                : "advance",
          expectedPointerVersion: row.pointerVersion,
          revisionId,
        } satisfies PromotionPlanItem;
      }),
    );
    const planBody = { bundleId, writerEpoch, items };
    const planned = {
      ...planBody,
      planHash: await sha256(stableJson(planBody)),
    } satisfies PromotionPlan;
    await this.#db.run(sql`
      UPDATE crawl_import_bundle
      SET plan_hash = ${planned.planHash},
          promotion_plan = ${JSON.stringify(planned)}
      WHERE id = ${bundleId}
        AND plan_hash IS NULL
        AND promotion_plan IS NULL
        AND status IN ('sealed', 'promoted')
        AND writer_epoch = ${writerEpoch}
        AND ${writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
    `);
    const [persisted] = await this.#db
      .select({
        planHash: bundle.planHash,
        promotionPlan: bundle.promotionPlan,
      })
      .from(bundle)
      .where(eq(bundle.id, bundleId))
      .limit(1);
    if (!persisted?.promotionPlan) throw new LostWriterEpochError();
    return parseStoredPlan(persisted.promotionPlan, persisted.planHash);
  }

  async promoteRecord(
    plan: PromotionPlan,
    item: PromotionPlanItem,
  ): Promise<PromoteRecordResult> {
    await this.#assertPlan(plan);
    const planned = plan.items.find(({ ordinal }) => ordinal === item.ordinal);
    if (!planned || stableJson(planned) !== stableJson(item)) {
      throw new CorpusRevisionConflictError("PROMOTION_ITEM_NOT_IN_PLAN");
    }

    const [staged] = await this.#db
      .select()
      .from(record)
      .where(
        and(
          eq(record.bundleId, plan.bundleId),
          eq(record.ordinal, item.ordinal),
        ),
      )
      .limit(1);
    const [storedBundle] = await this.#db
      .select()
      .from(bundle)
      .where(eq(bundle.id, plan.bundleId))
      .limit(1);
    if (!staged || !storedBundle || staged.recordHash !== item.recordHash) {
      throw new CorpusRevisionConflictError("PROMOTION_INPUT_CHANGED");
    }
    await this.#ensureCanonicalAuthor(staged);
    await this.#ensureCanonicalPoem(item.canonicalPoemId, staged);

    const sourceAuthorKey = sourceIdentity(
      staged.sourceName,
      staged.sourceAuthorId,
    );
    await this.#db.run(sql`
      INSERT INTO source_author_identity (
        id, source_name, external_id, canonical_url, name_arabic,
        canonical_author_id, first_observed_at, last_observed_at
      ) VALUES (
        ${sourceAuthorKey}, ${staged.sourceName}, ${staged.sourceAuthorId},
        ${staged.sourceAuthorUrl}, ${staged.authorNameArabic},
        ${staged.canonicalAuthorId}, ${toUnix(staged.observedAt)},
        ${toUnix(staged.observedAt)}
      )
      ON CONFLICT(source_name, external_id) DO UPDATE SET
        last_observed_at = MAX(last_observed_at, excluded.last_observed_at),
        name_arabic = excluded.name_arabic
      WHERE canonical_author_id = excluded.canonical_author_id
        AND canonical_url = excluded.canonical_url
    `);
    await this.#assertSourceAuthorOwnership(sourceAuthorKey, staged);
    await this.#db.run(sql`
      INSERT INTO source_poem_identity (
        id, source_name, external_id, source_author_id, canonical_url,
        canonical_poem_id, first_observed_at, last_observed_at
      ) VALUES (
        ${item.sourcePoemKey}, ${staged.sourceName}, ${staged.sourcePoemId},
        ${sourceAuthorKey}, ${staged.sourcePoemUrl}, ${item.canonicalPoemId},
        ${toUnix(staged.observedAt)}, ${toUnix(staged.observedAt)}
      )
      ON CONFLICT(source_name, external_id) DO UPDATE SET
        last_observed_at = MAX(last_observed_at, excluded.last_observed_at)
      WHERE source_author_id = excluded.source_author_id
        AND canonical_poem_id = excluded.canonical_poem_id
        AND canonical_url = excluded.canonical_url
    `);
    await this.#assertSourceOwnership(item.sourcePoemKey, item, staged);

    const inserted = await this.#db
      .insert(revision)
      .values({
        id: item.revisionId,
        sourcePoemId: item.sourcePoemKey,
        schemaVersion: storedBundle.schemaVersion,
        contentHash: staged.contentHash,
        titleArabic: staged.titleArabic,
        contentArabic: staged.contentArabic,
        observedAt: staged.observedAt,
        createdAt: sql`(unixepoch())`,
        importBundleId: plan.bundleId,
        importOrdinal: item.ordinal,
      })
      .onConflictDoNothing()
      .returning({ id: revision.id });
    const [storedRevision] = await this.#db
      .select()
      .from(revision)
      .where(eq(revision.id, item.revisionId))
      .limit(1);
    if (
      storedRevision?.sourcePoemId !== item.sourcePoemKey ||
      storedRevision.contentHash !== staged.contentHash ||
      storedRevision.schemaVersion !== storedBundle.schemaVersion ||
      storedRevision.titleArabic !== staged.titleArabic ||
      stableJson(storedRevision.contentArabic) !==
        stableJson(staged.contentArabic)
    ) {
      throw new CorpusRevisionConflictError("SOURCE_REVISION_ID_CONFLICT");
    }

    const lineNfcHash = await sha256(
      sourceLineNfcHashBody(staged.contentArabic.content),
    );
    const promptMaterialHash = await sha256(
      sourcePromptMaterialHashBody({
        authorArabic: staged.authorNameArabic,
        linesArabic: staged.contentArabic.content,
        titleArabic: staged.titleArabic,
      }),
    );
    await this.#db
      .insert(sourceFingerprint)
      .values({
        algorithm: "sha256-canonical-nfc-v1",
        createdAt: sql`(unixepoch())`,
        lineNfcHash,
        promptMaterialHash,
        sourceRevisionId: item.revisionId,
      })
      .onConflictDoNothing();
    const [storedFingerprint] = await this.#db
      .select()
      .from(sourceFingerprint)
      .where(eq(sourceFingerprint.sourceRevisionId, item.revisionId))
      .limit(1);
    if (
      storedFingerprint?.algorithm !== "sha256-canonical-nfc-v1" ||
      storedFingerprint.lineNfcHash !== lineNfcHash ||
      storedFingerprint.promptMaterialHash !== promptMaterialHash
    ) {
      throw new CorpusRevisionConflictError("SOURCE_FINGERPRINT_CONFLICT");
    }

    const pointerVersion = await this.#advanceSourcePointer(plan, item);
    await this.#projectSourceRevision(
      item.canonicalPoemId,
      item.sourcePoemKey,
      item.revisionId,
      pointerVersion,
      plan.writerEpoch,
      staged.titleArabic,
      staged.contentArabic,
    );
    return {
      pointerVersion,
      revisionId: item.revisionId,
      revisionState: inserted.length === 1 ? "inserted" : "reused",
      state:
        item.pointerAction === "unchanged" ? "already_current" : "promoted",
    };
  }

  async recordImportReceipt(
    plan: PromotionPlan,
    counts: Omit<ImportCounts, "bundleId">,
  ): Promise<void> {
    await this.#assertPlan(plan);
    for (const [name, value] of Object.entries(counts)) {
      assertNonnegativeInteger(value, name);
    }
    await this.#db
      .insert(receipt)
      .values({
        bundleId: plan.bundleId,
        planHash: plan.planHash,
        writerEpoch: plan.writerEpoch,
        ...counts,
        createdAt: sql`(unixepoch())`,
      })
      .onConflictDoNothing();
    const [stored] = await this.#db
      .select()
      .from(receipt)
      .where(eq(receipt.bundleId, plan.bundleId))
      .limit(1);
    if (
      stored?.planHash !== plan.planHash ||
      stored.writerEpoch !== plan.writerEpoch ||
      stored.insertedRevisions !== counts.insertedRevisions ||
      stored.reusedRevisions !== counts.reusedRevisions ||
      stored.advancedPointers !== counts.advancedPointers ||
      stored.unchangedPointers !== counts.unchangedPointers
    ) {
      throw new CorpusRevisionConflictError("IMPORT_RECEIPT_CONFLICT");
    }
    await this.#db.run(sql`
      UPDATE crawl_import_bundle
      SET status = 'promoted', promoted_at = COALESCE(promoted_at, unixepoch())
      WHERE id = ${plan.bundleId}
        AND status IN ('sealed', 'promoted')
        AND writer_epoch = ${plan.writerEpoch}
        AND ${plan.writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
    `);
    const [promoted] = await this.#db
      .select({ status: bundle.status })
      .from(bundle)
      .where(eq(bundle.id, plan.bundleId))
      .limit(1);
    if (promoted?.status !== "promoted") {
      throw new LostWriterEpochError();
    }
  }

  async putEnrichmentArtifact(input: EnrichmentArtifactInput): Promise<void> {
    assertHash(input.payloadHash, "payloadHash");
    assertNonnegativeInteger(input.variant, "variant");
    assertPositiveInteger(input.schemaVersion, "schemaVersion");
    const profile = publicationEnrichmentProfile(input);
    if (
      !profile ||
      (input.modelKey !== undefined && input.modelKey !== profile.modelKey)
    ) {
      throw new CorpusRevisionConflictError("ENRICHMENT_PROVENANCE_REJECTED");
    }
    const payload = AnyPoemEnrichmentOutputSchema.parse(input.payload);
    const expectedSchemaVersion = "schemaVersion" in payload ? 2 : 1;
    if (input.schemaVersion !== expectedSchemaVersion) {
      throw new CorpusRevisionConflictError(
        "ENRICHMENT_SCHEMA_VERSION_MISMATCH",
      );
    }
    if ((await hashCanonical(input.payload)) !== input.payloadHash) {
      throw new CorpusRevisionConflictError("ENRICHMENT_PAYLOAD_HASH_MISMATCH");
    }
    await this.#db
      .insert(artifact)
      .values({
        ...input,
        modelKey: profile.modelKey,
        createdAt: sql`(unixepoch())`,
      })
      .onConflictDoNothing();
    const [stored] = await this.#db
      .select()
      .from(artifact)
      .where(eq(artifact.id, input.id))
      .limit(1);
    if (
      stored?.sourceRevisionId !== input.sourceRevisionId ||
      stored.payloadHash !== input.payloadHash ||
      stored.taskKey !== input.taskKey ||
      stored.variant !== input.variant ||
      stored.schemaVersion !== input.schemaVersion ||
      stored.promptVersion !== input.promptVersion ||
      stored.model !== input.model ||
      stored.modelKey !== profile.modelKey ||
      stored.reasoningEffort !== input.reasoningEffort ||
      stableJson(stored.payload) !== stableJson(input.payload)
    ) {
      throw new CorpusRevisionConflictError("ENRICHMENT_ARTIFACT_ID_CONFLICT");
    }
  }

  async putEnrichmentValidation(
    input: EnrichmentValidationInput,
  ): Promise<void> {
    assertHash(input.reportHash, "reportHash");
    assertNonnegativeInteger(input.attempt, "attempt");
    if ((await hashCanonical(input.report)) !== input.reportHash) {
      throw new CorpusRevisionConflictError("VALIDATION_REPORT_HASH_MISMATCH");
    }
    await this.#db
      .insert(validation)
      .values({ ...input, createdAt: sql`(unixepoch())` })
      .onConflictDoNothing();
    const [stored] = await this.#db
      .select()
      .from(validation)
      .where(eq(validation.id, input.id))
      .limit(1);
    if (
      stored?.artifactId !== input.artifactId ||
      stored.validatorKey !== input.validatorKey ||
      stored.validatorVersion !== input.validatorVersion ||
      stored.attempt !== input.attempt ||
      stored.reportHash !== input.reportHash ||
      stored.outcome !== input.outcome ||
      stored.highestSeverity !== input.highestSeverity ||
      stableJson(stored.report) !== stableJson(input.report)
    ) {
      throw new CorpusRevisionConflictError("VALIDATION_ID_CONFLICT");
    }
  }

  async publishEnrichment(
    input: PublishEnrichmentInput,
  ): Promise<PublicationResult> {
    assertPositiveInteger(input.writerEpoch, "writerEpoch");
    await this.#assertWriterEpoch(input.writerEpoch);
    if (input.requiredValidations.length !== 2) {
      throw new CorpusRevisionConflictError("VALIDATION_SET_MUST_HAVE_TWO");
    }
    const validationKeys = input.requiredValidations.map(
      ({ validatorKey, validatorVersion, attempt }) =>
        stableJson([validatorKey, validatorVersion, attempt]),
    );
    if (new Set(validationKeys).size !== validationKeys.length) {
      throw new CorpusRevisionConflictError("VALIDATION_SET_DUPLICATED");
    }
    if (
      new Set(input.requiredValidations.map(({ validatorKey }) => validatorKey))
        .size !== 2
    ) {
      throw new CorpusRevisionConflictError("VALIDATORS_MUST_BE_DISTINCT");
    }
    const [storedArtifact] = await this.#db
      .select()
      .from(artifact)
      .where(eq(artifact.id, input.artifactId))
      .limit(1);
    if (!storedArtifact) {
      throw new CorpusRevisionConflictError("ENRICHMENT_ARTIFACT_MISSING");
    }
    const profile = publicationEnrichmentProfile(storedArtifact);
    if (!profile)
      throw new CorpusRevisionConflictError("ENRICHMENT_PROVENANCE_REJECTED");
    const validationRows = await Promise.all(
      input.requiredValidations.map((required) =>
        this.#db
          .select()
          .from(validation)
          .where(
            and(
              eq(validation.artifactId, input.artifactId),
              eq(validation.validatorKey, required.validatorKey),
              eq(validation.validatorVersion, required.validatorVersion),
              eq(validation.attempt, required.attempt),
            ),
          )
          .limit(1)
          .then(([row]) => row),
      ),
    );
    if (
      validationRows.some(
        (row) =>
          row?.outcome !== "pass" ||
          row.highestSeverity === "critical" ||
          row.highestSeverity === "major",
      )
    ) {
      throw new CorpusRevisionConflictError("ENRICHMENT_VALIDATION_NOT_PASSED");
    }
    const reviews = validationRows.map((row) =>
      PoemEnrichmentReviewSchema.parse(row?.report),
    );
    if (!reviewsAcceptEnrichment(reviews)) {
      throw new CorpusRevisionConflictError("ENRICHMENT_REVIEWS_REJECTED");
    }
    const [sourceIsCurrent] = FoundRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT 1 AS found
        FROM poem
        JOIN poem_source_revision source_revision
          ON source_revision.id = poem.active_source_revision_id
        JOIN model_enrichment_artifact candidate
          ON candidate.source_revision_id = source_revision.id
        WHERE poem.id = ${input.poemId}
          AND candidate.id = ${input.artifactId}
      `),
    );
    if (!sourceIsCurrent) {
      throw new CorpusRevisionConflictError("ENRICHMENT_SOURCE_NOT_CURRENT");
    }
    await this.#assertPublicationNotSuperseded(
      input.poemId,
      storedArtifact.sourceRevisionId,
      profile.promptVersion,
    );

    const [current] = PublicationPointerRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT author.slug AS author_slug,
          publication.enrichment_artifact_id,
          publication.poem_id,
          publication.pointer_version,
          publication.source_revision_id,
          publication.writer_epoch
        FROM poem_model_publication_pointer publication
        JOIN poem ON poem.id = publication.poem_id
        JOIN author ON author.id = poem.author_id
        WHERE publication.poem_id = ${input.poemId}
          AND publication.model_key = ${profile.modelKey}
      `),
    );
    if (
      current?.enrichmentArtifactId === input.artifactId &&
      current.sourceRevisionId === storedArtifact.sourceRevisionId &&
      current.writerEpoch === input.writerEpoch
    ) {
      return {
        authorSlug: current.authorSlug,
        poemId: current.poemId,
        pointerVersion: current.pointerVersion,
        state: "already_current",
      };
    }
    if (
      current &&
      profile.modelKey === "sol-5.6" &&
      profile.promptVersion === "sol-enrichment-v1"
    ) {
      throw new CorpusRevisionConflictError(
        "LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER",
      );
    }
    if ((current?.pointerVersion ?? null) !== input.expectedPointerVersion) {
      throw new LostPromotionClaimError();
    }

    if (current) {
      await this.#db.run(sql`
        UPDATE poem_model_publication_pointer
        SET source_revision_id = ${storedArtifact.sourceRevisionId},
            enrichment_artifact_id = ${input.artifactId},
            pointer_version = pointer_version + 1,
            writer_epoch = ${input.writerEpoch},
            updated_at = unixepoch()
        WHERE poem_id = ${input.poemId}
          AND model_key = ${profile.modelKey}
          AND pointer_version = ${input.expectedPointerVersion}
          AND ${input.writerEpoch} = (
            SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
          )
      `);
    } else {
      await this.#db.run(sql`
        INSERT INTO poem_model_publication_pointer (
          poem_id, model_key, source_revision_id, enrichment_artifact_id,
          pointer_version, writer_epoch, updated_at
        )
        SELECT ${input.poemId}, ${profile.modelKey}, ${storedArtifact.sourceRevisionId},
          ${input.artifactId}, 1, ${input.writerEpoch}, unixepoch()
        WHERE ${input.writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
        ON CONFLICT(poem_id, model_key) DO NOTHING
      `);
    }
    const [published] = PublicationPointerRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT author.slug AS author_slug,
          publication.enrichment_artifact_id,
          publication.poem_id,
          publication.pointer_version,
          publication.source_revision_id,
          publication.writer_epoch
        FROM poem_model_publication_pointer publication
        JOIN poem ON poem.id = publication.poem_id
        JOIN author ON author.id = poem.author_id
        WHERE publication.poem_id = ${input.poemId}
          AND publication.model_key = ${profile.modelKey}
      `),
    );
    if (
      published?.enrichmentArtifactId !== input.artifactId ||
      published.sourceRevisionId !== storedArtifact.sourceRevisionId ||
      published.writerEpoch !== input.writerEpoch
    ) {
      throw new LostPromotionClaimError();
    }
    return {
      authorSlug: published.authorSlug,
      poemId: published.poemId,
      pointerVersion: published.pointerVersion,
      state: "published",
    };
  }

  async publishBoundEnrichment(
    input: BoundEnrichmentPublication,
  ): Promise<EnrichmentPublicationReceiptV1> {
    assertHash(input.publicationIntentId, "publicationIntentId");
    assertHash(input.actionHash, "actionHash");
    const { actionHash: _actionHash, ...actionBody } = input;
    if (
      (await sha256(enrichmentPublicationActionHashBody(actionBody))) !==
      input.actionHash
    ) {
      throw new CorpusRevisionConflictError("PUBLICATION_ACTION_HASH_MISMATCH");
    }
    if (
      (await sha256(
        publicationIntentIdBody({
          artifactId: input.artifact.id,
          bindingId: input.binding.bindingId,
          modelKey: input.artifact.modelKey,
          promptVersion: input.artifact.promptVersion,
        }),
      )) !== input.publicationIntentId
    ) {
      throw new CorpusRevisionConflictError("PUBLICATION_INTENT_ID_MISMATCH");
    }
    const {
      admissionEvidence: _evidence,
      bindingId: _bindingId,
      ...bindingBody
    } = input.binding;
    if (
      (await sha256(canonicalPoemBindingIdBody(bindingBody))) !==
      input.binding.bindingId
    ) {
      throw new CorpusRevisionConflictError("PUBLICATION_BINDING_ID_MISMATCH");
    }

    const [existingReceipt] = await this.#db
      .select()
      .from(publicationReceipt)
      .where(eq(publicationReceipt.intentId, input.publicationIntentId))
      .limit(1);
    if (existingReceipt) {
      if (existingReceipt.actionHash !== input.actionHash) {
        throw new CorpusRevisionConflictError("PUBLICATION_INTENT_CONFLICT");
      }
      return publicationReceiptResult(existingReceipt);
    }

    const [bound] = BoundPublicationRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT poem.author_id,
        source.external_id,
        fingerprint.line_nfc_hash,
        artifact.model_key,
        poem.id AS poem_id,
        fingerprint.prompt_material_hash,
        source.source_name,
        source_pointer.pointer_version AS source_pointer_version,
        revision.id AS source_revision_id,
        writer.writer_epoch
      FROM poem_source_revision revision
      JOIN source_poem_identity source ON source.id = revision.source_poem_id
      JOIN poem ON poem.id = source.canonical_poem_id
      JOIN poem_source_pointer source_pointer
        ON source_pointer.source_poem_id = source.id
        AND source_pointer.revision_id = revision.id
      JOIN source_revision_fingerprint fingerprint
        ON fingerprint.source_revision_id = revision.id
      JOIN model_enrichment_artifact artifact
        ON artifact.id = ${input.artifact.id}
        AND artifact.source_revision_id = revision.id
      JOIN scraper_writer_control writer ON writer.singleton = 1
        WHERE revision.id = ${input.binding.sourceRevisionId}
          AND poem.active_source_revision_id = revision.id
          AND source.tombstoned_at IS NULL
      `),
    );
    if (
      bound?.poemId !== input.binding.poemId ||
      bound.authorId !== input.binding.authorId ||
      bound.sourceName !== input.binding.sourceName ||
      bound.externalId !== input.binding.externalPoemId ||
      bound.lineNfcHash !== input.binding.lineNfcHash ||
      bound.promptMaterialHash !== input.binding.promptMaterialHash ||
      bound.sourcePointerVersion !==
        input.binding.admissionEvidence.sourcePointerVersion
    ) {
      throw new CorpusRevisionConflictError("PUBLICATION_BINDING_NOT_CURRENT");
    }

    await this.#assertPublicationNotSuperseded(
      input.binding.poemId,
      input.binding.sourceRevisionId,
      input.artifact.promptVersion,
    );
    const [current] = ModelPointerRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT enrichment_artifact_id, pointer_version, source_revision_id,
          writer_epoch
        FROM poem_model_publication_pointer
        WHERE poem_id = ${input.binding.poemId}
          AND model_key = ${input.artifact.modelKey}
      `),
    );
    const alreadyPublished =
      current?.enrichmentArtifactId === input.artifact.id &&
      current.sourceRevisionId === input.binding.sourceRevisionId;
    const pointerVersion = alreadyPublished
      ? current.pointerVersion
      : (current?.pointerVersion ?? 0) + 1;
    const writerEpoch = alreadyPublished
      ? current.writerEpoch
      : bound.writerEpoch;
    const committedAt = new Date(Math.floor(Date.now() / 1_000) * 1_000);
    try {
      await this.#db.insert(publicationReceipt).values({
        actionHash: input.actionHash,
        committedAt,
        enrichmentArtifactId: input.artifact.id,
        expectedPointerVersion: current?.pointerVersion ?? null,
        intentId: input.publicationIntentId,
        modelKey: input.artifact.modelKey,
        outcome: alreadyPublished ? "already-published" : "published",
        poemId: input.binding.poemId,
        pointerVersion,
        promptVersion: input.artifact.promptVersion,
        sourceRevisionId: input.binding.sourceRevisionId,
        writerEpoch,
      });
    } catch (error) {
      const [racedReceipt] = await this.#db
        .select()
        .from(publicationReceipt)
        .where(eq(publicationReceipt.intentId, input.publicationIntentId))
        .limit(1);
      if (racedReceipt?.actionHash !== input.actionHash) {
        await this.#assertPublicationNotSuperseded(
          input.binding.poemId,
          input.binding.sourceRevisionId,
          input.artifact.promptVersion,
        );
        throw error;
      }
      return publicationReceiptResult(racedReceipt);
    }
    const [storedReceipt] = await this.#db
      .select()
      .from(publicationReceipt)
      .where(eq(publicationReceipt.intentId, input.publicationIntentId))
      .limit(1);
    if (storedReceipt?.actionHash !== input.actionHash) {
      throw new CorpusRevisionConflictError("PUBLICATION_RECEIPT_MISSING");
    }
    return publicationReceiptResult(storedReceipt);
  }

  async #assertPublicationNotSuperseded(
    poemId: string,
    sourceRevisionId: string,
    promptVersion: string,
  ): Promise<void> {
    if (promptVersion !== "sol-word-gloss-v2") return;
    const rows = FoundRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT 1 AS found
        FROM poem_model_publication_pointer pointer
        JOIN model_enrichment_artifact current
          ON current.id = pointer.enrichment_artifact_id
        WHERE pointer.poem_id = ${poemId}
          AND pointer.model_key = 'sol-5.6'
          AND pointer.source_revision_id = ${sourceRevisionId}
          AND current.prompt_version = 'sol-word-gloss-v3'
      `),
    );
    if (rows.length > 0)
      throw new CorpusRevisionConflictError(
        "LEGACY_SOL_PUBLICATION_SUPERSEDED",
      );
  }

  async #adoptLegacySourceLineageRow(
    value: unknown,
  ): Promise<"admitted" | "unchanged"> {
    const row = parseLegacySourceLineageRow(value);
    const sourcePoemId = legacySourcePoemId(row.poem_slug, this.#sourceProfile);
    const sourceAuthorId = legacySourceAuthorId(row.author_slug);
    const titleArabic = row.title_arabic.trim().normalize("NFC");
    if (titleArabic.length === 0) {
      throw new CorpusRevisionConflictError(
        "LEGACY_SOURCE_LINEAGE_TITLE_INVALID",
      );
    }
    const content = parseLegacySourceContent(row.content_arabic);
    const contentArabic = { content: content.content, titleArabic };
    const sourceContentSha256 = await hashCanonical(contentArabic);
    const sourceRevisionId = await revisionIdentity(
      this.#sourceName,
      sourcePoemId,
      CORPUS_REVISION_SCHEMA_VERSION,
      sourceContentSha256,
    );
    await this.#ensureLegacySourceAuthor({
      authorId: row.author_id,
      authorNameArabic: row.author_name_arabic,
      sourceAuthorId,
    });
    const body = {
      externalPoemId: sourcePoemId,
      lineNfcHash: await sha256(sourceLineNfcHashBody(content.content)),
      linesArabic: content.content,
      sourceAuthorId,
      sourceAuthorUrl: legacySourceAuthorUrl(
        this.#sourceBaseUrl,
        sourceAuthorId,
        this.#sourceProfile,
      ),
      sourceContentSha256,
      sourceName: this.#sourceName,
      sourcePoemUrl: legacySourcePoemUrl(
        this.#sourceBaseUrl,
        sourcePoemId,
        this.#sourceProfile,
      ),
      sourceRevisionId,
      titleArabic,
    };
    const admission = SourceAdmissionV2ItemSchema.safeParse({
      ...body,
      admissionId: await sha256(sourceAdmissionIdBody(body)),
    });
    if (!admission.success) {
      throw new CorpusRevisionConflictError(
        "LEGACY_SOURCE_LINEAGE_ADMISSION_INVALID",
      );
    }
    const result = await this.admitSource(admission.data, row.poem_id);
    return result.state;
  }

  async #advanceSourcePointer(
    plan: PromotionPlan,
    item: PromotionPlanItem,
  ): Promise<number> {
    if (item.expectedPointerVersion === null) {
      await this.#db.run(sql`
        INSERT INTO poem_source_pointer (
          source_poem_id, revision_id, pointer_version, writer_epoch, updated_at
        )
        SELECT ${item.sourcePoemKey}, ${item.revisionId}, 1,
          ${plan.writerEpoch}, unixepoch()
        WHERE ${plan.writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
        ON CONFLICT(source_poem_id) DO NOTHING
      `);
    } else if (item.pointerAction === "unchanged") {
      await this.#db.run(sql`
        UPDATE poem_source_pointer
        SET pointer_version = pointer_version + 1,
            writer_epoch = ${plan.writerEpoch}, updated_at = unixepoch()
        WHERE source_poem_id = ${item.sourcePoemKey}
          AND revision_id = ${item.revisionId}
          AND pointer_version = ${item.expectedPointerVersion}
          AND writer_epoch <> ${plan.writerEpoch}
          AND ${plan.writerEpoch} = (
            SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
          )
      `);
    } else {
      await this.#db.run(sql`
        UPDATE poem_source_pointer
        SET revision_id = ${item.revisionId},
            pointer_version = pointer_version + 1,
            writer_epoch = ${plan.writerEpoch},
            updated_at = unixepoch()
        WHERE source_poem_id = ${item.sourcePoemKey}
          AND pointer_version = ${item.expectedPointerVersion}
          AND ${plan.writerEpoch} = (
            SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
          )
      `);
    }
    const [pointer] = SourcePointerRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT pointer_version, revision_id, writer_epoch
        FROM poem_source_pointer
        WHERE source_poem_id = ${item.sourcePoemKey}
      `),
    );
    if (
      pointer?.revisionId !== item.revisionId ||
      pointer.writerEpoch !== plan.writerEpoch
    ) {
      throw new LostPromotionClaimError();
    }
    return pointer.pointerVersion;
  }

  async #projectSourceRevision(
    canonicalPoemId: string,
    sourcePoemKey: string,
    revisionId: string,
    pointerVersion: number,
    writerEpoch: number,
    titleArabic: string,
    contentArabic: { content: string[] },
  ): Promise<void> {
    const verses = Math.max(1, Math.ceil(contentArabic.content.length / 2));
    await this.#db.run(sql`
      UPDATE poem
      SET name_arabic = ${titleArabic},
          content_arabic = ${JSON.stringify(contentArabic)},
          verses = ${verses},
          active_source_revision_id = ${revisionId},
          active_enrichment_artifact_id = CASE
            WHEN active_source_revision_id = ${revisionId}
            THEN active_enrichment_artifact_id ELSE NULL END,
          translation_sol = CASE
            WHEN active_source_revision_id = ${revisionId}
            THEN translation_sol ELSE NULL END,
          insights_sol = CASE
            WHEN active_source_revision_id = ${revisionId}
            THEN insights_sol ELSE NULL END
      WHERE id = ${canonicalPoemId}
        AND EXISTS (
          SELECT 1 FROM poem_source_pointer
          WHERE source_poem_id = ${sourcePoemKey}
            AND revision_id = ${revisionId}
            AND pointer_version = ${pointerVersion}
            AND writer_epoch = ${writerEpoch}
        )
        AND ${writerEpoch} = (
          SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
        )
        AND (
          name_arabic IS NOT ${titleArabic}
          OR content_arabic IS NOT ${JSON.stringify(contentArabic)}
          OR verses IS NOT ${verses}
          OR active_source_revision_id IS NOT ${revisionId}
        )
    `);
    const [projected] = ProjectedRevisionRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT active_source_revision_id FROM poem WHERE id = ${canonicalPoemId}
      `),
    );
    if (projected?.activeSourceRevisionId !== revisionId) {
      throw new LostPromotionClaimError();
    }
  }

  async #assertPlan(plan: PromotionPlan): Promise<void> {
    await this.#assertWriterEpoch(plan.writerEpoch);
    const expectedHash = await sha256(
      stableJson({
        bundleId: plan.bundleId,
        writerEpoch: plan.writerEpoch,
        items: plan.items,
      }),
    );
    if (expectedHash !== plan.planHash) {
      throw new CorpusRevisionConflictError("PROMOTION_PLAN_HASH_MISMATCH");
    }
  }

  async #ensureLegacySourceAuthor(input: {
    readonly authorId: string;
    readonly authorNameArabic: string;
    readonly sourceAuthorId: string;
  }): Promise<void> {
    const sourceAuthorKey = sourceIdentity(
      this.#sourceName,
      input.sourceAuthorId,
    );
    const sourceAuthorUrl = legacySourceAuthorUrl(
      this.#sourceBaseUrl,
      input.sourceAuthorId,
      this.#sourceProfile,
    );
    await this.#db.run(sql`
      INSERT INTO source_author_identity (
        id, source_name, external_id, canonical_url, name_arabic,
        canonical_author_id, first_observed_at, last_observed_at
      ) VALUES (
        ${sourceAuthorKey}, ${this.#sourceName}, ${input.sourceAuthorId},
        ${sourceAuthorUrl}, ${input.authorNameArabic}, ${input.authorId},
        unixepoch(), unixepoch()
      )
      ON CONFLICT(source_name, external_id) DO NOTHING
    `);
    const [stored] = SourceAuthorOwnershipRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT source_name, external_id, canonical_url, canonical_author_id
        FROM source_author_identity WHERE id = ${sourceAuthorKey}
      `),
    );
    if (
      stored?.sourceName !== this.#sourceName ||
      stored.externalId !== input.sourceAuthorId ||
      stored.canonicalUrl !== sourceAuthorUrl ||
      stored.canonicalAuthorId !== input.authorId
    ) {
      throw new CorpusRevisionConflictError(
        "LEGACY_SOURCE_AUTHOR_OWNERSHIP_CONFLICT",
      );
    }
  }

  async #assertWriterEpoch(expected: number): Promise<void> {
    const [control] = await this.#db
      .select({ writerEpoch: writerControl.writerEpoch })
      .from(writerControl)
      .where(eq(writerControl.singleton, SINGLETON))
      .limit(1);
    if (control?.writerEpoch !== expected) {
      throw new LostWriterEpochError();
    }
  }

  async #ensureCanonicalAuthor(staged: StageRecordInput): Promise<void> {
    const [stored] = FoundRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT 1 AS found FROM author WHERE id = ${staged.canonicalAuthorId}
      `),
    );
    if (stored) return;
    const slug = `source-${await sha256(
      stableJson([staged.sourceName, staged.sourceAuthorId]),
    )}`;
    await this.#db
      .insert(author)
      .values({
        id: staged.canonicalAuthorId,
        slug,
        nameArabic: staged.authorNameArabic,
        sortNameArabic: staged.authorNameArabic,
        status: "init",
      })
      .onConflictDoNothing();
    const [created] = await this.#db
      .select({
        id: author.id,
        nameArabic: author.nameArabic,
        slug: author.slug,
      })
      .from(author)
      .where(eq(author.id, staged.canonicalAuthorId))
      .limit(1);
    if (
      created?.id !== staged.canonicalAuthorId ||
      created.nameArabic !== staged.authorNameArabic ||
      created.slug !== slug
    ) {
      throw new CorpusRevisionConflictError("CANONICAL_AUTHOR_ID_CONFLICT");
    }
  }

  async #ensureCanonicalPoem(
    canonicalPoemId: string,
    staged: StageRecordInput,
  ): Promise<void> {
    const generatedId = await canonicalPoemIdentity(
      staged.sourceName,
      staged.sourcePoemId,
    );
    if (staged.canonicalPoemId === null) {
      if (canonicalPoemId !== generatedId) {
        throw new CorpusRevisionConflictError("CANONICAL_POEM_PLAN_CONFLICT");
      }
      const verses = Math.max(
        1,
        Math.ceil(staged.contentArabic.content.length / 2),
      );
      await this.#db
        .insert(poem)
        .values({
          id: canonicalPoemId,
          authorId: staged.canonicalAuthorId,
          slug: generatedPoemSlug(generatedId),
          sitemapShard: sitemapShardForId(canonicalPoemId),
          verses,
          nameArabic: staged.titleArabic,
          contentArabic: staged.contentArabic,
        })
        .onConflictDoNothing();
    } else if (canonicalPoemId !== staged.canonicalPoemId) {
      throw new CorpusRevisionConflictError("CANONICAL_POEM_PLAN_CONFLICT");
    }
    const [stored] = await this.#db
      .select({ authorId: poem.authorId, id: poem.id, slug: poem.slug })
      .from(poem)
      .where(eq(poem.id, canonicalPoemId))
      .limit(1);
    if (
      stored?.authorId !== staged.canonicalAuthorId ||
      (staged.canonicalPoemId === null &&
        stored.slug !== generatedPoemSlug(generatedId))
    ) {
      throw new CorpusRevisionConflictError(
        "CANONICAL_POEM_OWNERSHIP_CONFLICT",
      );
    }
  }

  async #assertSourceAuthorOwnership(
    sourceAuthorKey: string,
    staged: StageRecordInput,
  ): Promise<void> {
    const [stored] = SourceAuthorOwnershipRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT source_name, external_id, canonical_url, canonical_author_id
        FROM source_author_identity WHERE id = ${sourceAuthorKey}
      `),
    );
    if (
      stored?.sourceName !== staged.sourceName ||
      stored.externalId !== staged.sourceAuthorId ||
      stored.canonicalUrl !== staged.sourceAuthorUrl ||
      stored.canonicalAuthorId !== staged.canonicalAuthorId
    ) {
      throw new CorpusRevisionConflictError("SOURCE_AUTHOR_OWNERSHIP_CONFLICT");
    }
  }

  async #assertSourceOwnership(
    sourcePoemKey: string,
    item: PromotionPlanItem,
    staged: StageRecordInput,
  ): Promise<void> {
    const [stored] = SourcePoemOwnershipRowSchema.array().parse(
      await this.#db.all(sql`
        SELECT canonical_poem_id, canonical_url, source_author_id
        FROM source_poem_identity WHERE id = ${sourcePoemKey}
      `),
    );
    if (
      stored?.canonicalPoemId !== item.canonicalPoemId ||
      stored.canonicalUrl !== staged.sourcePoemUrl ||
      stored.sourceAuthorId !==
        sourceIdentity(staged.sourceName, staged.sourceAuthorId)
    ) {
      throw new CorpusRevisionConflictError("SOURCE_POEM_OWNERSHIP_CONFLICT");
    }
  }
}

export class CorpusRevisionConflictError extends Error {}

export class LostPromotionClaimError extends Error {
  constructor() {
    super("PROMOTION_COMPARE_AND_SWAP_FAILED");
  }
}

export class LostWriterEpochError extends Error {
  constructor() {
    super("SCRAPER_WRITER_EPOCH_LOST");
  }
}

function publicationReceiptResult(input: {
  actionHash: string;
  committedAt: Date;
  enrichmentArtifactId: string;
  intentId: string;
  modelKey: string;
  outcome: string;
  poemId: string;
  pointerVersion: number;
  promptVersion: string;
  sourceRevisionId: string;
  writerEpoch: number;
}): EnrichmentPublicationReceiptV1 {
  if (input.outcome !== "published" && input.outcome !== "already-published") {
    throw new CorpusRevisionConflictError("PUBLICATION_RECEIPT_CORRUPT");
  }
  return EnrichmentPublicationReceiptV1Schema.parse({
    actionHash: input.actionHash,
    artifactId: input.enrichmentArtifactId,
    committedAt: input.committedAt.toISOString(),
    modelKey: input.modelKey,
    outcome: input.outcome,
    poemId: input.poemId,
    pointerVersion: input.pointerVersion,
    promptVersion: input.promptVersion,
    publicationIntentId: input.intentId,
    schemaId: "saqi.enrichment-publication-receipt",
    schemaVersion: 1,
    sourceRevisionId: input.sourceRevisionId,
    writerEpoch: input.writerEpoch,
  });
}

async function canonicalBinding(
  input: SourceAdmission,
  poemId: string,
  sourcePointerVersion: number,
  issuedAt: Date,
  authorId: string,
  authorNameArabic: string,
  promptMaterialHash: string,
): Promise<CanonicalPoemBindingV1> {
  const body = {
    authorId,
    authorNameArabic,
    externalPoemId: input.externalPoemId,
    lineNfcHash: input.lineNfcHash,
    poemId,
    promptMaterialHash,
    schemaId: "saqi.canonical-poem-binding" as const,
    schemaVersion: 1 as const,
    sourceName: input.sourceName,
    sourceRevisionId: input.sourceRevisionId,
  };
  return {
    ...body,
    admissionEvidence: {
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
      issuedAt: issuedAt.toISOString(),
      sourcePointerVersion,
    },
    bindingId: await sha256(canonicalPoemBindingIdBody(body)),
  };
}

function assertConfiguredSourceAdmission(
  input: SourceAdmission,
  sourceName: string,
  sourceBaseUrl: URL,
): void {
  if (input.sourceName !== sourceName) {
    throw new CorpusRevisionConflictError("SOURCE_CONFIGURATION_MISMATCH");
  }
  for (const value of [input.sourceAuthorUrl, input.sourcePoemUrl]) {
    const url = new URL(value);
    if (
      url.origin !== sourceBaseUrl.origin ||
      url.username !== "" ||
      url.password !== "" ||
      url.search !== "" ||
      url.hash !== ""
    ) {
      throw new CorpusRevisionConflictError("SOURCE_CONFIGURATION_MISMATCH");
    }
  }
}

function sourceIdentity(sourceName: string, externalId: string): string {
  assertIdentityPart(sourceName, "sourceName");
  assertIdentityPart(externalId, "externalId");
  return `${sourceName}\u{1F}${externalId}`;
}

function parseLegacySourceLineageRow(
  value: unknown,
): z.infer<typeof LegacySourceLineageRowSchema> {
  const parsed = LegacySourceLineageRowSchema.safeParse(value);
  if (!parsed.success) {
    throw new CorpusRevisionConflictError("LEGACY_SOURCE_LINEAGE_ROW_INVALID");
  }
  return parsed.data;
}

function parseLegacySourceContent(value: string): CorpusContentArabic {
  let decoded: unknown;
  try {
    decoded = JSON.parse(value);
  } catch {
    throw new CorpusRevisionConflictError(
      "LEGACY_SOURCE_LINEAGE_CONTENT_INVALID",
    );
  }
  const parsed = CorpusContentArabicSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new CorpusRevisionConflictError(
      "LEGACY_SOURCE_LINEAGE_CONTENT_INVALID",
    );
  }
  return parsed.data;
}

function legacySourcePoemId(
  slug: string,
  profile: SourceAdapterProfileV1,
): string {
  const sourcePoemId = sourcePathValue(profile.routes.poemSlug, "id", slug);
  if (
    sourcePoemId === undefined ||
    !Number.isSafeInteger(Number(sourcePoemId))
  ) {
    throw new CorpusRevisionConflictError("LEGACY_SOURCE_POEM_ID_INVALID");
  }
  return sourcePoemId;
}

function legacySourceAuthorId(slug: string): string {
  const normalized = slug.normalize("NFC");
  if (
    normalized.length > 512 ||
    !/^[\p{L}\p{N}_.~-]+(?: [\p{L}\p{N}_.~-]+)*$/u.test(normalized)
  ) {
    throw new CorpusRevisionConflictError("LEGACY_SOURCE_AUTHOR_ID_INVALID");
  }
  return normalized;
}

function legacySourceAuthorUrl(
  sourceBaseUrl: URL,
  sourceAuthorId: string,
  profile: SourceAdapterProfileV1,
): string {
  return new URL(
    renderSourcePath(
      profile.routes.authorPath,
      "slug",
      encodeURIComponent(sourceAuthorId),
    ),
    sourceBaseUrl,
  ).toString();
}

function legacySourcePoemUrl(
  sourceBaseUrl: URL,
  sourcePoemId: string,
  profile: SourceAdapterProfileV1,
): string {
  return new URL(
    renderSourcePath(profile.routes.poemPath, "id", sourcePoemId),
    sourceBaseUrl,
  ).toString();
}

async function revisionIdentity(
  sourceName: string,
  sourcePoemId: string,
  schemaVersion: number,
  contentHash: string,
): Promise<string> {
  return sha256(
    `revision\u{1F}${sourceName}\u{1F}${sourcePoemId}\u{1F}${String(schemaVersion)}\u{1F}${contentHash}`,
  );
}

async function canonicalPoemIdentity(
  sourceName: string,
  sourcePoemId: string,
): Promise<string> {
  return sha256(`poem\u{1F}${sourceName}\u{1F}${sourcePoemId}`);
}

function generatedPoemSlug(canonicalPoemId: string): string {
  return `source-${canonicalPoemId}`;
}

function recordHashBody(
  input: Omit<StageRecordInput, "recordHash">,
): Record<string, unknown> {
  return {
    authorNameArabic: input.authorNameArabic,
    canonicalAuthorId: input.canonicalAuthorId,
    canonicalPoemId: input.canonicalPoemId,
    contentArabic: input.contentArabic,
    contentHash: input.contentHash,
    observedAt: input.observedAt.toISOString(),
    ordinal: input.ordinal,
    sourceAuthorId: input.sourceAuthorId,
    sourceAuthorUrl: input.sourceAuthorUrl,
    sourceName: input.sourceName,
    sourcePoemId: input.sourcePoemId,
    sourcePoemUrl: input.sourcePoemUrl,
    titleArabic: input.titleArabic,
  };
}

function stageReplayBody(input: StageRecordInput): Record<string, unknown> {
  return {
    ...recordHashBody(input),
    bundleId: input.bundleId,
    recordHash: input.recordHash,
  };
}

function toUnix(date: Date): number {
  assertWholeSecond(date, "date");
  return date.getTime() / 1000;
}

function assertWholeSecond(value: Date, name: string): void {
  if (!Number.isFinite(value.getTime()) || value.getTime() % 1_000 !== 0) {
    throw new TypeError(`${name} must be a valid whole-second timestamp`);
  }
}

function assertIdentityPart(value: string, name: string): void {
  if (
    value.trim().length === 0 ||
    value.length > 1_024 ||
    /[\u{0}-\u{1F}\u{7F}]/u.test(value)
  ) {
    throw new TypeError(`${name} is not a safe identity component`);
  }
}

function assertContentEnvelope(
  contentArabic: CorpusContentArabic,
  titleArabic: string,
  schemaVersion: number,
): void {
  const keys = Object.keys(contentArabic).toSorted();
  if (
    schemaVersion === LEGACY_CORPUS_REVISION_SCHEMA_VERSION &&
    keys.length === 1 &&
    keys[0] === "content"
  ) {
    return;
  }
  if (
    schemaVersion === CORPUS_REVISION_SCHEMA_VERSION &&
    keys.length === 2 &&
    keys[0] === "content" &&
    keys[1] === "titleArabic" &&
    "titleArabic" in contentArabic &&
    contentArabic.titleArabic === titleArabic
  ) {
    return;
  }
  throw new CorpusRevisionConflictError("CONTENT_SCHEMA_MISMATCH");
}

function assertHash(value: string, name: string): void {
  if (!/^[\da-f]{64}$/.test(value))
    throw new TypeError(`${name} must be SHA-256`);
}

function assertNonnegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a nonnegative safe integer`);
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}

function stableJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError("Canonical JSON rejects non-finite numbers");
    }
    return Object.is(value, -0) ? "0" : JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    if (!value.every((_item, index) => index in value)) {
      throw new TypeError("Canonical JSON rejects sparse arrays");
    }
    return `[${value.map(stableJson).join(",")}]`;
  }
  if (typeof value !== "object") {
    throw new TypeError(`Canonical JSON rejects ${typeof value}`);
  }
  const prototype: unknown = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError("Canonical JSON accepts only plain objects");
  }
  return `{${Object.entries(value)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([key, nested]) => `${JSON.stringify(key)}:${stableJson(nested)}`)
    .join(",")}}`;
}

async function hashCanonical(value: unknown): Promise<string> {
  return sha256(stableJson(value));
}

async function parseStoredPlan(
  value: unknown,
  storedHash: null | string,
): Promise<PromotionPlan> {
  const parsed = PromotionPlanSchema.safeParse(value);
  if (!parsed.success || parsed.data.planHash !== storedHash) {
    throw new CorpusRevisionConflictError("PROMOTION_PLAN_CORRUPT");
  }
  const { planHash, ...body } = parsed.data;
  if ((await sha256(stableJson(body))) !== planHash) {
    throw new CorpusRevisionConflictError("PROMOTION_PLAN_HASH_MISMATCH");
  }
  return parsed.data;
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}
