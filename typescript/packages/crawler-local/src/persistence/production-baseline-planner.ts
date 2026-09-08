import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

import {
  CORPUS_REVISION_SCHEMA_VERSION,
  ENRICHMENT_INPUT_SCHEMA_ID,
  ENRICHMENT_INPUT_SCHEMA_VERSION,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
  READABLE_ENRICHMENT_PROFILES,
} from "@saqi/precedent-iso";
import {
  type AuthorPoemManifest,
  canonicalAuthorUrl,
  canonicalPoemUrl,
  currentSource,
  sourceAuthorUrl,
  sourcePoemIdFromSlug,
  sourcePoemUrl,
} from "@saqi/source-adapter";
import { z } from "zod";

import { collectionWorkKinds } from "../collection/collection-scheduler.js";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector.js";
import {
  canonicalPoemIdFromLegacySlug,
  sourcePoemIdFromLegacySlug,
} from "../collection/inventory-reconciliation.js";
import { SOL_ENRICHMENT_WORK_KIND } from "../enrichment/sol-coordinator.js";
import { SOL_PIPELINE_VERSION } from "../enrichment/sol-runner.js";
import { canonicalSourceRevisionId } from "../publication/corpus-import-actions.js";
import type { ArtifactStore } from "./artifact-store.js";
import type { Ledger } from "./ledger.js";
import type { WorkDefinition } from "./schema.js";
import { canonicalJson, inputHash, sha256 } from "./work-key.js";

const MAX_NDJSON_LINE_BYTES = 1_048_576;
const MAX_PRODUCTION_ROWS = 1_000_000;
const CanonicalIdSchema = z.uuid();
const RecoveryPrioritySchema = z.number().int().min(-1_000_000).max(1_000_000);
const BaselineManifestHashSchema = z.string().regex(/^[a-f\d]{64}$/);
const FileChunkSchema = z.instanceof(Uint8Array);
const ProductionPoemIdSchema = z.union([
  z.uuid(),
  z.string().regex(/^[a-f\d]{64}$/),
]);
const SourceBoundPoemIdSchema = z.string().regex(/^[a-f\d]{64}$/);
export const BASELINE_NO_TRANSLATION_PRIORITY = 200;
export const BASELINE_MISSING_INSIGHTS_PRIORITY = 100;
export const BASELINE_SHORTEST_POEM_BONUS = 70;

const AuthorRowSchema = z
  .object({
    id: z.uuid(),
    name_arabic: z.string().trim().min(1).max(512),
    slug: z.string().trim().min(1).max(512),
  })
  .strict();

const ContentSchema = z
  .object({
    content: z.array(z.string()).max(2_000),
    titleArabic: z.string().trim().min(1).max(512).optional(),
  })
  .strict();
const IneligibleReasonSchema = z.enum([
  "missing_arabic_content",
  "missing_arabic_author",
  "missing_arabic_title",
  "missing_arabic_title_and_content",
  "source_text_constraints",
  "unsafe_source_text",
]);
const SqliteBooleanSchema = z
  .union([z.boolean(), z.literal(0), z.literal(1)])
  .transform((value) => value === true || value === 1);
const ModelPublicationSchema = z
  .object({
    model_key: z.string().trim().min(1).max(128),
    prompt_version: z.string().trim().min(1).max(256),
    source_revision_id: z.string().regex(/^[a-f\d]{64}$/),
  })
  .strict();
const PoemRowSchema = z
  .object({
    active_source_revision_id: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullish(),
    author_id: z.uuid().nullable(),
    content_arabic: z.union([
      ContentSchema,
      z.string().max(MAX_NDJSON_LINE_BYTES),
    ]),
    id: ProductionPoemIdSchema,
    insights: z.unknown().nullish(),
    insights_missing: SqliteBooleanSchema.optional(),
    ineligible_reason: IneligibleReasonSchema.optional(),
    name_arabic: z.string().trim().max(512),
    model_publications: z.array(ModelPublicationSchema).max(100).optional(),
    slug: z.string().trim().min(1).max(512),
    translation: z.unknown().nullish(),
    translation_gemini: z.unknown().nullish(),
    translation_missing: SqliteBooleanSchema.optional(),
  })
  .strict()
  .superRefine((row, context) => {
    const modelKeys = row.model_publications?.map(
      (publication) => publication.model_key,
    );
    if (modelKeys && new Set(modelKeys).size !== modelKeys.length) {
      context.addIssue({
        code: "custom",
        message: "model publications must have unique model keys",
      });
    }
    if (row.name_arabic.length === 0 && row.ineligible_reason === undefined) {
      context.addIssue({
        code: "custom",
        message: "empty title requires an explicit ineligible reason",
      });
    }
    if (
      typeof row.content_arabic !== "string" &&
      row.content_arabic.titleArabic !== undefined &&
      row.content_arabic.titleArabic.normalize("NFC") !==
        row.name_arabic.normalize("NFC")
    ) {
      context.addIssue({
        code: "custom",
        message: "source-bound content title must match poem title",
      });
    }
    if (
      row.translation_missing === undefined &&
      (!("translation" in row) || !("translation_gemini" in row))
    ) {
      context.addIssue({
        code: "custom",
        message: "translation availability is required",
      });
    }
    if (row.insights_missing === undefined && !("insights" in row)) {
      context.addIssue({
        code: "custom",
        message: "insight availability is required",
      });
    }
  });

export interface BaselineOrphan {
  readonly canonicalSourceId: string;
  readonly poemId: string;
  readonly reason: "author_id_null" | "author_missing_from_export";
}

export interface BaselineIneligiblePoem {
  readonly canonicalSourceId: string;
  readonly poemId: string;
  readonly reason: z.infer<typeof IneligibleReasonSchema>;
  readonly slug: string;
}

export interface CertifiedOrphanRepair {
  readonly authorId: string;
  readonly canonicalAuthorId: string;
  readonly canonicalSourceId: string;
  readonly poemId: string;
}

export interface OrphanRepairPlan {
  readonly repairs: readonly CertifiedOrphanRepair[];
  readonly unresolved: readonly Readonly<{
    canonicalSourceId: string;
    poemId: string;
    reason: "certified_manifest_missing" | "source_author_identity_missing";
  }>[];
}

export interface ProductionBaselinePlan {
  readonly canonicalPoemIds: ReadonlySet<string>;
  readonly report: ProductionBaselineReport;
}

export interface ProductionBaselineReport {
  readonly authors: number;
  readonly batchSize: number;
  readonly currentProfileWork: number;
  readonly duplicateEnrichmentWork: number;
  /** @deprecated Use duplicateEnrichmentWork. */
  readonly duplicateSolWork: number;
  readonly eligiblePoems: number;
  readonly ineligible: readonly BaselineIneligiblePoem[];
  readonly ineligiblePoems: number;
  readonly missingInsights: number;
  readonly missingProfileWork: number;
  readonly noTranslation: number;
  readonly orphans: readonly BaselineOrphan[];
  readonly planHash: string;
  readonly poems: number;
  readonly seededEnrichmentWork: number;
  /** @deprecated Use seededEnrichmentWork. */
  readonly seededSolWork: number;
  readonly skippedComplete: number;
}

export interface ProductionBaselineOptions {
  readonly authors: AsyncIterable<unknown>;
  readonly batchSize?: number;
  readonly enrichmentProfiles?: readonly {
    readonly implementationVersion: string;
    readonly kind: string;
    readonly modelKey: string;
  }[];
  readonly ledger: Ledger;
  readonly poems: AsyncIterable<unknown>;
}

export async function planProductionBaseline(
  options: ProductionBaselineOptions,
): Promise<ProductionBaselinePlan> {
  const batchSize = options.batchSize ?? 500;
  const enrichmentProfiles = options.enrichmentProfiles ?? [
    {
      implementationVersion: SOL_PIPELINE_VERSION,
      kind: SOL_ENRICHMENT_WORK_KIND,
      modelKey: "sol-5.6",
    },
  ];
  const [requiredProfile] = enrichmentProfiles;
  if (
    enrichmentProfiles.length !== 1 ||
    requiredProfile?.implementationVersion !== SOL_PIPELINE_VERSION ||
    requiredProfile.kind !== SOL_ENRICHMENT_WORK_KIND ||
    requiredProfile.modelKey !== "sol-5.6"
  ) {
    throw new Error("BASELINE_CODEX_PROFILE_REQUIRED");
  }
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 10_000) {
    throw new Error("BASELINE_BATCH_SIZE_INVALID");
  }
  const authors = new Map<string, z.infer<typeof AuthorRowSchema>>();
  const authorHashes: string[] = [];
  let authorCount = 0;
  for await (const value of options.authors) {
    if (++authorCount > 50_000) throw new Error("BASELINE_AUTHOR_LIMIT");
    const author = AuthorRowSchema.parse(value);
    if (authors.has(author.id)) throw new Error("BASELINE_AUTHOR_DUPLICATE");
    authors.set(author.id, author);
    authorHashes.push(sha256(canonicalJson(author)));
  }

  const canonicalPoemIds = new Set<string>();
  const rowHashes: string[] = [];
  const orphans: BaselineOrphan[] = [];
  const ineligible: BaselineIneligiblePoem[] = [];
  let duplicateEnrichmentWork = 0;
  let currentProfileWork = 0;
  let missingInsights = 0;
  let noTranslation = 0;
  let missingProfileWork = 0;
  let poemCount = 0;
  let seededEnrichmentWork = 0;
  let skippedComplete = 0;
  let batch: WorkDefinition[] = [];

  const flush = (): void => {
    if (batch.length === 0) return;
    const results = options.ledger.seedMany(batch);
    seededEnrichmentWork += results.filter(({ inserted }) => inserted).length;
    duplicateEnrichmentWork += results.filter(
      ({ inserted }) => !inserted,
    ).length;
    batch = [];
  };

  for await (const value of options.poems) {
    if (++poemCount > MAX_PRODUCTION_ROWS)
      throw new Error("BASELINE_POEM_LIMIT");
    const poem = PoemRowSchema.parse(value);
    const canonicalSourceId = canonicalProductionPoemId(poem);
    const sourcePoemId = isSourceBoundProductionPoem(poem)
      ? null
      : sourcePoemIdFromLegacySlug(poem.slug);
    if (canonicalPoemIds.has(canonicalSourceId))
      throw new Error("BASELINE_POEM_SOURCE_ID_DUPLICATE");
    canonicalPoemIds.add(canonicalSourceId);
    rowHashes.push(sha256(canonicalJson(poem)));

    let author: undefined | z.infer<typeof AuthorRowSchema>;
    if (poem.author_id === null) {
      orphans.push({
        canonicalSourceId,
        poemId: poem.id,
        reason: "author_id_null",
      });
    } else {
      author = authors.get(poem.author_id);
    }
    if (poem.author_id !== null && !author) {
      orphans.push({
        canonicalSourceId,
        poemId: poem.id,
        reason: "author_missing_from_export",
      });
    }
    const linesArabic = parseContent(poem.content_arabic);
    const titleArabic = baselineTitleArabic(poem.name_arabic, linesArabic);
    const ineligibleReason = baselineIneligibleReason(
      titleArabic,
      linesArabic,
      author?.name_arabic ?? null,
    );
    if (ineligibleReason !== null) {
      if (poem.ineligible_reason !== ineligibleReason)
        throw new Error("BASELINE_INELIGIBLE_REASON_INVALID");
      ineligible.push({
        canonicalSourceId,
        poemId: poem.id,
        reason: ineligibleReason,
        slug: poem.slug,
      });
      continue;
    }
    if (poem.ineligible_reason !== undefined)
      throw new Error("BASELINE_INELIGIBLE_REASON_INVALID");
    if (!author) continue;
    const needsTranslation =
      poem.translation_missing ??
      (poem.translation === null && poem.translation_gemini === null);
    const needsInsights = poem.insights_missing ?? poem.insights === null;
    if (!needsTranslation && !needsInsights) skippedComplete += 1;
    if (needsTranslation) noTranslation += 1;
    else if (needsInsights) missingInsights += 1;
    const sourceContentSha256 = sha256(canonicalJson(linesArabic));
    const sourceRevisionContentHash = sha256(
      canonicalJson({ content: linesArabic, titleArabic }),
    );
    let sourceRevisionId = poem.active_source_revision_id;
    if (sourceRevisionId === null || sourceRevisionId === undefined) {
      if (sourcePoemId === null)
        throw new Error("BASELINE_SOURCE_BOUND_REVISION_REQUIRED");
      sourceRevisionId = canonicalSourceRevisionId(
        currentSource().name,
        sourcePoemId,
        CORPUS_REVISION_SCHEMA_VERSION,
        sourceRevisionContentHash,
      );
    }
    const input: PoemEnrichmentInput = PoemEnrichmentInputSchema.parse({
      authorArabic: author.name_arabic,
      linesArabic,
      poemId: poem.id,
      schemaId: ENRICHMENT_INPUT_SCHEMA_ID,
      schemaVersion: ENRICHMENT_INPUT_SCHEMA_VERSION,
      sourceContentSha256,
      sourceRevisionId,
      titleArabic,
    });
    const currentPublications = new Map(
      (poem.model_publications ?? []).map((publication) => [
        publication.model_key,
        publication,
      ]),
    );
    const missingProfiles = enrichmentProfiles.filter((profile) => {
      const publication = currentPublications.get(profile.modelKey);
      return !(
        publication?.source_revision_id === sourceRevisionId &&
        READABLE_ENRICHMENT_PROFILES.some(
          (readableProfile) =>
            readableProfile.modelKey === profile.modelKey &&
            readableProfile.promptVersion === publication.prompt_version,
        )
      );
    });
    currentProfileWork += enrichmentProfiles.length - missingProfiles.length;
    missingProfileWork += missingProfiles.length;
    if (missingProfiles.length === 0) continue;
    const basePriority = needsTranslation
      ? BASELINE_NO_TRANSLATION_PRIORITY
      : needsInsights
        ? BASELINE_MISSING_INSIGHTS_PRIORITY
        : BASELINE_NO_TRANSLATION_PRIORITY;
    const priority = basePriority + poemLengthPriorityBonus(linesArabic.length);
    batch.push(
      ...missingProfiles.map((profile) =>
        enrichmentDefinition(input, priority, profile),
      ),
    );
    if (batch.length >= batchSize) flush();
  }
  flush();
  const sortedRowHashes = rowHashes.toSorted();
  const sortedAuthorHashes = authorHashes.toSorted();
  const planHash = sha256(
    canonicalJson({
      authorHashes: sortedAuthorHashes,
      enrichmentProfiles: enrichmentProfiles.toSorted((left, right) =>
        left.modelKey.localeCompare(right.modelKey),
      ),
      rowHashes: sortedRowHashes,
      schema: "saqi.production-baseline-plan@2",
    }),
  );
  return {
    canonicalPoemIds,
    report: {
      authors: authors.size,
      batchSize,
      duplicateEnrichmentWork,
      duplicateSolWork: duplicateEnrichmentWork,
      eligiblePoems: poemCount - ineligible.length,
      ineligible,
      ineligiblePoems: ineligible.length,
      missingInsights,
      missingProfileWork,
      noTranslation,
      orphans,
      planHash,
      poems: poemCount,
      seededEnrichmentWork,
      seededSolWork: seededEnrichmentWork,
      currentProfileWork,
      skippedComplete,
    },
  };
}

export function poemLengthPriorityBonus(lineCount: number): number {
  if (lineCount <= 4) return BASELINE_SHORTEST_POEM_BONUS;
  if (lineCount <= 8) return 60;
  if (lineCount <= 16) return 50;
  if (lineCount <= 32) return 40;
  if (lineCount <= 64) return 30;
  if (lineCount <= 128) return 20;
  return 10;
}

export async function planProductionBaselineFiles(options: {
  readonly authorsPath: string;
  readonly batchSize?: number;
  readonly enrichmentProfiles?: ProductionBaselineOptions["enrichmentProfiles"];
  readonly ledger: Ledger;
  readonly poemsPath: string;
}): Promise<ProductionBaselinePlan> {
  return planProductionBaseline({
    authors: readNdjson(options.authorsPath),
    ...(options.batchSize === undefined
      ? {}
      : { batchSize: options.batchSize }),
    ledger: options.ledger,
    poems: readNdjson(options.poemsPath),
    ...(options.enrichmentProfiles === undefined
      ? {}
      : { enrichmentProfiles: options.enrichmentProfiles }),
  });
}

export async function* readNdjson(path: string): AsyncGenerator {
  const lines = createInterface({
    crlfDelay: Infinity,
    input: createReadStream(path, { encoding: "utf8" }),
  });
  let lineNumber = 0;
  for await (const line of lines) {
    lineNumber += 1;
    if (Buffer.byteLength(line) > MAX_NDJSON_LINE_BYTES)
      throw new Error(`NDJSON_LINE_TOO_LARGE:${String(lineNumber)}`);
    if (line.trim().length === 0) continue;
    try {
      const parsed: unknown = JSON.parse(line);
      yield parsed;
    } catch {
      throw new Error(`NDJSON_INVALID:${String(lineNumber)}`);
    }
  }
}

export interface DetailDeltaReport {
  readonly alreadyInProduction: number;
  readonly duplicateManifestIds: number;
  readonly seeded: number;
  readonly verifiedLedgerArtifacts: number;
}

export interface ProductionDetailRecoveryReport {
  readonly activeExisting: number;
  readonly applied: boolean;
  readonly candidateWork: number;
  readonly completedNeedsRefresh: number;
  readonly duplicateWork: null | number;
  readonly identityConflicts: null | number;
  readonly newWork: number;
  readonly priorityRaiseCandidates: number;
  readonly refreshedExisting: number;
  readonly refreshedSeeded: number;
  readonly requested: number;
  readonly seeded: number;
  readonly terminalExisting: number;
  readonly unresolvedPoemIds: readonly string[];
}

const RecoveryDetailInputSchema = z.object({
  authorHref: z.url(),
  authorNameArabic: z.string().trim().min(1).max(512).optional(),
  poemHref: z.url(),
  refreshGeneration: z.string().optional(),
});
const ACTIVE_WORK_STATES: ReadonlySet<string> = new Set([
  "pending",
  "quota_wait",
  "retry_wait",
  "running",
]);
export const PRODUCTION_DETAIL_RECOVERY_PRIORITY = 1_000;

/** Seeds source verification for a bounded set of legacy production poems.
 *
 * The baseline's source-derived slugs are discovery evidence only. Publication
 * still requires a freshly captured detail artifact and production admission,
 * so this cannot promote legacy model input into authoritative source text.
 */
export async function seedProductionDetailRecovery(options: {
  readonly apply?: boolean;
  readonly authors: AsyncIterable<unknown>;
  readonly ledger: Ledger;
  readonly manifestHash?: string;
  readonly now?: () => number;
  readonly poemIds: ReadonlySet<string>;
  readonly poems: AsyncIterable<unknown>;
  readonly priority?: number;
}): Promise<ProductionDetailRecoveryReport> {
  const requestedIds = new Set(
    [...options.poemIds].map((poemId) => CanonicalIdSchema.parse(poemId)),
  );
  const authors = new Map<string, z.infer<typeof AuthorRowSchema>>();
  for await (const value of options.authors) {
    const author = AuthorRowSchema.parse(value);
    if (authors.has(author.id)) throw new Error("BASELINE_AUTHOR_DUPLICATE");
    authors.set(author.id, author);
  }
  const priority = RecoveryPrioritySchema.parse(
    options.priority ?? PRODUCTION_DETAIL_RECOVERY_PRIORITY,
  );
  const generation = `legacy-sol-${BaselineManifestHashSchema.parse(
    options.manifestHash ?? "0".repeat(64),
  ).slice(0, 40)}`;
  const existingByPoemHref = new Map<
    string,
    ReturnType<Ledger["listWorkDefinitionsAfter"]>["items"]
  >();
  let cursor: null | string = null;
  do {
    const scan = options.ledger.listWorkDefinitionsAfter(
      cursor,
      collectionWorkKinds().poemDetail,
      1_000,
      {
        implementationVersion: collectorImplementationVersion(),
        schemaVersion: collectorSchemaVersion(),
      },
    );
    for (const work of scan.items) {
      const input = RecoveryDetailInputSchema.safeParse(work.input);
      if (!input.success) continue;
      const rows = existingByPoemHref.get(input.data.poemHref) ?? [];
      existingByPoemHref.set(input.data.poemHref, [...rows, work]);
    }
    cursor = scan.cursor;
    if (scan.done) break;
  } while (cursor !== null);
  const definitions: {
    definition: WorkDefinition;
    priorityOnly: boolean;
    refresh: boolean;
  }[] = [];
  const found = new Set<string>();
  const planned = new Set<string>();
  const plannedAuthors = new Map<
    string,
    Readonly<{ authorNameArabic: string; refreshGeneration: string }>
  >();
  let activeExisting = 0;
  let completedNeedsRefresh = 0;
  let newWork = 0;
  let priorityRaiseCandidates = 0;
  let refreshedExisting = 0;
  let terminalExisting = 0;
  for await (const value of options.poems) {
    const poem = PoemRowSchema.parse(value);
    if (!requestedIds.has(poem.id)) continue;
    found.add(poem.id);
    if (poem.author_id === null) continue;
    const author = authors.get(poem.author_id);
    if (!author) continue;
    const identity = {
      authorHref: sourceAuthorUrl(author.slug).href,
      poemHref: sourcePoemUrl(sourcePoemIdFromSlug(poem.slug)).href,
    };
    plannedAuthors.set(identity.authorHref, {
      authorNameArabic: author.name_arabic,
      refreshGeneration: generation,
    });
    planned.add(poem.id);
    const existing = existingByPoemHref.get(identity.poemHref) ?? [];
    const currentGeneration = existing.find(
      (work) =>
        RecoveryDetailInputSchema.safeParse(work.input).data
          ?.refreshGeneration === generation,
    );
    if (currentGeneration) {
      if (ACTIVE_WORK_STATES.has(currentGeneration.state)) {
        activeExisting += 1;
        priorityRaiseCandidates += 1;
        definitions.push({
          definition: {
            implementationVersion: currentGeneration.implementationVersion,
            input: currentGeneration.input,
            inputHash: currentGeneration.inputHash,
            kind: currentGeneration.kind,
            priority,
            schemaVersion: currentGeneration.schemaVersion,
          },
          priorityOnly: true,
          refresh: false,
        });
      } else if (["succeeded", "imported"].includes(currentGeneration.state))
        refreshedExisting += 1;
      else terminalExisting += 1;
      continue;
    }
    const active = existing.find((work) => ACTIVE_WORK_STATES.has(work.state));
    if (active) {
      activeExisting += 1;
      priorityRaiseCandidates += 1;
      definitions.push({
        definition: {
          implementationVersion: active.implementationVersion,
          input: active.input,
          inputHash: active.inputHash,
          kind: active.kind,
          priority,
          schemaVersion: active.schemaVersion,
        },
        priorityOnly: true,
        refresh: false,
      });
      continue;
    }
    const refresh = existing.some((work) =>
      ["succeeded", "imported", "dead_letter"].includes(work.state),
    );
    if (existing.some((work) => ["succeeded", "imported"].includes(work.state)))
      completedNeedsRefresh += 1;
    else if (existing.some((work) => work.state === "dead_letter"))
      terminalExisting += 1;
    else newWork += 1;
    const input = {
      ...identity,
      authorNameArabic: author.name_arabic,
      refreshGeneration: generation,
    };
    definitions.push({
      definition: {
        implementationVersion: collectorImplementationVersion(),
        input,
        inputHash: inputHash(input),
        kind: collectionWorkKinds().poemDetail,
        priority,
        schemaVersion: collectorSchemaVersion(),
      },
      priorityOnly: false,
      refresh,
    });
  }
  const unresolvedPoemIds = [...requestedIds]
    .filter((poemId) => !found.has(poemId) || !planned.has(poemId))
    .toSorted();
  if (options.apply === false) {
    return {
      applied: false,
      activeExisting,
      candidateWork: definitions.length,
      completedNeedsRefresh,
      duplicateWork: null,
      identityConflicts: null,
      newWork,
      priorityRaiseCandidates,
      refreshedExisting,
      refreshedSeeded: 0,
      requested: requestedIds.size,
      seeded: 0,
      terminalExisting,
      unresolvedPoemIds,
    };
  }
  const mutationAt = options.now?.() ?? Date.now();
  const results = options.ledger.seedMany(
    definitions.map(({ definition }) => definition),
    mutationAt,
  );
  for (const [authorHref, metadata] of plannedAuthors) {
    options.ledger.recordSourceAuthorMetadata(
      authorHref,
      metadata.authorNameArabic,
      metadata.refreshGeneration,
      mutationAt,
    );
  }
  const seeded = results.filter(({ inserted }) => inserted).length;
  const refreshedSeeded = results.filter(
    ({ inserted }, index) => inserted && definitions[index]?.refresh,
  ).length;
  return {
    activeExisting,
    applied: true,
    candidateWork: definitions.length,
    completedNeedsRefresh,
    duplicateWork: results.length - seeded,
    identityConflicts: 0,
    newWork,
    priorityRaiseCandidates,
    refreshedExisting,
    refreshedSeeded,
    requested: requestedIds.size,
    seeded,
    terminalExisting,
    unresolvedPoemIds,
  };
}

const ProductionDetailRecoveryManifestSchema = z
  .strictObject({
    authorsSha256: z.string().regex(/^[a-f\d]{64}$/),
    manifestHash: z.string().regex(/^[a-f\d]{64}$/),
    poemIds: z.array(z.uuid()).min(1).max(10_000),
    poemsSha256: z.string().regex(/^[a-f\d]{64}$/),
    schemaId: z.literal("saqi.production-detail-recovery-manifest"),
    schemaVersion: z.literal(2),
  })
  .superRefine((manifest, context) => {
    if (new Set(manifest.poemIds).size !== manifest.poemIds.length) {
      context.addIssue({ code: "custom", message: "poem IDs must be unique" });
    }
    if (
      !manifest.poemIds.every((id, index, ids) => {
        const previous = ids[index - 1];
        return previous === undefined || previous < id;
      })
    ) {
      context.addIssue({ code: "custom", message: "poem IDs must be sorted" });
    }
    const body = {
      authorsSha256: manifest.authorsSha256,
      poemIds: manifest.poemIds,
      poemsSha256: manifest.poemsSha256,
      schemaId: manifest.schemaId,
      schemaVersion: manifest.schemaVersion,
    };
    if (sha256(canonicalJson(body)) !== manifest.manifestHash) {
      context.addIssue({ code: "custom", message: "manifest hash mismatch" });
    }
  });

export function parseProductionDetailRecoveryManifest(input: unknown) {
  return ProductionDetailRecoveryManifestSchema.parse(input);
}

export async function seedProductionDetailRecoveryFiles(options: {
  readonly apply?: boolean;
  readonly authorsPath: string;
  readonly ledger: Ledger;
  readonly manifest: unknown;
  readonly poemsPath: string;
  readonly priority?: number;
}): Promise<ProductionDetailRecoveryReport> {
  const manifest = await verifyProductionDetailRecoveryFiles(options);
  return seedProductionDetailRecovery({
    ...(options.apply === undefined ? {} : { apply: options.apply }),
    authors: readNdjson(options.authorsPath),
    ledger: options.ledger,
    manifestHash: manifest.manifestHash,
    poemIds: new Set(manifest.poemIds),
    poems: readNdjson(options.poemsPath),
    ...(options.priority === undefined ? {} : { priority: options.priority }),
  });
}

export async function verifyProductionDetailRecoveryFiles(options: {
  readonly authorsPath: string;
  readonly manifest: unknown;
  readonly poemsPath: string;
}): Promise<ReturnType<typeof parseProductionDetailRecoveryManifest>> {
  const manifest = parseProductionDetailRecoveryManifest(options.manifest);
  const [authorsSha256, poemsSha256] = await Promise.all([
    sha256File(options.authorsPath),
    sha256File(options.poemsPath),
  ]);
  if (authorsSha256 !== manifest.authorsSha256)
    throw new Error("RECOVERY_AUTHORS_HASH_MISMATCH");
  if (poemsSha256 !== manifest.poemsSha256)
    throw new Error("RECOVERY_POEMS_HASH_MISMATCH");
  return manifest;
}

async function sha256File(path: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const rawChunk of createReadStream(path)) {
    const chunk: unknown = rawChunk;
    hash.update(FileChunkSchema.parse(chunk));
  }
  return hash.digest("hex");
}

export async function seedCertifiedDetailDelta(options: {
  readonly artifacts: ArtifactStore;
  readonly ledger: Ledger;
  readonly manifests: readonly AuthorPoemManifest[];
  readonly priority?: number;
  readonly productionCanonicalIds: ReadonlySet<string>;
}): Promise<DetailDeltaReport> {
  const seen = new Map<string, string>();
  const definitions: WorkDefinition[] = [];
  let alreadyInProduction = 0;
  let duplicateManifestIds = 0;
  let verifiedLedgerArtifacts = 0;
  for (const manifest of options.manifests) {
    const author = canonicalAuthorUrl(manifest.author.href);
    for (const poemRecord of manifest.poems) {
      const poem = canonicalPoemUrl(poemRecord.href);
      const priorAuthor = seen.get(poem.canonicalId);
      if (priorAuthor) {
        if (priorAuthor !== author.canonicalId)
          throw new Error("BASELINE_MANIFEST_POEM_AUTHOR_CONFLICT");
        duplicateManifestIds += 1;
        continue;
      }
      seen.set(poem.canonicalId, author.canonicalId);
      if (options.productionCanonicalIds.has(poem.canonicalId)) {
        alreadyInProduction += 1;
        continue;
      }
      const input = { authorHref: author.href, poemHref: poem.href };
      const priorArtifact = options.ledger.successfulArtifactForInput(
        collectionWorkKinds().poemDetail,
        collectorSchemaVersion(),
        inputHash(input),
      );
      let priorVerification: Awaited<
        ReturnType<ArtifactStore["verify"]>
      > | null = null;
      if (priorArtifact) {
        // eslint-disable-next-line no-await-in-loop -- Each manifest entry is verified before deciding whether its collection work is already durable.
        priorVerification = await options.artifacts.verify(priorArtifact);
      }
      if (priorVerification?.ok) {
        verifiedLedgerArtifacts += 1;
        continue;
      }
      definitions.push({
        implementationVersion: collectorImplementationVersion(),
        input,
        inputHash: inputHash(input),
        kind: collectionWorkKinds().poemDetail,
        priority: options.priority ?? 0,
        schemaVersion: collectorSchemaVersion(),
      });
    }
  }
  const results = options.ledger.seedMany(definitions);
  return {
    alreadyInProduction,
    duplicateManifestIds,
    seeded: results.filter(({ inserted }) => inserted).length,
    verifiedLedgerArtifacts,
  };
}

export function planCertifiedOrphanRepairs(options: {
  readonly manifests: readonly AuthorPoemManifest[];
  readonly orphans: readonly BaselineOrphan[];
  readonly sourceAuthorToProductionAuthorId: ReadonlyMap<string, string>;
}): OrphanRepairPlan {
  const poemAuthors = new Map<string, string>();
  for (const manifest of options.manifests) {
    const canonicalAuthorId = canonicalAuthorUrl(
      manifest.author.href,
    ).canonicalId;
    for (const poem of manifest.poems) {
      const canonicalSourceId = canonicalPoemUrl(poem.href).canonicalId;
      const existing = poemAuthors.get(canonicalSourceId);
      if (existing && existing !== canonicalAuthorId)
        throw new Error("BASELINE_ORPHAN_AUTHOR_EVIDENCE_CONFLICT");
      poemAuthors.set(canonicalSourceId, canonicalAuthorId);
    }
  }
  const repairs: CertifiedOrphanRepair[] = [];
  const unresolved: OrphanRepairPlan["unresolved"][number][] = [];
  for (const orphan of options.orphans) {
    const canonicalAuthorId = poemAuthors.get(orphan.canonicalSourceId);
    if (!canonicalAuthorId) {
      unresolved.push({
        canonicalSourceId: orphan.canonicalSourceId,
        poemId: orphan.poemId,
        reason: "certified_manifest_missing",
      });
      continue;
    }
    const authorId =
      options.sourceAuthorToProductionAuthorId.get(canonicalAuthorId);
    if (!authorId) {
      unresolved.push({
        canonicalSourceId: orphan.canonicalSourceId,
        poemId: orphan.poemId,
        reason: "source_author_identity_missing",
      });
      continue;
    }
    if (!CanonicalIdSchema.safeParse(authorId).success)
      throw new Error("BASELINE_ORPHAN_PRODUCTION_AUTHOR_ID_INVALID");
    repairs.push({
      authorId,
      canonicalAuthorId,
      canonicalSourceId: orphan.canonicalSourceId,
      poemId: orphan.poemId,
    });
  }
  return { repairs, unresolved };
}

function parseContent(
  value: z.infer<typeof PoemRowSchema>["content_arabic"],
): string[] {
  if (typeof value !== "string") return ContentSchema.parse(value).content;
  try {
    const parsed: unknown = JSON.parse(value);
    return ContentSchema.parse(parsed).content;
  } catch {
    throw new Error("BASELINE_CONTENT_ARABIC_INVALID");
  }
}

export function baselineTitleArabic(
  nameArabic: string,
  linesArabic: readonly string[],
): null | string {
  const title = nameArabic.trim();
  if (title.length > 0) return title;
  for (const line of linesArabic) {
    const candidate = line.trim().slice(0, 512).trim();
    if (candidate.length > 0 && /\p{Script=Arabic}/u.test(candidate))
      return candidate;
  }
  return null;
}

export function canonicalProductionPoemId(poem: {
  readonly id: string;
  readonly slug: string;
}): string {
  if (isSourceBoundProductionPoem(poem)) return `source-bound:${poem.id}`;
  if (
    SourceBoundPoemIdSchema.safeParse(poem.id).success ||
    poem.slug.startsWith("source-")
  ) {
    throw new Error("BASELINE_SOURCE_BOUND_IDENTITY_INVALID");
  }
  return canonicalPoemIdFromLegacySlug(poem.slug);
}

function isSourceBoundProductionPoem(poem: {
  readonly id: string;
  readonly slug: string;
}): boolean {
  return (
    SourceBoundPoemIdSchema.safeParse(poem.id).success &&
    poem.slug === `source-${poem.id}`
  );
}

export function baselineIneligibleReason(
  titleArabic: null | string,
  linesArabic: readonly string[],
  authorArabic: null | string,
): null | z.infer<typeof IneligibleReasonSchema> {
  const hasContent = linesArabic.some((line) => line.trim().length > 0);
  if (!hasContent)
    return titleArabic === null
      ? "missing_arabic_title_and_content"
      : "missing_arabic_content";
  if (titleArabic === null) return "missing_arabic_title";
  if (authorArabic === null || authorArabic.trim().length === 0)
    return "missing_arabic_author";
  const validation = PoemEnrichmentInputSchema.safeParse({
    authorArabic,
    linesArabic,
    poemId: "baseline-source-validation",
    schemaId: ENRICHMENT_INPUT_SCHEMA_ID,
    schemaVersion: ENRICHMENT_INPUT_SCHEMA_VERSION,
    sourceContentSha256: "0".repeat(64),
    sourceRevisionId: "0".repeat(64),
    titleArabic,
  });
  if (validation.success) return null;
  return validation.error.issues.some(({ message }) =>
    message.toLowerCase().includes("unsafe control characters"),
  )
    ? "unsafe_source_text"
    : "source_text_constraints";
}

function enrichmentDefinition(
  input: PoemEnrichmentInput,
  priority: number,
  profile: { readonly implementationVersion: string; readonly kind: string },
): WorkDefinition {
  return {
    implementationVersion: profile.implementationVersion,
    input,
    inputHash: inputHash(input),
    kind: profile.kind,
    priority,
    schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
  };
}

// Re-exported for schema-driven export documentation and tests.
export const ProductionBaselineAuthorRowSchema = AuthorRowSchema;
export const ProductionBaselineContentSchema = ContentSchema;
export const ProductionBaselinePoemIdSchema = ProductionPoemIdSchema;
export const ProductionBaselinePoemRowSchema = PoemRowSchema;
