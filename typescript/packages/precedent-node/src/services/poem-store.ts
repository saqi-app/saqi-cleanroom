import {
  AnyPoemEnrichmentOutputSchema,
  APPROVED_ENRICHMENT_PROFILES,
  approvedEnrichmentValidations,
  IdentityTextSchema,
  type LoadedPoem,
  type Poem,
  PoemContentSchema,
  type PoemCopy,
  PoemEnrichmentReviewSchema,
  type PoemModelEnrichment,
  readableEnrichmentProfile,
  reviewsAcceptEnrichment,
  RouteSegmentSchema,
  sitemapShardForId,
} from "@saqi/precedent-iso";
import { and, eq, inArray, sql } from "drizzle-orm";
import { z } from "zod";

import type { Database } from "./db-types";
import {
  ENRICHMENT_ARTIFACT_TABLE as enrichmentArtifact,
  ENRICHMENT_VALIDATION_TABLE as enrichmentValidation,
  POEM_MODEL_PUBLICATION_POINTER_TABLE as modelPublication,
  POEM_TABLE as poem,
} from "./schema";

export interface InsertPoem {
  authorId: string;
  content: string[];
  name: string;
  slug: string;
  verses: number;
}

export interface UntranslatedPoemBatch {
  failed: number;
  ids: string[];
  total: number;
}

export interface AdjacentPoems {
  next: null | string;
  prev: null | string;
}

export interface PoemScrapeSelection {
  slugs: Set<string>;
  total: number;
}

export interface PoemStore {
  getAdjacentPoems(poemId: string, authorId: string): Promise<AdjacentPoems>;
  getAdjacentPoemsBySlug(
    poemId: string,
    authorSlug: string,
  ): Promise<AdjacentPoems>;
  getArabicCopies(poemIds: string[]): Promise<Map<string, PoemCopy>>;
  getArabicCopy(poemId: string): Promise<PoemCopy>;
  getAuthorSlugForPoem(poemId: string): Promise<null | string>;
  getById(id: string): Promise<LoadedPoem>;
  getByIdOrNull(id: string): Promise<LoadedPoem | null>;
  getByIds(ids: string[]): Promise<Map<string, LoadedPoem>>;
  poemsForAuthor(authorId: string): Promise<Poem[]>;
  poemsToScrape(slugs: string[], limit: number): Promise<PoemScrapeSelection>;
  untranslatedPoemIdsForAuthor(
    authorId: string,
    limit: number,
  ): Promise<UntranslatedPoemBatch>;
  upsert(poem: InsertPoem): Promise<Poem>;
}

function rowToPoem(row: typeof poem.$inferSelect): Poem {
  const nameEnglish = row.poemTitleFirstLine ?? row.nameEnglish ?? undefined;

  return {
    id: row.id,
    slug: row.slug,
    authorId: row.authorId ?? "",
    verses: row.verses,
    nameArabic: row.nameArabic,
    nameEnglish,
  };
}

function rowToLoadedPoem(
  row: typeof poem.$inferSelect,
  modelEnrichments: readonly PoemModelEnrichment[] = [],
): LoadedPoem {
  const contentArabicParsed = PoemContentSchema.safeParse(row.contentArabic);
  const translationParsed = PoemContentSchema.safeParse(row.translation);
  const translationGeminiParsed = PoemContentSchema.safeParse(
    row.translationGemini,
  );
  const translationSolParsed = PoemContentSchema.safeParse(row.translationSol);
  const hasNormalizedSol = modelEnrichments.some(
    ({ modelKey }) => modelKey === "sol-5.6",
  );
  const allowLegacySol =
    row.activeSourceRevisionId === null && !hasNormalizedSol;
  const nameEnglish = row.poemTitleFirstLine ?? row.nameEnglish ?? undefined;

  return {
    id: row.id,
    slug: row.slug,
    authorId: row.authorId ?? "",
    verses: row.verses,
    nameArabic: row.nameArabic,
    nameEnglish,
    linesArabic: contentArabicParsed.success
      ? contentArabicParsed.data.content
      : [],
    linesEnglish: translationParsed.success
      ? translationParsed.data.content
      : undefined,
    linesEnglishGemini: translationGeminiParsed.success
      ? translationGeminiParsed.data.content
      : undefined,
    ...(allowLegacySol && translationSolParsed.success
      ? { linesEnglishSol: translationSolParsed.data.content }
      : {}),
    ...(modelEnrichments.length > 0
      ? { modelEnrichments: [...modelEnrichments] }
      : {}),
  };
}

interface ModelArtifactRow {
  id: string;
  model: string;
  modelKey: string;
  payload: unknown;
  poemId: string;
  promptVersion: string;
  reasoningEffort: string;
  schemaVersion: number;
}

interface ModelValidationRow {
  artifactId: string;
  attempt: number;
  highestSeverity: string;
  outcome: string;
  report: unknown;
  validatorKey: string;
  validatorVersion: string;
}

function parseJsonDocument(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try {
    const parsed: unknown = JSON.parse(value);
    return parsed;
  } catch {
    return undefined;
  }
}

function validationIdentity(input: {
  readonly attempt: number;
  readonly validatorKey: string;
  readonly validatorVersion: string;
}): string {
  return JSON.stringify([
    input.validatorKey,
    input.validatorVersion,
    input.attempt,
  ]);
}

function normalizedTablesUnavailable(error: unknown): boolean {
  const messages: string[] = [];
  let current: unknown = error;
  for (let depth = 0; depth < 5 && current instanceof Error; depth += 1) {
    messages.push(current.message);
    current = current.cause;
  }
  return messages.some((message) =>
    /no such (?:table:\s*|column:\s*["`']?)(?:model_enrichment_artifact|model_enrichment_validation|poem_model_publication_pointer)(?:["`']?|\.)/iu.test(
      message,
    ),
  );
}

const BATCH_SIZE = 50;
const READ_BATCH_CONCURRENCY = 4;
const SqliteCountSchema = z.number().int().nonnegative();

const UntranslatedPoemBatchRowsSchema = z
  .array(
    z.strictObject({
      failed: SqliteCountSchema,
      id: z.string().nullable(),
      total: SqliteCountSchema,
    }),
  )
  .min(1)
  .max(500)
  .transform((rows): UntranslatedPoemBatch => {
    const first = firstRow(rows);
    return {
      failed: first.failed,
      ids: rows.flatMap(({ id }) => (id ? [id] : [])),
      total: first.total,
    };
  });

const AdjacentPoemRowsSchema = z
  .array(
    z.strictObject({
      next_id: z.string().nullable(),
      prev_id: z.string().nullable(),
    }),
  )
  .max(1)
  .transform(([row]): AdjacentPoems => ({
    next: row?.next_id ?? null,
    prev: row?.prev_id ?? null,
  }));

const PoemScrapeSelectionRowsSchema = z
  .array(
    z.strictObject({
      slug: z.string().nullable(),
      total: SqliteCountSchema,
    }),
  )
  .min(1)
  .max(1200)
  .transform((rows): PoemScrapeSelection => ({
    slugs: new Set(rows.flatMap(({ slug }) => (slug ? [slug] : []))),
    total: firstRow(rows).total,
  }));

function firstRow<T>(rows: readonly T[]): T {
  const [row] = rows;
  if (row === undefined) {
    throw new TypeError("Expected at least one database result row");
  }
  return row;
}

function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

const PoemAuthorSlugRowSchema = z
  .object({ author_slug: z.string().nullish() })
  .nullish();

export class D1PoemStore implements PoemStore {
  readonly #db: Database;
  constructor(db: Database) {
    this.#db = db;
  }

  async getAuthorSlugForPoem(poemId: string): Promise<null | string> {
    const row = await this.#db.get(sql`
      SELECT author.slug AS author_slug
      FROM poem JOIN author ON author.id = poem.author_id
      WHERE poem.id = ${poemId}
    `);
    return PoemAuthorSlugRowSchema.parse(row)?.author_slug || null;
  }

  async getById(id: string): Promise<LoadedPoem> {
    const loaded = await this.getByIdOrNull(id);
    if (!loaded) {
      throw new Error(`Poem not found: ${id}`);
    }
    return loaded;
  }

  async getByIdOrNull(id: string): Promise<LoadedPoem | null> {
    const row = await this.#db.select().from(poem).where(eq(poem.id, id)).get();
    if (!row) return null;
    const enrichments = await this.#modelEnrichments([id]);
    return rowToLoadedPoem(row, enrichments.get(id));
  }

  async getByIds(ids: string[]): Promise<Map<string, LoadedPoem>> {
    if (ids.length === 0) {
      return new Map();
    }

    const loadedById = new Map<string, LoadedPoem>();
    const uniqueIds: string[] = [];
    const seenIds = new Set<string>();
    for (const id of ids) {
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      uniqueIds.push(id);
    }
    const chunks = chunkArray(uniqueIds, BATCH_SIZE);
    const loadChunkWave = async (offset: number): Promise<void> => {
      const rowBatches = await Promise.all(
        chunks
          .slice(offset, offset + READ_BATCH_CONCURRENCY)
          .map(async (chunk) => {
            const rows = await this.#db
              .select()
              .from(poem)
              .where(inArray(poem.id, chunk))
              .all();
            const enrichments = await this.#modelEnrichments(
              rows.map(({ id }) => id),
            );
            return { enrichments, rows };
          }),
      );
      for (const { enrichments, rows } of rowBatches) {
        for (const row of rows) {
          loadedById.set(row.id, rowToLoadedPoem(row, enrichments.get(row.id)));
        }
      }
      const nextOffset = offset + READ_BATCH_CONCURRENCY;
      if (nextOffset < chunks.length) await loadChunkWave(nextOffset);
    };
    await loadChunkWave(0);

    // SQL IN predicates do not guarantee row order. Preserve first-requested
    // order so callers can safely use Map iteration for stable pagination.
    const result = new Map<string, LoadedPoem>();
    for (const id of ids) {
      const loaded = loadedById.get(id);
      if (loaded) result.set(id, loaded);
    }
    return result;
  }

  async poemsForAuthor(authorId: string): Promise<Poem[]> {
    const rows = await this.#db
      .select({
        id: poem.id,
        slug: poem.slug,
        authorId: poem.authorId,
        verses: poem.verses,
        nameArabic: poem.nameArabic,
        nameEnglish: poem.nameEnglish,
        poemTitleFirstLine: poem.poemTitleFirstLine,
      })
      .from(poem)
      .where(eq(poem.authorId, authorId))
      .orderBy(
        sql`CASE WHEN poem_title_first_line IS NOT NULL OR name_english IS NOT NULL THEN 0 ELSE 1 END`,
        poem.nameArabic,
      )
      .all();

    return rows.map((row) => ({
      id: row.id,
      slug: row.slug,
      authorId: row.authorId ?? "",
      verses: row.verses,
      nameArabic: row.nameArabic,
      nameEnglish: row.poemTitleFirstLine ?? row.nameEnglish ?? undefined,
    }));
  }

  async untranslatedPoemIdsForAuthor(
    authorId: string,
    limit: number,
  ): Promise<UntranslatedPoemBatch> {
    const boundedLimit = Math.min(Math.max(limit, 1), 500);
    const rows = await this.#db.all(sql`
      WITH untranslated AS (
        SELECT poem.id
        FROM poem
        WHERE poem.author_id = ${authorId}
          AND NOT (
          CASE WHEN json_valid(poem.translation_gemini)
            THEN json_type(poem.translation_gemini, '$.content') = 'array'
              AND json_array_length(poem.translation_gemini, '$.content') > 0
              AND NOT EXISTS (
                SELECT 1
                FROM json_each(poem.translation_gemini, '$.content') line
                WHERE line.type <> 'text'
              )
              AND EXISTS (
                SELECT 1
                FROM json_each(poem.translation_gemini, '$.content') line
                WHERE line.type = 'text' AND trim(line.value) <> ''
              )
            ELSE 0
          END
          )
      ), uncovered AS (
        SELECT untranslated.id
        FROM untranslated
        WHERE NOT EXISTS (
          SELECT 1
          FROM task
          WHERE task.work_key = 'translate-poem:' || untranslated.id
            AND task.status IN ('pending', 'in_progress')
        )
      ), eligible_all AS (
        SELECT uncovered.id
        FROM uncovered
        WHERE NOT EXISTS (
          SELECT 1
          FROM task
          WHERE task.work_key = 'translate-poem:' || uncovered.id
            AND task.status = 'failed'
        )
      ), eligible AS (
        SELECT eligible_all.id
        FROM eligible_all
        ORDER BY eligible_all.id
        LIMIT ${boundedLimit}
      )
      SELECT eligible.id, totals.total, totals.failed
      FROM (
        SELECT
          (SELECT COUNT(*) FROM eligible_all) AS total,
          (SELECT COUNT(*) FROM uncovered) -
            (SELECT COUNT(*) FROM eligible_all) AS failed
      ) totals
      LEFT JOIN eligible ON 1 = 1
      ORDER BY eligible.id
    `);

    return UntranslatedPoemBatchRowsSchema.parse(rows);
  }

  async getAdjacentPoems(
    poemId: string,
    authorId: string,
  ): Promise<AdjacentPoems> {
    const result = await this.#db.all(sql`
      WITH ordered_poems AS (
        SELECT
          id,
          LAG(id) OVER (ORDER BY
            CASE WHEN poem_title_first_line IS NOT NULL OR name_english IS NOT NULL THEN 0 ELSE 1 END,
            name_arabic
          ) as prev_id,
          LEAD(id) OVER (ORDER BY
            CASE WHEN poem_title_first_line IS NOT NULL OR name_english IS NOT NULL THEN 0 ELSE 1 END,
            name_arabic
          ) as next_id
        FROM poem
        WHERE author_id = ${authorId}
      )
      SELECT prev_id, next_id FROM ordered_poems WHERE id = ${poemId}
    `);

    return AdjacentPoemRowsSchema.parse(result);
  }

  async getAdjacentPoemsBySlug(
    poemId: string,
    authorSlug: string,
  ): Promise<AdjacentPoems> {
    const result = await this.#db.all(sql`
      WITH ordered_poems AS (
        SELECT
          id,
          LAG(id) OVER (ORDER BY
            CASE WHEN poem_title_first_line IS NOT NULL OR name_english IS NOT NULL THEN 0 ELSE 1 END,
            name_arabic
          ) as prev_id,
          LEAD(id) OVER (ORDER BY
            CASE WHEN poem_title_first_line IS NOT NULL OR name_english IS NOT NULL THEN 0 ELSE 1 END,
            name_arabic
          ) as next_id
        FROM poem
        WHERE author_id = (SELECT id FROM author WHERE slug = ${authorSlug})
      )
      SELECT prev_id, next_id FROM ordered_poems WHERE id = ${poemId}
    `);

    return AdjacentPoemRowsSchema.parse(result);
  }

  async getArabicCopy(poemId: string): Promise<PoemCopy> {
    const row = await this.#db
      .select({
        id: poem.id,
        title: poem.nameArabic,
        content: poem.contentArabic,
      })
      .from(poem)
      .where(eq(poem.id, poemId))
      .get();

    if (!row) {
      throw new Error(`Poem not found: ${poemId}`);
    }

    const contentParsed = PoemContentSchema.safeParse(row.content);

    return {
      id: row.id,
      title: row.title,
      lines: contentParsed.success ? contentParsed.data.content : [],
    };
  }

  async getArabicCopies(poemIds: string[]): Promise<Map<string, PoemCopy>> {
    if (poemIds.length === 0) {
      return new Map();
    }

    const result = new Map<string, PoemCopy>();

    const rowBatches = await Promise.all(
      chunkArray(poemIds, BATCH_SIZE).map((chunk) =>
        this.#db
          .select({
            id: poem.id,
            title: poem.nameArabic,
            content: poem.contentArabic,
          })
          .from(poem)
          .where(inArray(poem.id, chunk))
          .all(),
      ),
    );
    for (const rows of rowBatches) {
      for (const row of rows) {
        const contentParsed = PoemContentSchema.safeParse(row.content);
        result.set(row.id, {
          id: row.id,
          title: row.title,
          lines: contentParsed.success ? contentParsed.data.content : [],
        });
      }
    }

    return result;
  }

  async upsert({ authorId, slug, name, content }: InsertPoem): Promise<Poem> {
    const poemId = crypto.randomUUID();
    const validatedAuthorId = IdentityTextSchema.parse(authorId);
    const validatedContent = PoemContentSchema.parse({ content }).content;
    const validatedName = IdentityTextSchema.parse(name);
    const validatedSlug = RouteSegmentSchema.parse(slug);
    const validatedVerses = Math.ceil(validatedContent.length / 2);

    const managed = await this.#db
      .select({ id: poem.id })
      .from(poem)
      .where(
        sql`${poem.slug} = ${validatedSlug} AND ${poem.activeSourceRevisionId} IS NOT NULL`,
      )
      .limit(1);
    if (managed.length > 0) {
      throw new Error("REVISION_MANAGED_POEM_REJECTS_LEGACY_UPSERT");
    }

    const rows = await this.#db
      .insert(poem)
      .values({
        id: poemId,
        authorId: validatedAuthorId,
        slug: validatedSlug,
        verses: validatedVerses,
        nameArabic: validatedName,
        sitemapShard: sitemapShardForId(poemId),
        contentArabic: { content: validatedContent },
      })
      .onConflictDoUpdate({
        target: poem.slug,
        setWhere: sql`${poem.activeSourceRevisionId} IS NULL`,
        set: {
          authorId: sql`excluded.author_id`,
          contentArabic: sql`excluded.content_arabic`,
          englishNameOriginalTranslation: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.englishNameOriginalTranslation} ELSE NULL END`,
          hasEnglish: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.hasEnglish} ELSE 0 END`,
          hasInsights: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.hasInsights} ELSE 0 END`,
          insights: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.insights} ELSE NULL END`,
          nameArabic: sql`excluded.name_arabic`,
          nameEnglish: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.nameEnglish} ELSE NULL END`,
          poemTitleFirstLine: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.poemTitleFirstLine} ELSE NULL END`,
          translation: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.translation} ELSE NULL END`,
          translationGemini: sql`CASE WHEN ${poem.contentArabic} = excluded.content_arabic THEN ${poem.translationGemini} ELSE NULL END`,
          verses: sql`excluded.verses`,
        },
      })
      .returning();

    const row = rows[0];
    if (!row) {
      const [revisionManaged] = await this.#db
        .select({ id: poem.id })
        .from(poem)
        .where(
          sql`${poem.slug} = ${validatedSlug} AND ${poem.activeSourceRevisionId} IS NOT NULL`,
        )
        .limit(1);
      if (revisionManaged) {
        throw new Error("REVISION_MANAGED_POEM_REJECTS_LEGACY_UPSERT");
      }
      throw new Error(`Poem not found after upsert: ${slug}`);
    }

    return rowToPoem(row);
  }

  async poemsToScrape(
    slugs: string[],
    limit: number,
  ): Promise<PoemScrapeSelection> {
    if (slugs.length === 0) {
      return { slugs: new Set(), total: 0 };
    }

    const boundedLimit = Math.min(Math.max(limit, 1), 1200);
    const rows = await this.#db.all(sql`
      WITH requested AS (
        SELECT CAST(value AS TEXT) AS slug, CAST(key AS INTEGER) AS position
        FROM json_each(${JSON.stringify(slugs)})
        WHERE type = 'text'
      ), unique_requested AS (
        SELECT slug, MIN(position) AS position
        FROM requested
        GROUP BY slug
      ), missing AS MATERIALIZED (
        SELECT requested.slug, requested.position
        FROM unique_requested requested
        WHERE NOT EXISTS (
          SELECT 1
          FROM poem
          WHERE poem.slug = requested.slug
        )
      ), selected AS (
        SELECT slug, position
        FROM missing
        ORDER BY position
        LIMIT ${boundedLimit}
      )
      SELECT selected.slug, totals.total
      FROM (SELECT COUNT(*) AS total FROM missing) totals
      LEFT JOIN selected ON 1 = 1
      ORDER BY selected.position
    `);

    return PoemScrapeSelectionRowsSchema.parse(rows);
  }

  async #modelEnrichments(
    poemIds: readonly string[],
  ): Promise<Map<string, PoemModelEnrichment[]>> {
    if (poemIds.length === 0) return new Map();
    let artifacts: ModelArtifactRow[];
    try {
      artifacts = await this.#db
        .select({
          id: enrichmentArtifact.id,
          model: enrichmentArtifact.model,
          modelKey: enrichmentArtifact.modelKey,
          // Selecting JSON-mode columns through raw SQL prevents the driver
          // decoder from throwing before one malformed document can be
          // isolated and rejected below.
          payload: sql<unknown>`${enrichmentArtifact.payload}`,
          poemId: modelPublication.poemId,
          promptVersion: enrichmentArtifact.promptVersion,
          reasoningEffort: enrichmentArtifact.reasoningEffort,
          schemaVersion: enrichmentArtifact.schemaVersion,
        })
        .from(modelPublication)
        .innerJoin(
          enrichmentArtifact,
          eq(enrichmentArtifact.id, modelPublication.enrichmentArtifactId),
        )
        .innerJoin(poem, eq(poem.id, modelPublication.poemId))
        .where(
          and(
            inArray(modelPublication.poemId, [...poemIds]),
            eq(modelPublication.modelKey, enrichmentArtifact.modelKey),
            eq(
              modelPublication.sourceRevisionId,
              enrichmentArtifact.sourceRevisionId,
            ),
            eq(modelPublication.sourceRevisionId, poem.activeSourceRevisionId),
          ),
        )
        .all();
    } catch (error) {
      if (normalizedTablesUnavailable(error)) return new Map();
      throw error;
    }
    if (artifacts.length === 0) return new Map();
    let validations: ModelValidationRow[];
    try {
      validations = await this.#db
        .select({
          artifactId: enrichmentValidation.artifactId,
          attempt: enrichmentValidation.attempt,
          highestSeverity: enrichmentValidation.highestSeverity,
          outcome: enrichmentValidation.outcome,
          report: sql<unknown>`${enrichmentValidation.report}`,
          validatorKey: enrichmentValidation.validatorKey,
          validatorVersion: enrichmentValidation.validatorVersion,
        })
        .from(enrichmentValidation)
        .innerJoin(
          enrichmentArtifact,
          eq(enrichmentArtifact.id, enrichmentValidation.artifactId),
        )
        .innerJoin(
          modelPublication,
          eq(modelPublication.enrichmentArtifactId, enrichmentArtifact.id),
        )
        .innerJoin(poem, eq(poem.id, modelPublication.poemId))
        .where(
          and(
            inArray(modelPublication.poemId, [...poemIds]),
            eq(modelPublication.modelKey, enrichmentArtifact.modelKey),
            eq(
              modelPublication.sourceRevisionId,
              enrichmentArtifact.sourceRevisionId,
            ),
            eq(modelPublication.sourceRevisionId, poem.activeSourceRevisionId),
          ),
        )
        .all();
    } catch (error) {
      if (normalizedTablesUnavailable(error)) return new Map();
      throw error;
    }
    const validationsByArtifact = new Map<string, ModelValidationRow[]>();
    for (const validation of validations) {
      const grouped = validationsByArtifact.get(validation.artifactId) ?? [];
      grouped.push(validation);
      validationsByArtifact.set(validation.artifactId, grouped);
    }
    const result = new Map<string, PoemModelEnrichment[]>();
    for (const artifact of artifacts) {
      const profile = readableEnrichmentProfile(artifact);
      if (profile?.modelKey !== artifact.modelKey) continue;
      const candidateValidations = validationsByArtifact.get(artifact.id) ?? [];
      if (
        candidateValidations.some(
          ({ highestSeverity, outcome }) =>
            outcome !== "pass" ||
            highestSeverity === "critical" ||
            highestSeverity === "major",
        )
      ) {
        continue;
      }
      const byIdentity = new Map(
        candidateValidations.map((validation) => [
          validationIdentity(validation),
          validation,
        ]),
      );
      const reviews = approvedEnrichmentValidations(profile).all.flatMap(
        (required) => {
          const validation = byIdentity.get(validationIdentity(required));
          if (!validation) return [];
          const review = PoemEnrichmentReviewSchema.safeParse(
            parseJsonDocument(validation.report),
          );
          return review.success ? [review.data] : [];
        },
      );
      if (!reviewsAcceptEnrichment(reviews)) continue;
      const payload = AnyPoemEnrichmentOutputSchema.safeParse(
        parseJsonDocument(artifact.payload),
      );
      if (!payload.success) continue;
      if (artifact.schemaVersion !== ("wordGlosses" in payload.data ? 2 : 1))
        continue;
      const enrichments = result.get(artifact.poemId) ?? [];
      enrichments.push({
        lines: payload.data.translation.lines,
        model: profile.model,
        modelKey: profile.modelKey,
        reasoningEffort: profile.reasoningEffort,
        ...("wordGlosses" in payload.data
          ? { wordGlosses: payload.data.wordGlosses }
          : { insights: payload.data.insights }),
      });
      result.set(artifact.poemId, enrichments);
    }
    const profileOrder: ReadonlyMap<string, number> = new Map(
      APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }, index) => [
        modelKey,
        index,
      ]),
    );
    for (const enrichments of result.values()) {
      enrichments.sort(
        (left, right) =>
          (profileOrder.get(left.modelKey) ?? Number.MAX_SAFE_INTEGER) -
          (profileOrder.get(right.modelKey) ?? Number.MAX_SAFE_INTEGER),
      );
    }
    return result;
  }
}
