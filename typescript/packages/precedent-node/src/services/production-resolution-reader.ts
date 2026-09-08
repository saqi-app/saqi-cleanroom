import {
  normalizeProductionResolutionRequest,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID,
  PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_SCHEMA_VERSION,
  type ProductionResolutionRequest,
  type ProductionResolutionResponse,
  type ProductionResolutionResponseBody,
  ProductionResolutionResponseBodySchema,
  ProductionResolutionResponseSchema,
  SourceNameSchema,
} from "@saqi/precedent-iso";
import {
  type SourceAdapterProfileV1,
  SourceAdapterProfileV1Schema,
} from "@saqi/source-adapter";
import { sql } from "drizzle-orm";
import { z } from "zod";

import type { Database } from "./db-types";

const DEFAULT_LIFETIME_MS = 15 * 60_000;
const ResolutionLifetimeMsSchema = z
  .number()
  .int()
  .min(60_000)
  .max(60 * 60_000);

const ResolutionRowSchema = z.strictObject({
  active_source_revision_id: z.string().nullable(),
  author_id: z.string().nullable(),
  author_name_arabic: z.string().nullable(),
  content_arabic: z.string().nullable(),
  identity_author_id: z.string().nullable(),
  identity_author_source_name: z.string().nullable(),
  identity_author_slug: z.string().nullable(),
  model_pointers: z.string(),
  poem_id: z.string().nullable(),
  requested_poem_id: z.string().nullable(),
  requested_source_revision_id: z.string().nullable(),
  requested_author_id: z.string().nullable(),
  source_author_slug: z.string().nullable(),
  source_identity_id: z.string().nullable(),
  source_identity_tombstoned_at: z.number().nullable(),
  source_pointer_revision_id: z.string().nullable(),
  source_pointer_version: z.number().int().positive().nullable(),
  source_poem_id: z.string().nullable(),
  source_revision_source_poem_id: z.string().nullable(),
  writer_epoch: z.number().int().positive(),
});

const FingerprintResolutionRowSchema = z.strictObject({
  active_candidate_count: z.number().int().nonnegative(),
  algorithm: z.string().nullable(),
  author_id: z.string().nullable(),
  author_name_arabic: z.string().nullable(),
  line_nfc_hash: z.string().nullable(),
  model_pointers: z.string().nullable(),
  poem_id: z.string().nullable(),
  prompt_material_hash: z.string().nullable(),
  raw_match_count: z.number().int().nonnegative(),
  source_author_slug: z.string().nullable(),
  source_poem_id: z.string().nullable(),
  source_pointer_version: z.number().int().positive().nullable(),
  source_revision_id: z.string().nullable(),
  writer_epoch: z.number().int().positive(),
});

const ModelPointersSchema = z.array(
  z.strictObject({
    modelKey: z.string(),
    pointerVersion: z.number().int().positive(),
  }),
);
const SourceContentDocumentSchema = z.looseObject({
  content: z.array(z.string()),
});

export class ProductionResolutionConflictError extends Error {}

export interface ProductionResolutionStore {
  resolve(input: unknown): Promise<ProductionResolutionResponse>;
}

export interface D1ProductionResolutionStoreOptions {
  readonly lifetimeMs?: number;
  readonly now?: () => number;
  readonly sourceName: string;
  readonly sourceProfile: SourceAdapterProfileV1;
}

/** Exact, bounded production identity resolution for progressive adoption. */
export class D1ProductionResolutionStore implements ProductionResolutionStore {
  readonly #db: Database;
  readonly #lifetimeMs: number;
  readonly #runtime: { readonly now: () => number };
  readonly #sourceName: string;
  readonly #sourcePoemSlugPrefix: string;
  readonly #sourcePoemSlugSuffix: string;

  constructor(db: Database, options: D1ProductionResolutionStoreOptions) {
    this.#db = db;
    this.#lifetimeMs = ResolutionLifetimeMsSchema.parse(
      options.lifetimeMs ?? DEFAULT_LIFETIME_MS,
    );
    this.#runtime = { now: options.now ?? Date.now };
    this.#sourceName = SourceNameSchema.parse(options.sourceName);
    const profile = SourceAdapterProfileV1Schema.parse(options.sourceProfile);
    const marker = "{id}";
    const markerIndex = profile.routes.poemSlug.indexOf(marker);
    this.#sourcePoemSlugPrefix = profile.routes.poemSlug.slice(0, markerIndex);
    this.#sourcePoemSlugSuffix = profile.routes.poemSlug.slice(
      markerIndex + marker.length,
    );
  }

  async resolve(input: unknown): Promise<ProductionResolutionResponse> {
    const request = normalizeProductionResolutionRequest(input);
    if (
      request.schemaVersion === PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION
    ) {
      return this.#resolveFingerprints(request);
    }
    const requestJson = JSON.stringify(request.targets);
    const rows = await this.#db.all(sql`
      WITH requested AS (
        SELECT
          CAST(key AS INTEGER) AS ordinal,
          json_extract(value, '$.poemId') AS poem_id,
          json_extract(value, '$.sourceRevisionId') AS source_revision_id,
          json_extract(value, '$.sourceAuthorSlug') AS source_author_slug,
          json_extract(value, '$.sourcePoemId') AS source_poem_id,
          json_extract(value, '$.modelKeys') AS model_keys
        FROM json_each(${requestJson})
      ), candidates AS (
        SELECT
          requested.ordinal,
          requested.source_author_slug,
          requested.source_poem_id,
          requested.poem_id AS requested_poem_id,
          requested.source_revision_id AS requested_source_revision_id,
          requested.model_keys,
          source_poem.id AS source_identity_id,
          source_poem.canonical_poem_id AS identity_poem_id,
          source_author.external_id AS identity_author_slug,
          source_author.source_name AS identity_author_source_name,
          source_author.canonical_author_id AS identity_author_id,
          source_poem.tombstoned_at AS source_identity_tombstoned_at,
          requested_author.canonical_author_id AS requested_author_id,
          legacy_poem.id AS legacy_poem_id
        FROM requested
        LEFT JOIN poem requested_poem
          ON requested_poem.id = requested.poem_id
        LEFT JOIN poem_source_revision requested_revision
          ON requested_revision.id = requested.source_revision_id
        LEFT JOIN poem_source_revision active_requested_revision
          ON active_requested_revision.id = requested_poem.active_source_revision_id
        LEFT JOIN source_poem_identity source_poem
          ON source_poem.source_name = ${this.#sourceName}
          AND (
            (requested.source_poem_id IS NOT NULL
              AND source_poem.external_id = requested.source_poem_id)
            OR (requested.poem_id IS NOT NULL
              AND source_poem.id = COALESCE(
                requested_revision.source_poem_id,
                active_requested_revision.source_poem_id
              )
              AND source_poem.canonical_poem_id = requested.poem_id)
          )
        LEFT JOIN source_author_identity source_author
          ON source_author.id = source_poem.source_author_id
        LEFT JOIN source_author_identity requested_author
          ON requested_author.source_name = ${this.#sourceName}
          AND requested_author.external_id = requested.source_author_slug
        LEFT JOIN poem legacy_poem
          ON requested.source_poem_id IS NOT NULL
          AND source_poem.id IS NULL
          AND legacy_poem.slug = ${this.#sourcePoemSlugPrefix}
            || requested.source_poem_id || ${this.#sourcePoemSlugSuffix}
          AND EXISTS (
            SELECT 1 FROM author legacy_author
            WHERE legacy_author.id = legacy_poem.author_id
              AND legacy_author.slug = requested.source_author_slug
          )
      ), resolved AS (
        SELECT
          candidates.ordinal,
          candidates.source_author_slug,
          candidates.source_poem_id,
          candidates.requested_poem_id,
          candidates.requested_source_revision_id,
          candidates.model_keys,
          candidates.source_identity_id,
          candidates.identity_poem_id,
          candidates.identity_author_slug,
          candidates.identity_author_source_name,
          candidates.identity_author_id,
          candidates.source_identity_tombstoned_at,
          candidates.requested_author_id,
          candidates.legacy_poem_id,
          CASE WHEN source_identity_id IS NULL
            THEN legacy_poem_id ELSE identity_poem_id END AS poem_id
        FROM candidates
      )
      SELECT
        poem.active_source_revision_id,
        author.id AS author_id,
        author.name_arabic AS author_name_arabic,
        source_revision.content_arabic,
        resolved.identity_author_id,
        resolved.identity_author_source_name,
        resolved.identity_author_slug,
        resolved.poem_id,
        resolved.requested_poem_id,
        resolved.requested_source_revision_id,
        resolved.requested_author_id,
        COALESCE(resolved.source_author_slug,
                 resolved.identity_author_slug) AS source_author_slug,
        resolved.source_identity_id,
        resolved.source_identity_tombstoned_at,
        source_pointer.revision_id AS source_pointer_revision_id,
        source_pointer.pointer_version AS source_pointer_version,
        COALESCE(resolved.source_poem_id,
                 source_identity.external_id) AS source_poem_id,
        source_revision.source_poem_id AS source_revision_source_poem_id,
        writer.writer_epoch,
        COALESCE((
          SELECT json_group_array(json(pointer.document))
          FROM (
            SELECT json_object(
              'modelKey', publication.model_key,
              'pointerVersion', publication.pointer_version
            ) AS document
            FROM json_each(resolved.model_keys) requested_model
            JOIN poem_model_publication_pointer publication
              ON publication.poem_id = resolved.poem_id
              AND publication.model_key = requested_model.value
              AND publication.source_revision_id = poem.active_source_revision_id
            ORDER BY publication.model_key
          ) pointer
        ), '[]') AS model_pointers
      FROM resolved
      CROSS JOIN scraper_writer_control writer
      LEFT JOIN poem ON poem.id = resolved.poem_id
      LEFT JOIN author ON author.id = poem.author_id
      LEFT JOIN poem_source_pointer source_pointer
        ON source_pointer.source_poem_id = resolved.source_identity_id
      LEFT JOIN source_poem_identity source_identity
        ON source_identity.id = resolved.source_identity_id
      LEFT JOIN poem_source_revision source_revision
        ON source_revision.id = source_pointer.revision_id
      WHERE writer.singleton = 1
      ORDER BY resolved.ordinal
    `);
    if (rows.length !== request.targets.length) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
      );
    }
    const targets = await Promise.all(
      rows.map(async (raw, index) => {
        const row = ResolutionRowSchema.parse(raw);
        const requested = request.targets.at(index);
        if (requested === undefined) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
          );
        }
        if (
          row.poem_id === null ||
          row.author_id === null ||
          row.author_name_arabic === null ||
          row.source_author_slug === null ||
          row.source_poem_id === null
        ) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
          );
        }
        if (
          "sourcePoemId" in requested &&
          (row.source_poem_id !== requested.sourcePoemId ||
            row.source_author_slug !== requested.sourceAuthorSlug)
        ) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
          );
        }
        if (
          "poemId" in requested &&
          (row.requested_poem_id !== requested.poemId ||
            row.requested_source_revision_id !== requested.sourceRevisionId)
        ) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
          );
        }
        if (row.source_identity_id === null) {
          if ("poemId" in requested) {
            throw new ProductionResolutionConflictError(
              "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
            );
          }
          if (
            row.active_source_revision_id !== null ||
            row.source_pointer_revision_id !== null ||
            row.source_pointer_version !== null ||
            (row.requested_author_id !== null &&
              row.requested_author_id !== row.author_id)
          ) {
            throw new ProductionResolutionConflictError(
              "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
            );
          }
        } else if (
          row.source_identity_tombstoned_at !== null ||
          row.identity_author_source_name !== this.#sourceName ||
          ("sourceAuthorSlug" in requested &&
            row.identity_author_slug !== requested.sourceAuthorSlug) ||
          row.identity_author_id !== row.author_id ||
          row.active_source_revision_id === null ||
          row.source_pointer_revision_id !== row.active_source_revision_id ||
          row.source_revision_source_poem_id !== row.source_identity_id ||
          ("poemId" in requested && row.poem_id !== requested.poemId)
        ) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
          );
        }
        const modelPointers = ModelPointersSchema.parse(
          JSON.parse(row.model_pointers),
        ).map(({ modelKey, pointerVersion }) => ({ modelKey, pointerVersion }));
        const allowedModelKeys = new Set<string>(requested.modelKeys);
        if (
          modelPointers.some(({ modelKey }) => !allowedModelKeys.has(modelKey))
        ) {
          throw new ProductionResolutionConflictError(
            "PRODUCTION_RESOLUTION_MODEL_SCOPE_CONFLICT",
          );
        }
        return {
          authorId: row.author_id,
          authorNameArabic: row.author_name_arabic,
          currentSourceNfcSha256:
            row.active_source_revision_id === null
              ? null
              : await normalizedSourceHash(row.content_arabic),
          currentSourceRevisionId: row.active_source_revision_id,
          modelPointers,
          poemId: row.poem_id,
          sourceAuthorSlug: row.source_author_slug,
          sourcePoemId: row.source_poem_id,
          sourcePointerVersion: row.source_pointer_version,
        };
      }),
    );
    const writerEpochs = new Set(
      rows.map((raw) => ResolutionRowSchema.parse(raw).writer_epoch),
    );
    if (writerEpochs.size !== 1) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
      );
    }
    const [writerEpoch] = writerEpochs;
    if (writerEpoch === undefined) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
      );
    }
    const now = Math.floor(this.#runtime.now() / 1_000) * 1_000;
    const body: ProductionResolutionResponseBody =
      ProductionResolutionResponseBodySchema.parse({
        expiresAt: new Date(now + this.#lifetimeMs).toISOString(),
        observedAt: new Date(now).toISOString(),
        schemaId: PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID,
        schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
        scopeHash: await sha256(stableJson(request)),
        targets,
        writerEpoch,
      });
    return ProductionResolutionResponseSchema.parse({
      ...body,
      manifestHash: await sha256(stableJson(body)),
    });
  }

  async #resolveFingerprints(
    request: Extract<ProductionResolutionRequest, { schemaVersion: 3 }>,
  ): Promise<ProductionResolutionResponse> {
    const requestJson = JSON.stringify(request.targets);
    const rows = await this.#db.all(sql`
      WITH requested AS (
        SELECT
          CAST(key AS INTEGER) AS ordinal,
          json_extract(value, '$.fingerprintAlgorithm') AS algorithm,
          json_extract(value, '$.lineNfcHash') AS line_nfc_hash,
          json_extract(value, '$.promptMaterialHash') AS prompt_material_hash,
          json_extract(value, '$.modelKeys') AS model_keys
        FROM json_each(${requestJson})
      ), fingerprint_matches AS (
        SELECT requested.ordinal, requested.model_keys,
               fingerprint.source_revision_id, fingerprint.algorithm,
               fingerprint.line_nfc_hash, fingerprint.prompt_material_hash
        FROM requested
        JOIN source_revision_fingerprint fingerprint
          ON fingerprint.algorithm = requested.algorithm
         AND fingerprint.line_nfc_hash = requested.line_nfc_hash
         AND fingerprint.prompt_material_hash = requested.prompt_material_hash
      ), raw_counts AS (
        SELECT ordinal, COUNT(*) AS raw_match_count
        FROM fingerprint_matches GROUP BY ordinal
      ), active_candidates AS (
        SELECT
          matched.ordinal, matched.model_keys, matched.source_revision_id,
          matched.algorithm, matched.line_nfc_hash,
          matched.prompt_material_hash, poem.id AS poem_id,
          author.id AS author_id, author.name_arabic AS author_name_arabic,
          source_author.external_id AS source_author_slug,
          source_poem.external_id AS source_poem_id,
          source_pointer.pointer_version AS source_pointer_version,
          COUNT(*) OVER (PARTITION BY matched.ordinal) AS active_candidate_count,
          ROW_NUMBER() OVER (
            PARTITION BY matched.ordinal ORDER BY matched.source_revision_id
          ) AS candidate_rank
        FROM fingerprint_matches matched
        JOIN poem_source_revision revision
          ON revision.id = matched.source_revision_id
        JOIN source_poem_identity source_poem
          ON source_poem.id = revision.source_poem_id
         AND source_poem.source_name = ${this.#sourceName}
         AND source_poem.tombstoned_at IS NULL
        JOIN poem
          ON poem.id = source_poem.canonical_poem_id
         AND poem.active_source_revision_id = revision.id
        JOIN poem_source_pointer source_pointer
          ON source_pointer.source_poem_id = source_poem.id
         AND source_pointer.revision_id = revision.id
        JOIN source_author_identity source_author
          ON source_author.id = source_poem.source_author_id
         AND source_author.source_name = ${this.#sourceName}
         AND source_author.canonical_author_id = poem.author_id
        JOIN author ON author.id = poem.author_id
      )
      SELECT
        COALESCE(active.active_candidate_count, 0) AS active_candidate_count,
        active.algorithm, active.author_id, active.author_name_arabic,
        active.line_nfc_hash, active.poem_id, active.prompt_material_hash,
        COALESCE(raw_counts.raw_match_count, 0) AS raw_match_count,
        active.source_author_slug, active.source_poem_id,
        active.source_pointer_version, active.source_revision_id,
        writer.writer_epoch,
        CASE WHEN active.source_revision_id IS NULL THEN NULL ELSE COALESCE((
          SELECT json_group_array(json(pointer.document))
          FROM (
            SELECT json_object(
              'modelKey', publication.model_key,
              'pointerVersion', publication.pointer_version
            ) AS document
            FROM json_each(requested.model_keys) requested_model
            JOIN poem_model_publication_pointer publication
              ON publication.poem_id = active.poem_id
             AND publication.model_key = requested_model.value
             AND publication.source_revision_id = active.source_revision_id
            ORDER BY publication.model_key
          ) pointer
        ), '[]') END AS model_pointers
      FROM requested
      CROSS JOIN scraper_writer_control writer
      LEFT JOIN raw_counts USING(ordinal)
      LEFT JOIN active_candidates active
        ON active.ordinal = requested.ordinal AND active.candidate_rank = 1
      WHERE writer.singleton = 1
      ORDER BY requested.ordinal
    `);
    if (rows.length !== request.targets.length) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
      );
    }
    const targets = rows.map((raw, index) => {
      const row = FingerprintResolutionRowSchema.parse(raw);
      const requested = request.targets.at(index);
      if (!requested) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
        );
      }
      if (row.raw_match_count === 0) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
        );
      }
      if (row.raw_match_count > 1 || row.active_candidate_count > 1) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
        );
      }
      if (row.active_candidate_count === 0) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
        );
      }
      if (
        row.algorithm !== PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM ||
        row.line_nfc_hash !== requested.lineNfcHash ||
        row.prompt_material_hash !== requested.promptMaterialHash ||
        row.source_revision_id === null ||
        row.source_pointer_version === null ||
        row.poem_id === null ||
        row.author_id === null ||
        row.author_name_arabic === null ||
        row.source_author_slug === null ||
        row.source_poem_id === null ||
        row.model_pointers === null
      ) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
        );
      }
      const modelPointers = ModelPointersSchema.parse(
        JSON.parse(row.model_pointers),
      ).map(({ modelKey, pointerVersion }) => ({ modelKey, pointerVersion }));
      const allowedModelKeys = new Set<string>(requested.modelKeys);
      if (
        modelPointers.some(({ modelKey }) => !allowedModelKeys.has(modelKey))
      ) {
        throw new ProductionResolutionConflictError(
          "PRODUCTION_RESOLUTION_MODEL_SCOPE_CONFLICT",
        );
      }
      return {
        activeSourceFingerprint: {
          algorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash: row.line_nfc_hash,
          promptMaterialHash: row.prompt_material_hash,
        },
        authorId: row.author_id,
        authorNameArabic: row.author_name_arabic,
        currentSourceNfcSha256: row.line_nfc_hash,
        currentSourceRevisionId: row.source_revision_id,
        modelPointers,
        poemId: row.poem_id,
        sourceAuthorSlug: row.source_author_slug,
        sourcePoemId: row.source_poem_id,
        sourcePointerVersion: row.source_pointer_version,
      };
    });
    const writerEpochs = new Set(
      rows.map((raw) => FingerprintResolutionRowSchema.parse(raw).writer_epoch),
    );
    if (writerEpochs.size !== 1) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
      );
    }
    const [writerEpoch] = writerEpochs;
    if (writerEpoch === undefined) {
      throw new ProductionResolutionConflictError(
        "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
      );
    }
    const now = Math.floor(this.#runtime.now() / 1_000) * 1_000;
    const body: ProductionResolutionResponseBody =
      ProductionResolutionResponseBodySchema.parse({
        expiresAt: new Date(now + this.#lifetimeMs).toISOString(),
        observedAt: new Date(now).toISOString(),
        schemaId: PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID,
        schemaVersion: PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
        scopeHash: await sha256(stableJson(request)),
        targets,
        writerEpoch,
      });
    return ProductionResolutionResponseSchema.parse({
      ...body,
      manifestHash: await sha256(stableJson(body)),
    });
  }
}

async function normalizedSourceHash(
  serialized: null | string,
): Promise<string> {
  const document = SourceContentDocumentSchema.parse(
    serialized === null ? null : JSON.parse(serialized),
  );
  return sha256(
    stableJson(document.content.map((line) => line.normalize("NFC"))),
  );
}

function stableJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string")
    return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Non-finite JSON number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (!isJsonObject(value)) throw new TypeError("Unsupported JSON value");
  return `{${Object.entries(value)
    .filter(([, nested]) => nested !== undefined)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([key, nested]) => `${JSON.stringify(key)}:${stableJson(nested)}`)
    .join(",")}}`;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function sha256(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
