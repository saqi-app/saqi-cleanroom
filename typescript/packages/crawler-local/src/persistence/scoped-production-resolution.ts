import { hash, randomUUID } from "node:crypto";
import {
  access,
  chmod,
  constants,
  lstat,
  open,
  rename,
  rm,
} from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  isProductionResolutionSourceTarget,
  normalizeProductionResolutionRequest,
  type ProductionResolutionRequest,
  ProductionResolutionRequestSchema,
  type ProductionResolutionResponse,
  ProductionResolutionResponseBodySchema,
  ProductionResolutionResponseSchema,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { z } from "zod";

import {
  assertCollectedArtifactBinding,
  type LocalEnrichmentResolver,
} from "../enrichment/local-enrichment-fanout.js";
import {
  type AuthenticatedPublicationTransportOptions,
  createAuthenticatedPublicationTransport,
} from "../publication/publication-auth-client.js";
import type { PublicationTransport } from "../publication/publication-client.js";
import { ProductionResolutionStore } from "./production-resolution-store.js";
import type { WorkItem } from "./schema.js";
import {
  queryMany,
  queryOptional,
  queryRequired,
  queryScalar,
  SqliteSafeIntegerSchema,
} from "./sqlite-query.js";
import { canonicalJson, sha256 } from "./work-key.js";

const SCOPED_PRODUCTION_RESOLUTION_SCHEMA_ID =
  "saqi.production-resolution-store";
const SCOPED_PRODUCTION_RESOLUTION_SCHEMA_VERSION = 3;
const MAXIMUM_RESPONSE_BYTES = 2 * 1024 * 1024;
const MAXIMUM_CLOCK_SKEW_MS = 5 * 60_000;
const MAXIMUM_SCOPE_LIFETIME_MS = 60 * 60_000;

const ApiEnvelopeSchema = z.strictObject({
  ok: z.literal(true),
  result: ProductionResolutionResponseSchema,
});
const ResolutionConflictCodeSchema = z.enum([
  "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
  "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
  "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
  "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
  "PRODUCTION_RESOLUTION_MODEL_SCOPE_CONFLICT",
  "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
  "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
  "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
  "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
]);
const ApiConflictEnvelopeSchema = z.strictObject({
  error: ResolutionConflictCodeSchema,
  ok: z.literal(false),
});
const Sha256Schema = z.string().regex(/^[a-f\d]{64}$/);
const ResolutionModelKeySchema = z.string().min(1).max(100);
const PointerVersionSchema = SqliteSafeIntegerSchema.positive();
const MetaSchema = z
  .strictObject({
    expires_at: z.iso.datetime({ offset: true }),
    manifest_sha256: Sha256Schema,
    model_count: SqliteSafeIntegerSchema.nonnegative(),
    observed_at: z.iso.datetime({ offset: true }),
    poem_count: SqliteSafeIntegerSchema.positive(),
    schema_id: z.literal(SCOPED_PRODUCTION_RESOLUTION_SCHEMA_ID),
    schema_version: z.literal(SCOPED_PRODUCTION_RESOLUTION_SCHEMA_VERSION),
    scope_sha256: Sha256Schema,
    writer_epoch: SqliteSafeIntegerSchema.positive(),
  })
  .transform(
    ({
      expires_at,
      manifest_sha256,
      model_count,
      observed_at,
      poem_count,
      schema_id,
      schema_version,
      scope_sha256,
      writer_epoch,
    }) => ({
      expiresAt: expires_at,
      manifestSha256: manifest_sha256,
      modelCount: model_count,
      observedAt: observed_at,
      poemCount: poem_count,
      schemaId: schema_id,
      schemaVersion: schema_version,
      scopeSha256: scope_sha256,
      writerEpoch: writer_epoch,
    }),
  );
const CollectedArtifactSchema = z.looseObject({
  source: z.looseObject({
    author: z.looseObject({ slug: z.string().min(1).max(1_000) }),
    numericId: z.string().regex(/^[1-9]\d*$/),
  }),
});
const StoredModelSchema = z
  .strictObject({
    model_key: z.string().min(1).max(100),
    pointer_version: PointerVersionSchema.nullable(),
  })
  .transform(({ model_key, pointer_version }) => ({
    modelKey: model_key,
    pointerVersion: pointer_version,
  }));
const StoredPoemSchema = z
  .strictObject({
    author_id: z.uuid(),
    author_name_arabic: z.string().trim().min(1).max(10_000),
    current_source_revision_id: Sha256Schema.nullable(),
    poem_id: z.uuid(),
    source_author_slug: z.string().min(1).max(1_000),
    source_poem_id: z.string().regex(/^[1-9]\d*$/),
    source_pointer_version: PointerVersionSchema.nullable(),
  })
  .transform(
    ({
      author_id,
      author_name_arabic,
      current_source_revision_id,
      poem_id,
      source_author_slug,
      source_poem_id,
      source_pointer_version,
    }) => ({
      authorId: author_id,
      authorNameArabic: author_name_arabic,
      currentSourceRevisionId: current_source_revision_id,
      poemId: poem_id,
      sourceAuthorSlug: source_author_slug,
      sourcePoemId: source_poem_id,
      sourcePointerVersion: source_pointer_version,
    }),
  );
const ResolutionCountsSchema = z
  .strictObject({
    models: SqliteSafeIntegerSchema.nonnegative(),
    poems: SqliteSafeIntegerSchema.nonnegative(),
  })
  .transform(({ models, poems }) => ({ modelCount: models, poemCount: poems }));
const ForeignKeyRowsSchema = z.array(z.unknown());
const TableInfoRowSchema = z
  .strictObject({
    cid: SqliteSafeIntegerSchema.nonnegative(),
    dflt_value: z.unknown().nullable(),
    name: z.string().min(1),
    notnull: z.literal([0, 1]),
    pk: SqliteSafeIntegerSchema.nonnegative(),
    type: z.string(),
  })
  .transform(({ name }) => name);
const PublicationRowSchema = z
  .strictObject({
    current_source_revision_id: Sha256Schema.nullable(),
    pointer_version: PointerVersionSchema.nullable(),
  })
  .transform(({ current_source_revision_id, pointer_version }) => ({
    currentSourceRevisionId: current_source_revision_id,
    pointerVersion: pointer_version,
  }));

export interface ScopedProductionResolutionReport {
  readonly expiresAt: string;
  readonly manifestSha256: string;
  readonly modelCount: number;
  readonly observedAt: string;
  readonly output: string;
  readonly poemCount: number;
  readonly promotion?: "created" | "replaced" | "replayed";
  readonly schemaId: typeof SCOPED_PRODUCTION_RESOLUTION_SCHEMA_ID;
  readonly schemaVersion: typeof SCOPED_PRODUCTION_RESOLUTION_SCHEMA_VERSION;
  readonly scopeSha256: string;
  readonly writerEpoch: number;
}

export interface RequestScopedProductionResolutionOptions extends Pick<
  AuthenticatedPublicationTransportOptions,
  "environment" | "fetcher" | "now"
> {
  readonly auth: unknown;
  readonly endpoint: string;
  readonly request: unknown;
  readonly signal?: AbortSignal;
}

export interface FetchScopedProductionResolutionOptions extends RequestScopedProductionResolutionOptions {
  readonly output: string;
}

export interface TransportScopedProductionResolutionOptions {
  readonly endpoint: string;
  readonly now?: () => number;
  readonly request: unknown;
  readonly signal?: AbortSignal;
  readonly transport: PublicationTransport;
}

interface ScopedProductionResolutionReader extends LocalEnrichmentResolver {
  close(): void;
  report(): Promise<ScopedProductionResolutionReport>;
  resolvePublication(
    poemId: string,
    modelKey: string,
    legacyFallback: boolean,
    requiredSourceRevisionId?: string,
  ): Promise<{
    expectedPointerVersion: null | number;
    writerEpoch: number;
  } | null>;
}

export type ReadableProductionResolutionStore =
  ProductionResolutionStore | ScopedProductionResolutionReader;

class ScopedResolutionDatabase {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  configureReadonly(): void {
    this.#database.pragma("query_only = ON");
  }

  initialize(): void {
    this.#database.pragma("foreign_keys = ON");
    this.#database.pragma("journal_mode = DELETE");
    this.#database.exec(schemaSql());
  }

  optimize(): void {
    this.#database.pragma("optimize");
  }

  readMeta(): z.infer<typeof MetaSchema> {
    return queryRequired(
      { operation: "scopedProductionResolution.meta" },
      () =>
        this.#database
          .prepare(
            `SELECT schema_id, schema_version, observed_at, expires_at,
                  writer_epoch, poem_count, model_count, scope_sha256,
                  manifest_sha256
             FROM resolution_meta WHERE singleton = 1`,
          )
          .get(),
      MetaSchema,
    );
  }

  readSchemaVersion(): number | undefined {
    return queryScalar(
      { operation: "scopedProductionResolution.schemaVersion" },
      () =>
        this.#database
          .prepare(
            "SELECT schema_version FROM resolution_meta WHERE singleton = 1",
          )
          .pluck()
          .get(),
      SqliteSafeIntegerSchema.nonnegative(),
    );
  }

  rowBySource(sourcePoemId: string) {
    return queryOptional(
      { operation: "scopedProductionResolution.source" },
      () =>
        this.#database
          .prepare(
            `SELECT source_poem_id, source_author_slug, poem_id, author_id,
                author_name_arabic, current_source_revision_id,
                source_pointer_version
           FROM poem_resolution WHERE source_poem_id = ?`,
          )
          .get(sourcePoemId),
      StoredPoemSchema,
    );
  }

  publicationRow(poemId: string, modelKey: string) {
    return queryOptional(
      { operation: "scopedProductionResolution.publication" },
      () =>
        this.#database
          .prepare(
            `SELECT poem.current_source_revision_id, model.pointer_version
           FROM poem_resolution poem
           JOIN scoped_model model ON model.source_poem_id = poem.source_poem_id
          WHERE poem.poem_id = ? AND model.model_key = ?`,
          )
          .get(poemId, modelKey),
      PublicationRowSchema,
    );
  }

  write(
    request: ProductionResolutionRequest,
    response: ProductionResolutionResponse,
  ): number {
    if (request.schemaVersion === 3 || response.schemaVersion !== 1)
      throw new Error("PRODUCTION_RESOLUTION_SCHEMA_NEGOTIATION_MISMATCH");
    const insertPoem = this.#database.prepare(
      `INSERT INTO poem_resolution (
         source_poem_id, source_author_slug, poem_id, author_id,
         author_name_arabic, current_source_revision_id, source_pointer_version
       ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    const insertModel = this.#database.prepare(
      `INSERT INTO scoped_model (source_poem_id, model_key, pointer_version)
       VALUES (?, ?, ?)`,
    );
    this.#database.exec("BEGIN IMMEDIATE");
    let modelCount = 0;
    try {
      for (const target of response.targets) {
        insertPoem.run(
          target.sourcePoemId,
          target.sourceAuthorSlug,
          target.poemId,
          target.authorId,
          target.authorNameArabic,
          target.currentSourceRevisionId,
          target.sourcePointerVersion,
        );
        const requested = request.targets.find((candidate) =>
          isProductionResolutionSourceTarget(candidate)
            ? candidate.sourcePoemId === target.sourcePoemId &&
              candidate.sourceAuthorSlug === target.sourceAuthorSlug
            : candidate.poemId === target.poemId &&
              candidate.sourceRevisionId === target.currentSourceRevisionId,
        );
        if (!requested)
          throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
        const pointers = new Map(
          target.modelPointers.map(({ modelKey, pointerVersion }) => [
            modelKey,
            pointerVersion,
          ]),
        );
        for (const modelKey of requested.modelKeys) {
          insertModel.run(
            target.sourcePoemId,
            modelKey,
            pointers.get(modelKey) ?? null,
          );
          modelCount += 1;
        }
      }
      this.#database
        .prepare(
          `INSERT INTO resolution_meta (
             singleton, schema_id, schema_version, observed_at, expires_at,
             writer_epoch, poem_count, model_count, scope_sha256,
             manifest_sha256
           ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          SCOPED_PRODUCTION_RESOLUTION_SCHEMA_ID,
          SCOPED_PRODUCTION_RESOLUTION_SCHEMA_VERSION,
          response.observedAt,
          response.expiresAt,
          response.writerEpoch,
          response.targets.length,
          modelCount,
          response.scopeHash,
          response.manifestHash,
        );
      this.#database.exec("COMMIT");
      return modelCount;
    } catch (error) {
      this.#database.exec("ROLLBACK");
      throw error;
    }
  }

  validateSchema(): void {
    for (const [table, columns] of [
      [
        "resolution_meta",
        [
          "expires_at",
          "manifest_sha256",
          "model_count",
          "observed_at",
          "poem_count",
          "schema_id",
          "schema_version",
          "scope_sha256",
          "writer_epoch",
        ],
      ],
      [
        "poem_resolution",
        [
          "author_id",
          "author_name_arabic",
          "current_source_revision_id",
          "poem_id",
          "source_author_slug",
          "source_poem_id",
          "source_pointer_version",
        ],
      ],
      ["scoped_model", ["model_key", "pointer_version", "source_poem_id"]],
    ] as const) {
      const actual = new Set(
        queryMany(
          { operation: "scopedProductionResolution.tableInfo" },
          () => this.#database.prepare(`PRAGMA table_info(${table})`).all(),
          TableInfoRowSchema,
        ),
      );
      if (columns.some((column) => !actual.has(column)))
        throw new Error(`PRODUCTION_RESOLUTION_SCHEMA_INVALID:${table}`);
    }
  }

  validateContents(meta: z.infer<typeof MetaSchema>): void {
    if (this.#database.pragma("integrity_check", { simple: true }) !== "ok")
      throw new Error("PRODUCTION_RESOLUTION_INTEGRITY_FAILED");
    if (
      ForeignKeyRowsSchema.parse(this.#database.pragma("foreign_key_check"))
        .length
    )
      throw new Error("PRODUCTION_RESOLUTION_FOREIGN_KEY_FAILED");
    const counts = queryRequired(
      { operation: "scopedProductionResolution.counts" },
      () =>
        this.#database
          .prepare(
            `SELECT (SELECT count(*) FROM poem_resolution) AS poems,
                  (SELECT count(*) FROM scoped_model) AS models`,
          )
          .get(),
      ResolutionCountsSchema,
    );
    if (
      counts.poemCount !== meta.poemCount ||
      counts.modelCount !== meta.modelCount
    )
      throw new Error("PRODUCTION_RESOLUTION_COUNT_MISMATCH");
    const targets = queryMany(
      { operation: "scopedProductionResolution.poems" },
      () =>
        this.#database
          .prepare(
            `SELECT source_poem_id, source_author_slug, poem_id, author_id,
                author_name_arabic, current_source_revision_id,
                source_pointer_version
           FROM poem_resolution ORDER BY source_poem_id, source_author_slug`,
          )
          .all(),
      StoredPoemSchema,
    ).map((poem) => {
      const models = queryMany(
        { operation: "scopedProductionResolution.models" },
        () =>
          this.#database
            .prepare(
              `SELECT model_key, pointer_version FROM scoped_model
              WHERE source_poem_id = ? ORDER BY model_key`,
            )
            .all(poem.sourcePoemId),
        StoredModelSchema,
      );
      return {
        authorId: poem.authorId,
        authorNameArabic: poem.authorNameArabic,
        currentSourceRevisionId: poem.currentSourceRevisionId,
        modelPointers: models.flatMap(({ modelKey, pointerVersion }) =>
          pointerVersion === null ? [] : [{ modelKey, pointerVersion }],
        ),
        poemId: poem.poemId,
        sourceAuthorSlug: poem.sourceAuthorSlug,
        sourcePoemId: poem.sourcePoemId,
        sourcePointerVersion: poem.sourcePointerVersion,
      };
    });
    const request = normalizeProductionResolutionRequest({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 1,
      targets: targets.map((target) => ({
        modelKeys: queryMany(
          { operation: "scopedProductionResolution.modelKeys" },
          () =>
            this.#database
              .prepare(
                "SELECT model_key FROM scoped_model WHERE source_poem_id = ?",
              )
              .pluck()
              .all(target.sourcePoemId),
          ResolutionModelKeySchema,
        ),
        sourceAuthorSlug: target.sourceAuthorSlug,
        sourcePoemId: target.sourcePoemId,
      })),
    });
    if (sha256(canonicalJson(request)) !== meta.scopeSha256)
      throw new Error("PRODUCTION_RESOLUTION_SCOPE_HASH_MISMATCH");
    const body = ProductionResolutionResponseBodySchema.parse({
      expiresAt: meta.expiresAt,
      observedAt: meta.observedAt,
      schemaId: "saqi.production-resolution-response",
      schemaVersion: 1,
      scopeHash: meta.scopeSha256,
      targets,
      writerEpoch: meta.writerEpoch,
    });
    if (sha256(canonicalJson(body)) !== meta.manifestSha256)
      throw new Error("PRODUCTION_RESOLUTION_MANIFEST_HASH_MISMATCH");
  }
}

/** Opens legacy full snapshots or exact-scope v3 snapshots without guessing. */
export async function openProductionResolutionStore(
  path: string,
  options: { readonly now?: () => number } = {},
): Promise<ReadableProductionResolutionStore> {
  const input = await existingRegularFile(path, "INPUT");
  const database = new Database(input, { fileMustExist: true, readonly: true });
  let version: unknown;
  try {
    const repository = new ScopedResolutionDatabase(database);
    repository.configureReadonly();
    version = repository.readSchemaVersion();
  } finally {
    database.close();
  }
  if (version === SCOPED_PRODUCTION_RESOLUTION_SCHEMA_VERSION)
    return ScopedProductionResolutionStore.open(input, options);
  if (version === 2) return ProductionResolutionStore.open(input, options);
  throw new Error("PRODUCTION_RESOLUTION_SCHEMA_VERSION_UNSUPPORTED");
}

export async function fetchScopedProductionResolution(
  options: FetchScopedProductionResolutionOptions,
): Promise<ScopedProductionResolutionReport> {
  const { request, response } =
    await requestScopedProductionResolution(options);
  return writeScopedProductionResolution({
    ...(options.now ? { now: options.now } : {}),
    output: options.output,
    request,
    response,
  });
}

export async function requestScopedProductionResolution(
  options: RequestScopedProductionResolutionOptions,
): Promise<{
  readonly request: ProductionResolutionRequest;
  readonly response: ProductionResolutionResponse;
}> {
  const auth = createAuthenticatedPublicationTransport({
    config: options.auth,
    ...(options.environment ? { environment: options.environment } : {}),
    ...(options.fetcher ? { fetcher: options.fetcher } : {}),
    ...(options.now ? { now: options.now } : {}),
  });
  const preflight = await auth.preflight(options.signal);
  if (preflight.state !== "ready")
    throw new Error(preflight.errorCode ?? "PRODUCTION_RESOLUTION_AUTH_PAUSED");
  return requestScopedProductionResolutionWithTransport({
    endpoint: options.endpoint,
    ...(options.now ? { now: options.now } : {}),
    request: options.request,
    ...(options.signal ? { signal: options.signal } : {}),
    transport: auth.transport,
  });
}

export async function requestScopedProductionResolutionWithTransport(
  options: TransportScopedProductionResolutionOptions,
): Promise<{
  readonly request: ProductionResolutionRequest;
  readonly response: ProductionResolutionResponse;
}> {
  const request = normalizeProductionResolutionRequest(
    ProductionResolutionRequestSchema.parse(options.request),
  );
  const response = await options.transport({
    body: canonicalJson(request),
    ...(options.signal ? { signal: options.signal } : {}),
    url: options.endpoint,
  });
  if (
    response.status === 401 ||
    response.status === 403 ||
    response.authFailure
  )
    throw new Error("PRODUCTION_RESOLUTION_AUTH_REJECTED");
  if (Buffer.byteLength(response.body) > MAXIMUM_RESPONSE_BYTES)
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_TOO_LARGE");
  if (response.status < 200 || response.status >= 300) {
    if (response.status === 409) {
      const conflict = ApiConflictEnvelopeSchema.safeParse(
        safeJson(response.body),
      );
      if (conflict.success) throw new Error(conflict.data.error);
    }
    throw new Error(`PRODUCTION_RESOLUTION_HTTP_${String(response.status)}`);
  }
  let envelope: z.infer<typeof ApiEnvelopeSchema>;
  try {
    envelope = ApiEnvelopeSchema.parse(JSON.parse(response.body));
  } catch {
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_INVALID");
  }
  validateFetchedProductionResolutionResponse(
    request,
    envelope.result,
    options.now?.() ?? Date.now(),
  );
  return { request, response: envelope.result };
}

function safeJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

export async function writeScopedProductionResolution(options: {
  readonly now?: () => number;
  readonly output: string;
  readonly request: ProductionResolutionRequest;
  readonly response: ProductionResolutionResponse;
}): Promise<ScopedProductionResolutionReport> {
  const request = normalizeProductionResolutionRequest(
    ProductionResolutionRequestSchema.parse(options.request),
  );
  const response = ProductionResolutionResponseSchema.parse(options.response);
  validateProductionResolutionResponse(
    request,
    response,
    options.now?.() ?? Date.now(),
  );
  const output = await safeOutputPath(options.output);
  const temporary = `${output}.${String(process.pid)}.${randomUUID()}.tmp`;
  let database: Database.Database | undefined;
  try {
    database = new Database(temporary);
    await chmod(temporary, 0o600);
    const repository = new ScopedResolutionDatabase(database);
    repository.initialize();
    repository.write(request, response);
    repository.optimize();
    database.close();
    database = undefined;
    await fsyncFile(temporary);
    const generated = await ScopedProductionResolutionStore.open(temporary, {
      now: () => Date.parse(response.observedAt),
    });
    const generatedReport = await generated.report();
    generated.close();
    if (await pathExists(output)) {
      // A refresh is specifically expected to replace an expired snapshot.
      // Inspect the stored snapshot without applying its serving-time freshness
      // gate, while still validating its schema, hashes, counts, and contents.
      const existingReport = await inspectStoredResolution(output);
      assertPromotionIsMonotonic(existingReport, generatedReport);
      if (sameSnapshot(existingReport, generatedReport)) {
        await rm(temporary);
        return { ...existingReport, promotion: "replayed" };
      }
    }
    const promotion = (await pathExists(output)) ? "replaced" : "created";
    await rename(temporary, output);
    await fsyncDirectory(dirname(output));
    return { ...generatedReport, output, promotion };
  } finally {
    database?.close();
    await rmIfExists(temporary);
  }
}

/** Read-only, exact-scope production identity and pointer snapshot. */
export class ScopedProductionResolutionStore implements ScopedProductionResolutionReader {
  #database: Database.Database;
  #repository: ScopedResolutionDatabase;
  #fileIdentity: string;
  #meta: z.infer<typeof MetaSchema>;
  readonly #now: () => number;
  #ownsDatabase = true;
  readonly #path: string;
  #refreshPromise: Promise<void> | undefined;

  static async open(
    path: string,
    options: { readonly now?: () => number } = {},
  ): Promise<ScopedProductionResolutionStore> {
    const input = await existingRegularFile(path, "INPUT");
    return new ScopedProductionResolutionStore(
      input,
      await fileIdentity(input),
      options,
    );
  }

  /** @internal Use {@link ScopedProductionResolutionStore.open}. */
  constructor(
    path: string,
    identity: string,
    options: { readonly now?: () => number } = {},
  ) {
    this.#path = path;
    this.#fileIdentity = identity;
    this.#now = options.now ?? Date.now;
    this.#database = new Database(this.#path, {
      fileMustExist: true,
      readonly: true,
    });
    this.#repository = new ScopedResolutionDatabase(this.#database);
    try {
      this.#repository.configureReadonly();
      this.#repository.validateSchema();
      this.#meta = this.#repository.readMeta();
      this.#assertFresh();
      this.#repository.validateContents(this.#meta);
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  close(): void {
    if (this.#ownsDatabase) this.#database.close();
    this.#ownsDatabase = false;
  }

  async report(): Promise<ScopedProductionResolutionReport> {
    await this.#refresh();
    this.#assertFresh();
    return {
      expiresAt: this.#meta.expiresAt,
      manifestSha256: this.#meta.manifestSha256,
      modelCount: this.#meta.modelCount,
      observedAt: this.#meta.observedAt,
      output: this.#path,
      poemCount: this.#meta.poemCount,
      schemaId: this.#meta.schemaId,
      schemaVersion: this.#meta.schemaVersion,
      scopeSha256: this.#meta.scopeSha256,
      writerEpoch: this.#meta.writerEpoch,
    };
  }

  async resolve(source: WorkItem, artifact: unknown) {
    await this.#refresh();
    assertCollectedArtifactBinding(source, artifact);
    const identity = CollectedArtifactSchema.parse(artifact).source;
    const row = this.#rowBySource(identity.numericId);
    if (!row) return null;
    if (row.sourceAuthorSlug !== identity.author.slug)
      throw new Error("PRODUCTION_RESOLUTION_AUTHOR_MISMATCH");
    return {
      mapping: {
        authorId: row.authorId,
        authorNameArabic: row.authorNameArabic,
        canonicalPoemId: row.poemId,
        poemId: row.poemId,
        sourceAuthorSlug: row.sourceAuthorSlug,
        sourcePoemId: row.sourcePoemId,
      },
      observedAt: this.#meta.observedAt,
      writerEpoch: this.#meta.writerEpoch,
    };
  }

  async resolvePublication(
    poemId: string,
    modelKey: string,
    _legacyFallback: boolean,
    requiredSourceRevisionId?: string,
  ): Promise<{
    expectedPointerVersion: null | number;
    writerEpoch: number;
  } | null> {
    await this.#refresh();
    this.#assertFresh();
    const row = this.#repository.publicationRow(poemId, modelKey);
    if (!row) return null;
    if (row.currentSourceRevisionId === null) return null;
    if (
      requiredSourceRevisionId !== undefined &&
      row.currentSourceRevisionId !== requiredSourceRevisionId
    )
      return null;
    return {
      expectedPointerVersion: row.pointerVersion,
      writerEpoch: this.#meta.writerEpoch,
    };
  }

  #assertFresh(): void {
    const now = this.#now();
    const observedAt = Date.parse(this.#meta.observedAt);
    const expiresAt = Date.parse(this.#meta.expiresAt);
    if (observedAt > now + MAXIMUM_CLOCK_SKEW_MS)
      throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE");
    if (expiresAt <= now) throw new Error("PRODUCTION_RESOLUTION_EXPIRED");
  }

  #rowBySource(sourcePoemId: string) {
    this.#assertFresh();
    return this.#repository.rowBySource(sourcePoemId);
  }

  async #refresh(): Promise<void> {
    const active = this.#refreshPromise;
    if (active) return active;
    const refresh = this.#refreshOnce();
    this.#refreshPromise = refresh;
    try {
      await refresh;
    } finally {
      if (this.#refreshPromise === refresh) this.#refreshPromise = undefined;
    }
  }

  async #refreshOnce(): Promise<void> {
    const identity = await fileIdentity(this.#path);
    if (identity === this.#fileIdentity) return;
    const replacement = await ScopedProductionResolutionStore.open(this.#path, {
      now: this.#now,
    });
    const prior = this.#database;
    this.#database = replacement.#database;
    this.#repository = replacement.#repository;
    this.#fileIdentity = replacement.#fileIdentity;
    this.#meta = replacement.#meta;
    replacement.#ownsDatabase = false;
    prior.close();
  }
}

function validateProductionResolutionResponse(
  requestInput: ProductionResolutionRequest,
  responseInput: ProductionResolutionResponse,
  now: number,
): void {
  const request = normalizeProductionResolutionRequest(requestInput);
  const response = ProductionResolutionResponseSchema.parse(responseInput);
  if (request.schemaVersion === 3 || response.schemaVersion !== 1)
    throw new Error("PRODUCTION_RESOLUTION_SCHEMA_NEGOTIATION_MISMATCH");
  const scopeHash = sha256(canonicalJson(request));
  if (response.scopeHash !== scopeHash)
    throw new Error("PRODUCTION_RESOLUTION_SCOPE_HASH_MISMATCH");
  const { manifestHash: _manifestHash, ...bodyInput } = response;
  const body = ProductionResolutionResponseBodySchema.parse(bodyInput);
  if (response.manifestHash !== sha256(canonicalJson(body)))
    throw new Error("PRODUCTION_RESOLUTION_MANIFEST_HASH_MISMATCH");
  const unmatched = new Set(request.targets);
  if (response.targets.length !== unmatched.size)
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
  for (const target of response.targets) {
    const identityMatches = [...unmatched].filter((candidate) =>
      isProductionResolutionSourceTarget(candidate)
        ? candidate.sourcePoemId === target.sourcePoemId &&
          candidate.sourceAuthorSlug === target.sourceAuthorSlug
        : candidate.poemId === target.poemId,
    );
    const requested = identityMatches.find((candidate) => {
      const allowedModels = new Set(candidate.modelKeys);
      return target.modelPointers.every(({ modelKey }) =>
        allowedModels.has(modelKey),
      );
    });
    if (identityMatches.length > 0 && !requested)
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_MODEL_SCOPE_MISMATCH");
    if (!requested)
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
    const allowedModels = new Set(requested.modelKeys);
    if (
      target.modelPointers.some(({ modelKey }) => !allowedModels.has(modelKey))
    )
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_MODEL_SCOPE_MISMATCH");
    unmatched.delete(requested);
  }
  if (unmatched.size > 0)
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
  const observedAt = Date.parse(response.observedAt);
  const expiresAt = Date.parse(response.expiresAt);
  if (
    expiresAt <= observedAt ||
    expiresAt - observedAt > MAXIMUM_SCOPE_LIFETIME_MS
  )
    throw new Error("PRODUCTION_RESOLUTION_EXPIRY_INVALID");
  if (observedAt > now + MAXIMUM_CLOCK_SKEW_MS)
    throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE");
  if (expiresAt <= now) throw new Error("PRODUCTION_RESOLUTION_EXPIRED");
}

/** Validates the response version negotiated by each request schema. */
export function validateFetchedProductionResolutionResponse(
  requestInput: ProductionResolutionRequest,
  responseInput: ProductionResolutionResponse,
  now: number,
): void {
  const request = normalizeProductionResolutionRequest(requestInput);
  const response = ProductionResolutionResponseSchema.parse(responseInput);
  if (request.schemaVersion === 3 && response.schemaVersion === 2) {
    validateFingerprintResolutionResponse(request, response, now);
    return;
  }
  validateProductionResolutionResponse(request, response, now);
}

function validateFingerprintResolutionResponse(
  request: Extract<ProductionResolutionRequest, { schemaVersion: 3 }>,
  response: Extract<ProductionResolutionResponse, { schemaVersion: 2 }>,
  now: number,
): void {
  if (response.scopeHash !== sha256(canonicalJson(request)))
    throw new Error("PRODUCTION_RESOLUTION_SCOPE_HASH_MISMATCH");
  const { manifestHash: _manifestHash, ...body } = response;
  if (response.manifestHash !== sha256(canonicalJson(body)))
    throw new Error("PRODUCTION_RESOLUTION_MANIFEST_HASH_MISMATCH");
  const unmatched = new Set(request.targets);
  if (response.targets.length !== unmatched.size)
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
  for (const target of response.targets) {
    const requested = [...unmatched].find(
      (candidate) =>
        candidate.lineNfcHash === target.activeSourceFingerprint.lineNfcHash &&
        candidate.promptMaterialHash ===
          target.activeSourceFingerprint.promptMaterialHash,
    );
    if (!requested)
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
    const allowedModels = new Set(requested.modelKeys);
    if (
      target.modelPointers.some(({ modelKey }) => !allowedModels.has(modelKey))
    )
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_MODEL_SCOPE_MISMATCH");
    unmatched.delete(requested);
  }
  if (unmatched.size > 0)
    throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
  const observedAt = Date.parse(response.observedAt);
  const expiresAt = Date.parse(response.expiresAt);
  if (
    expiresAt <= observedAt ||
    expiresAt - observedAt > MAXIMUM_SCOPE_LIFETIME_MS
  )
    throw new Error("PRODUCTION_RESOLUTION_EXPIRY_INVALID");
  if (observedAt > now + MAXIMUM_CLOCK_SKEW_MS)
    throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE");
  if (expiresAt <= now) throw new Error("PRODUCTION_RESOLUTION_EXPIRED");
}

function schemaSql(): string {
  return `
    CREATE TABLE resolution_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      schema_id TEXT NOT NULL,
      schema_version INTEGER NOT NULL CHECK (schema_version = 3),
      observed_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
      poem_count INTEGER NOT NULL CHECK (poem_count BETWEEN 1 AND 50),
      model_count INTEGER NOT NULL CHECK (model_count BETWEEN 1 AND 150),
      scope_sha256 TEXT NOT NULL CHECK (length(scope_sha256) = 64),
      manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64)
    ) STRICT;
    CREATE TABLE poem_resolution (
      source_poem_id TEXT PRIMARY KEY,
      source_author_slug TEXT NOT NULL,
      poem_id TEXT NOT NULL UNIQUE,
      author_id TEXT NOT NULL,
      author_name_arabic TEXT NOT NULL,
      current_source_revision_id TEXT,
      source_pointer_version INTEGER CHECK (source_pointer_version >= 1)
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE scoped_model (
      source_poem_id TEXT NOT NULL REFERENCES poem_resolution(source_poem_id),
      model_key TEXT NOT NULL,
      pointer_version INTEGER CHECK (pointer_version >= 1),
      PRIMARY KEY (source_poem_id, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX poem_resolution_poem_id ON poem_resolution(poem_id);
  `;
}

function sameSnapshot(
  left: ScopedProductionResolutionReport,
  right: ScopedProductionResolutionReport,
): boolean {
  return (
    left.expiresAt === right.expiresAt &&
    left.manifestSha256 === right.manifestSha256 &&
    left.observedAt === right.observedAt &&
    left.scopeSha256 === right.scopeSha256 &&
    left.writerEpoch === right.writerEpoch
  );
}

function assertPromotionIsMonotonic(
  existing: ScopedProductionResolutionReport,
  replacement: ScopedProductionResolutionReport,
): void {
  if (replacement.writerEpoch < existing.writerEpoch)
    throw new Error("PRODUCTION_RESOLUTION_WRITER_EPOCH_ROLLBACK");
  if (
    replacement.writerEpoch === existing.writerEpoch &&
    Date.parse(replacement.observedAt) < Date.parse(existing.observedAt)
  )
    throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_ROLLBACK");
  if (
    replacement.writerEpoch === existing.writerEpoch &&
    replacement.observedAt === existing.observedAt &&
    !sameSnapshot(existing, replacement)
  )
    throw new Error("PRODUCTION_RESOLUTION_SNAPSHOT_EQUIVOCATION");
}

/** Validates an on-disk snapshot for promotion, regardless of TTL freshness. */
async function inspectStoredResolution(
  path: string,
): Promise<ScopedProductionResolutionReport> {
  const input = await existingRegularFile(path, "INPUT");
  const database = new Database(input, { fileMustExist: true, readonly: true });
  try {
    const repository = new ScopedResolutionDatabase(database);
    repository.configureReadonly();
    repository.validateSchema();
    const meta = repository.readMeta();
    repository.validateContents(meta);
    return {
      expiresAt: meta.expiresAt,
      manifestSha256: meta.manifestSha256,
      modelCount: meta.modelCount,
      observedAt: meta.observedAt,
      output: input,
      poemCount: meta.poemCount,
      schemaId: meta.schemaId,
      schemaVersion: meta.schemaVersion,
      scopeSha256: meta.scopeSha256,
      writerEpoch: meta.writerEpoch,
    };
  } finally {
    database.close();
  }
}

async function existingRegularFile(
  value: string,
  label: string,
): Promise<string> {
  const path = resolve(value);
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink() || !metadata.isFile())
    throw new Error(`PRODUCTION_RESOLUTION_${label}_UNSAFE`);
  return path;
}

async function fileIdentity(path: string): Promise<string> {
  const metadata = await lstat(path);
  return `${String(metadata.dev)}:${String(metadata.ino)}`;
}

async function safeOutputPath(value: string): Promise<string> {
  const path = resolve(value);
  const metadata = await lstat(dirname(path));
  if (metadata.isSymbolicLink() || !metadata.isDirectory())
    throw new Error("PRODUCTION_RESOLUTION_OUTPUT_DIRECTORY_UNSAFE");
  if (await pathExists(path)) await existingRegularFile(path, "OUTPUT");
  return path;
}

async function fsyncFile(path: string): Promise<void> {
  const handle = await open(path, constants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, constants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export function productionResolutionManifestHash(
  response: Omit<ProductionResolutionResponse, "manifestHash">,
): string {
  return hash(
    "sha256",
    canonicalJson(ProductionResolutionResponseBodySchema.parse(response)),
  );
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return false;
    throw error;
  }
}

async function rmIfExists(path: string): Promise<void> {
  try {
    await rm(path);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT"))
      throw error;
  }
}
