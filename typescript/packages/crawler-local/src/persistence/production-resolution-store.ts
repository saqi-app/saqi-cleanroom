import { createHash, randomUUID } from "node:crypto";
import {
  access,
  chmod,
  constants,
  link,
  lstat,
  open,
  rename,
  unlink,
} from "node:fs/promises";
import { dirname, resolve } from "node:path";

import {
  canonicalAuthorUrl,
  canonicalPoemUrl,
  currentSource,
} from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { z } from "zod";

import type {
  LocalEnrichmentResolution,
  LocalEnrichmentResolver,
} from "../enrichment/local-enrichment-fanout.js";
import { assertCollectedArtifactBinding } from "../enrichment/local-enrichment-fanout.js";
import type { WorkItem } from "./schema.js";
import {
  queryMany,
  queryOptional,
  queryRequired,
  queryScalar,
  queryStream,
  SqliteSafeIntegerSchema,
} from "./sqlite-query.js";
import { canonicalJson, sha256 } from "./work-key.js";

export const PRODUCTION_RESOLUTION_SCHEMA_ID =
  "saqi.production-resolution-store";
export const PRODUCTION_RESOLUTION_SCHEMA_VERSION = 2;
const ForeignKeyViolationsSchema = z.array(z.unknown());
const ExistenceFlagSchema = z.literal(1);
const MaximumResolutionAgeMsSchema = z
  .number()
  .int()
  .min(60_000)
  .max(365 * 24 * 60 * 60_000);
const MAXIMUM_POEMS = 1_000_000;
const MAXIMUM_MODEL_POINTERS = 5_000_000;
const DEFAULT_MAXIMUM_AGE_MS = 30 * 24 * 60 * 60_000;

const ObservedAtSchema = z.iso.datetime({ offset: true });
const SourcePoemIdSchema = z.string().regex(/^[1-9]\d*$/);
const IdentifierSchema = z.string().min(1).max(512);
const PointerVersionSchema = SqliteSafeIntegerSchema.positive();
const SourcePoemRowSchema = z
  .strictObject({
    author_id: IdentifierSchema,
    author_name_arabic: z.string().trim().min(1).max(10_000),
    poem_id: IdentifierSchema,
    source_author_slug: z.string().min(1).max(1_000),
    source_author_url: z.url(),
    source_poem_id: SourcePoemIdSchema,
    source_poem_url: z.url(),
    expected_pointer_version: PointerVersionSchema.nullable(),
    current_revision_id: IdentifierSchema,
  })
  .transform(
    ({
      author_id,
      author_name_arabic,
      current_revision_id,
      expected_pointer_version,
      poem_id,
      source_author_slug,
      source_author_url,
      source_poem_id,
      source_poem_url,
    }) => ({
      authorId: author_id,
      authorNameArabic: author_name_arabic,
      currentRevisionId: current_revision_id,
      expectedPointerVersion: expected_pointer_version,
      poemId: poem_id,
      sourceAuthorSlug: source_author_slug,
      sourceAuthorUrl: source_author_url,
      sourcePoemId: source_poem_id,
      sourcePoemUrl: source_poem_url,
    }),
  );
const PointerRowSchema = z
  .strictObject({
    model_key: z.string().min(1).max(100),
    poem_id: IdentifierSchema,
    pointer_version: PointerVersionSchema,
  })
  .transform(({ model_key, poem_id, pointer_version }) => ({
    modelKey: model_key,
    poemId: poem_id,
    pointerVersion: pointer_version,
  }));
const MetaRowSchema = z
  .strictObject({
    manifest_sha256: z.string().regex(/^[a-f\d]{64}$/),
    model_pointer_count: SqliteSafeIntegerSchema.nonnegative(),
    observed_at: ObservedAtSchema,
    poem_count: SqliteSafeIntegerSchema.nonnegative(),
    schema_id: z.literal(PRODUCTION_RESOLUTION_SCHEMA_ID),
    schema_version: z.literal(PRODUCTION_RESOLUTION_SCHEMA_VERSION),
    writer_epoch: SqliteSafeIntegerSchema.positive(),
  })
  .transform(
    ({
      manifest_sha256,
      model_pointer_count,
      observed_at,
      poem_count,
      schema_id,
      schema_version,
      writer_epoch,
    }) => ({
      manifestSha256: manifest_sha256,
      modelPointerCount: model_pointer_count,
      observedAt: observed_at,
      poemCount: poem_count,
      schemaId: schema_id,
      schemaVersion: schema_version,
      writerEpoch: writer_epoch,
    }),
  );
const ArtifactIdentitySchema = z.looseObject({
  source: z.looseObject({
    author: z.looseObject({ slug: z.string().min(1).max(1_000) }),
    numericId: SourcePoemIdSchema,
  }),
  sourceContext: z
    .strictObject({
      authorNameArabic: z.string().trim().min(1).max(512),
      refreshGeneration: z.string().regex(/^[\w.-]{1,64}$/),
    })
    .optional(),
});
const CollectedIdentityInputSchema = z.looseObject({
  authorNameArabic: z.string().trim().min(1).max(512).optional(),
});
const WriterControlRowSchema = z
  .strictObject({
    singleton: z.literal(1),
    writer_epoch: SqliteSafeIntegerSchema.positive(),
  })
  .transform(({ writer_epoch }) => ({ writerEpoch: writer_epoch }));
const ResolutionCountsSchema = z
  .strictObject({
    models: SqliteSafeIntegerSchema.nonnegative(),
    poems: SqliteSafeIntegerSchema.nonnegative(),
  })
  .transform(({ models, poems }) => ({ modelCount: models, poemCount: poems }));
const QueryPlanRowSchema = z
  .strictObject({
    detail: z.string(),
    id: SqliteSafeIntegerSchema,
    notused: SqliteSafeIntegerSchema,
    parent: SqliteSafeIntegerSchema,
  })
  .transform(({ detail }) => ({ detail }));
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
const ResolutionRowSchema = z
  .strictObject({
    author_id: IdentifierSchema,
    author_name_arabic: z.string().trim().min(1).max(10_000),
    expected_pointer_version: PointerVersionSchema.nullable(),
    current_revision_id: IdentifierSchema,
    poem_id: IdentifierSchema,
    source_author_slug: z.string().min(1).max(1_000),
    source_poem_id: SourcePoemIdSchema,
  })
  .transform(
    ({
      author_id,
      author_name_arabic,
      current_revision_id,
      expected_pointer_version,
      poem_id,
      source_author_slug,
      source_poem_id,
    }) => ({
      authorId: author_id,
      authorNameArabic: author_name_arabic,
      currentRevisionId: current_revision_id,
      expectedPointerVersion: expected_pointer_version,
      poemId: poem_id,
      sourceAuthorSlug: source_author_slug,
      sourcePoemId: source_poem_id,
    }),
  );
const PoemPointerRowSchema = z
  .strictObject({
    current_revision_id: IdentifierSchema,
    expected_pointer_version: PointerVersionSchema.nullable(),
  })
  .transform(({ current_revision_id, expected_pointer_version }) => ({
    currentRevisionId: current_revision_id,
    expectedPointerVersion: expected_pointer_version,
  }));
const ModelPointerRowSchema = z
  .strictObject({ pointer_version: PointerVersionSchema })
  .transform(({ pointer_version }) => ({ pointerVersion: pointer_version }));

export interface ProductionResolutionExportOptions {
  readonly database: string;
  readonly observedAt: string;
  readonly output: string;
}

export interface ProductionResolutionStoreOptions {
  readonly maximumAgeMs?: number;
  readonly now?: () => number;
}

export interface ProductionResolutionStorePort extends LocalEnrichmentResolver {
  close(): void;
  report(): Promise<ProductionResolutionReport>;
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

export interface ProductionResolutionReport {
  readonly database?: string;
  readonly manifestSha256: string;
  readonly modelPointerCount: number;
  readonly observedAt: string;
  readonly output: string;
  readonly poemCount: number;
  readonly promotion?: "created" | "replayed";
  readonly replayed?: boolean;
  readonly schemaId: typeof PRODUCTION_RESOLUTION_SCHEMA_ID;
  readonly schemaVersion: typeof PRODUCTION_RESOLUTION_SCHEMA_VERSION;
  readonly writerEpoch: number;
}

class ProductionResolutionDatabase {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  configureReadonly(): void {
    this.#database.pragma("query_only = ON");
  }

  meta(): z.infer<typeof MetaRowSchema> {
    return queryRequired(
      { operation: "productionResolution.meta" },
      () =>
        this.#database
          .prepare(
            `SELECT schema_id, schema_version, observed_at, writer_epoch,
                  poem_count, model_pointer_count, manifest_sha256
             FROM resolution_meta WHERE singleton = 1`,
          )
          .get(),
      MetaRowSchema,
    );
  }

  validateSchema(): void {
    this.#validateColumns("resolution_meta", [
      "manifest_sha256",
      "model_pointer_count",
      "observed_at",
      "poem_count",
      "schema_id",
      "schema_version",
      "singleton",
      "writer_epoch",
    ]);
    this.#validateColumns("poem_resolution", [
      "author_id",
      "author_name_arabic",
      "current_revision_id",
      "expected_pointer_version",
      "poem_id",
      "source_author_slug",
      "source_poem_id",
    ]);
    this.#validateColumns("model_pointer", [
      "model_key",
      "poem_id",
      "pointer_version",
    ]);
  }

  validateContents(meta: z.infer<typeof MetaRowSchema>): void {
    const integrity = this.#database.pragma("integrity_check", {
      simple: true,
    });
    if (integrity !== "ok")
      throw new Error("PRODUCTION_RESOLUTION_INTEGRITY_FAILED");
    const foreignKeys = ForeignKeyViolationsSchema.parse(
      this.#database.pragma("foreign_key_check"),
    );
    if (foreignKeys.length > 0)
      throw new Error("PRODUCTION_RESOLUTION_FOREIGN_KEY_FAILED");
    const counts = queryRequired(
      { operation: "productionResolution.counts" },
      () =>
        this.#database
          .prepare(
            `SELECT (SELECT count(*) FROM poem_resolution) AS poems,
                  (SELECT count(*) FROM model_pointer) AS models`,
          )
          .get(),
      ResolutionCountsSchema,
    );
    if (
      counts.poemCount !== meta.poemCount ||
      counts.modelCount !== meta.modelPointerCount
    )
      throw new Error("PRODUCTION_RESOLUTION_COUNT_MISMATCH");
    for (const [sql, parameters] of [
      ["SELECT poem_id FROM poem_resolution WHERE source_poem_id = ?", ["1"]],
      ["SELECT source_poem_id FROM poem_resolution WHERE poem_id = ?", ["x"]],
      [
        "SELECT pointer_version FROM model_pointer WHERE poem_id = ? AND model_key = ?",
        ["x", "model"],
      ],
    ] as const) {
      const plans = queryMany(
        { operation: "productionResolution.queryPlan" },
        () =>
          this.#database
            .prepare(`EXPLAIN QUERY PLAN ${sql}`)
            .all(...parameters),
        QueryPlanRowSchema,
      );
      if (!plans.some(({ detail }) => detail.includes("SEARCH")))
        throw new Error("PRODUCTION_RESOLUTION_INDEX_MISSING");
    }
    const manifest = createHash("sha256");
    for (const row of queryStream(
      { operation: "productionResolution.manifestPoems" },
      () =>
        this.#database
          .prepare(
            `SELECT source_poem_id, poem_id, author_id, author_name_arabic,
                    source_author_slug, current_revision_id,
                    expected_pointer_version
               FROM poem_resolution ORDER BY poem_id`,
          )
          .iterate(),
      ResolutionRowSchema,
    )) {
      manifest.update(
        `${canonicalJson({
          kind: "poem",
          ...row,
        })}\n`,
      );
    }
    for (const row of queryStream(
      { operation: "productionResolution.manifestPointers" },
      () =>
        this.#database
          .prepare(
            `SELECT poem_id, model_key, pointer_version FROM model_pointer
              ORDER BY poem_id, model_key`,
          )
          .iterate(),
      PointerRowSchema,
    )) {
      manifest.update(
        `${canonicalJson({
          kind: "model",
          model_key: row.modelKey,
          poem_id: row.poemId,
          pointer_version: row.pointerVersion,
        })}\n`,
      );
    }
    if (manifest.digest("hex") !== meta.manifestSha256)
      throw new Error("PRODUCTION_RESOLUTION_MANIFEST_MISMATCH");
  }

  #validateColumns(table: string, required: readonly string[]): void {
    if (!/^[a-z_]+$/.test(table))
      throw new Error("PRODUCTION_RESOLUTION_TABLE_NAME_INVALID");
    const columns = new Set(
      queryMany(
        { operation: "productionResolution.tableInfo" },
        () => this.#database.prepare(`PRAGMA table_info(${table})`).all(),
        TableInfoRowSchema,
      ),
    );
    if (columns.size === 0)
      throw new Error(`PRODUCTION_RESOLUTION_TABLE_MISSING:${table}`);
    for (const column of required)
      if (!columns.has(column))
        throw new Error(
          `PRODUCTION_RESOLUTION_COLUMN_MISSING:${table}.${column}`,
        );
  }
}

class ProductionResolutionQueries {
  readonly #byPoem: Database.Statement;
  readonly #bySource: Database.Statement;
  readonly #modelPointer: Database.Statement;

  constructor(database: Database.Database) {
    // eslint-disable-next-line @sarj/require-sql-access-class -- This query-store constructor prepares its source lookup on the injected connection.
    this.#bySource = database.prepare(
      `SELECT source_poem_id, poem_id, author_id, author_name_arabic,
              source_author_slug, expected_pointer_version,
              current_revision_id
         FROM poem_resolution WHERE source_poem_id = ?`,
    );
    // eslint-disable-next-line @sarj/require-sql-access-class -- This query-store constructor prepares its poem lookup on the injected connection.
    this.#byPoem = database.prepare(
      `SELECT current_revision_id, expected_pointer_version
         FROM poem_resolution WHERE poem_id = ?`,
    );
    // eslint-disable-next-line @sarj/require-sql-access-class -- This query-store constructor prepares its pointer lookup on the injected connection.
    this.#modelPointer = database.prepare(
      `SELECT pointer_version FROM model_pointer
        WHERE poem_id = ? AND model_key = ?`,
    );
  }

  modelPointer(poemId: string, modelKey: string) {
    return queryOptional(
      { operation: "productionResolution.modelPointer" },
      () => this.#modelPointer.get(poemId, modelKey),
      ModelPointerRowSchema,
    );
  }

  poem(poemId: string) {
    return queryOptional(
      { operation: "productionResolution.poem" },
      () => this.#byPoem.get(poemId),
      PoemPointerRowSchema,
    );
  }

  source(sourcePoemId: string) {
    return queryOptional(
      { operation: "productionResolution.source" },
      () => this.#bySource.get(sourcePoemId),
      ResolutionRowSchema,
    );
  }
}

class ProductionSourceDatabase {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  begin(): void {
    this.#database.exec("BEGIN");
  }

  configureReadonly(): void {
    this.#database.pragma("query_only = ON");
  }

  get inTransaction(): boolean {
    return this.#database.inTransaction;
  }

  modelPointerRows() {
    return queryStream(
      { operation: "productionSource.modelPointers" },
      () =>
        this.#database
          .prepare(
            `SELECT poem_id, model_key, pointer_version
               FROM poem_model_publication_pointer
              ORDER BY poem_id, model_key`,
          )
          .iterate(),
      PointerRowSchema,
    );
  }

  poemRows(legacy: boolean) {
    return queryStream(
      { operation: "productionSource.poems" },
      () =>
        this.#database
          .prepare(
            `SELECT poem.id AS poem_id,
                author.id AS author_id, author.name_arabic AS author_name_arabic,
                source_author.external_id AS source_author_slug,
                source_author.canonical_url AS source_author_url,
                source_poem.external_id AS source_poem_id,
                source_poem.canonical_url AS source_poem_url,
                source_pointer.revision_id AS current_revision_id,
                ${legacy ? "legacy.pointer_version" : "NULL"} AS expected_pointer_version
           FROM source_poem_identity AS source_poem
           JOIN source_author_identity AS source_author
             ON source_author.id = source_poem.source_author_id
            AND source_author.source_name = ?
           JOIN poem ON poem.id = source_poem.canonical_poem_id
           JOIN poem_source_pointer AS source_pointer
             ON source_pointer.source_poem_id = source_poem.id
           JOIN author ON author.id = source_author.canonical_author_id
            AND author.id = poem.author_id
           ${legacy ? "LEFT JOIN poem_publication_pointer AS legacy ON legacy.poem_id = poem.id" : ""}
          WHERE source_poem.source_name = ?
            AND source_poem.tombstoned_at IS NULL
          ORDER BY poem.id`,
          )
          .iterate(currentSource().name, currentSource().name),
      SourcePoemRowSchema,
    );
  }

  readWriterEpoch(): number {
    const rows = queryMany(
      { operation: "productionSource.writerControl" },
      () =>
        this.#database
          .prepare("SELECT singleton, writer_epoch FROM scraper_writer_control")
          .all(),
      WriterControlRowSchema,
    );
    if (rows.length !== 1)
      throw new Error("PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID");
    const [row] = rows;
    if (!row) throw new Error("PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID");
    return row.writerEpoch;
  }

  rollback(): void {
    this.#database.exec("ROLLBACK");
  }

  tableExists(table: string): boolean {
    return (
      queryScalar(
        { operation: "productionSource.tableExists" },
        () =>
          this.#database
            .prepare(
              "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .pluck()
            .get(table),
        ExistenceFlagSchema,
      ) !== undefined
    );
  }

  validateSchema(): void {
    this.#validateColumns("author", ["id", "name_arabic", "slug"]);
    this.#validateColumns("poem", ["author_id", "id", "slug"]);
    this.#validateColumns("scraper_writer_control", [
      "singleton",
      "writer_epoch",
    ]);
    this.#validateColumns("source_author_identity", [
      "canonical_author_id",
      "canonical_url",
      "external_id",
      "id",
      "source_name",
    ]);
    this.#validateColumns("source_poem_identity", [
      "canonical_poem_id",
      "canonical_url",
      "external_id",
      "source_author_id",
      "source_name",
      "tombstoned_at",
    ]);
    this.#validateColumns("poem_source_pointer", [
      "revision_id",
      "source_poem_id",
    ]);
    if (this.tableExists("poem_publication_pointer"))
      this.#validateColumns("poem_publication_pointer", [
        "poem_id",
        "pointer_version",
      ]);
    if (this.tableExists("poem_model_publication_pointer"))
      this.#validateColumns("poem_model_publication_pointer", [
        "model_key",
        "poem_id",
        "pointer_version",
      ]);
    const orphan = queryScalar(
      { operation: "productionSource.orphanPoem" },
      () =>
        this.#database
          .prepare(
            `SELECT 1 FROM poem LEFT JOIN author ON author.id = poem.author_id
              WHERE author.id IS NULL LIMIT 1`,
          )
          .pluck()
          .get(),
      ExistenceFlagSchema,
    );
    if (orphan !== undefined)
      throw new Error("PRODUCTION_RESOLUTION_ORPHAN_POEM");
    const invalidSourceOwnership = queryScalar(
      { operation: "productionSource.invalidOwnership" },
      () =>
        this.#database
          .prepare(
            `SELECT 1
           FROM source_poem_identity AS source_poem
           JOIN source_author_identity AS source_author
             ON source_author.id = source_poem.source_author_id
           LEFT JOIN poem ON poem.id = source_poem.canonical_poem_id
          WHERE source_poem.source_name = ?
            AND source_poem.tombstoned_at IS NULL
            AND (
              source_author.source_name <> ?
              OR poem.id IS NULL
              OR poem.author_id <> source_author.canonical_author_id
            )
          LIMIT 1`,
          )
          .pluck()
          .get(currentSource().name, currentSource().name),
      ExistenceFlagSchema,
    );
    if (invalidSourceOwnership !== undefined)
      throw new Error("PRODUCTION_RESOLUTION_SOURCE_OWNERSHIP_MISMATCH");
    const unmappedPoem = queryScalar(
      { operation: "productionSource.unmappedPoem" },
      () =>
        this.#database
          .prepare(
            `SELECT 1 FROM poem
          WHERE NOT EXISTS (
            SELECT 1 FROM source_poem_identity AS source_poem
            JOIN source_author_identity AS source_author
              ON source_author.id = source_poem.source_author_id
            WHERE source_poem.source_name = ?
              AND source_author.source_name = ?
              AND source_poem.tombstoned_at IS NULL
              AND source_poem.canonical_poem_id = poem.id
              AND source_author.canonical_author_id = poem.author_id
          )
          LIMIT 1`,
          )
          .pluck()
          .get(currentSource().name, currentSource().name),
      ExistenceFlagSchema,
    );
    if (unmappedPoem !== undefined)
      throw new Error("PRODUCTION_RESOLUTION_UNMAPPED_POEM");
  }

  #validateColumns(table: string, required: readonly string[]): void {
    if (!/^[a-z_]+$/.test(table))
      throw new Error("PRODUCTION_RESOLUTION_TABLE_NAME_INVALID");
    const columns = new Set(
      queryMany(
        { operation: "productionSource.tableInfo" },
        () => this.#database.prepare(`PRAGMA table_info(${table})`).all(),
        TableInfoRowSchema,
      ),
    );
    if (columns.size === 0)
      throw new Error(`PRODUCTION_RESOLUTION_TABLE_MISSING:${table}`);
    for (const column of required)
      if (!columns.has(column))
        throw new Error(
          `PRODUCTION_RESOLUTION_COLUMN_MISSING:${table}.${column}`,
        );
  }
}

class ProductionSnapshotDatabase {
  readonly #database: Database.Database;
  readonly #insertModel: Database.Statement;
  readonly #insertPoem: Database.Statement;

  constructor(database: Database.Database) {
    this.#database = database;
    this.#database.pragma("foreign_keys = ON");
    this.#database.pragma("journal_mode = DELETE");
    this.#database.exec(resolutionSchemaSql());
    this.#insertPoem = this.#database.prepare(
      `INSERT INTO poem_resolution (
         source_poem_id, poem_id, author_id, author_name_arabic,
         source_author_slug, current_revision_id, expected_pointer_version
       ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    this.#insertModel = this.#database.prepare(
      `INSERT INTO model_pointer (poem_id, model_key, pointer_version)
       VALUES (?, ?, ?)`,
    );
  }

  begin(): void {
    this.#database.exec("BEGIN IMMEDIATE");
  }

  commit(): void {
    this.#database.exec("COMMIT");
  }

  get inTransaction(): boolean {
    return this.#database.inTransaction;
  }

  insertMeta(
    observedAt: string,
    writerEpoch: number,
    poemCount: number,
    modelPointerCount: number,
    manifestSha256: string,
  ): void {
    this.#database
      .prepare(
        `INSERT INTO resolution_meta (
           singleton, schema_id, schema_version, observed_at, writer_epoch,
           poem_count, model_pointer_count, manifest_sha256
         ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        PRODUCTION_RESOLUTION_SCHEMA_ID,
        PRODUCTION_RESOLUTION_SCHEMA_VERSION,
        observedAt,
        writerEpoch,
        poemCount,
        modelPointerCount,
        manifestSha256,
      );
  }

  insertModel(row: z.infer<typeof PointerRowSchema>): void {
    this.#insertModel.run(row.poemId, row.modelKey, row.pointerVersion);
  }

  insertPoem(
    row: {
      readonly authorId: string;
      readonly authorNameArabic: string;
      readonly expectedPointerVersion: null | number;
      readonly poemId: string;
      readonly sourceAuthorSlug: string;
      readonly sourcePoemId: string;
    },
    currentRevisionId: string,
  ): void {
    this.#insertPoem.run(
      row.sourcePoemId,
      row.poemId,
      row.authorId,
      row.authorNameArabic,
      row.sourceAuthorSlug,
      currentRevisionId,
      row.expectedPointerVersion,
    );
  }

  optimize(): void {
    this.#database.pragma("optimize");
  }

  rollback(): void {
    this.#database.exec("ROLLBACK");
  }
}

/** Indexed, read-only production identity/pointer snapshot. */
export class ProductionResolutionStore implements ProductionResolutionStorePort {
  #database: Database.Database;
  #repository: ProductionResolutionDatabase;
  #queries: ProductionResolutionQueries;
  #fileIdentity: string;
  #ownsDatabase = true;
  readonly #path: string;
  #meta: z.infer<typeof MetaRowSchema>;
  readonly #maximumAgeMs: number;
  readonly #now: () => number;
  #refreshPromise: Promise<void> | undefined;

  static async open(
    path: string,
    options: ProductionResolutionStoreOptions = {},
  ): Promise<ProductionResolutionStore> {
    const input = await existingRegularFile(path, "OUTPUT");
    return new ProductionResolutionStore(
      input,
      await fileIdentity(input),
      options,
    );
  }

  /** @internal Use {@link ProductionResolutionStore.open}. */
  constructor(
    path: string,
    identity: string,
    options: ProductionResolutionStoreOptions = {},
  ) {
    this.#path = path;
    this.#fileIdentity = identity;
    this.#maximumAgeMs = MaximumResolutionAgeMsSchema.parse(
      options.maximumAgeMs ?? DEFAULT_MAXIMUM_AGE_MS,
    );
    this.#now = options.now ?? Date.now;
    this.#database = new Database(this.#path, {
      fileMustExist: true,
      readonly: true,
    });
    this.#repository = new ProductionResolutionDatabase(this.#database);
    try {
      this.#repository.configureReadonly();
      this.#repository.validateSchema();
      this.#meta = this.#repository.meta();
      this.#assertFresh();
      this.#repository.validateContents(this.#meta);
      this.#queries = new ProductionResolutionQueries(this.#database);
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  close(): void {
    if (this.#ownsDatabase) this.#database.close();
    this.#ownsDatabase = false;
  }

  async report(): Promise<ProductionResolutionReport> {
    await this.#refresh();
    this.#assertFresh();
    return {
      manifestSha256: this.#meta.manifestSha256,
      modelPointerCount: this.#meta.modelPointerCount,
      observedAt: this.#meta.observedAt,
      output: this.#path,
      poemCount: this.#meta.poemCount,
      schemaId: this.#meta.schemaId,
      schemaVersion: this.#meta.schemaVersion,
      writerEpoch: this.#meta.writerEpoch,
    };
  }

  async resolve(
    source: WorkItem,
    artifact: unknown,
  ): Promise<LocalEnrichmentResolution | null> {
    await this.#refresh();
    this.#assertFresh();
    assertCollectedArtifactBinding(source, artifact);
    const collected = ArtifactIdentitySchema.parse(artifact);
    const identity = collected.source;
    const row = this.#queries.source(identity.numericId);
    if (!row) {
      const { authorNameArabic: legacyAuthorNameArabic } =
        CollectedIdentityInputSchema.parse(source.input);
      const authorNameArabic =
        collected.sourceContext?.authorNameArabic ?? legacyAuthorNameArabic;
      if (!authorNameArabic) return null;
      const { name: sourceName } = currentSource();
      return {
        mapping: {
          authorId: sha256(
            `author\u{1F}${sourceName}\u{1F}${identity.author.slug}`,
          ),
          authorNameArabic,
          canonicalPoemId: null,
          poemId: sha256(`poem\u{1F}${sourceName}\u{1F}${identity.numericId}`),
          sourceAuthorSlug: identity.author.slug,
          sourcePoemId: identity.numericId,
        },
        observedAt: this.#meta.observedAt,
        writerEpoch: this.#meta.writerEpoch,
      };
    }
    if (row.sourceAuthorSlug !== identity.author.slug)
      throw new Error("PRODUCTION_RESOLUTION_AUTHOR_MISMATCH");
    return {
      mapping: {
        authorId: row.authorId,
        authorNameArabic: row.authorNameArabic,
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
    legacyFallback: boolean,
    requiredSourceRevisionId?: string,
  ): Promise<{
    expectedPointerVersion: null | number;
    writerEpoch: number;
  } | null> {
    await this.#refresh();
    this.#assertFresh();
    const poem = this.#queries.poem(poemId);
    if (!poem) return null;
    if (
      requiredSourceRevisionId !== undefined &&
      poem.currentRevisionId !== requiredSourceRevisionId
    )
      return null;
    const model = this.#queries.modelPointer(poemId, modelKey);
    return {
      expectedPointerVersion:
        model?.pointerVersion ??
        (legacyFallback ? poem.expectedPointerVersion : null),
      writerEpoch: this.#meta.writerEpoch,
    };
  }

  #assertFresh(): void {
    const observedAt = Date.parse(this.#meta.observedAt);
    const age = this.#now() - observedAt;
    if (age < -5 * 60_000)
      throw new Error("PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE");
    if (age > this.#maximumAgeMs)
      throw new Error("PRODUCTION_RESOLUTION_STALE");
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
    const replacement = await ProductionResolutionStore.open(this.#path, {
      maximumAgeMs: this.#maximumAgeMs,
      now: this.#now,
    });
    const prior = this.#database;
    this.#database = replacement.#database;
    this.#repository = replacement.#repository;
    this.#queries = replacement.#queries;
    this.#meta = replacement.#meta;
    this.#fileIdentity = replacement.#fileIdentity;
    replacement.#ownsDatabase = false;
    prior.close();
  }
}

export async function exportProductionResolution(
  options: ProductionResolutionExportOptions,
): Promise<ProductionResolutionReport> {
  const sourcePath = await existingRegularFile(options.database, "DATABASE");
  const outputPath = await safeOutputPath(options.output);
  if (sourcePath === outputPath)
    throw new Error("PRODUCTION_RESOLUTION_PATHS_MUST_BE_DISTINCT");
  const observedAt = ObservedAtSchema.parse(options.observedAt);
  const temporary = `${outputPath}.${String(process.pid)}.${randomUUID()}.tmp`;
  const source = new Database(sourcePath, {
    fileMustExist: true,
    readonly: true,
  });
  let destination: Database.Database | undefined;
  try {
    const sourceRepository = new ProductionSourceDatabase(source);
    sourceRepository.configureReadonly();
    sourceRepository.validateSchema();
    sourceRepository.begin();
    const writerEpoch = sourceRepository.readWriterEpoch();
    destination = new Database(temporary);
    await chmod(temporary, 0o600);
    const snapshot = new ProductionSnapshotDatabase(destination);
    const manifest = createHash("sha256");
    let poemCount = 0;
    let modelPointerCount = 0;
    snapshot.begin();
    try {
      const legacy = sourceRepository.tableExists("poem_publication_pointer");
      const poemRows = sourceRepository.poemRows(legacy);
      for (const base of poemRows) {
        if (++poemCount > MAXIMUM_POEMS)
          throw new Error("PRODUCTION_RESOLUTION_POEM_LIMIT");
        const expectedPointerVersion = base.expectedPointerVersion;
        const sourcePoem = canonicalPoemUrl(base.sourcePoemUrl);
        const sourceAuthor = canonicalAuthorUrl(base.sourceAuthorUrl);
        if (
          sourcePoem.numericId !== base.sourcePoemId ||
          sourceAuthor.slug !== base.sourceAuthorSlug
        )
          throw new Error("PRODUCTION_RESOLUTION_SOURCE_IDENTITY_MISMATCH");
        const row = {
          authorId: base.authorId,
          authorNameArabic: base.authorNameArabic,
          expectedPointerVersion,
          poemId: base.poemId,
          sourceAuthorSlug: sourceAuthor.slug,
          sourcePoemId: sourcePoem.numericId,
        };
        snapshot.insertPoem(row, base.currentRevisionId);
        manifest.update(
          `${canonicalJson({ currentRevisionId: base.currentRevisionId, kind: "poem", ...row })}\n`,
        );
      }
      if (sourceRepository.tableExists("poem_model_publication_pointer")) {
        for (const row of sourceRepository.modelPointerRows()) {
          if (++modelPointerCount > MAXIMUM_MODEL_POINTERS)
            throw new Error("PRODUCTION_RESOLUTION_MODEL_POINTER_LIMIT");
          snapshot.insertModel(row);
          manifest.update(
            `${canonicalJson({
              kind: "model",
              model_key: row.modelKey,
              poem_id: row.poemId,
              pointer_version: row.pointerVersion,
            })}\n`,
          );
        }
      }
      const manifestSha256 = manifest.digest("hex");
      snapshot.insertMeta(
        observedAt,
        writerEpoch,
        poemCount,
        modelPointerCount,
        manifestSha256,
      );
      snapshot.commit();
      sourceRepository.rollback();
      snapshot.optimize();
      destination.close();
      destination = undefined;
      await fsyncFile(temporary);

      const generated = await ProductionResolutionStore.open(temporary);
      const generatedReport = await generated.report();
      generated.close();
      if (await pathExists(outputPath)) {
        const existing = await ProductionResolutionStore.open(outputPath);
        const existingReport = await existing.report();
        existing.close();
        if (sameSnapshot(existingReport, generatedReport)) {
          await unlink(temporary);
          return {
            ...generatedReport,
            database: sourcePath,
            output: outputPath,
            promotion: "replayed",
            replayed: true,
          };
        }
      }
      const generationPath = `${outputPath}.${generatedReport.manifestSha256}.sqlite`;
      try {
        await link(temporary, generationPath);
      } catch (error) {
        if (!isAlreadyExists(error)) throw error;
        const raced = await ProductionResolutionStore.open(generationPath);
        const racedReport = await raced.report();
        raced.close();
        if (!sameSnapshot(racedReport, generatedReport))
          throw new Error("PRODUCTION_RESOLUTION_GENERATION_CONFLICT");
      }
      const current = `${outputPath}.${String(process.pid)}.${randomUUID()}.current`;
      await link(generationPath, current);
      await fsyncFile(current);
      await rename(current, outputPath);
      await unlink(temporary);
      await fsyncDirectory(dirname(outputPath));
      return {
        ...generatedReport,
        database: sourcePath,
        output: outputPath,
        promotion: "created",
        replayed: false,
      };
    } catch (error) {
      if (sourceRepository.inTransaction) sourceRepository.rollback();
      if (snapshot.inTransaction) snapshot.rollback();
      throw error;
    }
  } finally {
    destination?.close();
    source.close();
    await unlinkIfExists(temporary);
  }
}

function resolutionSchemaSql(): string {
  return `
    CREATE TABLE resolution_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      schema_id TEXT NOT NULL,
      schema_version INTEGER NOT NULL CHECK (schema_version = 2),
      observed_at TEXT NOT NULL,
      writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
      poem_count INTEGER NOT NULL CHECK (poem_count >= 0),
      model_pointer_count INTEGER NOT NULL CHECK (model_pointer_count >= 0),
      manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64)
    ) STRICT;
    CREATE TABLE poem_resolution (
      source_poem_id TEXT PRIMARY KEY,
      poem_id TEXT NOT NULL UNIQUE,
      author_id TEXT NOT NULL,
      author_name_arabic TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      current_revision_id TEXT NOT NULL,
      expected_pointer_version INTEGER CHECK (expected_pointer_version >= 1)
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE model_pointer (
      poem_id TEXT NOT NULL REFERENCES poem_resolution(poem_id),
      model_key TEXT NOT NULL,
      pointer_version INTEGER NOT NULL CHECK (pointer_version >= 1),
      PRIMARY KEY (poem_id, model_key)
    ) STRICT, WITHOUT ROWID;
  `;
}

function sameSnapshot(
  left: ProductionResolutionReport,
  right: ProductionResolutionReport,
): boolean {
  return (
    left.manifestSha256 === right.manifestSha256 &&
    left.modelPointerCount === right.modelPointerCount &&
    left.observedAt === right.observedAt &&
    left.poemCount === right.poemCount &&
    left.writerEpoch === right.writerEpoch
  );
}

function isAlreadyExists(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "EEXIST";
}

async function fileIdentity(path: string): Promise<string> {
  const metadata = await lstat(path);
  return `${String(metadata.dev)}:${String(metadata.ino)}`;
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

async function safeOutputPath(value: string): Promise<string> {
  const path = resolve(value);
  const directory = dirname(path);
  const directoryMetadata = await lstat(directory);
  if (directoryMetadata.isSymbolicLink() || !directoryMetadata.isDirectory())
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

async function unlinkIfExists(path: string): Promise<void> {
  try {
    await unlink(path);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT"))
      throw error;
  }
}
