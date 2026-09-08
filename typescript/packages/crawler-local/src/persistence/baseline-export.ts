import { createHash, randomUUID } from "node:crypto";
import { access, constants, link, lstat, open, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

import {
  type BaselineIneligiblePoem,
  baselineIneligibleReason,
  baselineTitleArabic,
  canonicalProductionPoemId,
  ProductionBaselineAuthorRowSchema,
  ProductionBaselineContentSchema,
  ProductionBaselinePoemIdSchema,
  ProductionBaselinePoemRowSchema,
  readNdjson,
} from "./production-baseline-planner.js";
import { queryAll, queryOne } from "./sqlite-query.js";
import { canonicalJson } from "./work-key.js";

const MAX_AUTHORS = 50_000;
const MAX_POEMS = 1_000_000;
const MAX_LINE_BYTES = 1_048_576;
const COPY_BUFFER_BYTES = 64 * 1_024;

const PoemDatabaseRowSchema = z.strictObject({
  active_source_revision_id: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable(),
  author_id: z.uuid().nullable(),
  content_arabic: z.string().min(1).max(MAX_LINE_BYTES),
  id: ProductionBaselinePoemIdSchema,
  insights_missing: z.literal([0, 1]),
  name_arabic: z.string().max(512),
  model_publications_json: z.string().max(MAX_LINE_BYTES),
  slug: z.string().trim().min(1).max(512),
  translation_missing: z.literal([0, 1]),
});
const TableInfoRowSchema = z
  .looseObject({
    name: z.string().min(1),
    pk: z.number().int().nonnegative(),
    type: z.string(),
  })
  .transform(({ name, pk, type }) => ({
    isPrimaryKey: pk === 1,
    name,
    type,
  }));
const TableExistsRowSchema = z
  .strictObject({ found: z.literal(1) })
  .transform(() => true);

export interface BaselineExportOptions {
  readonly authorsOutput: string;
  /** Test-only fault boundary after both durable files validate. */
  readonly beforePromote?: (() => void) | undefined;
  readonly database: string;
  readonly poemsOutput: string;
}

export interface BaselineExportReport {
  readonly authors: Readonly<{
    bytes: number;
    rows: number;
    sha256: string;
  }>;
  readonly database: string;
  readonly outputs: Readonly<{
    authors: string;
    poems: string;
  }>;
  readonly poems: Readonly<{
    bytes: number;
    eligibleRows: number;
    ineligible: readonly BaselineIneligiblePoem[];
    orphans: number;
    rows: number;
    sha256: string;
  }>;
  readonly promotion: "created" | "replayed" | "resumed";
  readonly replayed: boolean;
  readonly schemaId: "saqi.production-baseline-export";
  readonly schemaVersion: 1;
}

interface WrittenFile {
  readonly bytes: number;
  readonly path: string;
  readonly rows: number;
  readonly sha256: string;
}

interface ProductionBaselineExportPort {
  export(options: BaselineExportOptions): Promise<BaselineExportReport>;
}

/** Owns every production-baseline query over an injected read-only connection. */
export class ProductionBaselineExporter implements ProductionBaselineExportPort {
  readonly #database: Database.Database;
  readonly #schema: BaselineSchemaInspector;

  constructor(database: Database.Database) {
    this.#database = database;
    this.#schema = new BaselineSchemaInspector(database);
  }

  async export(options: BaselineExportOptions): Promise<BaselineExportReport> {
    const databasePath = await existingRegularFile(
      options.database,
      "DATABASE",
    );
    const authorsPath = await outputPath(options.authorsOutput);
    const poemsPath = await outputPath(options.poemsOutput);
    if (
      new Set([databasePath, authorsPath, poemsPath]).size !== 3 ||
      authorsPath === poemsPath
    ) {
      throw new Error("BASELINE_EXPORT_PATHS_MUST_BE_DISTINCT");
    }
    await validateExistingOutput(authorsPath);
    await validateExistingOutput(poemsPath);

    const authorsTemporary = temporaryPath(authorsPath);
    const poemsTemporary = temporaryPath(poemsPath);
    let authors: undefined | WrittenFile;
    let poems: undefined | WrittenFile;
    try {
      this.#database.pragma("query_only = ON");
      this.#schema.validate();
      this.#database.exec("BEGIN");
      try {
        const authorNames = new Map<string, string>();
        authors = await writeRows(
          authorsTemporary,
          this.#database
            .prepare("SELECT id, name_arabic, slug FROM author ORDER BY id")
            .iterate(),
          MAX_AUTHORS,
          (value) => {
            const author = ProductionBaselineAuthorRowSchema.parse(value);
            authorNames.set(author.id, author.name_arabic);
            return author;
          },
        );
        const canonicalPoemIds = new Set<string>();
        poems = await writeRows(
          poemsTemporary,
          this.#database
            .prepare(
              `SELECT poem.id, poem.author_id, poem.name_arabic,
                    poem.content_arabic, poem.slug,
                    CASE WHEN translation IS NULL AND translation_gemini IS NULL
                      THEN 1 ELSE 0 END AS translation_missing,
                    CASE WHEN insights IS NULL THEN 1 ELSE 0 END AS insights_missing,
                    ${this.#schema.modelPublicationProjection()}
               FROM poem AS poem ORDER BY poem.id`,
            )
            .iterate(),
          MAX_POEMS,
          (value) => {
            const poem = leanPoem(value, authorNames);
            const canonicalId = canonicalProductionPoemId(poem);
            if (canonicalPoemIds.has(canonicalId))
              throw new Error("BASELINE_EXPORT_POEM_SOURCE_ID_DUPLICATE");
            canonicalPoemIds.add(canonicalId);
            return poem;
          },
        );
      } finally {
        this.#database.exec("ROLLBACK");
      }
      const validation = await validateOutputs(authors, poems);
      options.beforePromote?.();
      const promotion = await reconcileExisting(
        authorsPath,
        authors,
        poemsPath,
        poems,
      );
      if (promotion === "new")
        await promotePair(authors, authorsPath, poems, poemsPath);
      else if (promotion === "replayed") {
        await unlinkIfExists(authors.path);
        await unlinkIfExists(poems.path);
      }
      return {
        authors: {
          bytes: authors.bytes,
          rows: authors.rows,
          sha256: authors.sha256,
        },
        database: databasePath,
        outputs: { authors: authorsPath, poems: poemsPath },
        poems: {
          bytes: poems.bytes,
          eligibleRows: poems.rows - validation.ineligible.length,
          ineligible: validation.ineligible,
          orphans: validation.orphans,
          rows: poems.rows,
          sha256: poems.sha256,
        },
        promotion:
          promotion === "new"
            ? "created"
            : promotion === "resumed"
              ? "resumed"
              : "replayed",
        replayed: promotion !== "new",
        schemaId: "saqi.production-baseline-export",
        schemaVersion: 1,
      };
    } finally {
      await unlinkIfExists(authorsTemporary);
      await unlinkIfExists(poemsTemporary);
    }
  }
}

/** Composition boundary: opens the connection and injects it into the SQL owner. */
export async function exportProductionBaseline(
  options: BaselineExportOptions,
): Promise<BaselineExportReport> {
  const databasePath = await existingRegularFile(options.database, "DATABASE");
  const database = new Database(databasePath, {
    fileMustExist: true,
    readonly: true,
  });
  try {
    return await new ProductionBaselineExporter(database).export(options);
  } finally {
    database.close();
  }
}

class BaselineSchemaInspector {
  readonly #database: Database.Database;

  constructor(database: Database.Database) {
    this.#database = database;
  }

  validate(): void {
    this.#validateTable("author", ["id", "name_arabic", "slug"]);
    this.#validateTable("poem", [
      "author_id",
      "content_arabic",
      "id",
      "insights",
      "name_arabic",
      "slug",
      "translation",
      "translation_gemini",
    ]);
    const activeRevision = this.#tableColumns("poem").has(
      "active_source_revision_id",
    );
    const pointerTable = this.#tableExists("poem_model_publication_pointer");
    const artifactTable = this.#tableExists("model_enrichment_artifact");
    if (pointerTable !== artifactTable || (pointerTable && !activeRevision)) {
      throw new Error("BASELINE_EXPORT_MODEL_SCHEMA_INCOMPLETE");
    }
    if (pointerTable) {
      this.#validateTable("poem_model_publication_pointer", [
        "enrichment_artifact_id",
        "model_key",
        "poem_id",
        "source_revision_id",
      ]);
      this.#validateTable("model_enrichment_artifact", [
        "id",
        "prompt_version",
      ]);
    }
  }

  modelPublicationProjection(): string {
    const activeRevision = this.#tableColumns("poem").has(
      "active_source_revision_id",
    );
    if (!this.#tableExists("poem_model_publication_pointer")) {
      return `${activeRevision ? "poem.active_source_revision_id" : "NULL"} AS active_source_revision_id,
      '[]' AS model_publications_json`;
    }
    return `poem.active_source_revision_id AS active_source_revision_id,
    COALESCE((
      SELECT json_group_array(json_object(
        'model_key', current_publication.model_key,
        'prompt_version', current_publication.prompt_version,
        'source_revision_id', current_publication.source_revision_id
      ))
      FROM (
        SELECT publication.model_key, artifact.prompt_version,
               publication.source_revision_id
        FROM poem_model_publication_pointer AS publication
        JOIN model_enrichment_artifact AS artifact
          ON artifact.id = publication.enrichment_artifact_id
        WHERE publication.poem_id = poem.id
        ORDER BY publication.model_key
      ) AS current_publication
    ), '[]') AS model_publications_json`;
  }

  #validateTable(
    table:
      | "author"
      | "model_enrichment_artifact"
      | "poem_model_publication_pointer"
      | "poem",
    required: readonly string[],
  ): void {
    const rows = this.#tableInfo(table);
    const columns = new Map(rows.map((row) => [row.name, row]));
    if (rows.length === 0)
      throw new Error(`BASELINE_EXPORT_TABLE_MISSING:${table}`);
    for (const name of required) {
      const column = columns.get(name);
      if (!column)
        throw new Error(`BASELINE_EXPORT_COLUMN_MISSING:${table}.${name}`);
      if (column.type.trim().toUpperCase() !== "TEXT")
        throw new Error(`BASELINE_EXPORT_COLUMN_TYPE:${table}.${name}`);
    }
    if (columns.has("id") && !columns.get("id")?.isPrimaryKey)
      throw new Error(`BASELINE_EXPORT_PRIMARY_KEY:${table}.id`);
  }

  #tableInfo(table: string): readonly z.output<typeof TableInfoRowSchema>[] {
    if (!/^[a-z_]+$/.test(table)) throw new Error("BASELINE_EXPORT_TABLE_NAME");
    return queryAll(
      () => this.#database.prepare(`PRAGMA table_info(${table})`).all(),
      TableInfoRowSchema,
    );
  }

  #tableColumns(table: string): ReadonlySet<string> {
    return new Set(this.#tableInfo(table).map(({ name }) => name));
  }

  #tableExists(table: string): boolean {
    return (
      queryOne(
        () =>
          this.#database
            .prepare(
              "SELECT 1 AS found FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .get(table),
        TableExistsRowSchema,
      ) ?? false
    );
  }
}

function leanPoem(
  value: unknown,
  authorNames: ReadonlyMap<string, string>,
): z.infer<typeof ProductionBaselinePoemRowSchema> {
  const row = PoemDatabaseRowSchema.parse(value);
  let modelPublications: unknown;
  try {
    modelPublications = JSON.parse(row.model_publications_json);
  } catch {
    throw new Error(`BASELINE_EXPORT_MODEL_POINTER_INVALID:${row.id}`);
  }
  let content: unknown;
  try {
    content = JSON.parse(row.content_arabic);
  } catch {
    throw new Error(`BASELINE_EXPORT_CONTENT_INVALID:${row.id}`);
  }
  const parsedContent = ProductionBaselineContentSchema.parse(content);
  const titleArabic = baselineTitleArabic(
    row.name_arabic,
    parsedContent.content,
  );
  const ineligibleReason = baselineIneligibleReason(
    titleArabic,
    parsedContent.content,
    row.author_id === null ? null : (authorNames.get(row.author_id) ?? null),
  );
  return ProductionBaselinePoemRowSchema.parse({
    ...(row.active_source_revision_id === null
      ? {}
      : { active_source_revision_id: row.active_source_revision_id }),
    author_id: row.author_id,
    content_arabic: parsedContent,
    id: row.id,
    ...(ineligibleReason === null
      ? {}
      : { ineligible_reason: ineligibleReason }),
    insights_missing: row.insights_missing,
    model_publications: modelPublications,
    name_arabic: titleArabic ?? "",
    slug: row.slug,
    translation_missing: row.translation_missing,
  });
}

async function writeRows(
  path: string,
  rows: Iterable<unknown>,
  maximumRows: number,
  parse: (value: unknown) => unknown,
): Promise<WrittenFile> {
  const handle = await open(
    path,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY,
    0o600,
  );
  const hash = createHash("sha256");
  let bytes = 0;
  let count = 0;
  try {
    for (const value of rows) {
      if (++count > maximumRows) throw new Error("BASELINE_EXPORT_ROW_LIMIT");
      const line = `${ndjsonJson(parse(value))}\n`;
      const lineBytes = Buffer.byteLength(line);
      if (lineBytes > MAX_LINE_BYTES)
        throw new Error(`BASELINE_EXPORT_LINE_TOO_LARGE:${String(count)}`);
      // eslint-disable-next-line no-await-in-loop -- NDJSON rows must preserve database iteration order and descriptor position.
      await writeAll(handle, line);
      hash.update(line);
      bytes += lineBytes;
    }
    await handle.sync();
  } finally {
    await handle.close();
  }
  return { bytes, path, rows: count, sha256: hash.digest("hex") };
}

function ndjsonJson(value: unknown): string {
  return canonicalJson(value)
    .replaceAll("\u{2028}", String.raw`\u2028`)
    .replaceAll("\u{2029}", String.raw`\u2029`);
}

async function validateOutputs(
  authors: WrittenFile,
  poems: WrittenFile,
): Promise<{
  readonly ineligible: readonly BaselineIneligiblePoem[];
  readonly orphans: number;
}> {
  if (
    (await fileDigest(authors.path)) !== authors.sha256 ||
    (await fileDigest(poems.path)) !== poems.sha256
  ) {
    throw new Error("BASELINE_EXPORT_VALIDATION_HASH_MISMATCH");
  }
  const authorIds = new Set<string>();
  let authorCount = 0;
  for await (const value of readNdjson(authors.path)) {
    const author = ProductionBaselineAuthorRowSchema.parse(value);
    authorIds.add(author.id);
    authorCount += 1;
  }
  let orphanCount = 0;
  const ineligible: BaselineIneligiblePoem[] = [];
  let poemCount = 0;
  for await (const value of readNdjson(poems.path)) {
    const poem = ProductionBaselinePoemRowSchema.parse(value);
    if (poem.ineligible_reason !== undefined) {
      ineligible.push({
        canonicalSourceId: canonicalProductionPoemId(poem),
        poemId: poem.id,
        reason: poem.ineligible_reason,
        slug: poem.slug,
      });
    }
    if (poem.author_id === null || !authorIds.has(poem.author_id))
      orphanCount += 1;
    poemCount += 1;
  }
  if (authorCount !== authors.rows || poemCount !== poems.rows)
    throw new Error("BASELINE_EXPORT_VALIDATION_COUNT_MISMATCH");
  return { ineligible, orphans: orphanCount };
}

async function reconcileExisting(
  authorsPath: string,
  authors: WrittenFile,
  poemsPath: string,
  poems: WrittenFile,
): Promise<"new" | "replayed" | "resumed"> {
  const authorsExists = await pathExists(authorsPath);
  const poemsExists = await pathExists(poemsPath);
  if (!authorsExists && !poemsExists) return "new";
  if (authorsExists && (await fileDigest(authorsPath)) !== authors.sha256)
    throw new Error("BASELINE_EXPORT_OUTPUT_EXISTS_DIFFERENT");
  if (poemsExists && (await fileDigest(poemsPath)) !== poems.sha256)
    throw new Error("BASELINE_EXPORT_OUTPUT_EXISTS_DIFFERENT");
  if (authorsExists && poemsExists) return "replayed";
  if (authorsExists) {
    await promoteOne(poems.path, poemsPath);
    await unlinkIfExists(poems.path);
    await unlinkIfExists(authors.path);
  } else {
    await promoteOne(authors.path, authorsPath);
    await unlinkIfExists(authors.path);
    await unlinkIfExists(poems.path);
  }
  await fsyncDirectory(dirname(authorsPath));
  if (dirname(poemsPath) !== dirname(authorsPath))
    await fsyncDirectory(dirname(poemsPath));
  return "resumed";
}

async function promotePair(
  authors: WrittenFile,
  authorsPath: string,
  poems: WrittenFile,
  poemsPath: string,
): Promise<void> {
  await validateExistingOutput(authorsPath);
  await validateExistingOutput(poemsPath);
  if ((await pathExists(authorsPath)) || (await pathExists(poemsPath)))
    throw new Error("BASELINE_EXPORT_OUTPUT_RACE");
  let authorsPromoted = false;
  let poemsPromoted = false;
  try {
    await promoteOne(authors.path, authorsPath);
    authorsPromoted = true;
    await unlinkIfExists(authors.path);
    await promoteOne(poems.path, poemsPath);
    poemsPromoted = true;
    await unlinkIfExists(poems.path);
    await fsyncDirectory(dirname(authorsPath));
    if (dirname(poemsPath) !== dirname(authorsPath))
      await fsyncDirectory(dirname(poemsPath));
  } catch (error) {
    if (authorsPromoted) await unlinkIfExists(authorsPath);
    if (poemsPromoted) await unlinkIfExists(poemsPath);
    await fsyncDirectory(dirname(authorsPath));
    if (dirname(poemsPath) !== dirname(authorsPath))
      await fsyncDirectory(dirname(poemsPath));
    throw error;
  }
}

async function promoteOne(temporary: string, output: string): Promise<void> {
  await link(temporary, output);
}

async function outputPath(value: string): Promise<string> {
  const path = resolve(value);
  const directory = dirname(path);
  if (!(await pathExists(directory)))
    throw new Error("BASELINE_EXPORT_OUTPUT_DIRECTORY_MISSING");
  const metadata = await lstat(directory);
  if (metadata.isSymbolicLink() || !metadata.isDirectory())
    throw new Error("BASELINE_EXPORT_OUTPUT_DIRECTORY_UNSAFE");
  return path;
}

async function existingRegularFile(
  value: string,
  label: string,
): Promise<string> {
  const path = resolve(value);
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink() || !metadata.isFile())
    throw new Error(`BASELINE_EXPORT_${label}_UNSAFE`);
  return path;
}

async function validateExistingOutput(path: string): Promise<void> {
  if (!(await pathExists(path))) return;
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink() || !metadata.isFile())
    throw new Error("BASELINE_EXPORT_OUTPUT_UNSAFE");
}

function temporaryPath(path: string): string {
  return `${path}.${String(process.pid)}.${randomUUID()}.tmp`;
}

async function fileDigest(path: string): Promise<string> {
  const handle = await open(path, constants.O_RDONLY);
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(COPY_BUFFER_BYTES);
  try {
    for (;;) {
      // eslint-disable-next-line no-await-in-loop -- A single descriptor is streamed sequentially into the digest.
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
      if (bytesRead === 0) break;
      hash.update(buffer.subarray(0, bytesRead));
    }
  } finally {
    await handle.close();
  }
  return hash.digest("hex");
}

async function writeAll(
  handle: Awaited<ReturnType<typeof open>>,
  value: string,
): Promise<void> {
  const buffer = Buffer.from(value);
  let offset = 0;
  while (offset < buffer.length) {
    // eslint-disable-next-line no-await-in-loop -- Partial writes must complete before advancing the descriptor offset.
    const { bytesWritten } = await handle.write(
      buffer,
      offset,
      buffer.length - offset,
    );
    if (bytesWritten < 1) throw new Error("BASELINE_EXPORT_WRITE_STALLED");
    offset += bytesWritten;
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

async function unlinkIfExists(path: string): Promise<void> {
  try {
    await unlink(path);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT"))
      throw error;
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
