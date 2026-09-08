import { z } from "zod";

const SqliteQueryOperationSchema = z
  .string()
  .regex(/^[A-Za-z][A-Za-z0-9_.:-]{0,127}$/);

export const SqliteSafeIntegerSchema = z
  .number()
  .int()
  .min(Number.MIN_SAFE_INTEGER)
  .max(Number.MAX_SAFE_INTEGER);

export const SqliteBooleanSchema = z
  .literal([0, 1])
  .transform((value) => value === 1);

export interface SqliteQueryContext {
  readonly operation: string;
}

export type SqliteQueryCardinality =
  "many" | "optional" | "required" | "scalar" | "stream";

/** A deliberately lossy validation error. It retains enough structure for
 * diagnostics without retaining the SQL, binds, raw row, or Zod error. */
export class SqliteQueryValidationError extends Error {
  readonly cardinality: SqliteQueryCardinality;
  readonly operation: string;
  readonly paths: readonly (readonly (number | string)[])[];
  readonly rowIndex: null | number;

  constructor(input: {
    readonly cardinality: SqliteQueryCardinality;
    readonly operation: string;
    readonly paths: readonly (readonly (number | string)[])[];
    readonly rowIndex: null | number;
  }) {
    const paths = input.paths
      .map((path) => (path.length === 0 ? "$" : path.join(".")))
      .join(",");
    super(
      `SQLITE_ROW_VALIDATION_FAILED:${input.operation}:${input.cardinality}:${input.rowIndex === null ? "row" : String(input.rowIndex)}:${paths}`,
    );
    this.name = "SqliteQueryValidationError";
    this.cardinality = input.cardinality;
    this.operation = input.operation;
    this.paths = input.paths;
    this.rowIndex = input.rowIndex;
  }
}

export function sqliteJsonText<Output>(schema: z.ZodType<Output>) {
  return z
    .string()
    .transform((value, context): unknown => {
      try {
        return JSON.parse(value);
      } catch {
        context.addIssue({ code: "custom", message: "Invalid JSON text" });
        return z.NEVER;
      }
    })
    .pipe(schema);
}

export function queryOptional<Output>(
  context: SqliteQueryContext,
  execute: () => unknown,
  schema: z.ZodType<Output>,
): Output | undefined {
  const operation = operationId(context);
  const row = execute();
  return row === undefined
    ? undefined
    : parseValue(operation, "optional", schema, row, null);
}

export function queryRequired<Output>(
  context: SqliteQueryContext,
  execute: () => unknown,
  schema: z.ZodType<Output>,
): Output {
  const operation = operationId(context);
  const row = execute();
  if (row === undefined) {
    throw validationError(operation, "required", null, [[]]);
  }
  return parseValue(operation, "required", schema, row, null);
}

export function queryMany<Output>(
  context: SqliteQueryContext,
  execute: () => unknown,
  schema: z.ZodType<Output>,
): readonly Output[] {
  const operation = operationId(context);
  const rows = execute();
  if (!Array.isArray(rows)) {
    throw validationError(operation, "many", null, [[]]);
  }
  return rows.map((row, index) =>
    parseValue(operation, "many", schema, row, index),
  );
}

export function queryScalar<Output>(
  context: SqliteQueryContext,
  execute: () => unknown,
  schema: z.ZodType<Output>,
): Output | undefined {
  const operation = operationId(context);
  const value = execute();
  return value === undefined
    ? undefined
    : parseValue(operation, "scalar", schema, value, null);
}

export function queryStream<Output>(
  context: SqliteQueryContext,
  execute: () => unknown,
  schema: z.ZodType<Output>,
): IterableIterator<Output> {
  const operation = operationId(context);
  return (function* parseRows() {
    const rows = execute();
    if (!isIterable(rows)) {
      throw validationError(operation, "stream", null, [[]]);
    }
    let index = 0;
    for (const row of rows) {
      yield parseValue(operation, "stream", schema, row, index);
      index += 1;
    }
  })();
}

/** Executes and parses a non-streaming SQLite query boundary. The callback
 * keeps better-sqlite3 bind parameter types local to the prepared statement,
 * while callers receive only schema-validated domain values. */
export function queryOne<Output>(
  execute: () => unknown,
  schema: z.ZodType<Output>,
): Output | undefined {
  const row = execute();
  return row === undefined ? undefined : schema.parse(row);
}

export function queryAll<Output>(
  execute: () => unknown,
  schema: z.ZodType<Output>,
): readonly Output[] {
  return z.array(schema).parse(execute());
}

function operationId(context: SqliteQueryContext): string {
  const parsed = SqliteQueryOperationSchema.safeParse(context.operation);
  if (!parsed.success) throw new Error("INVALID_SQLITE_QUERY_OPERATION");
  return parsed.data;
}

function parseValue<Output>(
  operation: string,
  cardinality: SqliteQueryCardinality,
  schema: z.ZodType<Output>,
  value: unknown,
  rowIndex: null | number,
): Output {
  try {
    const parsed = schema.safeParse(value);
    if (parsed.success) return parsed.data;
    throw validationError(
      operation,
      cardinality,
      rowIndex,
      parsed.error.issues.map(({ path }) => sanitizePath(path)),
    );
  } catch (error) {
    if (error instanceof SqliteQueryValidationError) throw error;
    throw validationError(operation, cardinality, rowIndex, [[]]);
  }
}

function validationError(
  operation: string,
  cardinality: SqliteQueryCardinality,
  rowIndex: null | number,
  paths: readonly (readonly (number | string)[])[],
): SqliteQueryValidationError {
  return new SqliteQueryValidationError({
    cardinality,
    operation,
    paths,
    rowIndex,
  });
}

function sanitizePath(path: PropertyKey[]): readonly (number | string)[] {
  return path.slice(0, 16).map((part) => {
    if (typeof part === "number") return part;
    if (typeof part === "string" && /^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(part))
      return part;
    return "*";
  });
}

function isIterable(value: unknown): value is Iterable<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.iterator in value &&
    typeof value[Symbol.iterator] === "function"
  );
}
