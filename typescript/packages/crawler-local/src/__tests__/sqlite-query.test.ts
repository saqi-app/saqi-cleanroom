import Database from "better-sqlite3";
import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";

import {
  queryAll,
  queryMany,
  queryOne,
  queryOptional,
  queryRequired,
  queryScalar,
  queryStream,
  SqliteBooleanSchema,
  sqliteJsonText,
  SqliteQueryValidationError,
  SqliteSafeIntegerSchema,
} from "../persistence/sqlite-query.js";

const CONTEXT = { operation: "ledger.profileProgress" } as const;
const RowSchema = z
  .strictObject({ item_count: SqliteSafeIntegerSchema, work_key: z.string() })
  .transform(({ item_count, work_key }) => ({
    itemCount: item_count,
    workKey: work_key,
  }));

const ItemRowSchema = z
  .strictObject({ item_id: z.string(), item_total: z.number().int() })
  .transform(({ item_id, item_total }) => ({
    itemId: item_id,
    itemTotal: item_total,
  }));

describe("backward-compatible SQLite queries", () => {
  it("parses real query results and preserves Zod errors", () => {
    const database = new Database(":memory:");
    try {
      database.exec(
        "CREATE TABLE item (item_id TEXT PRIMARY KEY, item_total INTEGER NOT NULL)",
      );
      database
        .prepare("INSERT INTO item (item_id, item_total) VALUES (?, ?), (?, ?)")
        .run("a", 1, "b", 2);

      expect(
        queryAll(
          () =>
            database
              .prepare("SELECT item_id, item_total FROM item ORDER BY item_id")
              .all(),
          ItemRowSchema,
        ),
      ).toEqual([
        { itemId: "a", itemTotal: 1 },
        { itemId: "b", itemTotal: 2 },
      ]);
      expect(
        queryOne(
          () =>
            database
              .prepare("SELECT item_id, item_total FROM item WHERE item_id = ?")
              .get("b"),
          ItemRowSchema,
        ),
      ).toEqual({ itemId: "b", itemTotal: 2 });
      expect(() =>
        queryOne(
          () => database.prepare("SELECT item_id FROM item LIMIT 1").get(),
          ItemRowSchema,
        ),
      ).toThrow(z.ZodError);
    } finally {
      database.close();
    }
  });

  it("preserves an absent queryOne row", () => {
    const database = new Database(":memory:");
    try {
      expect(
        queryOne(
          () => database.prepare("SELECT 1 WHERE 0").get(),
          ItemRowSchema,
        ),
      ).toBeUndefined();
    } finally {
      database.close();
    }
  });
});

describe("SQLite query validation", () => {
  it("distinguishes an absent optional row from a nullable scalar", () => {
    expect(queryOptional(CONTEXT, () => undefined, RowSchema)).toBeUndefined();
    expect(queryScalar(CONTEXT, () => undefined, z.null())).toBeUndefined();
    expect(queryScalar(CONTEXT, () => null, z.null())).toBeNull();
    expect(() => queryRequired(CONTEXT, () => undefined, RowSchema)).toThrow(
      SqliteQueryValidationError,
    );
  });

  it("rejects missing and extra strict-row columns", () => {
    expect(() =>
      queryRequired(CONTEXT, () => ({ item_count: 1 }), RowSchema),
    ).toThrow(SqliteQueryValidationError);
    expect(() =>
      queryRequired(
        CONTEXT,
        () => ({ item_count: 1, secret: "do-not-retain", work_key: "work" }),
        RowSchema,
      ),
    ).toThrow(SqliteQueryValidationError);
  });

  it("validates arrays per row and reports the failing index", () => {
    expect(
      queryMany(
        CONTEXT,
        () => [
          { item_count: 1, work_key: "a" },
          { item_count: 2, work_key: "b" },
        ],
        RowSchema,
      ),
    ).toEqual([
      { itemCount: 1, workKey: "a" },
      { itemCount: 2, workKey: "b" },
    ]);
    try {
      queryMany(
        CONTEXT,
        () => [
          { item_count: 1, work_key: "a" },
          { item_count: "private-value", work_key: "b" },
        ],
        RowSchema,
      );
      expect.unreachable();
    } catch (error) {
      if (!(error instanceof SqliteQueryValidationError)) throw error;
      expect(error.rowIndex).toBe(1);
    }
  });

  it("keeps streaming execution and row parsing lazy", () => {
    const events: string[] = [];
    const rows = queryStream(
      CONTEXT,
      () => {
        events.push("execute");
        return (function* values() {
          events.push("first");
          yield { item_count: 1, work_key: "a" };
          events.push("second");
          yield { item_count: 2, work_key: "b" };
        })();
      },
      RowSchema,
    );
    expect(events).toEqual([]);
    expect(rows.next().value).toEqual({ itemCount: 1, workKey: "a" });
    expect(events).toEqual(["execute", "first"]);
    expect(rows.next().value).toEqual({ itemCount: 2, workKey: "b" });
    expect(events).toEqual(["execute", "first", "second"]);
  });

  it("reports a streaming row index", () => {
    const rows = queryStream(
      CONTEXT,
      () => [
        { item_count: 1, work_key: "a" },
        { item_count: 2, work_key: null },
      ],
      RowSchema,
    );
    rows.next();
    try {
      rows.next();
      expect.unreachable();
    } catch (error) {
      if (!(error instanceof SqliteQueryValidationError)) throw error;
      expect(error.rowIndex).toBe(1);
    }
  });

  it("rejects unsafe integers and transforms SQLite booleans", () => {
    expect(() =>
      queryScalar(
        CONTEXT,
        () => Number.MAX_SAFE_INTEGER + 1,
        SqliteSafeIntegerSchema,
      ),
    ).toThrow(SqliteQueryValidationError);
    expect(queryScalar(CONTEXT, () => 0, SqliteBooleanSchema)).toBe(false);
    expect(queryScalar(CONTEXT, () => 1, SqliteBooleanSchema)).toBe(true);
    expect(queryScalar(CONTEXT, () => 9_007_199_254_740_993n, z.bigint())).toBe(
      9_007_199_254_740_993n,
    );
    expect(() => queryScalar(CONTEXT, () => 2, SqliteBooleanSchema)).toThrow(
      SqliteQueryValidationError,
    );
  });

  it("parses JSON text through its domain schema", () => {
    const schema = sqliteJsonText(z.strictObject({ enabled: z.boolean() }));
    expect(queryScalar(CONTEXT, () => '{"enabled":true}', schema)).toEqual({
      enabled: true,
    });
    expect(() => queryScalar(CONTEXT, () => "not-json", schema)).toThrow(
      SqliteQueryValidationError,
    );
    expect(() =>
      queryScalar(CONTEXT, () => '{"enabled":true,"extra":1}', schema),
    ).toThrow(SqliteQueryValidationError);
  });

  it("never retains raw rows, binds, or unsafe schema paths", () => {
    const secret = "highly-sensitive-bind-and-row-value";
    const schema = z.record(z.string(), z.number());
    try {
      queryRequired(
        { operation: "publication.receipt" },
        () => ({ [secret]: secret }),
        schema,
      );
      expect.unreachable();
    } catch (error) {
      if (!(error instanceof SqliteQueryValidationError)) throw error;
      expect(error.message).not.toContain(secret);
      const diagnostics = [
        error.cardinality,
        error.message,
        error.operation,
        String(error.rowIndex),
        ...error.paths.flatMap((path) => path.map(String)),
      ].join("|");
      expect(diagnostics).not.toContain(secret);
      expect(error.paths).toEqual([["*"]]);
      expect("cause" in error).toBe(false);
    }
  });

  it("rejects unsafe operation identifiers without echoing them", () => {
    const secret = "secret operation with spaces";
    expect(() =>
      queryOptional({ operation: secret }, () => undefined, RowSchema),
    ).toThrow("INVALID_SQLITE_QUERY_OPERATION");
  });

  it("infers transformed outputs without a result-row assertion", () => {
    const row = queryRequired(
      CONTEXT,
      () => ({ item_count: 1, work_key: "a" }),
      RowSchema,
    );
    expectTypeOf(row).toEqualTypeOf<{ itemCount: number; workKey: string }>();
  });
});
