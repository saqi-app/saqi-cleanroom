import assert from "node:assert/strict";
import test from "node:test";

import type {
  D1Database,
  D1DatabaseSession,
  D1PreparedStatement,
  D1Result,
} from "@cloudflare/workers-types";

import { AUTHOR_PAGE_SIZE, CatalogRepository } from "../src/lib/catalog";

function sessionFixture(batchFailure?: Error) {
  const prepared: D1PreparedStatement[] = [];
  const bound: D1PreparedStatement[] = [];
  const bindings: unknown[][] = [];
  const batches: D1PreparedStatement[][] = [];
  const allReceivers: D1PreparedStatement[] = [];
  let sessionCalls = 0;
  function unexpected(): never {
    throw new Error("Unexpected base database operation");
  }
  function statement(): D1PreparedStatement {
    const native: D1PreparedStatement =
      new (class implements D1PreparedStatement {
        async all<T>(): Promise<D1Result<T>> {
          allReceivers.push(this);
          return {
            results: [],
            success: true,
            meta: {
              changes: 0,
              changed_db: false,
              duration: 0,
              last_row_id: 0,
              rows_read: 0,
              rows_written: 0,
              size_after: 0,
            },
          };
        }
        bind(...values: unknown[]) {
          assert.equal(this, native);
          const result = statement();
          bindings.push(values);
          bound.push(result);
          return result;
        }
        first = unexpected;
        raw = unexpected;
        run = unexpected;
      })();
    return native;
  }
  const session: D1DatabaseSession = new (class implements D1DatabaseSession {
    async batch<T>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]> {
      assert.equal(this, session);
      batches.push(statements);
      if (batchFailure) throw batchFailure;
      return [];
    }
    getBookmark = unexpected;
    prepare() {
      assert.equal(this, session);
      const native = statement();
      prepared.push(native);
      return native;
    }
  })();
  const database: D1Database = new (class implements D1Database {
    batch = unexpected;
    dump = unexpected;
    exec = unexpected;
    prepare = unexpected;
    withSession(...args: unknown[]) {
      assert.equal(this, database);
      assert.deepEqual(args, []);
      sessionCalls += 1;
      return session;
    }
  })();
  return {
    allReceivers,
    batches,
    bindings,
    bound,
    prepared,
    repository: CatalogRepository.fromD1(database),
    sessionCalls: () => sessionCalls,
  };
}

void test("D1 catalog batches the exact bound native statements through one session", async () => {
  const fixture = sessionFixture();
  assert.equal(await fixture.repository.getAuthorPage("poet", 2), undefined);
  assert.equal(fixture.sessionCalls(), 1);
  assert.equal(fixture.batches.length, 1);
  assert.deepEqual(fixture.bindings, [
    ["poet"],
    ["poet", AUTHOR_PAGE_SIZE, AUTHOR_PAGE_SIZE],
  ]);
  assert.equal(fixture.batches[0]?.[0], fixture.bound[0]);
  assert.equal(fixture.batches[0]?.[1], fixture.bound[1]);
  assert.notEqual(fixture.bound[0], fixture.prepared[0]);
  assert.equal(fixture.allReceivers.length, 0);
});

void test("D1 catalog direct queries retain the native statement receiver", async () => {
  const fixture = sessionFixture();
  assert.deepEqual(await fixture.repository.listAuthors(), []);
  assert.equal(fixture.allReceivers[0], fixture.prepared[0]);
  assert.equal(fixture.batches.length, 0);
});

void test("D1 catalog propagates non-schema session failures without replay", async () => {
  const failure = new Error("D1 unavailable");
  const fixture = sessionFixture(failure);
  await assert.rejects(
    fixture.repository.getAuthorPage("poet"),
    (error) => error === failure,
  );
  assert.equal(fixture.batches.length, 1);
  assert.equal(fixture.sessionCalls(), 1);
});

void test("D1 catalog schema fallback keeps every native batch in the same session", async () => {
  const failure = new Error("no such table: poem_model_publication");
  const fixture = sessionFixture(failure);
  await assert.rejects(
    fixture.repository.getAuthorPage("poet"),
    (error) => error === failure,
  );
  assert.equal(fixture.sessionCalls(), 1);
  assert.equal(fixture.batches.length, 3);
  assert.deepEqual(fixture.batches.flat(), fixture.bound);
  assert.equal(fixture.allReceivers.length, 0);
});
