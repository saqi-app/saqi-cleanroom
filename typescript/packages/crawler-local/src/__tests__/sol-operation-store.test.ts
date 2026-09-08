import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import Database from "better-sqlite3";
import { afterEach, expect, test } from "vitest";

import { LedgerMigrator } from "../persistence/migrations.js";
import {
  type SolOperationClaim,
  SolOperationStore,
} from "../persistence/sol-operation-store.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const DATABASES: Database.Database[] = [];
const execFileAsync = promisify(execFile);
afterEach(() => {
  for (const connection of DATABASES) connection.close();
  DATABASES.length = 0;
});

function database(path = ":memory:"): Database.Database {
  const connection = new Database(path, { timeout: 10 });
  DATABASES.push(connection);
  connection.pragma("foreign_keys = ON");
  connection.pragma("synchronous = FULL");
  return connection;
}

function fixture() {
  const path = join(
    trackedMkdtempSync(join(tmpdir(), "sol-operation-")),
    "ledger.sqlite3",
  );
  const connection = database(path);
  connection.pragma("journal_mode = WAL");
  new LedgerMigrator(connection).migrate();
  connection.exec(
    "INSERT INTO runtime_control VALUES('sol_operation_import_complete', 1)",
  );
  connection
    .prepare("INSERT INTO sol_operation_import_receipt VALUES(1, ?, 0, 0, 0)")
    .run("c".repeat(64));
  return { connection, path, store: new SolOperationStore(connection) };
}

function claim(operationKey = "a".repeat(64)): SolOperationClaim {
  return {
    attemptId: randomUUID(),
    inputHash: "b".repeat(64),
    operationKey,
    kind: "generation",
    model: "gpt-5.6-sol",
    modelKey: "sol",
    pipelineVersion: "sol-word-gloss-v2",
    provider: "sol",
    reasoningEffort: "high",
  };
}

test("historical cleanup is fenced to its session and cannot alter a newer attempt", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  const sessionId = randomUUID();
  expect(store.recordSession(first, sessionId, 2)).toBe(true);
  expect(
    store.recordTerminal(first, {
      state: "known_rejection",
      exitCode: 1,
      signal: null,
      finishedAt: 3,
    }),
  ).toBe(true);
  const second = store.claim(claim(), 4).current;
  const pending = {
    code: "CODEX_SESSION_CLEANUP_PENDING",
    detail: "fixture",
    observedAt: 5,
    sessionId,
  } as const;
  expect(store.recordSessionCleanupPending(first, pending)).toBe(true);
  expect(store.readAttempt(first)?.observations.cleanupPending).toEqual(
    pending,
  );
  expect(store.readCurrent(first.operationKey)).toEqual(second);
  expect(
    store.recordSessionCleanupPending(
      { ...first, claimEpoch: second.claimEpoch },
      pending,
    ),
  ).toBe(false);
  expect(
    store.recordSessionCleanupPending(first, {
      ...pending,
      sessionId: randomUUID(),
    }),
  ).toBe(false);
  expect(
    store.recordSessionCleanupPending(first, { ...pending, observedAt: 2 }),
  ).toBe(false);
  expect(
    store.recordReconciliation(first, {
      checkedAt: 6,
      state: "unresolved_no_valid_retained_artifact",
      strategy: "artifact_only_v2",
    }),
  ).toBe(false);
  const cleanup = {
    cleanedAt: 6,
    sessionId,
    state: "removed_after_result_promotion",
  } as const;
  expect(store.recordSessionCleanup(first, cleanup)).toBe(true);
  expect(store.recordSessionCleanup(first, cleanup)).toBe(true);
  expect(
    store.recordSessionCleanupPending(first, { ...pending, observedAt: 7 }),
  ).toBe(false);
  expect(store.readAttempt(first)?.observations).toMatchObject({
    cleanup,
    cleanupPending: null,
  });
  expect(store.readCurrent(first.operationKey)).toEqual(second);
});

test("cleanup cannot predate a late recovered session observation", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  store.recordTerminal(first, {
    state: "known_success",
    exitCode: 0,
    signal: null,
    finishedAt: 2,
  });
  const sessionId = randomUUID();
  expect(store.recordSession(first, sessionId, 10)).toBe(true);
  const cleanup = {
    state: "removed_after_result_promotion",
    sessionId,
    cleanedAt: 9,
  } as const;
  const pending = {
    code: "CODEX_SESSION_CLEANUP_PENDING",
    detail: "fixture",
    observedAt: 9,
    sessionId,
  } as const;
  expect(store.recordSessionCleanup(first, cleanup)).toBe(false);
  expect(store.recordSessionCleanupPending(first, pending)).toBe(false);
  expect(store.readAttempt(first)?.observations).toMatchObject({
    cleanup: null,
    cleanupPending: null,
  });
  expect(
    store.recordSessionCleanupPending(first, { ...pending, observedAt: 10 }),
  ).toBe(true);
  expect(store.recordSessionCleanup(first, { ...cleanup, cleanedAt: 10 })).toBe(
    true,
  );
});

test("reconciliation preserves ambiguous spend fencing and rejects cleanup", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  const sessionId = randomUUID();
  store.recordSession(first, sessionId, 2);
  store.recordTerminal(first, {
    state: "unknown",
    exitCode: null,
    signal: null,
    finishedAt: 3,
  });
  const observation = {
    checkedAt: 4,
    state: "unresolved_no_valid_retained_artifact",
    strategy: "artifact_only_v2",
  } as const;
  expect(store.recordReconciliation(first, observation)).toBe(true);
  expect(
    store.recordReconciliation(first, { ...observation, checkedAt: 2 }),
  ).toBe(false);
  expect(
    store.recordSessionCleanup(first, {
      cleanedAt: 5,
      sessionId,
      state: "removed_after_result_promotion",
    }),
  ).toBe(false);
  expect(store.readCurrent(first.operationKey)?.state).toBe("unknown");
  expect(store.claim(claim(), 1000).claimed).toBe(false);
});

test("credential observations allow one fenced completion without replacing before evidence", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  const pending = {
    schemaId: "saqi.sol-credential-observation",
    schemaVersion: 1,
    before: { state: "transient", observedAt: 2 },
    after: null,
    classification: "pending",
  } as const;
  expect(store.recordCredentialObservation(first, pending)).toBe(true);
  const completed = {
    ...pending,
    after: { state: "transient", observedAt: 3 },
    classification: "transient",
  } as const;
  expect(store.recordCredentialObservation(first, completed)).toBe(true);
  expect(store.recordCredentialObservation(first, pending)).toBe(false);
  expect(
    store.recordCredentialObservation(first, {
      ...completed,
      before: { state: "transient", observedAt: 1 },
    }),
  ).toBe(false);
  expect(
    store.recordCredentialObservation({ ...first, claimEpoch: 2 }, completed),
  ).toBe(false);
  expect(store.readAttempt(first)?.observations.credential).toEqual(completed);
});

test("malformed persisted observation JSON fails closed", () => {
  const { connection, store } = fixture();
  const first = store.claim(claim(), 1).current;
  connection
    .prepare(
      "UPDATE sol_invocation_attempt SET observations_json = ? WHERE attempt_id = ?",
    )
    .run('{"unexpected":true}', first.attemptId);
  expect(() => store.readCurrent(first.operationKey)).toThrow();
  expect(() => store.readAttempt(first)).toThrow();
  expect(() => store.claim(claim(), 2)).toThrow();
});

test("committed intent fences a second connection and process restart without expiry rearming", () => {
  const { path, store } = fixture();
  const first = store.claim(claim(), 1);
  const reopened = new SolOperationStore(database(path));
  expect(first.claimed).toBe(true);
  expect(reopened.claim(claim(), 10 ** 12)).toEqual({
    claimed: false,
    current: first.current,
  });
});

test("independent processes racing the same key authorize exactly one dispatch", async () => {
  const { path } = fixture();
  const moduleUrl = new URL(
    "../persistence/sol-operation-store.ts",
    import.meta.url,
  ).href;
  const invoke = () =>
    execFileAsync(
      process.execPath,
      [
        "--import",
        "tsx",
        "--input-type=module",
        "-e",
        `
    import Database from 'better-sqlite3';
    import { SolOperationStore } from ${JSON.stringify(moduleUrl)};
    const database = new Database(${JSON.stringify(path)}, { timeout: 1000 });
    database.pragma('foreign_keys = ON');
    database.pragma('synchronous = FULL');
    try { console.log(new SolOperationStore(database).claim(${JSON.stringify(claim())}, 1).claimed); }
    finally { database.close(); }
  `,
      ],
      { timeout: 10000 },
    );
  const results = await Promise.all([invoke(), invoke()]);
  expect(results.map(({ stdout }) => stdout.trim()).toSorted()).toEqual([
    "false",
    "true",
  ]);
});

test("claim requires completed import and cannot treat database failure as permission", () => {
  const { connection, store } = fixture();
  connection.exec("DELETE FROM runtime_control");
  expect(() => store.claim(claim(), 1)).toThrow();
  expect(store.readCurrent("a".repeat(64))).toBeUndefined();
  connection.exec("DROP TABLE sol_operation");
  expect(() => store.claim(claim(), 1)).toThrow();
});

test("marker without its immutable import receipt cannot authorize a claim", () => {
  const { connection, store } = fixture();
  connection.exec(
    "DROP TRIGGER sol_operation_import_receipt_reject_delete; DELETE FROM sol_operation_import_receipt",
  );
  expect(() => store.assertImported()).toThrow();
  expect(() => store.claim(claim(), 1)).toThrow();
  expect(store.readCurrent("a".repeat(64))).toBeUndefined();
});

test("unknown outcome never permits rejection downgrade or new spend", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  expect(store.recordTurnStarted(first, 2)).toBe(true);
  expect(
    store.recordTerminal(first, {
      state: "known_rejection",
      exitCode: 1,
      signal: null,
      finishedAt: 3,
    }),
  ).toBe(false);
  expect(
    store.recordTerminal(first, {
      state: "unknown",
      exitCode: 1,
      signal: null,
      finishedAt: 3,
    }),
  ).toBe(true);
  expect(
    store.recordTerminal(first, {
      state: "known_rejection",
      exitCode: 1,
      signal: null,
      finishedAt: 4,
    }),
  ).toBe(false);
  expect(store.claim(claim(), 10 ** 12).claimed).toBe(false);
  expect(store.markInvalid(first)).toBe(false);
});

test("known rejection admits one new epoch and rejects stale callbacks", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  expect(
    store.recordTerminal(first, {
      state: "known_rejection",
      exitCode: 1,
      signal: null,
      finishedAt: 2,
    }),
  ).toBe(true);
  const second = store.claim(claim(), 3);
  expect(second).toMatchObject({ claimed: true, current: { claimEpoch: 2 } });
  expect(
    store.recordTerminal(first, {
      state: "unknown",
      exitCode: null,
      signal: "SIGTERM",
      finishedAt: 4,
    }),
  ).toBe(false);
  expect(store.recordSession(first, randomUUID(), 4)).toBe(false);
  expect(store.markInvalid(first)).toBe(false);
  expect(store.readCurrent(first.operationKey)?.attemptId).toBe(
    second.current.attemptId,
  );
});

test("successful operation remains fenced unless explicitly invalidated", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  store.recordTerminal(first, {
    state: "known_success",
    exitCode: 0,
    signal: null,
    finishedAt: 2,
  });
  expect(store.claim(claim(), 3).claimed).toBe(false);
  expect(store.markInvalid(first)).toBe(true);
  expect(store.claim(claim(), 4).claimed).toBe(true);
});

test("session identity is stable and material-sharing does not require equal input hashes", () => {
  const { store } = fixture();
  const first = store.claim(claim(), 1).current;
  const sessionId = randomUUID();
  expect(store.recordSession(first, sessionId, 2)).toBe(true);
  expect(store.recordSession(first, sessionId, 3)).toBe(true);
  expect(() => store.recordSession(first, randomUUID(), 4)).toThrow(
    "SOL_OPERATION_SESSION_MISMATCH",
  );
  expect(
    store.claim({ ...claim(), inputHash: "c".repeat(64) }, 5).claimed,
  ).toBe(false);
  expect(() =>
    store.claim({ ...claim(), pipelineVersion: "other" }, 5),
  ).toThrow("SOL_OPERATION_IDENTITY_MISMATCH");
});

test("locked claim fails atomically and retries only after the transaction is available", () => {
  const { connection, path, store } = fixture();
  const other = database(path);
  connection.exec("BEGIN IMMEDIATE");
  expect(() => new SolOperationStore(other).claim(claim(), 1)).toThrow();
  connection.exec("ROLLBACK");
  expect(store.readCurrent("a".repeat(64))).toBeUndefined();
  expect(new SolOperationStore(other).claim(claim(), 2).claimed).toBe(true);
});

test("failed head write rolls back its attempt insertion", () => {
  const { connection, store } = fixture();
  connection.exec(`CREATE TRIGGER reject_head BEFORE INSERT ON sol_operation
    BEGIN SELECT RAISE(ABORT, 'test failure'); END`);
  expect(() => store.claim(claim(), 1)).toThrow();
  expect(
    connection
      .prepare("SELECT COUNT(*) AS count FROM sol_invocation_attempt")
      .get(),
  ).toEqual({ count: 0 });
});

test("claim cannot authorize dispatch from an uncommitted outer transaction", () => {
  const { connection, store } = fixture();
  connection.exec("BEGIN IMMEDIATE");
  expect(() => store.claim(claim(), 1)).toThrow(
    "SOL_OPERATION_CLAIM_REQUIRES_TOP_LEVEL_TRANSACTION",
  );
  connection.exec("ROLLBACK");
  expect(store.readCurrent("a".repeat(64))).toBeUndefined();
});

test("store rejects nondurable or unconstrained SQLite connections", () => {
  const { connection } = fixture();
  connection.pragma("synchronous = NORMAL");
  expect(() => new SolOperationStore(connection)).toThrow();
  connection.pragma("synchronous = FULL");
  connection.pragma("foreign_keys = OFF");
  expect(() => new SolOperationStore(connection)).toThrow();
});

test("schema rejects mutable attempt identity and inconsistent success evidence", () => {
  const { connection, store } = fixture();
  const first = store.claim(claim(), 1).current;
  expect(() =>
    connection
      .prepare(
        "UPDATE sol_invocation_attempt SET input_hash = ? WHERE attempt_id = ?",
      )
      .run("c".repeat(64), first.attemptId),
  ).toThrow("SOL_INVOCATION_IDENTITY_IMMUTABLE");
  expect(() =>
    connection.exec("UPDATE sol_operation SET model = 'other'"),
  ).toThrow("SOL_OPERATION_IDENTITY_IMMUTABLE");
  expect(() =>
    connection.exec(
      "UPDATE sol_invocation_attempt SET state = 'known_success', finished_at = 2",
    ),
  ).toThrow();
  expect(store.readCurrent(first.operationKey)?.state).toBe("intent");
});

test("corrupt retained head fails row parsing rather than appearing absent", () => {
  const { connection, store } = fixture();
  const first = store.claim(claim(), 1).current;
  connection.pragma("foreign_keys = OFF");
  connection
    .prepare("DELETE FROM sol_invocation_attempt WHERE attempt_id = ?")
    .run(first.attemptId);
  expect(() => store.claim(claim(), 2)).toThrow("SQLITE_ROW_VALIDATION_FAILED");
});
