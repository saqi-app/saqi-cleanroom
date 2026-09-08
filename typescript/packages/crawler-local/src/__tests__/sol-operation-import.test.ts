import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { expect, test } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { importLegacySolOperations } from "../persistence/sol-operation-import.js";
import { SolOperationLegacyReader } from "../persistence/sol-operation-legacy-reader.js";
import { SolOperationStore } from "../persistence/sol-operation-store.js";
import { canonicalJson, sha256 } from "../persistence/work-key.js";
import { acquireRunLock, readRunLock } from "../runtime/run-lock.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

function fixture() {
  const root = trackedMkdtempSync(join(tmpdir(), "sol-import-"));
  const path = join(root, "ledger.sqlite3");
  Ledger.initialize(path).close();
  const database = new Database(path);
  database.exec(`INSERT INTO runtime_control VALUES('service_enabled', 0)
    ON CONFLICT(control_key) DO UPDATE SET enabled = 0;
    INSERT INTO runtime_control VALUES('global_paused', 1), ('paid_work_paused', 1),
      ('legacy_pause_imported', 1), ('legacy_service_imported', 1)
    ON CONFLICT(control_key) DO UPDATE SET enabled = 1;`);
  database.close();
  const index = join(root, "sol-attempts", "operation-index");
  mkdirSync(index, { recursive: true });
  return { root, path, index };
}

function legacy(
  root: string,
  state?: "completed" | "known_rejection" | "known_success" | "unknown",
) {
  const attemptId = randomUUID();
  const operationKey = sha256(attemptId);
  const attemptPath = join(root, "sol-attempts", attemptId);
  mkdirSync(attemptPath);
  const input = { input: { poem: "fixture" }, repairContext: null };
  const intent = {
    attemptId,
    inputHash: sha256(canonicalJson(input)),
    kind: "generation",
    model: "gpt-5.6-sol",
    modelKey: "sol",
    pipelineVersion: "sol-word-gloss-v2",
    provider: "sol",
    reasoningEffort: "high",
  };
  writeFileSync(
    join(root, "sol-attempts", "operation-index", `${operationKey}.json`),
    canonicalJson(intent),
  );
  writeFileSync(join(attemptPath, "manifest.json"), canonicalJson(intent));
  writeFileSync(join(attemptPath, "input.json"), canonicalJson(input));
  if (state !== undefined)
    writeFileSync(
      join(attemptPath, "invocation-terminal.json"),
      canonicalJson({
        state,
        exitCode: 0,
        signal: null,
        finishedAt: Date.now(),
      }),
    );
  return { attemptId, operationKey, attemptPath, intent };
}

function inspect<T>(path: string, read: (database: Database.Database) => T): T {
  const database = new Database(path);
  database.pragma("foreign_keys = ON");
  database.pragma("synchronous = FULL");
  try {
    return read(database);
  } finally {
    database.close();
  }
}

async function digest(root: string): Promise<string> {
  const plan = await importLegacySolOperations({ stateDirectory: root });
  if (plan.mode !== "dry_run") throw new Error("Expected dry run");
  return plan.sourceDigest;
}

function operatorState(path: string) {
  return inspect(path, (database) => ({
    controls: database
      .prepare(
        "SELECT control_key, enabled FROM runtime_control WHERE control_key != 'sol_operation_import_complete' ORDER BY control_key",
      )
      .all(),
    budgets: database
      .prepare("SELECT * FROM sol_paid_usage_budget ORDER BY budget_id")
      .all(),
    reservations: database
      .prepare(
        "SELECT * FROM sol_paid_usage_reservation ORDER BY budget_id, attempt_id",
      )
      .all(),
  }));
}

function historicalIntent(root: string) {
  const item = legacy(root, "known_success");
  const intent = {
    attemptId: item.attemptId,
    inputHash: item.intent.inputHash,
    kind: item.intent.kind,
    model: "gpt-5.6-sol",
    pipelineVersion: "sol-enrichment-v1",
    reasoningEffort: "high",
  };
  const operationKey = sha256(
    canonicalJson({ inputHash: intent.inputHash, kind: intent.kind }),
  );
  unlinkSync(
    join(root, "sol-attempts", "operation-index", `${item.operationKey}.json`),
  );
  const indexPath = join(
    root,
    "sol-attempts",
    "operation-index",
    `${operationKey}.json`,
  );
  writeFileSync(indexPath, canonicalJson(intent));
  writeFileSync(join(item.attemptPath, "manifest.json"), canonicalJson(intent));
  return { ...item, intent, operationKey, indexPath };
}

const HistoricalReconciliationCases = [
  {
    observation: { state: "known_success", exitCode: 0, signal: null },
    continuation: true,
  },
  {
    observation: { state: "known_failure", exitCode: 1, signal: null },
    continuation: true,
  },
  {
    observation: { state: "predispatch_retryable", exitCode: 1, signal: null },
    continuation: true,
  },
  {
    observation: {
      state: "unresolved_after_turn_started",
      exitCode: null,
      signal: "SIGTERM",
    },
    continuation: true,
  },
  {
    observation: {
      state: "unresolved_continuation_intent",
      strategy: "codex_exec_resume_v1",
    },
    continuation: true,
  },
  {
    observation: {
      state: "unresolved_intent_without_terminal",
      strategy: "artifact_only_v1",
    },
    continuation: false,
  },
  {
    observation: {
      state: "unavailable_no_persisted_session",
      strategy: "artifact_only_v1",
    },
    continuation: false,
  },
];

test.each(HistoricalReconciliationCases)(
  "preserves historical $observation.state evidence without promoting the original invocation",
  async ({ observation, continuation }) => {
    const { root, path } = fixture();
    const item = legacy(root, "unknown");
    const ledger = Ledger.open(path);
    ledger.armSolPaidUsageBudget(3);
    ledger.close();
    const before = operatorState(path);
    const original = { ...observation, finishedAt: Date.now() };
    const evidencePath = join(item.attemptPath, "reconciliation-terminal.json");
    const evidenceBytes = JSON.stringify(original, null, 2);
    writeFileSync(evidencePath, evidenceBytes);
    const session = { sessionId: randomUUID(), observedAt: Date.now() + 0.25 };
    const sessionPath = join(item.attemptPath, "codex-session.json");
    const sessionBytes = JSON.stringify(session, null, 2);
    writeFileSync(sessionPath, sessionBytes);
    const plan = await importLegacySolOperations({ stateDirectory: root });
    expect(plan).toMatchObject({
      legacyObservations: {
        continuations: Number(continuation),
        artifactReconciliations: Number(!continuation),
        fractionalSessionTimestamps: 1,
      },
    });
    if (plan.mode !== "dry_run") throw new Error("Expected dry run");
    await importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest: plan.sourceDigest,
    });
    inspect(path, (database) => {
      const store = new SolOperationStore(database);
      const current = store.readCurrent(item.operationKey);
      expect(current).toMatchObject({
        state: "unknown",
        sessionId: session.sessionId,
        sessionObservedAt: Math.floor(session.observedAt),
        observations: {
          reconciliation: null,
          legacySessionTimestamp: {
            strategy: "legacy_mtime_ms_floor_v1",
            original: session,
            sourceSha256: sha256(sessionBytes),
          },
        },
      });
      expect(
        continuation
          ? current?.observations.legacyContinuationEvidence
          : current?.observations.legacyArtifactReconciliationEvidence,
      ).toMatchObject({ original, sourceSha256: sha256(evidenceBytes) });
      expect(
        store.claim(
          {
            ...item.intent,
            attemptId: randomUUID(),
            operationKey: item.operationKey,
            kind: "generation",
            provider: "sol",
          },
          Date.now(),
        ).claimed,
      ).toBe(false);
      if (!current) throw new Error("Missing imported operation");
      expect(
        store.recordSessionCleanup(current, {
          state: "removed_after_result_promotion",
          cleanedAt: Date.now(),
          sessionId: session.sessionId,
        }),
      ).toBe(false);
    });
    expect(readFileSync(evidencePath, "utf8")).toBe(evidenceBytes);
    expect(readFileSync(sessionPath, "utf8")).toBe(sessionBytes);
    expect(operatorState(path)).toEqual(before);
  },
);

test("historical continuation success never supplies a missing original invocation terminal", async () => {
  const { root, path } = fixture();
  const item = legacy(root);
  writeFileSync(
    join(item.attemptPath, "reconciliation-terminal.json"),
    canonicalJson({
      state: "known_success",
      exitCode: 0,
      signal: null,
      finishedAt: Date.now(),
    }),
  );
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: await digest(root),
  });
  expect(
    inspect(path, (database) =>
      new SolOperationStore(database).readCurrent(item.operationKey),
    ),
  ).toMatchObject({
    state: "intent",
    finishedAt: null,
    observations: {
      reconciliation: null,
      legacyContinuationEvidence: { original: { state: "known_success" } },
    },
  });
});

test.each([
  { state: "invented", exitCode: 0, signal: null, finishedAt: 1 },
  { state: "known_success", exitCode: 1, signal: null, finishedAt: 1 },
  {
    state: "unresolved_continuation_intent",
    strategy: "artifact_only_v1",
    finishedAt: 1,
  },
  {
    state: "unresolved_intent_without_terminal",
    strategy: "artifact_only_v1",
    finishedAt: -1,
  },
])(
  "rejects unproven historical reconciliation shape %#",
  async (observation) => {
    const { root } = fixture();
    const item = legacy(root, "unknown");
    writeFileSync(
      join(item.attemptPath, "reconciliation-terminal.json"),
      canonicalJson(observation),
    );
    await expect(
      importLegacySolOperations({ stateDirectory: root }),
    ).rejects.toThrow("SOL_IMPORT_RECORD_INVALID");
  },
);

test.each([-0.25, Number.MAX_SAFE_INTEGER + 1, "1788021722689.785"])(
  "rejects out-of-range or nonnumeric historical session timestamp %s",
  async (observedAt) => {
    const { root } = fixture();
    const item = legacy(root, "unknown");
    writeFileSync(
      join(item.attemptPath, "codex-session.json"),
      JSON.stringify({ sessionId: randomUUID(), observedAt }),
    );
    await expect(
      importLegacySolOperations({ stateDirectory: root }),
    ).rejects.toThrow("SOL_IMPORT_RECORD_INVALID");
  },
);

test("imports the proven six-field Sol identity without rekeying or altering source files", async () => {
  const { root, path } = fixture();
  const item = historicalIntent(root);
  const before = operatorState(path);
  const plan = await importLegacySolOperations({ stateDirectory: root });
  expect(plan).toMatchObject({
    legacyIntentNormalizations: 1,
    quarantine: { records: 0 },
  });
  if (plan.mode !== "dry_run") throw new Error("Expected dry run");
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: plan.sourceDigest,
  });
  inspect(path, (database) => {
    const store = new SolOperationStore(database);
    const current = store.readCurrent(item.operationKey);
    expect(current).toMatchObject({
      ...item.intent,
      operationKey: item.operationKey,
      modelKey: "sol-5.6",
      provider: "sol",
      state: "known_success",
      observations: {
        legacyIntentNormalization: {
          strategy: "sol_only_intent_v1",
          sourceShape:
            "attemptId,inputHash,kind,model,pipelineVersion,reasoningEffort",
        },
      },
    });
    expect(
      store.claim(
        {
          ...item.intent,
          attemptId: randomUUID(),
          operationKey: item.operationKey,
          modelKey: "sol-5.6",
          provider: "sol",
          kind: "generation",
        },
        Date.now(),
      ).claimed,
    ).toBe(false);
  });
  expect(readFileSync(item.indexPath, "utf8")).toBe(canonicalJson(item.intent));
  expect(readFileSync(join(item.attemptPath, "manifest.json"), "utf8")).toBe(
    canonicalJson(item.intent),
  );
  expect(operatorState(path)).toEqual(before);
});

test.each(["index", "manifest"])(
  "rejects mismatched original intent generations in %s despite equivalent normalized fields",
  async (side) => {
    const { root } = fixture();
    const item = historicalIntent(root);
    writeFileSync(
      side === "index"
        ? item.indexPath
        : join(item.attemptPath, "manifest.json"),
      canonicalJson({ ...item.intent, modelKey: "sol-5.6", provider: "sol" }),
    );
    await expect(
      importLegacySolOperations({ stateDirectory: root }),
    ).rejects.toThrow("SOL_IMPORT_MANIFEST_MISMATCH");
  },
);

test("dry run is nonmutating and exact import preserves known success and artifact evidence", async () => {
  const { root, path } = fixture();
  const item = legacy(root, "known_success");
  const artifact = join(item.attemptPath, "result.json");
  writeFileSync(artifact, "retained output");
  const expectedDigest = await digest(root);
  expect(
    inspect(path, (db) =>
      db.prepare("SELECT COUNT(*) AS count FROM sol_operation").get(),
    ),
  ).toEqual({ count: 0 });
  expect(existsSync(join(root, "RUN.lock"))).toBe(false);
  const applied = await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest,
  });
  expect(applied).toMatchObject({
    mode: "applied",
    receipt: { records: 1, sourceDigest: expectedDigest },
  });
  expect(
    inspect(
      path,
      (db) => new SolOperationStore(db).readCurrent(item.operationKey)?.state,
    ),
  ).toBe("known_success");
  expect(readFileSync(artifact, "utf8")).toBe("retained output");
  expect(existsSync(join(root, "RUN.lock"))).toBe(false);
});

test("legacy completed and missing terminals preserve ambiguity without inference permission", async () => {
  const { root, path } = fixture();
  const completed = legacy(root, "completed");
  const missing = legacy(root);
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: await digest(root),
  });
  inspect(path, (db) => {
    const store = new SolOperationStore(db);
    expect(store.readCurrent(completed.operationKey)?.state).toBe("unknown");
    expect(store.readCurrent(missing.operationKey)?.state).toBe("intent");
    expect(
      store.claim(
        {
          ...completed.intent,
          attemptId: randomUUID(),
          operationKey: completed.operationKey,
          kind: "generation",
          provider: "sol",
        },
        Date.now(),
      ).claimed,
    ).toBe(false);
  });
});

test("structured-error exit-zero rejection remains retryable without turn-start evidence", async () => {
  const { root, path } = fixture();
  const item = legacy(root, "known_rejection");
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: await digest(root),
  });
  expect(
    inspect(
      path,
      (db) => new SolOperationStore(db).readCurrent(item.operationKey)?.state,
    ),
  ).toBe("known_rejection");
});

test("idempotent readback ignores later stale legacy edits", async () => {
  const { root } = fixture();
  const item = legacy(root, "unknown");
  const expectedDigest = await digest(root);
  const first = await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest,
  });
  writeFileSync(join(item.attemptPath, "manifest.json"), "corrupt stale file");
  const second = await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest,
  });
  expect(first.mode).toBe("applied");
  expect(second).toMatchObject({
    mode: "already_imported",
    receipt: { sourceDigest: expectedDigest, records: 1 },
  });
});

test("corruption and changed dry-run sources cannot partially import", async () => {
  const { root, path } = fixture();
  const item = legacy(root, "known_success");
  const expectedDigest = await digest(root);
  writeFileSync(join(item.attemptPath, "input.json"), "{}");
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("SOL_IMPORT_INPUT_HASH_MISMATCH");
  expect(
    inspect(path, (db) =>
      db.prepare("SELECT COUNT(*) AS count FROM sol_operation").get(),
    ),
  ).toEqual({ count: 0 });
  expect(existsSync(join(root, "RUN.lock"))).toBe(false);
});

test("failed receipt commit rolls back every row and permits restart", async () => {
  const { root, path } = fixture();
  legacy(root, "known_success");
  const expectedDigest = await digest(root);
  inspect(path, (db) =>
    db.exec(`CREATE TRIGGER fail_receipt BEFORE INSERT ON sol_operation_import_receipt
    BEGIN SELECT RAISE(ABORT, 'forced failure'); END`),
  );
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("forced failure");
  inspect(path, (db) => {
    expect(
      db.prepare("SELECT COUNT(*) AS count FROM sol_invocation_attempt").get(),
    ).toEqual({ count: 0 });
    expect(
      db
        .prepare(
          "SELECT enabled FROM runtime_control WHERE control_key = 'sol_operation_import_complete'",
        )
        .get(),
    ).toBeUndefined();
    db.exec("DROP TRIGGER fail_receipt");
  });
  const restarted = await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest,
  });
  expect(restarted.mode).toBe("applied");
});

test("owner, enabled service, and legacy locks block import without deleting them", async () => {
  const { root, path, index } = fixture();
  const item = legacy(root);
  const expectedDigest = await digest(root);
  const lock = join(index, `${item.operationKey}.json.lock.stale.fixture`);
  writeFileSync(lock, "do not delete");
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("SOL_IMPORT_LEGACY_LOCK_PRESENT");
  expect(readFileSync(lock, "utf8")).toBe("do not delete");
  inspect(path, (db) =>
    db.exec(
      "UPDATE runtime_control SET enabled = 1 WHERE control_key = 'service_enabled'",
    ),
  );
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow();
  inspect(path, (db) =>
    db.exec(
      "UPDATE runtime_control SET enabled = 0 WHERE control_key = 'service_enabled'",
    ),
  );
  writeFileSync(
    join(root, "RUN.lock"),
    canonicalJson({
      configDigest: expectedDigest,
      pid: process.pid,
      runId: randomUUID(),
      schemaVersion: 1,
      startedAt: new Date().toISOString(),
    }),
  );
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("RUNTIME_OWNER_LEGACY_LOCK_PRESENT");
});

test("quarantined rejection does not bypass oversized evidence blockers", async () => {
  const { root } = fixture();
  const item = legacy(root, "known_rejection");
  writeFileSync(
    join(item.attemptPath, "turn-started.json"),
    canonicalJson({ observedAt: Date.now() }),
  );
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).resolves.toMatchObject({
    quarantine: { records: 1, turnStarted: 1 },
  });
  writeFileSync(
    join(item.attemptPath, "input.json"),
    "x".repeat(512 * 1024 + 1),
  );
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("SOL_IMPORT_FILE_LIMIT");
});

test("valid but changed evidence requires a fresh dry-run digest", async () => {
  const { root } = fixture();
  const item = legacy(root, "known_success");
  const expectedDigest = await digest(root);
  writeFileSync(
    join(item.attemptPath, "invocation-terminal.json"),
    canonicalJson({
      state: "unknown",
      exitCode: null,
      signal: "SIGTERM",
      finishedAt: Date.now(),
    }),
  );
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("SOL_IMPORT_SOURCE_CHANGED");
});

test("event-stream turn evidence cannot be hidden by a missing turn snapshot", async () => {
  const { root } = fixture();
  const item = legacy(root, "known_rejection");
  writeFileSync(
    join(item.attemptPath, "events.jsonl"),
    `${canonicalJson({ type: "turn.started" })}\n`,
  );
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).resolves.toMatchObject({ quarantine: { records: 1, turnStarted: 1 } });
});

test.each(["event-turn", "snapshot-turn", "missing-exit", "signal"])(
  "quarantines %s rejection with immutable evidence and no replay or cleanup",
  async (reason) => {
    const { root, path } = fixture();
    const ledger = Ledger.open(path);
    ledger.armSolPaidUsageBudget(3);
    ledger.close();
    const beforeOperatorState = operatorState(path);
    const item = legacy(root, "known_rejection");
    const sessionId = randomUUID();
    const observedAt = Date.now();
    writeFileSync(
      join(item.attemptPath, "codex-session.json"),
      canonicalJson({ sessionId, observedAt }),
    );
    const terminal = {
      state: "known_rejection",
      exitCode: reason === "missing-exit" ? null : 0,
      signal: reason === "signal" ? "SIGTERM" : null,
      finishedAt: observedAt,
    };
    const terminalPath = join(item.attemptPath, "invocation-terminal.json");
    writeFileSync(terminalPath, canonicalJson(terminal));
    if (reason === "event-turn")
      writeFileSync(
        join(item.attemptPath, "events.jsonl"),
        `${canonicalJson({ type: "turn.started" })}\n`,
      );
    if (reason === "snapshot-turn")
      writeFileSync(
        join(item.attemptPath, "turn-started.json"),
        canonicalJson({ observedAt }),
      );
    const expectedReasons = {
      missingExitCode: reason === "missing-exit",
      signaled: reason === "signal",
      turnStarted: reason.endsWith("turn"),
    };
    const plan = await importLegacySolOperations({ stateDirectory: root });
    expect(plan).toMatchObject({
      quarantine: {
        records: 1,
        missingExitCode: Number(expectedReasons.missingExitCode),
        signaled: Number(expectedReasons.signaled),
        turnStarted: Number(expectedReasons.turnStarted),
      },
    });
    if (plan.mode !== "dry_run") throw new Error("Expected dry run");
    await importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest: plan.sourceDigest,
    });
    inspect(path, (db) => {
      const store = new SolOperationStore(db);
      const current = store.readCurrent(item.operationKey);
      expect(current).toMatchObject({
        state: "unknown",
        exitCode: terminal.exitCode,
        signal: terminal.signal,
        finishedAt: terminal.finishedAt,
        sessionId,
        sessionObservedAt: observedAt,
        observations: {
          legacyImportQuarantine: {
            strategy: "legacy_rejection_quarantine_v1",
            originalState: "known_rejection",
            reasons: expectedReasons,
          },
        },
      });
      expect(
        store.claim(
          {
            ...item.intent,
            attemptId: randomUUID(),
            operationKey: item.operationKey,
            kind: "generation",
            provider: "sol",
          },
          Date.now(),
        ).claimed,
      ).toBe(false);
      if (!current) throw new Error("Missing imported attempt");
      expect(
        store.recordSessionCleanup(current, {
          sessionId,
          cleanedAt: Date.now(),
          state: "removed_after_result_promotion",
        }),
      ).toBe(false);
    });
    expect(readFileSync(terminalPath, "utf8")).toBe(canonicalJson(terminal));
    expect(operatorState(path)).toEqual(beforeOperatorState);
  },
);

test("session and pending cleanup evidence survive import without deleting provider sessions", async () => {
  const { root, path } = fixture();
  const item = legacy(root, "known_success");
  const sessionId = randomUUID();
  const pending = {
    code: "CODEX_SESSION_CLEANUP_PENDING",
    detail: "fixture",
    observedAt: Date.now(),
    sessionId,
  };
  writeFileSync(
    join(item.attemptPath, "codex-session.json"),
    canonicalJson({ sessionId, observedAt: Date.now() }),
  );
  writeFileSync(
    join(item.attemptPath, "session-cleanup-pending.json"),
    canonicalJson(pending),
  );
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: await digest(root),
  });
  expect(
    inspect(
      path,
      (db) =>
        new SolOperationStore(db).readCurrent(item.operationKey)?.sessionId,
    ),
  ).toBe(sessionId);
  expect(
    inspect(
      path,
      (db) =>
        new SolOperationStore(db).readCurrent(item.operationKey)?.observations,
    ),
  ).toEqual({
    credential: null,
    cleanup: null,
    cleanupPending: pending,
    reconciliation: null,
  });
  expect(
    inspect(path, (db) =>
      db.prepare("SELECT observations_json FROM sol_invocation_attempt").get(),
    ),
  ).toEqual({
    observations_json: canonicalJson({
      credential: null,
      cleanup: null,
      cleanupPending: pending,
      reconciliation: null,
    }),
  });
  expect(
    existsSync(join(item.attemptPath, "session-cleanup-pending.json")),
  ).toBe(true);
});

test("record and aggregate byte budgets stop scans without unbounded reads", async () => {
  const { root } = fixture();
  legacy(root);
  legacy(root);
  const attemptRoot = join(root, "sol-attempts");
  await expect(
    new SolOperationLegacyReader(attemptRoot, { records: 1 }).scan(),
  ).rejects.toThrow("SOL_IMPORT_RECORD_LIMIT");
  await expect(
    new SolOperationLegacyReader(attemptRoot, { bytes: 1 }).scan(),
  ).rejects.toThrow("SOL_IMPORT_BYTE_LIMIT");
});

test("finite spent budget and explicit pauses are unchanged by import", async () => {
  const { root, path } = fixture();
  legacy(root, "known_success");
  inspect(path, (db) =>
    db.exec(
      "INSERT INTO sol_paid_usage_budget VALUES('fixture-budget', 9, 9, 'exhausted', 1, 1)",
    ),
  );
  const before = inspect(path, (db) =>
    db.prepare("SELECT * FROM sol_paid_usage_budget").all(),
  );
  await importLegacySolOperations({
    stateDirectory: root,
    apply: true,
    expectedDigest: await digest(root),
  });
  expect(
    inspect(path, (db) =>
      db.prepare("SELECT * FROM sol_paid_usage_budget").all(),
    ),
  ).toEqual(before);
  expect(
    inspect(path, (db) =>
      db
        .prepare(
          "SELECT enabled FROM runtime_control WHERE control_key IN ('global_paused','paid_work_paused')",
        )
        .all(),
    ),
  ).toEqual([{ enabled: 1 }, { enabled: 1 }]);
});

test("expired but still-running Sol ownership blocks the offline apply", async () => {
  const { root, path } = fixture();
  legacy(root);
  const expectedDigest = await digest(root);
  inspect(path, (db) =>
    db
      .prepare(
        `INSERT INTO work_item(work_key, kind, input_json, input_hash,
    schema_version, implementation_version, available_at, created_at, updated_at,
    state, lease_owner, lease_token, lease_expires_at)
    VALUES(?,'poem-enrichment-sol','{}',?,'fixture','fixture',1,1,1,'running','fixture','token',1)`,
      )
      .run(sha256("fixture-work"), "a".repeat(64)),
  );
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("solImport.running");
  expect(existsSync(join(root, "RUN.lock"))).toBe(false);
});

test("symlinked evidence and duplicate attempt identities are not skipped", async () => {
  const { root, index } = fixture();
  const item = legacy(root);
  const input = join(item.attemptPath, "input.json");
  const original = readFileSync(input);
  unlinkSync(input);
  symlinkSync(join(item.attemptPath, "manifest.json"), input);
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("SOL_IMPORT_RECORD_INVALID");
  unlinkSync(input);
  writeFileSync(input, original);
  writeFileSync(
    join(index, `${sha256("duplicate")}.json`),
    canonicalJson(item.intent),
  );
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("SOL_IMPORT_DUPLICATE_ATTEMPT");
});

test("orphaned completion marker cannot authorize a second import", async () => {
  const { root, path } = fixture();
  legacy(root);
  inspect(path, (db) =>
    db.exec(
      "INSERT INTO runtime_control VALUES('sol_operation_import_complete',1)",
    ),
  );
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("SOL_IMPORT_RECEIPT_INCONSISTENT");
});

test("strict acquisition does not recover a stale owner appearing after preflight", async () => {
  const { root } = fixture();
  const lockPath = join(root, "RUN.lock");
  const raw = canonicalJson({
    configDigest: "a".repeat(64),
    pid: 2_147_483_647,
    runId: randomUUID(),
    schemaVersion: 1,
    startedAt: new Date(1).toISOString(),
  });
  writeFileSync(lockPath, raw);
  await expect(
    acquireRunLock(lockPath, "a".repeat(64), new Date(), {
      recoverStale: false,
    }),
  ).rejects.toThrow("RUNTIME_OWNER_LEGACY_LOCK_PRESENT");
  expect(readFileSync(lockPath, "utf8")).toBe(raw);
});

test("root and lock symlinks are refused without following their targets", async () => {
  const { root } = fixture();
  const alias = join(root, "alias");
  symlinkSync(root, alias);
  await expect(
    importLegacySolOperations({ stateDirectory: alias }),
  ).rejects.toThrow("SOL_IMPORT_UNSAFE_ROOT");
  const target = join(root, "target");
  writeFileSync(target, "not a lock");
  const lockPath = join(root, "RUN.lock");
  symlinkSync(target, lockPath);
  await expect(
    importLegacySolOperations({ stateDirectory: root }),
  ).rejects.toThrow("RUNTIME_OWNER_LEGACY_LOCK_PRESENT");
  await expect(readRunLock(lockPath)).rejects.toThrow(
    "RUNTIME_OWNER_LEGACY_LOCK_PRESENT",
  );
  expect(readFileSync(target, "utf8")).toBe("not a lock");
});

test("symlinked ledger is rejected before opening or changing an external budget and controls", async () => {
  const source = fixture();
  const target = fixture();
  const expectedDigest = await digest(source.root);
  inspect(target.path, (db) =>
    db.exec(
      "INSERT INTO sol_paid_usage_budget VALUES('external-budget', 9, 9, 'exhausted', 1, 1)",
    ),
  );
  const before = readFileSync(target.path);
  unlinkSync(source.path);
  symlinkSync(target.path, source.path);
  await expect(
    importLegacySolOperations({ stateDirectory: source.root }),
  ).rejects.toThrow("SOL_IMPORT_UNSAFE_LEDGER");
  await expect(
    importLegacySolOperations({
      stateDirectory: source.root,
      apply: true,
      expectedDigest,
    }),
  ).rejects.toThrow("SOL_IMPORT_UNSAFE_LEDGER");
  expect(readFileSync(target.path)).toEqual(before);
  expect(existsSync(join(source.root, "RUN.lock"))).toBe(false);
});

test("nonregular ledger paths are rejected before SQLite initialization", async () => {
  const { root, path } = fixture();
  unlinkSync(path);
  mkdirSync(path);
  await expect(
    importLegacySolOperations({
      stateDirectory: root,
      apply: true,
      expectedDigest: "a".repeat(64),
    }),
  ).rejects.toThrow("SOL_IMPORT_UNSAFE_LEDGER");
  expect(existsSync(join(root, "RUN.lock"))).toBe(false);
});
