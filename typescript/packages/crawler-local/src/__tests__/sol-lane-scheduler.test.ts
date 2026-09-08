import { readdirSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";

import {
  parseQuotaResetTimestamp,
  QuotaAwareSolLaneScheduler,
  schedulerStateDigest,
  type SolSchedulerGates,
  type SolTaskOutcome,
} from "../enrichment/sol-lane-scheduler";
import { Ledger } from "../persistence/ledger";
import { sha256 } from "../persistence/work-key";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const CONFIG_DIGEST = "a".repeat(64);
const SCHEDULER_STORES = new Map<string, Ledger>();
function schedulerStore(identity: string): Ledger {
  const existing = SCHEDULER_STORES.get(identity);
  if (existing) return existing;
  const ledger = Ledger.initialize(":memory:");
  SCHEDULER_STORES.set(identity, ledger);
  return ledger;
}
function persistedScheduler(identity: string): unknown {
  const stored = schedulerStore(identity).loadSchedulerState("provider:test");
  if (!stored) throw new Error("Expected persisted scheduler");
  return JSON.parse(stored.serialized);
}
afterEach(() => {
  for (const ledger of SCHEDULER_STORES.values()) ledger.close();
  SCHEDULER_STORES.clear();
});
const OPEN: SolSchedulerGates = {
  circuitOpen: false,
  diskWritable: true,
  paused: false,
  quotaWaitUntil: null,
};

function scheduler(
  ceiling = 4,
  now: () => number = () => 1_000,
  digest = CONFIG_DIGEST,
  credentialGeneration?: () => null | string,
) {
  const root = mkdtempSync(join(tmpdir(), "saqi-sol-scheduler-"));
  return {
    create: () =>
      new QuotaAwareSolLaneScheduler({
        stateStore: schedulerStore(join(root, "scheduler.json")),
        stateKey: "provider:test",
        ceiling,
        configDigest: digest,
        ...(credentialGeneration ? { credentialGeneration } : {}),
        now,
        promotionSuccesses: 2,
        legacyCurrentStatePath: join(root, "scheduler.json"),
      }),
    root,
  };
}

async function complete(
  target: QuotaAwareSolLaneScheduler,
  outcome: SolTaskOutcome,
  now = 1_100,
): Promise<void> {
  const permit = await target.acquire(OPEN);
  if (!permit) throw new Error("Expected scheduler permit");
  await permit.complete(outcome, now);
}

describe("quota-aware Sol lane scheduler", () => {
  it.each(["ancestor-file", "symlink", "directory", "oversized"])(
    "rejects unsafe legacy input %s without initializing authority",
    async (kind) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-unsafe-"));
      const evidence = join(root, "evidence.json");
      const original =
        kind === "oversized" ? "x".repeat(65 * 1024) : "retained evidence";
      writeFileSync(evidence, original);
      const symlink = join(root, "symlink.json");
      if (kind === "symlink") symlinkSync(evidence, symlink);
      const input =
        kind === "ancestor-file"
          ? join(evidence, "state.json")
          : kind === "directory"
            ? root
            : kind === "symlink"
              ? symlink
              : evidence;
      const ledger = schedulerStore(root);
      const target = new QuotaAwareSolLaneScheduler({
        ceiling: 2,
        configDigest: CONFIG_DIGEST,
        stateKey: "provider:test",
        stateStore: ledger,
        legacyCurrentStatePath: input,
      });
      await expect(target.snapshot(OPEN)).rejects.toThrow(
        "SOL_SCHEDULER_STATE_INVALID",
      );
      expect(ledger.loadSchedulerState("provider:test")).toBeNull();
      expect(readFileSync(evidence, "utf8")).toBe(original);
    },
  );
  it("fails closed after SQLite authority disappears without reimporting legacy bytes", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-missing-row-"));
    const databasePath = join(root, "ledger.sqlite3");
    const legacyPath = join(root, "scheduler.json");
    const ledger = Ledger.initialize(databasePath);
    try {
      const target = new QuotaAwareSolLaneScheduler({
        ceiling: 2,
        configDigest: CONFIG_DIGEST,
        now: () => 1_000,
        stateKey: "provider:test",
        stateStore: ledger,
        legacyCurrentStatePath: legacyPath,
      });
      const permit = await target.acquire(OPEN);
      if (!permit) throw new Error("Expected permit");
      const current = ledger.loadSchedulerState("provider:test");
      if (!current) throw new Error("Expected current authority");
      writeFileSync(legacyPath, current.serialized);
      const database = new Database(databasePath);
      try {
        database
          .prepare("DELETE FROM scheduler_state WHERE state_key = ?")
          .run("provider:test");
      } finally {
        database.close();
      }
      await expect(target.snapshot(OPEN)).rejects.toThrow(
        "SOL_SCHEDULER_CURRENT_AUTHORITY_MISSING",
      );
      await expect(target.acquire(OPEN)).rejects.toThrow(
        "SOL_SCHEDULER_CURRENT_AUTHORITY_MISSING",
      );
      await expect(permit.complete({ kind: "success" })).rejects.toThrow(
        "SOL_SCHEDULER_CURRENT_AUTHORITY_MISSING",
      );
      expect(ledger.loadSchedulerState("provider:test")).toBeNull();
      expect(readFileSync(legacyPath, "utf8")).toBe(current.serialized);
    } finally {
      ledger.close();
    }
  });
  it("does not turn internal budget exhaustion into provider quota pressure", async () => {
    const target = scheduler().create();
    const before = await target.snapshot(OPEN);
    const permit = await target.acquire(OPEN);
    if (!permit) throw new Error("Expected scheduler permit");

    await expect(
      permit.complete({ kind: "budget_exhausted" }, 1_100),
    ).resolves.toEqual({ accepted: true, quotaCleared: false });

    await expect(target.snapshot(OPEN, 1_100)).resolves.toMatchObject({
      providerUntil: before.providerUntil,
      quotaProbeAt: before.quotaProbeAt,
      quotaUntil: before.quotaUntil,
      recoveryCause: before.recoveryCause,
      selectedConcurrency: before.selectedConcurrency,
    });
  });

  it("does not clear or extend an existing quota gate when a probe finds the budget exhausted", async () => {
    let now = 1_000;
    const target = scheduler(4, () => now).create();
    await complete(
      target,
      { kind: "quota_wait", retryAt: now + 86_400_000 },
      now,
    );
    now += 15 * 60_000;
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    const before = await target.snapshot(OPEN, now);

    await expect(
      probe?.complete({ kind: "budget_exhausted" }, now + 1),
    ).resolves.toEqual({ accepted: true, quotaCleared: false });

    await expect(target.snapshot(OPEN, now + 1)).resolves.toMatchObject({
      providerUntil: before.providerUntil,
      quotaProbeAt: before.quotaProbeAt,
      quotaUntil: before.quotaUntil,
      recoveryCause: "quota",
      selectedConcurrency: before.selectedConcurrency,
    });
    await expect(target.acquire(OPEN)).resolves.toBeNull();
  });

  it("retains promotion progress across a restart between successful tasks", async () => {
    const fixture = scheduler(4);
    await complete(fixture.create(), { kind: "success" });
    await expect(fixture.create().snapshot(OPEN)).resolves.toMatchObject({
      selectedConcurrency: 2,
      successStreak: 1,
    });
    await complete(fixture.create(), { kind: "success" }, 1_200);
    await expect(fixture.create().snapshot(OPEN)).resolves.toMatchObject({
      selectedConcurrency: 4,
      successStreak: 0,
    });
  });

  it("coalesces completion telemetry at the ceiling until a safety checkpoint", async () => {
    let writes = 0;
    let stored: { digest: string; serialized: string } | null = null;
    const stateStore = {
      loadSchedulerState: () => stored,
      saveSchedulerState: (
        _stateKey: string,
        serialized: string,
        digest: string,
        expectedDigest: null | string,
      ) => {
        if ((stored?.digest ?? null) !== expectedDigest) return false;
        stored = { digest, serialized };
        writes += 1;
        return true;
      },
    };
    const target = new QuotaAwareSolLaneScheduler({
      ceiling: 4,
      configDigest: CONFIG_DIGEST,
      initialConcurrency: 4,
      now: () => 1_000,
      promotionSuccesses: 2,
      stateKey: "provider:test",
      legacyCurrentStatePath: "/unused/scheduler.json",
      stateStore,
    });

    await target.snapshot(OPEN);
    expect(writes).toBe(1);
    await complete(target, { kind: "success" });
    expect(writes).toBe(1);
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      successStreak: 1,
    });

    await complete(target, { kind: "success" }, 1_200);
    expect(writes).toBe(1);
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      selectedConcurrency: 4,
      successStreak: 2,
    });
    await complete(target, { kind: "quota_wait", retryAt: 5_000 }, 1_300);
    expect(writes).toBe(2);
  });

  it("persists ordinary success clearing an expired provider fault before promotion", async () => {
    const fixture = scheduler(4);
    const statePath = join(fixture.root, "scheduler.json");
    const ledger = schedulerStore(statePath);
    await fixture.create().snapshot(OPEN);
    const current = ledger.loadSchedulerState("provider:test");
    if (!current) throw new Error("Expected scheduler state");
    const serialized = JSON.stringify({
      ...z.looseObject({}).parse(JSON.parse(current.serialized)),
      consecutiveProviderFailures: 2,
      providerErrorCode: "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
      providerUntil: 500,
      recoveryLease: null,
      selectedConcurrency: 1,
      successStreak: 0,
    });
    expect(
      ledger.saveSchedulerState(
        "provider:test",
        serialized,
        sha256(Buffer.from(serialized)),
        current.digest,
      ),
    ).toBe(true);
    const recovering = fixture.create();
    const permit = await recovering.acquire(OPEN);
    expect(permit?.purpose).toBe("normal");
    await permit?.complete({ kind: "success" }, 1_100);
    await expect(fixture.create().snapshot(OPEN)).resolves.toMatchObject({
      consecutiveProviderFailures: 0,
      providerErrorCode: null,
      providerUntil: 0,
      selectedConcurrency: 1,
      successStreak: 1,
    });
  });

  it("imports legacy JSON once and keeps SQLite authoritative", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-sqlite-"));
    const statePath = join(root, "scheduler.json");
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const legacy = {
      configDigest: CONFIG_DIGEST,
      consecutiveErrors: 0,
      consecutiveProviderFailures: 0,
      consecutiveRateLimits: 0,
      errorDampenerUntil: 0,
      ewmaLatencyMs: null,
      providerCredentialGeneration: null,
      providerErrorCode: null,
      providerUntil: 0,
      quotaProbeAt: 0,
      quotaUntil: 0,
      rateLimitedUntil: 0,
      samples: 2,
      schemaVersion: 10,
      selectedConcurrency: 3,
      successStreak: 0,
      updatedAt: 1_000,
    };
    writeFileSync(statePath, JSON.stringify(legacy));
    try {
      const imported = new QuotaAwareSolLaneScheduler({
        ceiling: 8,
        configDigest: CONFIG_DIGEST,
        now: () => 2_000,
        stateKey: "provider:sol",
        legacyCurrentStatePath: statePath,
        stateStore: ledger,
      });
      await expect(imported.snapshot(OPEN)).resolves.toMatchObject({
        samples: 2,
        selectedConcurrency: 3,
      });
      expect(ledger.loadSchedulerState("provider:sol")).not.toBeNull();
      expect(readFileSync(statePath, "utf8")).toBe(JSON.stringify(legacy));

      writeFileSync(
        statePath,
        JSON.stringify({ ...legacy, selectedConcurrency: 7 }),
      );
      const restarted = new QuotaAwareSolLaneScheduler({
        ceiling: 8,
        configDigest: CONFIG_DIGEST,
        now: () => 3_000,
        stateKey: "provider:sol",
        legacyCurrentStatePath: statePath,
        stateStore: ledger,
      });
      await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
        samples: 2,
        selectedConcurrency: 3,
      });
    } finally {
      ledger.close();
    }
  });

  it("requires the proven current SQLite authority without consulting legacy state", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-authority-"));
    const statePath = join(root, "scheduler.json");
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    writeFileSync(
      statePath,
      JSON.stringify({ schemaVersion: 10, selectedConcurrency: 7 }),
    );
    try {
      const target = new QuotaAwareSolLaneScheduler({
        ceiling: 8,
        configDigest: CONFIG_DIGEST,
        legacyStateKey: "provider:sol",
        legacyStatePath: statePath,
        requireCurrentStateStoreAuthority: true,
        stateKey: "provider-v10:sol",
        legacyCurrentStatePath: statePath,
        stateStore: ledger,
      });

      await expect(target.snapshot(OPEN)).rejects.toThrow(
        "SOL_SCHEDULER_STATE_INVALID",
      );
      expect(ledger.loadSchedulerState("provider-v10:sol")).toBeNull();
    } finally {
      ledger.close();
    }
  });

  it("rejects a corrupt current SQLite authority even when legacy state is valid", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-corrupt-"));
    const statePath = join(root, "scheduler.json");
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const bootstrap = new QuotaAwareSolLaneScheduler({
      ceiling: 8,
      configDigest: CONFIG_DIGEST,
      stateKey: "provider-v10:sol",
      legacyCurrentStatePath: statePath,
      stateStore: ledger,
    });
    await bootstrap.snapshot(OPEN);
    const current = ledger.loadSchedulerState("provider-v10:sol");
    if (!current) throw new Error("Expected current scheduler authority");
    ledger.saveSchedulerState(
      "provider-v10:sol",
      current.serialized,
      "f".repeat(64),
      current.digest,
    );
    writeFileSync(statePath, current.serialized);
    try {
      const target = new QuotaAwareSolLaneScheduler({
        ceiling: 8,
        configDigest: CONFIG_DIGEST,
        legacyStateKey: "provider:sol",
        legacyStatePath: statePath,
        requireCurrentStateStoreAuthority: true,
        stateKey: "provider-v10:sol",
        legacyCurrentStatePath: statePath,
        stateStore: ledger,
      });

      await expect(target.snapshot(OPEN)).rejects.toThrow(
        "SOL_SCHEDULER_STATE_INVALID",
      );
    } finally {
      ledger.close();
    }
  });

  it("holds a fixed operating target while retaining safety gates", async () => {
    let now = 1_000;
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-fixed-scheduler-"));
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      fixedConcurrency: true,
      initialConcurrency: 2,
      now: () => now,
      legacyCurrentStatePath: join(root, "scheduler.json"),
    });

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      availableSlots: 128,
      selectedConcurrency: 128,
    });
    const permits = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    expect(permits.every(Boolean)).toBe(true);
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    await permits[0]?.complete({ kind: "rate_limited", retryAt: 2_000 }, 1_100);
    const limited = await target.snapshot(OPEN);
    expect(limited).toMatchObject({
      blockReason: "at_capacity",
      recoveryCause: "rate_limit",
      selectedConcurrency: 1,
    });
    for (const permit of permits.slice(1))
      await permit?.complete({ kind: "success" }, 1_200);
    now = 2_001;
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    await probe?.complete({ kind: "success" }, 2_100);
    await expect(target.snapshot(OPEN, 2_100)).resolves.toMatchObject({
      selectedConcurrency: 128,
    });
  });

  it("ramps exponentially to 256 and never exceeds the configured ceiling", async () => {
    const target = scheduler(256).create();
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(2);

    const expectedConcurrency = [4, 8, 16, 32, 64, 128, 256];
    for (const expected of expectedConcurrency) {
      await complete(target, { kind: "success" });
      await complete(target, { kind: "success" });
      // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
      expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(expected);
    }

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      ewmaLatencyMs: 100,
      samples: 14,
      selectedConcurrency: 256,
    });

    const permits = await Promise.all(
      Array.from({ length: 256 }, async () => target.acquire(OPEN)),
    );
    expect(permits.every(Boolean)).toBe(true);
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).blockReason).toBe("at_capacity");
    for (const permit of permits)
      await permit?.complete({ kind: "success" }, 1_200);
  });

  it("never starts while any global safety gate is closed", async () => {
    const target = scheduler().create();
    for (const [gate, reason] of [
      [{ ...OPEN, paused: true }, "paused"],
      [{ ...OPEN, diskWritable: false }, "disk_pressure"],
      [{ ...OPEN, circuitOpen: true }, "circuit_open"],
      [{ ...OPEN, quotaWaitUntil: 2_000 }, "quota_wait"],
    ] as const) {
      await expect(target.acquire(gate)).resolves.toBeNull();
      // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
      expect((await target.snapshot(gate)).blockReason).toBe(reason);
    }
  });

  it("never lets pre-pressure successes undo a rate-limit reduction", async () => {
    const target = scheduler(100).create();
    for (let step = 0; step < 6; step += 1) {
      await complete(target, { kind: "success" });
      await complete(target, { kind: "success" });
    }
    const admitted = await Promise.all(
      Array.from({ length: 100 }, async () => target.acquire(OPEN)),
    );
    await admitted[0]?.complete(
      { kind: "rate_limited", retryAt: 5_000 },
      1_100,
    );
    for (const permit of admitted.slice(1))
      await permit?.complete({ kind: "success" }, 1_100);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      recoveryCause: "rate_limit",
      selectedConcurrency: 1,
      successStreak: 0,
    });
  });

  it("uses only a short global dampener for an isolated task error", async () => {
    let now = 1_000;
    const target = scheduler(4, () => now).create();
    await complete(target, { kind: "error", retryAt: 60_000 }, now);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "error_dampener",
      nextWakeAt: 2_000,
      selectedConcurrency: 2,
    });
    now = 2_000;
    await expect(target.acquire(OPEN)).resolves.not.toBeNull();
  });

  it("tolerates one ambiguous paid outcome but halves on a repeated cohort signal", async () => {
    let now = 1_000;
    const target = scheduler(128, () => now).create();
    for (let step = 0; step < 12; step += 1)
      await complete(target, { kind: "success" }, now);
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(128);

    const cohort = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    await cohort[0]?.complete({ kind: "ambiguous_outcome" }, now + 1);

    await expect(target.snapshot(OPEN, now + 1)).resolves.toMatchObject({
      selectedConcurrency: 128,
      successStreak: 0,
    });

    await cohort[1]?.complete({ kind: "ambiguous_outcome" }, now + 2);
    for (const permit of cohort.slice(2))
      await permit?.complete({ kind: "success" }, now + 3);

    await expect(target.snapshot(OPEN, now + 3)).resolves.toMatchObject({
      blockReason: "error_dampener",
      selectedConcurrency: 64,
      successStreak: 0,
    });

    now += 31_000;
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).blockReason).toBeNull();
  });

  it("does not combine isolated ambiguous outcomes across the pressure window", async () => {
    let now = 1_000;
    const target = scheduler(16, () => now).create();
    for (let step = 0; step < 6; step += 1)
      await complete(target, { kind: "success" }, now);
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(16);

    await complete(target, { kind: "ambiguous_outcome" }, now);
    now += 31 * 60_000;
    await complete(target, { kind: "ambiguous_outcome" }, now);
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(16);
  });

  it("persists an ambiguous pressure signal across a host restart", async () => {
    let now = 1_000;
    const fixture = scheduler(16, () => now);
    const firstProcess = fixture.create();
    for (let step = 0; step < 6; step += 1)
      await complete(firstProcess, { kind: "success" }, now);
    await complete(firstProcess, { kind: "ambiguous_outcome" }, now);

    now += 1_000;
    const restarted = fixture.create();
    await complete(restarted, { kind: "ambiguous_outcome" }, now);

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "error_dampener",
      selectedConcurrency: 8,
    });
  });

  it("collapses a same-cohort process failure storm to one lane", async () => {
    const now = 1_000;
    const fixture = scheduler(32, () => now);
    const target = fixture.create();
    for (let step = 0; step < 8; step += 1) {
      await complete(target, { kind: "success" });
    }
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(32);
    const cohort = await Promise.all(
      Array.from({ length: 32 }, async () => target.acquire(OPEN)),
    );
    for (const permit of cohort) await permit?.complete({ kind: "error" }, now);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "error_dampener",
      consecutiveErrors: 32,
      nextWakeAt: 31_000,
      selectedConcurrency: 1,
    });

    const restarted = fixture.create();

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "error_dampener",
      selectedConcurrency: 1,
    });
  });

  it("preserves adaptive promotion evidence across task-quality failures", async () => {
    const target = scheduler(8).create();
    await complete(target, { kind: "success" });

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      selectedConcurrency: 2,
      successStreak: 1,
    });

    await complete(target, { kind: "task_failure" });

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      consecutiveErrors: 0,
      selectedConcurrency: 2,
      successStreak: 1,
    });

    await complete(target, { kind: "success" });

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      selectedConcurrency: 4,
      successStreak: 0,
    });
  });

  it("clears stale isolated errors without erasing accepted successes", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-neutral-task-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 48,
        consecutiveProviderFailures: 0,
        consecutiveRateLimits: 0,
        errorDampenerUntil: 5_000,
        ewmaLatencyMs: 100,
        providerCredentialGeneration: null,
        providerErrorCode: null,
        providerUntil: 0,
        quotaUntil: 0,
        rateLimitedUntil: 0,
        samples: 48,
        schemaVersion: 7,
        selectedConcurrency: 2,
        successStreak: 1,
        updatedAt: 1_000,
      }),
    );
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      now: () => 5_000,
      promotionSuccesses: 2,
      legacyCurrentStatePath: statePath,
    });

    await complete(target, { kind: "task_failure" }, 5_100);

    await expect(target.snapshot(OPEN, 5_100)).resolves.toMatchObject({
      blockReason: null,
      consecutiveErrors: 0,
      selectedConcurrency: 2,
      successStreak: 1,
    });
  });

  it("opens one bounded recovery slot when migrating stale neutral errors", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-v6-errors-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 48,
        consecutiveProviderFailures: 0,
        consecutiveRateLimits: 0,
        errorDampenerUntil: 5_000,
        ewmaLatencyMs: 100,
        providerCredentialGeneration: null,
        providerErrorCode: null,
        providerUntil: 0,
        quotaUntil: 0,
        rateLimitedUntil: 0,
        samples: 48,
        schemaVersion: 6,
        selectedConcurrency: 1,
        successStreak: 0,
        updatedAt: 1_000,
      }),
    );

    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      initialConcurrency: 1,
      now: () => 1_000,
      legacyCurrentStatePath: statePath,
    });

    await expect(migrated.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      consecutiveErrors: 0,
      selectedConcurrency: 2,
      successStreak: 0,
    });
    expect(persistedScheduler(statePath)).toMatchObject({
      errorDampenerUntil: 0,
      schemaVersion: 11,
    });
  });

  it("does not recover persisted concurrency while real pressure is active", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-v6-pressure-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 0,
        consecutiveProviderFailures: 0,
        consecutiveRateLimits: 1,
        errorDampenerUntil: 0,
        ewmaLatencyMs: 100,
        providerCredentialGeneration: null,
        providerErrorCode: null,
        providerUntil: 0,
        quotaUntil: 0,
        rateLimitedUntil: 5_000,
        samples: 1,
        schemaVersion: 6,
        selectedConcurrency: 1,
        successStreak: 0,
        updatedAt: 1_000,
      }),
    );

    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      initialConcurrency: 1,
      now: () => 1_000,
      legacyCurrentStatePath: statePath,
    });

    await expect(migrated.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "rate_limited",
      selectedConcurrency: 1,
    });
  });

  it("never shortens an existing provider embargo", async () => {
    const now = 1_000;
    const target = scheduler(4, () => now).create();
    const first = await target.acquire(OPEN);
    const second = await target.acquire(OPEN);
    if (!first || !second) throw new Error("Expected scheduler permits");

    await first.complete({ kind: "rate_limited", retryAt: 60_000 }, now);
    await second.complete({ kind: "error" }, now + 100);

    await expect(target.snapshot(OPEN, now + 100)).resolves.toMatchObject({
      blockReason: "rate_limited",
      nextWakeAt: 60_000,
    });
  });

  it("uses exponential provider backoff when no retry hint is supplied", async () => {
    let now = 1_000;
    const target = scheduler(4, () => now).create();

    await complete(target, { kind: "rate_limited" }, now);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      consecutiveRateLimits: 1,
      nextWakeAt: 61_000,
    });

    now = 61_000;
    await complete(target, { kind: "rate_limited" }, now);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      consecutiveRateLimits: 2,
      nextWakeAt: 181_000,
    });
  });

  it("globally gates quota, decreases on pressure, and persists across restart", async () => {
    let now = 1_000;
    const fixture = scheduler(4, () => now);
    const first = fixture.create();
    await complete(first, { kind: "success" });
    await complete(first, { kind: "success" });
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await first.snapshot(OPEN)).selectedConcurrency).toBe(4);

    await complete(first, { kind: "rate_limited", retryAt: 5_000 });

    await expect(first.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "rate_limited",
      nextWakeAt: 5_000,
      selectedConcurrency: 1,
    });
    now = 5_000;
    await complete(
      first,
      {
        kind: "quota_wait",
        resetText: "usage limit; try again at 2099-01-02T03:04:05Z",
      },
      5_100,
    );
    const { quotaUntil } = await first.snapshot(OPEN);
    expect(quotaUntil).toBeGreaterThan(now);
    await expect(first.acquire(OPEN)).resolves.toBeNull();

    const restarted = fixture.create();

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "quota_wait",
      nextWakeAt: 905_100,
      quotaProbeAt: 905_100,
      quotaUntil,
      selectedConcurrency: 1,
    });
  });

  it("admits one periodic real-work probe and clears quota after provider completion", async () => {
    let now = 1_000;
    const fixture = scheduler(128, () => now);
    const target = fixture.create();
    await complete(
      target,
      { kind: "quota_wait", retryAt: now + 86_400_000 },
      now,
    );
    await expect(target.acquire(OPEN)).resolves.toBeNull();

    now += 15 * 60_000;
    const probes = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    expect(probes.filter((permit) => permit !== null)).toHaveLength(1);
    const probe = probes.find((permit) => permit !== null);
    expect(probe?.purpose).toBe("quota_probe");
    await probe?.complete({ kind: "task_failure" }, now + 1);

    await expect(target.snapshot(OPEN, now + 1)).resolves.toMatchObject({
      blockReason: null,
      quotaUntil: 0,
      selectedConcurrency: 1,
    });

    await expect(
      fixture.create().snapshot(OPEN, now + 1),
    ).resolves.toMatchObject({
      blockReason: null,
      quotaProbeAt: 0,
      quotaUntil: 0,
      selectedConcurrency: 1,
    });
  });

  it("persists probe admission before launch so restart cannot fan out", async () => {
    let now = 1_000;
    const fixture = scheduler(128, () => now);
    const target = fixture.create();
    await complete(
      target,
      { kind: "quota_wait", retryAt: now + 86_400_000 },
      now,
    );

    now += 15 * 60_000;
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    const restarted = fixture.create();
    await expect(restarted.acquire(OPEN)).resolves.toBeNull();

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "quota_wait",
      nextWakeAt: now + 105 * 60_000 + 10_000,
    });
  });

  it("admits one recovery probe across competing SQLite schedulers", async () => {
    let now = 1_000;
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-cas-probe-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const create = () =>
      new QuotaAwareSolLaneScheduler({
        ceiling: 128,
        configDigest: CONFIG_DIGEST,
        fixedConcurrency: true,
        now: () => now,
        stateKey: "provider:sol",
        legacyCurrentStatePath: join(root, "scheduler.json"),
        stateStore: ledger,
      });
    try {
      const first = create();
      await complete(first, { kind: "quota_wait", retryAt: 86_401_000 }, now);
      const second = create();
      await second.snapshot(OPEN);
      now += 15 * 60_000;
      const probes = await Promise.all([
        first.acquire(OPEN),
        second.acquire(OPEN),
      ]);
      expect(probes.filter((probe) => probe !== null)).toHaveLength(1);
      expect(probes.find((probe) => probe !== null)?.purpose).toBe(
        "quota_probe",
      );
    } finally {
      ledger.close();
    }
  });

  it("fences a crashed probe through generation plus two maximum-duration reviews", async () => {
    let now = 1_000;
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-crashed-probe-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const create = () =>
      new QuotaAwareSolLaneScheduler({
        ceiling: 128,
        configDigest: CONFIG_DIGEST,
        fixedConcurrency: true,
        now: () => now,
        stateKey: "provider:sol",
        legacyCurrentStatePath: join(root, "scheduler.json"),
        stateStore: ledger,
      });
    try {
      const crashedOwner = create();
      await complete(
        crashedOwner,
        { kind: "quota_wait", retryAt: 86_401_000 },
        now,
      );
      now += 15 * 60_000;
      const abandonedProbe = await crashedOwner.acquire(OPEN);
      expect(abandonedProbe?.purpose).toBe("quota_probe");
      const ownerSnapshot = await crashedOwner.snapshot(OPEN);
      const leaseUntil = ownerSnapshot.recoveryLeaseUntil;
      expect(leaseUntil - now).toBe(105 * 60_000 + 10_000);

      const firstReplacement = create();
      now = leaseUntil - 1;
      await expect(firstReplacement.acquire(OPEN)).resolves.toBeNull();
      await expect(firstReplacement.snapshot(OPEN)).resolves.toMatchObject({
        blockReason: "quota_wait",
        nextWakeAt: leaseUntil,
      });

      now = leaseUntil;
      const secondReplacement = create();
      const recoveryAttempts = await Promise.all([
        firstReplacement.acquire(OPEN),
        secondReplacement.acquire(OPEN),
      ]);
      expect(recoveryAttempts.filter((permit) => permit !== null)).toHaveLength(
        1,
      );
      expect(recoveryAttempts.find((permit) => permit !== null)?.purpose).toBe(
        "quota_probe",
      );
    } finally {
      ledger.close();
    }
  });

  it("keeps fixed 128 fenced while an account-switch probe is in flight", async () => {
    let now = 1_000;
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-hung-probe-"));
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => generation,
      fixedConcurrency: true,
      initialConcurrency: 128,
      now: () => now,
      legacyCurrentStatePath: join(root, "scheduler.json"),
    });
    await complete(target, { kind: "success" }, now);
    generation = "b".repeat(64);
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    now += 10 * 60_000;
    const contenders = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    expect(contenders.every((permit) => permit === null)).toBe(true);
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      active: 1,
      recoveryCause: "account_switch",
      selectedConcurrency: 1,
    });
    await probe?.complete({ kind: "success" }, now);
    await expect(target.snapshot(OPEN, now)).resolves.toMatchObject({
      recoveryCause: null,
      selectedConcurrency: 128,
    });
  });

  it("atomically releases the exact same-owner account-switch lease on idle", async () => {
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-graceful-switch-"));
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 32,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => generation,
      fixedConcurrency: true,
      initialConcurrency: 32,
      now: () => 1_000,
      legacyCurrentStatePath: join(root, "scheduler.json"),
    });
    await complete(target, { kind: "success" });
    generation = "b".repeat(64);
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    expect(probe?.recoveryLease).not.toBeNull();
    await probe?.complete({ kind: "idle" });

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      active: 0,
      recoveryCause: null,
      selectedConcurrency: 32,
    });
    if (probe?.recoveryLease === null || probe === null)
      throw new Error("Expected recovery ownership");
    await expect(
      target.releaseOwnedIdleAccountSwitchLease(probe.recoveryLease),
    ).resolves.toBe(false);
    const normalPermit = await target.acquire(OPEN);
    expect(normalPermit?.purpose).toBe("normal");
  });

  it("atomically releases an account-switch lease when the budget is exhausted", async () => {
    let generation = "a".repeat(64);
    const target = scheduler(
      4,
      () => 1_000,
      CONFIG_DIGEST,
      () => generation,
    ).create();
    await complete(target, { kind: "success" });
    generation = "b".repeat(64);
    const probe = await target.acquire(OPEN);
    if (probe?.recoveryLease === null || probe === null)
      throw new Error("Expected account-switch recovery ownership");

    await probe.complete({ kind: "budget_exhausted" }, 1_100);
    await expect(target.snapshot(OPEN, 1_100)).resolves.toMatchObject({
      recoveryCause: null,
      selectedConcurrency: 1,
    });
    await expect(
      target.releaseOwnedIdleAccountSwitchLease(probe.recoveryLease),
    ).resolves.toBe(false);

    const rearmed = await target.acquire(OPEN);
    expect(rearmed?.purpose).toBe("normal");
    await rearmed?.complete({ kind: "success" }, 1_101);
  });

  it.each([
    [
      "authentication",
      {
        errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
        kind: "provider_wait",
      } as const,
      5 * 60_000 + 1_000,
    ],
    [
      "network",
      {
        errorCode: "ENRICHMENT_NETWORK_UNAVAILABLE",
        kind: "provider_wait",
        retryAt: 2_000,
      } as const,
      2_000,
    ],
    ["quota", { kind: "quota_wait", retryAt: 2_000 } as const, 2_000],
  ])(
    "never releases an owned %s recovery lease through the account-switch seam",
    async (cause, pressure, dueAt) => {
      let now = 1_000;
      const target = scheduler(4, () => now).create();
      await complete(target, pressure, now);
      now = dueAt;
      const probe = await target.acquire(OPEN);
      if (probe?.recoveryLease === null || probe === null)
        throw new Error("Expected recovery ownership");
      await probe.complete({ kind: "idle" }, now + 1);

      await expect(
        target.releaseOwnedIdleAccountSwitchLease(probe.recoveryLease),
      ).resolves.toBe(false);
      await expect(target.snapshot(OPEN, now + 1)).resolves.toMatchObject({
        recoveryCause: cause,
      });
    },
  );

  it("rejects stale lease and pressure-epoch ownership tokens", async () => {
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-stale-ownership-"));
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 32,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => generation,
      fixedConcurrency: true,
      initialConcurrency: 32,
      now: () => 1_000,
      legacyCurrentStatePath: join(root, "scheduler.json"),
    });
    await complete(target, { kind: "success" });
    generation = "b".repeat(64);
    const probe = await target.acquire(OPEN);
    if (probe?.recoveryLease === null || probe === null)
      throw new Error("Expected recovery ownership");
    await expect(
      target.releaseOwnedIdleAccountSwitchLease({
        ...probe.recoveryLease,
        leaseId: "stale-lease-id",
      }),
    ).resolves.toBe(false);
    await expect(
      target.releaseOwnedIdleAccountSwitchLease({
        ...probe.recoveryLease,
        pressureEpoch: probe.recoveryLease.pressureEpoch + 1,
      }),
    ).resolves.toBe(false);
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      recoveryCause: "account_switch",
      selectedConcurrency: 1,
    });
    await probe.complete({ kind: "idle" });
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      recoveryCause: null,
    });
  });

  it("retains an active old-process lease even with its exact token", async () => {
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-live-orphan-"));
    const target = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 32,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => generation,
      fixedConcurrency: true,
      initialConcurrency: 32,
      now: () => 1_000,
      legacyCurrentStatePath: join(root, "scheduler.json"),
    });
    await complete(target, { kind: "success" });
    generation = "b".repeat(64);
    const probe = await target.acquire(OPEN);
    if (probe?.recoveryLease === null || probe === null)
      throw new Error("Expected recovery ownership");
    await expect(
      target.releaseOwnedIdleAccountSwitchLease(probe.recoveryLease),
    ).resolves.toBe(false);
    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      active: 1,
      recoveryCause: "account_switch",
    });
    await probe.complete({ kind: "idle" });
  });

  it("prevents a restarted foreign scheduler from releasing the old owner", async () => {
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-foreign-restart-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const create = () =>
      new QuotaAwareSolLaneScheduler({
        ceiling: 32,
        configDigest: CONFIG_DIGEST,
        credentialGeneration: () => generation,
        fixedConcurrency: true,
        initialConcurrency: 32,
        now: () => 1_000,
        stateKey: "provider:sol",
        legacyCurrentStatePath: join(root, "scheduler.json"),
        stateStore: ledger,
      });
    try {
      const oldOwner = create();
      await complete(oldOwner, { kind: "success" });
      generation = "b".repeat(64);
      const probe = await oldOwner.acquire(OPEN);
      if (probe?.recoveryLease === null || probe === null)
        throw new Error("Expected recovery ownership");
      const restarted = create();
      await expect(
        restarted.releaseOwnedIdleAccountSwitchLease(probe.recoveryLease),
      ).resolves.toBe(false);
      await expect(restarted.acquire(OPEN)).resolves.toBeNull();
      await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
        recoveryCause: "account_switch",
        selectedConcurrency: 1,
      });
    } finally {
      ledger.close();
    }
  });

  it("persists an idle account-switch completion before restart", async () => {
    let generation = "a".repeat(64);
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-completed-switch-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const create = () =>
      new QuotaAwareSolLaneScheduler({
        ceiling: 4,
        configDigest: CONFIG_DIGEST,
        credentialGeneration: () => generation,
        now: () => 1_000,
        stateKey: "provider:sol",
        legacyCurrentStatePath: join(root, "scheduler.json"),
        stateStore: ledger,
      });
    try {
      const original = create();
      await complete(original, { kind: "success" });
      generation = "b".repeat(64);
      const probe = await original.acquire(OPEN);
      expect(probe?.purpose).toBe("quota_probe");
      await probe?.complete({ kind: "idle" }, 1_100);

      const restarted = create();
      await expect(restarted.snapshot(OPEN, 1_100)).resolves.toMatchObject({
        recoveryCause: null,
      });
      const normal = await restarted.acquire(OPEN);
      expect(normal?.purpose).toBe("normal");
      await normal?.complete({ kind: "success" }, 1_101);
    } finally {
      ledger.close();
    }
  });

  it("probes immediately when the Codex account generation changes during quota wait", async () => {
    let generation = "a".repeat(64);
    const target = scheduler(
      128,
      () => 1_000,
      CONFIG_DIGEST,
      () => generation,
    ).create();
    await complete(target, { kind: "quota_wait", retryAt: 86_401_000 }, 1_000);
    await expect(target.acquire(OPEN)).resolves.toBeNull();

    generation = "b".repeat(64);
    const probes = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    expect(probes.filter((permit) => permit !== null)).toHaveLength(1);
    await probes
      .find((permit) => permit !== null)
      ?.complete({ kind: "success" }, 1_001);

    await expect(target.snapshot(OPEN, 1_001)).resolves.toMatchObject({
      blockReason: null,
      quotaUntil: 0,
      selectedConcurrency: 1,
    });
  });

  it("ignores an old-account probe completion after credentials change", async () => {
    let now = 1_000;
    let generation = "a".repeat(64);
    const target = scheduler(
      128,
      () => now,
      CONFIG_DIGEST,
      () => generation,
    ).create();
    await complete(
      target,
      { kind: "quota_wait", retryAt: now + 86_400_000 },
      now,
    );

    now += 15 * 60_000;
    const staleProbe = await target.acquire(OPEN);
    expect(staleProbe?.purpose).toBe("quota_probe");
    generation = "b".repeat(64);
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    await expect(
      staleProbe?.complete({ kind: "task_failure" }, now + 1),
    ).resolves.toEqual({
      accepted: false,
      quotaCleared: false,
    });

    const switchedAccountProbe = await target.acquire(OPEN);
    expect(switchedAccountProbe?.purpose).toBe("quota_probe");
    await switchedAccountProbe?.complete({ kind: "success" }, now + 2);
    await expect(target.snapshot(OPEN, now + 2)).resolves.toMatchObject({
      quotaUntil: 0,
      selectedConcurrency: 1,
    });
  });

  it("opens one bounded quota probe when migrating schema v7", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-v7-quota-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 0,
        consecutiveProviderFailures: 0,
        consecutiveRateLimits: 0,
        errorDampenerUntil: 0,
        ewmaLatencyMs: 100,
        providerCredentialGeneration: null,
        providerErrorCode: null,
        providerUntil: 0,
        quotaUntil: 86_401_000,
        rateLimitedUntil: 0,
        samples: 1,
        schemaVersion: 7,
        selectedConcurrency: 1,
        successStreak: 0,
        updatedAt: 1_000,
      }),
    );
    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      now: () => 1_000,
      legacyCurrentStatePath: statePath,
    });

    const probes = await Promise.all(
      Array.from({ length: 128 }, async () => migrated.acquire(OPEN)),
    );
    expect(probes.filter((permit) => permit !== null)).toHaveLength(1);
    expect(persistedScheduler(statePath)).toMatchObject({
      schemaVersion: 11,
      selectedConcurrency: 1,
    });
  });

  it("imports v8 into SQLite and leaves rollback state untouched", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-rollback-"));
    const legacyStatePath = join(root, "scheduler.json");
    const statePath = join(root, "scheduler.json.v9");
    const legacy = {
      configDigest: CONFIG_DIGEST,
      consecutiveErrors: 0,
      consecutiveProviderFailures: 0,
      consecutiveRateLimits: 0,
      errorDampenerUntil: 0,
      ewmaLatencyMs: 100,
      providerCredentialGeneration: null,
      providerErrorCode: null,
      providerUntil: 0,
      quotaProbeAt: 1_000,
      quotaUntil: 86_401_000,
      rateLimitedUntil: 0,
      samples: 1,
      schemaVersion: 8,
      selectedConcurrency: 1,
      successStreak: 0,
      updatedAt: 1_000,
    };
    writeFileSync(legacyStatePath, JSON.stringify(legacy));

    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      legacyStatePath,
      now: () => 1_000,
      legacyCurrentStatePath: statePath,
    });
    await migrated.snapshot(OPEN);
    expect(JSON.parse(readFileSync(legacyStatePath, "utf8"))).toEqual(legacy);
    expect(persistedScheduler(statePath)).toMatchObject({
      quotaProbeAt: 1_000,
      schemaVersion: 11,
    });
  });

  it("migrates a legacy global digest without losing a provider embargo", async () => {
    let now = 1_000;
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-digest-"));
    const statePath = join(root, "scheduler.json");
    const providerDigest = schedulerStateDigest({
      configuration: { concurrency: 8 },
      provider: "retired-provider",
    });
    const legacy = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 8,
      configDigest: CONFIG_DIGEST,
      now: () => now,
      legacyCurrentStatePath: statePath,
    });
    await complete(legacy, { kind: "quota_wait", retryAt: 50_000 }, now);

    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 8,
      configDigest: providerDigest,
      legacyConfigDigests: [CONFIG_DIGEST],
      now: () => now,
      legacyCurrentStatePath: statePath,
    });

    await expect(migrated.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "quota_wait",
      quotaUntil: 50_000,
      selectedConcurrency: 1,
    });
    now = 50_000;
    await expect(migrated.acquire(OPEN)).resolves.not.toBeNull();
  });

  it("migrates persisted schema v4 without losing tuning or embargoes", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-v4-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 2,
        consecutiveProviderFailures: 1,
        consecutiveRateLimits: 0,
        ewmaLatencyMs: 100,
        providerErrorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE",
        providerUntil: 5_000,
        quotaUntil: 0,
        rateLimitedUntil: 0,
        samples: 3,
        schemaVersion: 4,
        selectedConcurrency: 3,
        successStreak: 1,
        updatedAt: 1_000,
      }),
    );

    const migrated = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 4,
      configDigest: CONFIG_DIGEST,
      now: () => 1_000,
      legacyCurrentStatePath: statePath,
    });

    await expect(migrated.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      consecutiveErrors: 2,
      nextWakeAt: 5_000,
      selectedConcurrency: 1,
    });
    expect(persistedScheduler(statePath)).toMatchObject({
      errorDampenerUntil: 0,
      schemaVersion: 11,
    });
  });

  it("opens an immediate probe for a stale persisted auth timeout", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-scheduler-auth-timeout-"));
    const statePath = join(root, "scheduler.json");
    writeFileSync(
      statePath,
      JSON.stringify({
        ambiguousOutcomeCount: 0,
        ambiguousWindowStartedAt: 0,
        configDigest: CONFIG_DIGEST,
        consecutiveErrors: 0,
        consecutiveProviderFailures: 5,
        consecutiveRateLimits: 0,
        errorDampenerUntil: 0,
        ewmaLatencyMs: null,
        providerCredentialGeneration: "a".repeat(64),
        providerErrorCode: "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
        providerUntil: 14_401_000,
        quotaProbeAt: 0,
        quotaUntil: 0,
        rateLimitedUntil: 0,
        samples: 0,
        schemaVersion: 9,
        selectedConcurrency: 1,
        successStreak: 0,
        updatedAt: 1_000,
      }),
    );
    const restarted = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => "a".repeat(64),
      now: () => 3_601_000,
      legacyCurrentStatePath: statePath,
    });

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      providerErrorCode: "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
      providerUntil: 3_601_000,
      selectedConcurrency: 1,
    });
    const recovered = await restarted.acquire(OPEN);
    expect(recovered).not.toBeNull();
    await recovered?.complete({ kind: "success" }, 3_601_100);
    const afterRecovery = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(statePath),
      stateKey: "provider:test",
      ceiling: 128,
      configDigest: CONFIG_DIGEST,
      credentialGeneration: () => "a".repeat(64),
      now: () => 3_601_200,
      legacyCurrentStatePath: statePath,
    });
    await expect(afterRecovery.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      providerErrorCode: null,
      providerUntil: 0,
      selectedConcurrency: 1,
      successStreak: 1,
    });
  });

  it("single-flight probes a switched account after old calls drain", async () => {
    let generation = "a".repeat(64);
    let now = 1_000;
    const target = scheduler(
      3,
      () => now,
      CONFIG_DIGEST,
      () => generation,
    ).create();
    const oldOne = await target.acquire(OPEN);
    const oldTwo = await target.acquire(OPEN);
    if (!oldOne || !oldTwo) throw new Error("Expected initial permits");
    now = 2_000;
    await oldOne.complete({ kind: "quota_wait", retryAt: 100_000 }, now);

    generation = "b".repeat(64);
    now = 3_000;
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    await oldTwo.complete({ kind: "success" }, 3_500);
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    await probe?.complete({ kind: "success" }, 4_000);
    await expect(target.snapshot(OPEN, 4_000)).resolves.toMatchObject({
      quotaUntil: 0,
    });
    await expect(target.snapshot(OPEN, 5_000)).resolves.toMatchObject({
      blockReason: null,
      quotaUntil: 0,
    });
  });

  it("hashes provider scheduler identity canonically", async () => {
    expect(
      schedulerStateDigest({ provider: "sol", tuning: { ceiling: 32 } }),
    ).toBe(schedulerStateDigest({ tuning: { ceiling: 32 }, provider: "sol" }));
    expect(
      schedulerStateDigest({ provider: "sol", tuning: { ceiling: 32 } }),
    ).not.toBe(
      schedulerStateDigest({
        provider: "retired-provider",
        tuning: { ceiling: 32 },
      }),
    );
  });

  it("persists a provider circuit and admits only one probe when it expires", async () => {
    let now = 1_000;
    const fixture = scheduler(16, () => now);
    const first = fixture.create();
    await complete(
      first,
      {
        errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
        kind: "provider_wait",
        retryAt: 10_000,
      },
      now,
    );

    await expect(first.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      consecutiveProviderFailures: 1,
      nextWakeAt: 61_000,
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
      providerUntil: 61_000,
      selectedConcurrency: 1,
    });

    const restarted = fixture.create();
    await expect(restarted.acquire(OPEN)).resolves.toBeNull();
    now = 61_000;
    const probes = await Promise.all(
      Array.from({ length: 16 }, async () => restarted.acquire(OPEN)),
    );
    expect(probes.filter((permit) => permit !== null)).toHaveLength(1);
    await probes
      .find((permit) => permit !== null)
      ?.complete({ kind: "success" }, now + 1);

    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      consecutiveProviderFailures: 0,
      providerErrorCode: null,
      providerUntil: 0,
    });
  });

  it("counts one provider outage cohort at 256-way concurrency", async () => {
    const target = scheduler(256).create();
    for (let step = 0; step < 7; step += 1) {
      await complete(target, { kind: "success" });
      await complete(target, { kind: "success" });
    }
    const admitted = await Promise.all(
      Array.from({ length: 256 }, async () => target.acquire(OPEN)),
    );
    expect(admitted.every(Boolean)).toBe(true);
    await admitted[0]?.complete(
      { errorCode: "ENRICHMENT_AUTH_REQUIRED", kind: "provider_wait" },
      1_100,
    );
    await admitted[1]?.complete(
      { errorCode: "CODEX_OAUTH_TOKEN_REVOKED", kind: "provider_wait" },
      1_100,
    );
    for (const permit of admitted.slice(2))
      await permit?.complete(
        { errorCode: "ENRICHMENT_AUTH_REQUIRED", kind: "provider_wait" },
        1_100,
      );

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      consecutiveProviderFailures: 1,
      providerErrorCode: "ENRICHMENT_AUTH_REQUIRED",
      providerUntil: 61_100,
      selectedConcurrency: 1,
    });
  }, 10_000);

  it("admits one recovery probe when the opaque credential generation changes", async () => {
    let generation = "a".repeat(64);
    const target = scheduler(
      128,
      () => 1_000,
      CONFIG_DIGEST,
      () => generation,
    ).create();
    await complete(target, {
      errorCode: "CODEX_OAUTH_TOKEN_REVOKED",
      kind: "provider_wait",
    });
    await expect(target.acquire(OPEN)).resolves.toBeNull();
    generation = "b".repeat(64);
    const probes = await Promise.all(
      Array.from({ length: 128 }, async () => target.acquire(OPEN)),
    );
    expect(probes.filter((probe) => probe !== null)).toHaveLength(1);
    await probes
      .find((permit) => permit !== null)
      ?.complete({ kind: "success" }, 1_101);

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      consecutiveProviderFailures: 0,
      providerErrorCode: null,
      providerUntil: 0,
      selectedConcurrency: 1,
    });
  });

  it.each([
    "CODEX_CHATGPT_AUTH_REQUIRED",
    "CODEX_OAUTH_TOKEN_INVALIDATED",
    "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
  ])(
    "opens one immediate %s recovery probe after a credential change",
    async (errorCode) => {
      let generation = "a".repeat(64);
      const target = scheduler(
        16,
        () => 1_000,
        CONFIG_DIGEST,
        () => generation,
      ).create();
      await complete(target, { errorCode, kind: "provider_wait" });
      await expect(target.acquire(OPEN)).resolves.toBeNull();
      generation = "b".repeat(64);
      const probes = await Promise.all(
        Array.from({ length: 16 }, async () => target.acquire(OPEN)),
      );
      expect(probes.filter((probe) => probe !== null)).toHaveLength(1);
    },
  );

  it("uses fixed free auth probes while exponentially backing off other provider failures", async () => {
    let now = 1_000;
    const auth = scheduler(4, () => now).create();
    await complete(
      auth,
      { errorCode: "CODEX_CHATGPT_AUTH_REQUIRED", kind: "provider_wait" },
      now,
    );
    now = 61_000;
    await complete(
      auth,
      { errorCode: "CODEX_CHATGPT_AUTH_REQUIRED", kind: "provider_wait" },
      now,
    );
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await auth.snapshot(OPEN)).providerUntil).toBe(121_000);

    now = 1_000;
    const outage = scheduler(4, () => now).create();
    await complete(
      outage,
      { errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE", kind: "provider_wait" },
      now,
    );
    now = 901_000;
    await complete(
      outage,
      { errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE", kind: "provider_wait" },
      now,
    );
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await outage.snapshot(OPEN)).providerUntil).toBe(2_701_000);
  });

  it("pauses for the jittered network probe without discarding learned concurrency", async () => {
    let now = 1_000;
    const target = scheduler(16, () => now).create();
    for (let step = 0; step < 6; step += 1) {
      await complete(target, { kind: "success" }, now);
      await complete(target, { kind: "success" }, now);
    }
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).selectedConcurrency).toBe(16);

    await complete(
      target,
      {
        errorCode: "ENRICHMENT_NETWORK_UNAVAILABLE",
        kind: "provider_wait",
        retryAt: 6_000,
      },
      now,
    );

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      consecutiveProviderFailures: 0,
      nextWakeAt: 6_000,
      providerErrorCode: "ENRICHMENT_NETWORK_UNAVAILABLE",
      selectedConcurrency: 1,
    });

    now = 6_000;

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: null,
      selectedConcurrency: 1,
    });
    const probe = await target.acquire(OPEN);
    expect(probe?.purpose).toBe("quota_probe");
    await expect(
      probe?.complete({ kind: "success" }, 6_001),
    ).resolves.toMatchObject({ accepted: true });
    await expect(target.snapshot(OPEN, 6_001)).resolves.toMatchObject({
      selectedConcurrency: 16,
    });
  });

  it("uses exponential provider backoff even when a shorter hint repeats", async () => {
    let now = 1_000;
    const target = scheduler(4, () => now).create();
    await complete(
      target,
      {
        errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE",
        kind: "provider_wait",
        retryAt: now + 15 * 60_000,
      },
      now,
    );
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await target.snapshot(OPEN)).providerUntil).toBe(901_000);

    now = 901_000;
    await complete(
      target,
      {
        errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE",
        kind: "provider_wait",
        retryAt: now + 15 * 60_000,
      },
      now,
    );

    await expect(target.snapshot(OPEN)).resolves.toMatchObject({
      consecutiveProviderFailures: 2,
      providerUntil: 2_701_000,
    });
  });

  it("resets tuning on config change and enforces the hard 1..256 ceiling", async () => {
    const fixture = scheduler(2);
    const first = fixture.create();
    await complete(first, { kind: "success" });
    await complete(first, { kind: "success" });
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await first.snapshot(OPEN)).selectedConcurrency).toBe(2);

    const changed = new QuotaAwareSolLaneScheduler({
      stateStore: schedulerStore(join(fixture.root, "scheduler.json")),
      stateKey: "provider:test",
      ceiling: 1,
      configDigest: "b".repeat(64),
      now: () => 1_000,
      legacyCurrentStatePath: join(fixture.root, "scheduler.json"),
    });

    await expect(changed.snapshot(OPEN)).resolves.toMatchObject({
      ceiling: 1,
      samples: 0,
      selectedConcurrency: 1,
    });
    expect(
      () =>
        new QuotaAwareSolLaneScheduler({
          stateStore: schedulerStore(join(fixture.root, "invalid.json")),
          stateKey: "provider:test",
          ceiling: 257,
          configDigest: CONFIG_DIGEST,
          legacyCurrentStatePath: join(fixture.root, "invalid.json"),
        }),
    ).toThrow();
  });

  it("rejects corrupt legacy state without rewriting or deleting evidence", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-sol-scheduler-corrupt-"));
    const statePath = join(root, "scheduler.json");
    const truncated = '{"schemaVersion":9,"selectedConcurrency":128';
    writeFileSync(statePath, truncated);
    const ledger = schedulerStore(statePath);
    const target = new QuotaAwareSolLaneScheduler({
      ceiling: 256,
      configDigest: CONFIG_DIGEST,
      initialConcurrency: 128,
      legacyCurrentStatePath: statePath,
      stateKey: "provider:test",
      stateStore: ledger,
    });
    await expect(target.snapshot(OPEN)).rejects.toThrow(
      "SOL_SCHEDULER_STATE_INVALID",
    );
    expect(ledger.loadSchedulerState("provider:test")).toBeNull();
    expect(readFileSync(statePath, "utf8")).toBe(truncated);
    expect(readdirSync(root)).toEqual(["scheduler.json"]);
  });

  it("ignores a truncated crash temporary when the committed state is valid", async () => {
    const fixture = scheduler(16);
    const first = fixture.create();
    await complete(first, { kind: "success" });
    await complete(first, { kind: "success" });
    const crashTemporary = join(fixture.root, "scheduler.json.999999.tmp");
    writeFileSync(crashTemporary, '{"schemaVersion":');

    const restarted = fixture.create();

    const restartedSnapshot = await restarted.snapshot(OPEN);
    expect(restartedSnapshot.selectedConcurrency).toBe(4);
    expect(readFileSync(crashTemporary, "utf8")).toBe('{"schemaVersion":');
    expect(
      readdirSync(fixture.root).filter((name) => name.includes(".corrupt-")),
    ).toEqual([]);
  });

  it("refreshes a concurrent scheduler before accepting new pressure", async () => {
    const fixture = scheduler(16);
    const first = fixture.create();
    const stale = fixture.create();
    await complete(first, { kind: "success" });
    await complete(first, { kind: "success" });
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited scheduler snapshot.
    expect((await first.snapshot(OPEN)).selectedConcurrency).toBe(4);

    await complete(stale, { kind: "rate_limited", retryAt: 60_000 });

    await expect(stale.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "rate_limited",
      selectedConcurrency: 1,
    });

    await expect(fixture.create().snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "rate_limited",
      selectedConcurrency: 1,
    });
  });

  it("parses bounded ISO and epoch reset timestamps", async () => {
    const now = Date.parse("2099-01-01T00:00:00Z");
    expect(
      parseQuotaResetTimestamp(
        "usage limit; try again at 2099-01-02T03:04:05Z",
        now,
      ),
    ).toBe(Date.parse("2099-01-02T03:04:05Z"));
    expect(parseQuotaResetTimestamp("reset at 4070999045", now)).toBe(
      4_070_999_045_000,
    );
    expect(
      parseQuotaResetTimestamp("connection reset by peer", now),
    ).toBeNull();
  });
});
