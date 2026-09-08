import { readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it, vi } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { RuntimeOwnerStore } from "../persistence/runtime-owner-store.js";
import { RotatingJsonlLogger } from "../runtime/jsonl-logger.js";
import { parseScraperOperationConfig } from "../runtime/operations-contract.js";
import {
  acquireRunLock,
  readRunLock,
  RunLockBusyError,
} from "../runtime/run-lock.js";
import {
  CoalescingLaneWake,
  MAXIMUM_RUNTIME_DRAIN_REASON,
  type SupervisorLane,
  supervisorMaximumListeners,
  UnifiedSupervisor,
} from "../runtime/supervisor.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DIGEST = "a".repeat(64);

function directory(): string {
  const root = mkdtempSync(join(tmpdir(), "saqi-supervisor-"));
  Ledger.initialize(join(root, "ledger.sqlite3")).close();
  return root;
}

function testConfig(input: Record<string, unknown>) {
  return parseScraperOperationConfig({
    resources: { enabled: false },
    ...input,
  });
}

async function waitForCondition(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise<void>((resolvePromise) => setTimeout(resolvePromise, 1));
  }
  throw new Error("Timed out waiting for test condition");
}

function controlledSleeps(): {
  readonly pending: { milliseconds: number; resolve(): void }[];
  readonly sleep: (milliseconds: number, signal: AbortSignal) => Promise<void>;
} {
  const pending: { milliseconds: number; resolve(): void }[] = [];
  return {
    pending,
    sleep: (milliseconds, signal) =>
      new Promise<void>((resolvePromise) => {
        const finish = () => {
          signal.removeEventListener("abort", finish);
          resolvePromise();
        };
        pending.push({ milliseconds, resolve: finish });
        signal.addEventListener("abort", finish, { once: true });
      }),
  };
}

describe("persistent unified supervisor", () => {
  it("publishes and refreshes lightweight provider state during a long cycle", async () => {
    {
      const root = directory();
      const abort = new AbortController();
      const heartbeat = controlledSleeps();
      const secondHeartbeatWaiting = Promise.withResolvers<undefined>();
      let heartbeatWaits = 0;
      let now = 0;
      const cycle = Promise.withResolvers<{
        nextWakeAt: null;
        result: string;
      }>();
      const entered = Promise.withResolvers<undefined>();
      let providerCalls = 0;
      let fullCalls = 0;
      const supervisor = new UnifiedSupervisor({
        config: testConfig({ schemaVersion: 1, stateDirectory: root }),
        configDigest: DIGEST,
        createLanes: () => [
          {
            close: () => undefined,
            name: "sol-1",
            runOnce: () => {
              entered.resolve(undefined);
              return cycle.promise;
            },
          },
        ],
        now: () => now,
        paused: () => false,
        providerStatus: () => ({ active: ++providerCalls }),
        statusSleep: (milliseconds, signal) => {
          heartbeatWaits += 1;
          if (heartbeatWaits === 2) secondHeartbeatWaiting.resolve(undefined);
          return heartbeat.sleep(milliseconds, signal);
        },
        status: () => ({ full: ++fullCalls }),
      });

      const running = supervisor.run(abort.signal, { maximumCycles: 1 });
      await entered.promise;
      expect(providerCalls).toBe(1);
      expect(fullCalls).toBe(0);
      expect(
        JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
      ).toMatchObject({
        loadedConfigDigest: DIGEST,
        ownerPid: process.pid,
        workload: { active: 1 },
      });

      expect(heartbeat.pending[0]?.milliseconds).toBe(60_000);
      now = 60_001;
      heartbeat.pending[0]?.resolve();
      await secondHeartbeatWaiting.promise;
      expect(providerCalls).toBe(2);
      expect(fullCalls).toBe(0);
      expect(heartbeat.pending[1]?.milliseconds).toBe(60_000);
      now = 120_002;
      heartbeat.pending[1]?.resolve();
      await waitForCondition(() => providerCalls === 3);
      expect(providerCalls).toBe(3);
      expect(fullCalls).toBe(0);
      cycle.resolve({ nextWakeAt: null, result: "idle" });
      await running;
      expect(fullCalls).toBe(1);
    }
  });

  it("coalesces provider heartbeats and never overlaps slow snapshots", async () => {
    {
      const root = directory();
      const abort = new AbortController();
      const heartbeat = controlledSleeps();
      let now = 0;
      const cycle = Promise.withResolvers<{
        nextWakeAt: null;
        result: string;
      }>();
      const entered = Promise.withResolvers<undefined>();
      const firstSnapshot = Promise.withResolvers<{ active: number }>();
      const snapshotEntered = Promise.withResolvers<undefined>();
      let activeSnapshots = 0;
      let fullCalls = 0;
      let maximumSnapshots = 0;
      let providerCalls = 0;
      const supervisor = new UnifiedSupervisor({
        config: testConfig({
          schemaVersion: 1,
          stateDirectory: root,
        }),
        configDigest: DIGEST,
        createLanes: () => [
          {
            close: () => undefined,
            name: "sol-1",
            runOnce: () => {
              entered.resolve(undefined);
              return cycle.promise;
            },
          },
        ],
        now: () => now,
        paused: () => false,
        providerStatus: async () => {
          providerCalls += 1;
          snapshotEntered.resolve(undefined);
          activeSnapshots += 1;
          maximumSnapshots = Math.max(maximumSnapshots, activeSnapshots);
          const result =
            providerCalls === 1
              ? await firstSnapshot.promise
              : { active: providerCalls };
          activeSnapshots -= 1;
          return result;
        },
        statusSleep: heartbeat.sleep,
        status: () => ({ full: ++fullCalls }),
      });

      const running = supervisor.run(abort.signal, { maximumCycles: 1 });
      await snapshotEntered.promise;
      expect(providerCalls).toBe(1);
      now = 5 * 60_000;
      expect(providerCalls).toBe(1);
      expect(maximumSnapshots).toBe(1);
      expect(heartbeat.pending).toHaveLength(0);

      firstSnapshot.resolve({ active: 1 });
      await entered.promise;
      await waitForCondition(() => heartbeat.pending.length >= 1);
      expect(heartbeat.pending[0]?.milliseconds).toBe(60_000);
      now += 60_001;
      heartbeat.pending[0]?.resolve();
      await waitForCondition(() => heartbeat.pending.length >= 2);
      expect(providerCalls).toBe(1);
      expect(fullCalls).toBe(1);
      const aggregateObservedAt = new Date(now).toISOString();
      now += 60_001;
      heartbeat.pending[1]?.resolve();
      await waitForCondition(() => heartbeat.pending.length >= 3);
      expect(providerCalls).toBe(2);
      expect(maximumSnapshots).toBe(1);
      expect(
        JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
      ).toMatchObject({
        observedAt: new Date(now).toISOString(),
        workload: { active: 2, full: 1 },
        workloadObservedAt: aggregateObservedAt,
      });
      cycle.resolve({ nextWakeAt: null, result: "idle" });
      await running;
    }
  });

  it("fences initial admission after a failed provider status and recovers after a successful retry", async () => {
    const heartbeat = controlledSleeps();
    const laneSleep = controlledSleeps();
    const heartbeatWaiting = Promise.withResolvers<undefined>();
    const laneWaiting = Promise.withResolvers<undefined>();
    let now = 0;
    let runs = 0;
    let providerCalls = 0;
    const error = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    const supervisor = new UnifiedSupervisor({
      config: testConfig({ schemaVersion: 1, stateDirectory: directory() }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "sol-1",
          runOnce: () => {
            runs += 1;
            return Promise.resolve({ nextWakeAt: null, result: "idle" });
          },
        },
      ],
      now: () => now,
      paused: () => false,
      providerStatus: () => {
        providerCalls += 1;
        if (providerCalls === 1)
          throw new Error("STATUS_PROVIDER_SNAPSHOT_FAILED");
        return { active: 0 };
      },
      sleep: (milliseconds, signal) => {
        laneWaiting.resolve(undefined);
        return laneSleep.sleep(milliseconds, signal);
      },
      status: () => ({ full: true }),
      statusSleep: (milliseconds, signal) => {
        heartbeatWaiting.resolve(undefined);
        return heartbeat.sleep(milliseconds, signal);
      },
    });

    const running = supervisor.run(new AbortController().signal, {
      maximumCycles: 1,
    });
    await Promise.all([heartbeatWaiting.promise, laneWaiting.promise]);
    expect(runs).toBe(0);
    now = 60_001;
    heartbeat.pending[0]?.resolve();
    await waitForCondition(
      () => providerCalls === 2 && heartbeat.pending.length >= 2,
    );
    laneSleep.pending[0]?.resolve();
    await running;

    expect(runs).toBe(1);
    expect(error).toHaveBeenCalledWith(
      "SAQI_STATUS_WRITE_FAILED",
      "STATUS_PROVIDER_SNAPSHOT_FAILED",
    );
    error.mockRestore();
  });
  it("coalesces producer hints before and during the lane-local wait", async () => {
    const wake = new CoalescingLaneWake();
    const signal = new AbortController().signal;
    wake.notify();
    wake.notify();
    await expect(wake.waitForNextRun(30_000, signal)).resolves.toBeUndefined();

    let settled = false;
    const waiting = wake.waitForNextRun(30_000, signal).then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    wake.notify();
    await waiting;
    expect(settled).toBe(true);
  });

  it("uses a lane-local wake only for its normal post-cycle wait", async () => {
    let waits = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 5_000 },
        schemaVersion: 1,
        stateDirectory: directory(),
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "producer-driven",
          runOnce: () =>
            Promise.resolve({ nextWakeAt: 30_000, result: "idle" }),
          waitForNextRun: () => {
            waits += 1;
            return Promise.resolve();
          },
        },
      ],
      now: () => 0,
      paused: () => false,
    });

    const result = await supervisor.run(new AbortController().signal, {
      maximumCycles: 2,
    });
    expect(result).toMatchObject({ cycles: 2, stopped: "maximum" });
    expect(result).not.toHaveProperty("drainReason");
    expect(waits).toBe(1);
  });

  it("sizes abort listeners from enabled Codex concurrency", () => {
    const config = testConfig({
      collector: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 50, enabled: true },
      stateDirectory: directory(),
    });
    expect(supervisorMaximumListeners(config)).toBe(82);
  });

  it("acknowledges process identity while lane construction is still pending", async () => {
    const root = directory();
    const { promise: laneConstruction, resolve: finishLaneConstruction } =
      Promise.withResolvers<readonly SupervisorLane[]>();
    const { promise: constructing, resolve: markConstructing } =
      Promise.withResolvers<undefined>();
    let workloadSnapshots = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({ schemaVersion: 1, stateDirectory: root }),
      configDigest: DIGEST,
      createLanes: () => {
        markConstructing(undefined);
        return laneConstruction;
      },
      now: () => 100,
      paused: () => false,
      runtimeRelease: {
        closureBytes: 1,
        closureEntries: 2,
        closureSha256: "c".repeat(64),
        commit: "d".repeat(40),
        manifestPath: "/immutable/runtime-release.json",
        nodeVersion: process.version,
        schemaId: "saqi.runtime-release",
        schemaVersion: 2,
      },
      status: () => {
        workloadSnapshots += 1;
        return { paidLane: "must-not-be-reported-ready" };
      },
    });

    const running = supervisor.run(new AbortController().signal, {
      maximumCycles: 1,
    });
    await constructing;

    expect(
      JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
    ).toMatchObject({
      configDigest: DIGEST,
      loadedConfigDigest: DIGEST,
      ownerPid: process.pid,
      runtimeRelease: { commit: "d".repeat(40) },
      state: "starting",
      startup: { lanesReady: false, phase: "creating_lanes" },
    });
    expect(workloadSnapshots).toBe(0);

    finishLaneConstruction([]);
    await expect(running).resolves.toMatchObject({ stopped: "maximum" });
    expect(workloadSnapshots).toBeGreaterThan(0);
  });

  it("persists a sanitized lane-construction failure without claiming readiness", async () => {
    const root = directory();
    const supervisor = new UnifiedSupervisor({
      config: testConfig({ schemaVersion: 1, stateDirectory: root }),
      configDigest: DIGEST,
      createLanes: () => {
        throw new Error("secret upstream diagnostic");
      },
      now: () => 100,
      paused: () => false,
      status: () => {
        throw new Error("workload snapshot must not run during startup fault");
      },
    });

    await expect(
      supervisor.run(new AbortController().signal, { maximumCycles: 1 }),
    ).rejects.toThrow("secret upstream diagnostic");

    const status = JSON.parse(
      readFileSync(join(root, "status.json"), "utf8"),
    ) as Record<string, unknown>;
    expect(status).toMatchObject({
      state: "stopped",
      startup: {
        errorCode: "LANE_CONSTRUCTION_FAILED",
        lanesReady: false,
        phase: "failed",
      },
    });
    expect(status).not.toHaveProperty("workload");
    const diagnostics = readdirSync(join(root, "logs"))
      .map((file) => readFileSync(join(root, "logs", file), "utf8"))
      .join("\n");
    expect(diagnostics).toContain('"event":"startup_fault"');
    expect(diagnostics).toContain('"errorCode":"LANE_CONSTRUCTION_FAILED"');
    expect(diagnostics).not.toContain("secret upstream diagnostic");
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
  });

  it("fences duplicate owners and conservatively recovers a definitely dead pid", async () => {
    const path = join(directory(), "RUN.lock");
    const first = await acquireRunLock(path, DIGEST);
    await expect(acquireRunLock(path, DIGEST)).rejects.toThrow(
      RunLockBusyError,
    );
    await first.release();
    const database = new Database(join(path, "..", "ledger.sqlite3"));
    try {
      new RuntimeOwnerStore(database).claim(
        {
          configDigest: DIGEST,
          pid: 2_147_483_647,
          runId: "2d39ce36-2dc8-4be6-b23c-70b12ee30db6",
          schemaVersion: 1,
          startedAt: "2026-08-25T00:00:00.000Z",
        },
        "supervisor",
        true,
      );
    } finally {
      database.close();
    }
    const recovered = await acquireRunLock(path, DIGEST);
    expect(recovered.record.pid).toBe(process.pid);
    await recovered.release();
  });

  it("rotates bounded JSONL files containing structured events", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-supervisor-logs-"));
    const logger = new RotatingJsonlLogger(root, 180, 2);
    for (let index = 0; index < 12; index += 1) {
      await logger.write({
        event: "cycle",
        lane: "fake",
        result: String(index),
        runId: "run",
        timestamp: new Date(index).toISOString(),
      });
    }
    const files = readdirSync(root);
    expect(files.filter((name) => name !== "events.jsonl")).toHaveLength(2);
    for (const file of files) {
      for (const line of readFileSync(join(root, file), "utf8")
        .trim()
        .split("\n"))
        expect((): unknown => JSON.parse(line) as unknown).not.toThrow();
    }
    expect(logger.health()).toEqual({
      droppedEvents: 0,
      lastErrorCode: null,
      retryAt: null,
      state: "ready",
    });
  });

  it("drops logs under filesystem pressure and retries without stopping work", async () => {
    let now = 0;
    let attempts = 0;
    const appended: string[] = [];
    const logger = new RotatingJsonlLogger(directory(), 1_024, 1, {
      append: (_path, value) => {
        attempts += 1;
        if (attempts === 1)
          throw Object.assign(new Error("synthetic disk pressure"), {
            code: "ENOSPC",
          });
        appended.push(value);
      },
      now: () => now,
    });
    const event = {
      event: "cycle",
      lane: "fake",
      runId: "run",
      timestamp: new Date(0).toISOString(),
    };
    await logger.write(event);
    now = 1_000;
    await logger.write(event);
    expect(attempts).toBe(1);
    expect(logger.health()).toMatchObject({
      droppedEvents: 2,
      lastErrorCode: "ENOSPC",
      retryAt: 30_000,
      state: "pressure_wait",
    });

    now = 30_000;
    await logger.write(event);
    expect(appended).toHaveLength(1);
    expect(logger.health()).toMatchObject({
      droppedEvents: 2,
      lastErrorCode: null,
      retryAt: null,
      state: "ready",
    });
  });

  it("starts lanes concurrently, restarts after a fault, and releases its lock", async () => {
    const root = directory();
    const config = testConfig({
      resources: { enabled: false },
      restart: { errorBackoffMs: 1_000, idlePollMs: 1_000 },
      schemaVersion: 1,
      stateDirectory: root,
    });
    const calls: string[] = [];
    const { promise: barrier, resolve: releaseBarrier } =
      Promise.withResolvers<undefined>();
    let first = true;
    const lane = (name: string, fault = false): SupervisorLane => ({
      name,
      close: () => {
        calls.push(`close:${name}`);
      },
      runOnce: async () => {
        calls.push(`start:${name}`);
        if (calls.filter((entry) => entry.startsWith("start:")).length === 2)
          releaseBarrier(undefined);
        await barrier;
        if (fault && first) {
          first = false;
          throw new Error("SYNTHETIC_LANE_FAILURE: private diagnostic text");
        }
        return { nextWakeAt: null, result: "idle" };
      },
    });
    const sleeps: number[] = [];
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: () => [lane("collector", true), lane("sol-1")],
      desiredConfigDigest: () => DIGEST,
      now: () => 100,
      paused: () => false,
      runtimeRelease: {
        closureBytes: 1,
        closureEntries: 2,
        closureSha256: "c".repeat(64),
        commit: "d".repeat(40),
        manifestPath: "/immutable/runtime-release.json",
        nodeVersion: process.version,
        schemaId: "saqi.runtime-release",
        schemaVersion: 2,
      },
      sleep: (milliseconds) => {
        sleeps.push(milliseconds);
        return Promise.resolve();
      },
    });
    const result = await supervisor.run(new AbortController().signal, {
      maximumCycles: 2,
    });
    expect(result.stopped).toBe("maximum");
    expect(calls.slice(0, 2).toSorted()).toEqual([
      "start:collector",
      "start:sol-1",
    ]);
    expect(calls.filter((entry) => entry === "start:collector")).toHaveLength(
      2,
    );
    expect(sleeps).toEqual([1_000, 1_000]);
    expect(calls).toContain("close:collector");
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
    const events = readdirSync(join(root, "logs")).flatMap((file) =>
      readFileSync(join(root, "logs", file), "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>),
    );
    expect(events).toContainEqual(
      expect.objectContaining({
        errorCode: "SYNTHETIC_LANE_FAILURE",
        event: "fault",
        lane: "collector",
      }),
    );
    expect(JSON.stringify(events)).not.toContain("private diagnostic text");
    expect(
      JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
    ).toMatchObject({
      desiredConfigDigest: DIGEST,
      loadedConfigDigest: DIGEST,
      runtimeRelease: {
        commit: "d".repeat(40),
        manifestPath: "/immutable/runtime-release.json",
      },
      schemaId: "saqi.unified-rig",
      state: "stopped",
    });
  });

  it("drains before admission when a freshly observed source changes desired identity", async () => {
    const root = directory();
    let laneRuns = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({ schemaVersion: 1, stateDirectory: root }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "source-sensitive",
          runOnce: () => {
            laneRuns += 1;
            return Promise.resolve({ nextWakeAt: null, result: "idle" });
          },
        },
      ],
      desiredConfigDigest: () => "b".repeat(64),
      paused: () => false,
    });

    await expect(
      supervisor.run(new AbortController().signal),
    ).resolves.toMatchObject({
      cycles: 0,
      stopped: "config_changed",
    });
    expect(laneRuns).toBe(0);
  });

  it("bounds opted-in future and error wakes while preserving default idle polling", async () => {
    const run = async (options: {
      fault?: boolean;
      honorNextWakeAt?: boolean;
      maximumSleepMs?: number;
      nextWakeAt?: number;
    }) => {
      let now = 0;
      const sleeps: number[] = [];
      const supervisor = new UnifiedSupervisor({
        config: testConfig({
          restart: { errorBackoffMs: 60_000, idlePollMs: 5_000 },
          schemaVersion: 1,
          stateDirectory: directory(),
        }),
        configDigest: DIGEST,
        createLanes: () => [
          {
            close: () => undefined,
            ...(options.honorNextWakeAt === undefined
              ? {}
              : { honorNextWakeAt: options.honorNextWakeAt }),
            ...(options.maximumSleepMs === undefined
              ? {}
              : { maximumSleepMs: options.maximumSleepMs }),
            name: "future-wake",
            runOnce: () => {
              if (options.fault) throw new Error("SYNTHETIC_WAKE_FAILURE");
              return Promise.resolve({
                nextWakeAt: options.nextWakeAt ?? 60_000,
                result: "idle",
              });
            },
          },
        ],
        now: () => now,
        paused: () => false,
        sleep: (milliseconds) => {
          sleeps.push(milliseconds);
          now += milliseconds;
          return Promise.resolve();
        },
      });
      await supervisor.run(new AbortController().signal, {
        maximumCycles: 2,
      });
      return sleeps;
    };

    await expect(run({})).resolves.toEqual([5_000]);
    await expect(
      run({ honorNextWakeAt: true, maximumSleepMs: 30_000 }),
    ).resolves.toEqual([30_000]);
    await expect(
      run({
        honorNextWakeAt: true,
        maximumSleepMs: 30_000,
        nextWakeAt: 20_000,
      }),
    ).resolves.toEqual([20_000]);
    await expect(
      run({ fault: true, honorNextWakeAt: true, maximumSleepMs: 30_000 }),
    ).resolves.toEqual([30_000]);
  });

  it("rejects invalid lane maximum sleeps during construction", async () => {
    const supervisor = new UnifiedSupervisor({
      config: testConfig({ schemaVersion: 1, stateDirectory: directory() }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          maximumSleepMs: 0,
          name: "invalid-sleep",
          runOnce: () => Promise.resolve({ nextWakeAt: null, result: "idle" }),
        },
      ],
      paused: () => false,
    });

    await expect(supervisor.run(new AbortController().signal)).rejects.toThrow(
      "Lane maximum sleep must be a positive integer",
    );
  });

  it("pauses admission without consuming a cycle and resumes after pressure clears", async () => {
    const root = directory();
    let now = 0;
    let pressured = true;
    let calls = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 1_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "work",
          close: () => undefined,
          runOnce: () => {
            calls += 1;
            return Promise.resolve({ nextWakeAt: null, result: "idle" });
          },
        },
      ],
      now: () => now,
      paused: () => false,
      resourcePressure: {
        snapshot: async () => ({
          availableDiskBytes: pressured ? 0 : 10_000,
          freeMemoryBytes: 10_000,
          nextProbeAt: now + 1_000,
          openFileDescriptors: 10,
          processRssBytes: 10,
          reasons: pressured ? (["DISK_PRESSURE"] as const) : [],
          state: pressured ? "resource_wait" : "ready",
        }),
      },
      sleep: (milliseconds) => {
        expect(calls).toBe(0);
        now += milliseconds;
        pressured = false;
        return Promise.resolve();
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 1 });
    expect(calls).toBe(1);
    expect(
      JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
    ).toMatchObject({ resourcePressure: { state: "ready" } });
  });

  it("allows explicitly marked retention maintenance under pressure", async () => {
    const root = directory();
    let calls = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "maintenance-retention",
          close: () => undefined,
          resourcePressureExempt: true,
          runOnce: () => {
            calls += 1;
            return Promise.resolve({ nextWakeAt: null, result: "dry_run" });
          },
        },
      ],
      paused: () => false,
      resourcePressure: {
        snapshot: async () => ({
          availableDiskBytes: 0,
          freeMemoryBytes: 0,
          nextProbeAt: 1_000,
          openFileDescriptors: 1_000,
          processRssBytes: 10_000,
          reasons: ["DISK_PRESSURE"],
          state: "resource_wait",
        }),
      },
    });
    await supervisor.run(new AbortController().signal, { maximumCycles: 1 });
    expect(calls).toBe(1);
  });

  it("does not scan aggregate workload before provider admission", async () => {
    const root = directory();
    let now = 0;
    let statusCalls = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 1_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () =>
        Array.from({ length: 100 }, (_, index) => ({
          name: `lane-${String(index)}`,
          close: () => undefined,
          runOnce: () =>
            Promise.resolve({ nextWakeAt: null, result: "at_capacity" }),
        })),
      now: () => now,
      paused: () => false,
      sleep: () => Promise.resolve(),
      status: () => {
        statusCalls += 1;
        now += 5_000;
        return { ok: true };
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 2 });

    // Ready identity is published without invoking the aggregate workload;
    // the only workload call is the mandatory terminal snapshot.
    expect(statusCalls).toBe(1);
    expect(
      JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
    ).toMatchObject({ observedAt: "1970-01-01T00:00:05.000Z" });
  });

  it("flushes urgent integrity diagnostics without waiting for the throttle", async () => {
    const root = directory();
    let statusCalls = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 1_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "collector",
          close: () => undefined,
          runOnce: () =>
            Promise.resolve({
              nextWakeAt: null,
              result: "maximum",
              urgentStatus: true,
            }),
        },
      ],
      now: () => 0,
      paused: () => false,
      sleep: () => Promise.resolve(),
      status: () => {
        statusCalls += 1;
        return { ok: true };
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 2 });

    // Ready identity omits aggregate work, both urgent cycles bypass the
    // throttle, and shutdown adds the final snapshot.
    expect(statusCalls).toBe(3);
  });

  it("refreshes status on schedule when a lane outcome stays unchanged", async () => {
    const root = directory();
    let now = 0;
    let statusCalls = 0;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 10_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "stable",
          close: () => undefined,
          runOnce: () =>
            Promise.resolve({ nextWakeAt: null, result: "at_capacity" }),
        },
      ],
      now: () => now,
      paused: () => false,
      sleep: (milliseconds) => {
        now += milliseconds;
        return Promise.resolve();
      },
      status: () => {
        statusCalls += 1;
        return { ok: true };
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 62 });

    expect(statusCalls).toBe(3);
  });

  it("refreshes status once when an opted-in lane result changes", async () => {
    const root = directory();
    const results = ["service_wait", "service_wait", "idle"];
    let currentResult = "starting";
    const snapshots: string[] = [];
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 1_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "publication",
          close: () => undefined,
          refreshStatusOnResultChange: true,
          runOnce: () => {
            currentResult = results.shift() ?? "idle";
            return Promise.resolve({
              nextWakeAt: null,
              result: currentResult,
            });
          },
        },
      ],
      now: () => 0,
      paused: () => false,
      sleep: () => Promise.resolve(),
      status: () => {
        snapshots.push(currentResult);
        return { currentResult };
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 3 });

    // The first service wait and its recovery bypass the five-minute throttle;
    // an identical retry does not cause another aggregate snapshot. Shutdown
    // contributes the final mandatory snapshot.
    expect(snapshots).toEqual(["service_wait", "idle", "idle"]);
  });

  it("drains after the active cycle without aborting paid work", async () => {
    const root = directory();
    const config = testConfig({
      schemaVersion: 1,
      stateDirectory: root,
    });
    const drain = new AbortController();
    const { promise: entered, resolve: markEntered } =
      Promise.withResolvers<AbortSignal>();
    const { promise: finish, resolve: finishCycle } =
      Promise.withResolvers<undefined>();
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "sol-1",
          runOnce: async (signal) => {
            markEntered(signal);
            await finish;
            return { nextWakeAt: null, result: "succeeded" };
          },
        },
      ],
      paused: () => false,
    });
    const running = supervisor.run(new AbortController().signal, {
      drainSignal: drain.signal,
    });
    const operationSignal = await entered;
    drain.abort(MAXIMUM_RUNTIME_DRAIN_REASON);
    expect(operationSignal.aborted).toBe(false);
    finishCycle(undefined);

    await expect(running).resolves.toMatchObject({
      cycles: 1,
      drainReason: "maximum_runtime",
      stopped: "drained",
    });
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
  });

  it("starts a 90-second processing budget only after lanes are ready", async () => {
    vi.useFakeTimers();
    try {
      const root = directory();
      const { promise: laneConstruction, resolve: finishLaneConstruction } =
        Promise.withResolvers<readonly SupervisorLane[]>();
      const { promise: constructing, resolve: markConstructing } =
        Promise.withResolvers<undefined>();
      const { promise: firstCycle, resolve: markFirstCycle } =
        Promise.withResolvers<undefined>();
      let runs = 0;
      const supervisor = new UnifiedSupervisor({
        config: testConfig({ schemaVersion: 1, stateDirectory: root }),
        configDigest: DIGEST,
        createLanes: () => {
          markConstructing(undefined);
          return laneConstruction;
        },
        paused: () => false,
      });
      const running = supervisor.run(new AbortController().signal, {
        maximumRuntimeMs: 90_000,
      });

      await constructing;
      await vi.advanceTimersByTimeAsync(90_000);
      expect(runs).toBe(0);

      finishLaneConstruction([
        {
          close: () => undefined,
          name: "free-publication",
          runOnce: () => {
            runs += 1;
            markFirstCycle(undefined);
            return Promise.resolve({ nextWakeAt: null, result: "succeeded" });
          },
          waitForNextRun: (_milliseconds, signal) => {
            if (signal.aborted) return Promise.resolve();
            return new Promise((resolvePromise) =>
              signal.addEventListener("abort", () => resolvePromise(), {
                once: true,
              }),
            );
          },
        },
      ]);
      await firstCycle;
      expect(runs).toBe(1);

      await vi.advanceTimersByTimeAsync(89_999);
      expect(runs).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      await expect(running).resolves.toMatchObject({
        cycles: 1,
        drainReason: "maximum_runtime",
        stopped: "drained",
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("gracefully restarts after a collector fence while in-flight providers finish", async () => {
    const root = directory();
    const config = testConfig({ schemaVersion: 1, stateDirectory: root });
    const { promise: providerEntered, resolve: markProviderEntered } =
      Promise.withResolvers<AbortSignal>();
    const { promise: providerMayFinish, resolve: finishProvider } =
      Promise.withResolvers<undefined>();
    let providerCompleted = false;
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "collector",
          runOnce: () =>
            Promise.resolve({
              nextWakeAt: null,
              restartRequiredCode: "SOURCE_BROWSER_RESTART_REQUIRED",
              result: "restart_required",
            }),
        },
        {
          close: () => undefined,
          name: "sol-1",
          runOnce: async (signal) => {
            markProviderEntered(signal);
            await providerMayFinish;
            providerCompleted = true;
            return { nextWakeAt: null, result: "succeeded" };
          },
        },
      ],
      paused: () => false,
    });

    const running = supervisor.run(new AbortController().signal);
    const providerSignal = await providerEntered;
    await Promise.resolve();
    expect(providerSignal.aborted).toBe(false);
    finishProvider(undefined);
    await expect(running).resolves.toMatchObject({
      stopped: "restart_requested",
    });
    expect(providerCompleted).toBe(true);
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
  });

  it("schedules lane cycles independently without overlapping one lane", async () => {
    const root = directory();
    const config = testConfig({
      restart: { errorBackoffMs: 1_000, idlePollMs: 1_000 },
      schemaVersion: 1,
      stateDirectory: root,
    });
    const { promise: solBarrier, resolve: releaseSol } =
      Promise.withResolvers<undefined>();
    const { promise: collectorSecondRun, resolve: signalCollectorSecondRun } =
      Promise.withResolvers<undefined>();
    let collectorRuns = 0;
    let collectorActive = 0;
    let collectorMaximumActive = 0;
    let solRuns = 0;
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "collector",
          close: () => Promise.resolve(),
          runOnce: async () => {
            collectorRuns += 1;
            collectorActive += 1;
            collectorMaximumActive = Math.max(
              collectorMaximumActive,
              collectorActive,
            );
            await Promise.resolve();
            collectorActive -= 1;
            if (collectorRuns === 2) signalCollectorSecondRun(undefined);
            return { nextWakeAt: null, result: "collected" };
          },
        },
        {
          name: "sol-1",
          close: () => Promise.resolve(),
          runOnce: async () => {
            solRuns += 1;
            if (solRuns === 1) await solBarrier;
            return { nextWakeAt: null, result: "succeeded" };
          },
        },
      ],
      now: () => 100,
      paused: () => false,
      sleep: () => Promise.resolve(),
    });

    const running = supervisor.run(new AbortController().signal, {
      maximumCycles: 2,
    });
    await collectorSecondRun;
    expect({ collectorRuns, solRuns }).toEqual({
      collectorRuns: 2,
      solRuns: 1,
    });
    expect(collectorMaximumActive).toBe(1);
    releaseSol(undefined);
    await expect(running).resolves.toMatchObject({
      cycles: 2,
      stopped: "maximum",
    });
    expect(solRuns).toBe(2);
  });

  it("stays alive while paused and resumes without constructing another lane", async () => {
    const root = directory();
    const config = testConfig({
      schemaVersion: 1,
      stateDirectory: root,
    });
    let paused = true;
    let runs = 0;
    let factories = 0;
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: () => {
        factories += 1;
        return [
          {
            name: "collector",
            close: () => undefined,
            runOnce: () => {
              runs += 1;
              return Promise.resolve({ nextWakeAt: null, result: "idle" });
            },
          },
        ];
      },
      now: () => 100,
      paused: () => paused,
      sleep: () => {
        paused = false;
        return Promise.resolve();
      },
    });
    await supervisor.run(new AbortController().signal, { maximumCycles: 1 });
    expect({ factories, runs }).toEqual({ factories: 1, runs: 1 });
  });

  it("coalesces high-concurrency pause and resume transitions", async () => {
    const root = directory();
    let paused = true;
    let statusCalls = 0;
    let pausedLanes = 0;
    const { promise: pauseBarrier, resolve: releasePausedLanes } =
      Promise.withResolvers<undefined>();
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 1_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () =>
        Array.from({ length: 128 }, (_, index) => ({
          name: `lane-${String(index)}`,
          close: () => undefined,
          runOnce: () => Promise.resolve({ nextWakeAt: null, result: "idle" }),
        })),
      now: () => 0,
      paused: () => paused,
      sleep: () => {
        pausedLanes += 1;
        if (pausedLanes === 128) {
          paused = false;
          releasePausedLanes(undefined);
        }
        return pauseBarrier;
      },
      status: () => {
        statusCalls += 1;
        return { ok: true };
      },
    });

    await supervisor.run(new AbortController().signal, { maximumCycles: 1 });

    const events = readdirSync(join(root, "logs")).flatMap((file) =>
      readFileSync(join(root, "logs", file), "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>),
    );
    expect(events.filter(({ event }) => event === "paused")).toHaveLength(1);
    expect(events.filter(({ event }) => event === "resumed")).toHaveLength(1);
    expect(pausedLanes).toBe(128);
    expect(statusCalls).toBe(2);
  });

  it("drains immediately while paused without starting lane work", async () => {
    const root = directory();
    const drain = new AbortController();
    let runs = 0;
    const { promise: sleeping, resolve: markSleeping } =
      Promise.withResolvers<undefined>();
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        restart: { idlePollMs: 60_000 },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "collector",
          close: () => undefined,
          runOnce: () => {
            runs += 1;
            return Promise.resolve({ nextWakeAt: null, result: "idle" });
          },
        },
      ],
      paused: () => true,
      sleep: (_milliseconds, signal) => {
        markSleeping(undefined);
        return new Promise((resolvePromise) =>
          signal.addEventListener("abort", () => resolvePromise(), {
            once: true,
          }),
        );
      },
    });

    const running = supervisor.run(new AbortController().signal, {
      drainSignal: drain.signal,
    });
    await sleeping;
    drain.abort();

    await expect(running).resolves.toMatchObject({
      cycles: 0,
      drainReason: "external",
      stopped: "drained",
    });
    expect(runs).toBe(0);
  });

  it("closes initialized lanes and releases fencing on an already aborted run", async () => {
    const root = directory();
    const controller = new AbortController();
    controller.abort();
    let closed = false;
    const supervisor = new UnifiedSupervisor({
      config: testConfig({
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          name: "collector",
          close: () => {
            closed = true;
          },
          runOnce: () => Promise.reject(new Error("must not run")),
        },
      ],
      paused: () => false,
    });
    const result = await supervisor.run(controller.signal);
    expect(result).toMatchObject({
      cycles: 0,
      stopped: "aborted",
    });
    expect(result).not.toHaveProperty("drainReason");
    expect(closed).toBe(true);
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
  });
});
