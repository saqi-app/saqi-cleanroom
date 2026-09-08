import { randomUUID } from "node:crypto";
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import {
  AllowedConcurrencySchema,
  applyRigConcurrency,
  updateRigConcurrency,
} from "../runtime/concurrency-control.js";
import {
  type LaunchdControlSnapshot,
  withLaunchdControlLock,
} from "../runtime/launchd-control.js";
import { loadScraperOperationConfig } from "../runtime/operations-contract.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const CONCURRENCY_CASES = ([2, 4, 8, 16, 32, 128, 256] as const).map(
  (value) => ["sol", value] as const,
);

describe("concurrency control", () => {
  it.each([2, 4, 8, 16, 32, 128, 256])("accepts %i", (value) => {
    expect(AllowedConcurrencySchema.parse(value)).toBe(value);
  });

  it.each([1, 3, 64, 127, 257, "16"])("rejects %s", (value) => {
    expect(() => AllowedConcurrencySchema.parse(value)).toThrow();
  });

  it("atomically updates only the selected provider without exposing the config", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-concurrency-"));
    const path = join(root, "rig.json");
    writeFileSync(
      path,
      `${JSON.stringify({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 128, enabled: true, initialConcurrency: 16 },
        stateDirectory: "./state",
      })}\n`,
      { mode: 0o600 },
    );
    chmodSync(path, 0o600);

    Ledger.initialize(join(root, "state", "ledger.sqlite3")).close();
    const before = readFileSync(path, "utf8");
    const update = await updateRigConcurrency(path, "sol", 32);
    expect(readFileSync(path, "utf8")).toBe(before);
    const loaded = await loadScraperOperationConfig(path);
    expect(loaded.config.sol).toMatchObject({
      concurrency: 32,
      initialConcurrency: 32,
    });
    expect(update.configDigest).toBe(loaded.configDigest);
  });

  it("sets target and ceiling together", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-concurrency-"));
    const path = join(root, "rig.json");
    writeFileSync(
      path,
      `${JSON.stringify({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 256, enabled: true, initialConcurrency: 128 },
        stateDirectory: "./state",
      })}\n`,
      { mode: 0o600 },
    );
    Ledger.initialize(join(root, "state", "ledger.sqlite3")).close();
    await updateRigConcurrency(path, "sol", 8);
    const loaded = await loadScraperOperationConfig(path);
    expect(loaded.config.sol).toMatchObject({
      concurrency: 8,
      initialConcurrency: 8,
    });
  });

  it("sets a staged initial value below the target ceiling", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-concurrency-staged-"));
    const path = join(root, "rig.json");
    writeFileSync(
      path,
      `${JSON.stringify({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 2, enabled: true, initialConcurrency: 2 },
        stateDirectory: "./state",
      })}\n`,
      { mode: 0o600 },
    );

    Ledger.initialize(join(root, "state", "ledger.sqlite3")).close();
    const update = await updateRigConcurrency(path, "sol", 4, undefined, 2);
    const loaded = await loadScraperOperationConfig(path);
    expect(update).toMatchObject({
      initialValue: 2,
      previousInitialValue: 2,
      previousValue: 2,
      value: 4,
    });
    expect(loaded.config.sol).toMatchObject({
      concurrency: 4,
      initialConcurrency: 2,
    });
  });

  it("rejects an initial value above the target without changing the file", async () => {
    const { path } = fixture();
    const before = readFileSync(path, "utf8");
    await expect(
      updateRigConcurrency(path, "sol", 4, undefined, 8),
    ).rejects.toThrow("INITIAL_CONCURRENCY_EXCEEDS_TARGET");
    expect(readFileSync(path, "utf8")).toBe(before);
  });

  it("rejects unknown concurrency controls without changing the file", async () => {
    const { path } = fixture();
    const before = readFileSync(path, "utf8");
    await expect(
      updateRigConcurrency(path, "retiredProvider", 8),
    ).rejects.toThrow();
    expect(readFileSync(path, "utf8")).toBe(before);
  });

  it.each(CONCURRENCY_CASES)(
    "updates schema-defaulted %s independently to %i",
    async (provider, value) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-concurrency-defaulted-"));
      const path = join(root, "rig.json");
      writeFileSync(
        path,
        `${JSON.stringify({ collector: { enabled: true }, schemaVersion: 1, stateDirectory: "./state" })}\n`,
        { mode: 0o600 },
      );
      Ledger.initialize(join(root, "state", "ledger.sqlite3")).close();
      const update = await updateRigConcurrency(path, provider, value);
      expect(update.previousValue).toBe(2);
      const loaded = await loadScraperOperationConfig(path);
      expect(loaded.config[provider]).toMatchObject({
        concurrency: value,
        initialConcurrency: value,
      });
    },
  );

  it("rejects a stale compare-and-swap without changing the file", async () => {
    const { path } = fixture();
    const before = readFileSync(path, "utf8");
    await expect(
      updateRigConcurrency(path, "sol", 8, "0".repeat(64)),
    ).rejects.toThrow("CONCURRENCY_CONFIG_CHANGED");
    expect(readFileSync(path, "utf8")).toBe(before);
  });

  it("preserves a stopped service and leaves the setting pending start", async () => {
    const { path } = fixture();
    const calls: string[] = [];
    const result = await applyRigConcurrency(
      path,
      "sol",
      16,
      "net.saqi.test",
      async (input) => {
        calls.push(String(input.action));
        const loaded = await loadScraperOperationConfig(path);
        return service("stopped", loaded.configDigest);
      },
    );
    expect(calls).toEqual(["status", "status"]);
    expect(result.applicationState).toBe("pending_start");
    expect(result.service?.actualState).toBe("stopped");
  });

  it("reports desired and running state after a partial restart failure", async () => {
    const { path } = fixture();
    const loaded = await loadScraperOperationConfig(path);
    const runningDigest = loaded.configDigest;
    const result = await applyRigConcurrency(
      path,
      "sol",
      16,
      undefined,
      async (input) => {
        if (input.action === "restart") throw new Error("restart timed out");
        return service("running_outdated", runningDigest);
      },
    );
    expect(result).toMatchObject({
      applicationState: "restart_failed",
      restartError: "restart timed out",
    });
    expect(result.concurrency.configDigest).not.toBe(runningDigest);
    expect(result.service?.loadedConfigDigest).toBe(runningDigest);
  });

  it("retains the saved mutation when restart and status both fail", async () => {
    const { path } = fixture();
    const result = await applyRigConcurrency(
      path,
      "sol",
      32,
      undefined,
      async (input) => {
        if (input.action === "status") {
          const loaded = await loadScraperOperationConfig(path);
          if (loaded.config.sol.concurrency === 32)
            throw new Error("status unavailable");
          return service("running", loaded.configDigest);
        }
        throw new Error("restart timed out");
      },
    );
    expect(result).toMatchObject({
      applicationState: "status_unavailable",
      service: null,
    });
    expect(result.restartError).toContain("restart timed out");
    expect(result.restartError).toContain("status unavailable");
    const loaded = await loadScraperOperationConfig(path);
    expect(loaded.config.sol).toMatchObject({
      concurrency: 32,
      initialConcurrency: 32,
    });
  });

  it("acknowledges a desired-running transient stop instead of treating it as operator stopped", async () => {
    const { path } = fixture();
    const calls: string[] = [];
    const result = await applyRigConcurrency(
      path,
      "sol",
      16,
      undefined,
      async (input) => {
        calls.push(String(input.action));
        const loaded = await loadScraperOperationConfig(path);
        const digest = loaded.configDigest;
        if (input.action === "restart") return service("running", digest);
        return { ...service("stopped", digest), serviceEnabled: true };
      },
    );
    expect(calls).toEqual(["status", "restart"]);
    expect(result.applicationState).toBe("applied");
    expect(result.service?.loadedConfigDigest).toBe(
      result.concurrency.configDigest,
    );
  });

  it("rejects concurrent control mutations while the first owns the lock", async () => {
    const { path } = fixture();
    const { promise: held, resolve: release } =
      Promise.withResolvers<undefined>();
    const { promise: entered, resolve: markEntered } =
      Promise.withResolvers<undefined>();
    const first = withLaunchdControlLock(path, "restart", () => {
      markEntered(undefined);
      return held;
    });
    await entered;
    await expect(
      withLaunchdControlLock(path, "restart", async () => undefined),
    ).rejects.toThrow("LAUNCHD_CONTROL_BUSY");
    release(undefined);
    await first;
  });

  it("treats a newly-created owner file as busy while its metadata initializes", async () => {
    const { path } = fixture();
    const state = join(dirname(path), "state");
    mkdirSync(state, { recursive: true });
    writeFileSync(join(state, "CONTROL.lock"), "");
    await expect(
      withLaunchdControlLock(path, "restart", async () => undefined),
    ).rejects.toThrow("LAUNCHD_CONTROL_BUSY: owner initializing");
  });
});

function fixture(): { path: string } {
  const root = mkdtempSync(join(tmpdir(), "saqi-concurrency-"));
  const path = join(root, "rig.json");
  writeFileSync(
    path,
    `${JSON.stringify({
      collector: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 128, enabled: true, initialConcurrency: 16 },
      stateDirectory: "./state",
    })}\n`,
    { mode: 0o600 },
  );
  Ledger.initialize(join(root, "state", "ledger.sqlite3")).close();
  return { path };
}

function service(
  actualState: LaunchdControlSnapshot["actualState"],
  loadedConfigDigest: string,
): LaunchdControlSnapshot {
  return {
    action: "status",
    actualState,
    allowedActions: ["status"],
    configDigest: loadedConfigDigest,
    desiredConfigDigest: loadedConfigDigest,
    desiredRuntimeCommit: null,
    detail: null,
    loadedConfigDigest,
    observedAt: new Date().toISOString(),
    ownerPid: actualState === "stopped" ? null : 123,
    runId: actualState === "stopped" ? null : randomUUID(),
    runtimeCommit: null,
    schemaId: "saqi.launchd-control",
    schemaVersion: 1,
    serviceEnabled: actualState !== "stopped",
  };
}
