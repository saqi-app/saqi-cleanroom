import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { updateRigConcurrency } from "../runtime/concurrency-control.js";
import { ConcurrencyStore } from "../runtime/concurrency-store.js";
import { loadScraperOperationConfig } from "../runtime/operations-contract.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

function fixture() {
  const root = trackedMkdtempSync(join(tmpdir(), "saqi-concurrency-store-"));
  Ledger.initialize(join(root, "ledger.sqlite3")).close();
  return root;
}

describe("SQLite concurrency authority", () => {
  it("rejects a present authority row when its import marker is missing", async () => {
    const root = fixture();
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize({ concurrency: 8, initialConcurrency: 2 }),
    );
    const database = new Database(join(root, "ledger.sqlite3"));
    database.exec(
      "DELETE FROM runtime_control WHERE control_key = 'legacy_concurrency_imported'",
    );
    database.close();
    expect(() => ConcurrencyStore.inspect(root)).toThrow();
    expect(() =>
      ConcurrencyStore.withDatabase(root, false, (store) =>
        store.initialize({ concurrency: 16, initialConcurrency: 4 }),
      ),
    ).toThrow();
    const configPath = join(root, "rig.json");
    writeFileSync(
      configPath,
      JSON.stringify({ schemaVersion: 1, stateDirectory: root }),
    );
    await expect(loadScraperOperationConfig(configPath)).rejects.toThrow();
  });

  it("returns the fresh effective digest while preserving nondefault provider tuning", async () => {
    const root = fixture();
    const configPath = join(root, "rig.json");
    const raw = JSON.stringify({
      schemaVersion: 1,
      stateDirectory: ".",
      collector: { enabled: false },
      sol: {
        concurrency: 16,
        initialConcurrency: 4,
        minimumLaunchIntervalMs: 37,
        maximumPerCyclePerLane: 3,
        promotionSuccesses: 17,
      },
    });
    writeFileSync(configPath, raw);
    const before = await loadScraperOperationConfig(configPath);
    const update = await updateRigConcurrency(
      configPath,
      "sol",
      32,
      before.configDigest,
      8,
    );
    const fresh = await loadScraperOperationConfig(configPath);
    expect(update.configDigest).toBe(fresh.configDigest);
    expect(update.configDigest).not.toBe(before.configDigest);
    expect(fresh.config.sol).toEqual({
      ...before.config.sol,
      concurrency: 32,
      initialConcurrency: 8,
    });
    expect(fresh.config.stateDirectory).toBe(root);
    expect(readFileSync(configPath, "utf8")).toBe(raw);
  });

  it("keeps read-only bootstrap inspection non-mutating", () => {
    const root = trackedMkdtempSync(
      join(tmpdir(), "saqi-concurrency-missing-"),
    );
    expect(ConcurrencyStore.inspect(root)).toBeNull();
    expect(existsSync(join(root, "ledger.sqlite3"))).toBe(false);
    const initialized = fixture();
    expect(ConcurrencyStore.inspect(initialized)).toBeNull();
    expect(ConcurrencyStore.inspect(initialized)).toBeNull();
  });

  it("imports once and ignores subsequent legacy values without changing configuration bytes", async () => {
    const root = fixture();
    const path = join(root, "rig.json");
    const source = JSON.stringify({
      schemaVersion: 1,
      stateDirectory: root,
      sol: { concurrency: 16, initialConcurrency: 2 },
    });
    writeFileSync(path, source);
    const bootstrap = await loadScraperOperationConfig(path);
    const imported = ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize(bootstrap.config.sol),
    );
    expect(imported).toEqual({
      concurrency: 16,
      initialConcurrency: 2,
      revision: 0,
    });
    const afterImport = await loadScraperOperationConfig(path);
    expect(afterImport.configDigest).toBe(bootstrap.configDigest);
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.update(0, { concurrency: 32, initialConcurrency: 4 }),
    );
    expect(readFileSync(path, "utf8")).toBe(source);
    expect(
      ConcurrencyStore.withDatabase(root, false, (store) =>
        store.initialize({ concurrency: 1, initialConcurrency: 1 }),
      ),
    ).toEqual({ concurrency: 32, initialConcurrency: 4, revision: 1 });
    const afterUpdate = await loadScraperOperationConfig(path);
    expect(afterUpdate.config.sol).toMatchObject({
      concurrency: 32,
      initialConcurrency: 4,
    });
    // Even stale invalid tuning cannot override the imported SQLite pair.
    writeFileSync(
      path,
      JSON.stringify({
        schemaVersion: 1,
        stateDirectory: root,
        sol: { concurrency: 1, initialConcurrency: 999 },
      }),
    );
    const afterStaleEdit = await loadScraperOperationConfig(path);
    expect(afterStaleEdit.configDigest).toBe(afterUpdate.configDigest);
  });

  it("preserves operator pauses and budget while changing desired concurrency", () => {
    const root = fixture();
    const ledger = Ledger.initialize(join(root, "ledger.sqlite3"));
    ledger.pauseControls.set("global", true);
    ledger.pauseControls.set("paid", true);
    const before = ledger.armSolPaidUsageBudget(3);
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize({ concurrency: 8, initialConcurrency: 2 }),
    );
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.update(0, { concurrency: 32, initialConcurrency: 4 }),
    );
    expect(ledger.pauseControls.read()).toMatchObject({
      paused: true,
      paidWorkPaused: true,
    });
    expect(ledger.solPaidUsageBudgetStatus()).toEqual(before);
    ledger.close();
  });

  it("rejects stale revisions without reverting the winning desired state", () => {
    const root = fixture();
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize({ concurrency: 8, initialConcurrency: 2 }),
    );
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.update(0, { concurrency: 16, initialConcurrency: 4 }),
    );
    expect(() =>
      ConcurrencyStore.withDatabase(root, false, (store) =>
        store.update(0, { concurrency: 32, initialConcurrency: 32 }),
      ),
    ).toThrow("CONCURRENCY_CONFIG_CHANGED");
    expect(ConcurrencyStore.inspect(root)).toEqual({
      concurrency: 16,
      initialConcurrency: 4,
      revision: 1,
    });
  });

  it("does not fall back to JSON when imported authority is missing", () => {
    const root = fixture();
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize({ concurrency: 8, initialConcurrency: 2 }),
    );
    const database = new Database(join(root, "ledger.sqlite3"));
    database.exec("DELETE FROM runtime_provider_concurrency");
    database.close();
    expect(() => ConcurrencyStore.inspect(root)).toThrow();
    expect(() =>
      ConcurrencyStore.withDatabase(root, false, (store) =>
        store.initialize({ concurrency: 1, initialConcurrency: 1 }),
      ),
    ).toThrow();
  });

  it("rejects invalid persisted controls instead of casting", () => {
    const root = fixture();
    ConcurrencyStore.withDatabase(root, false, (store) =>
      store.initialize({ concurrency: 8, initialConcurrency: 2 }),
    );
    const database = new Database(join(root, "ledger.sqlite3"));
    database.pragma("ignore_check_constraints = ON");
    database.exec("UPDATE runtime_provider_concurrency SET initial = 256");
    database.close();
    expect(() => ConcurrencyStore.inspect(root)).toThrow();
  });
});
