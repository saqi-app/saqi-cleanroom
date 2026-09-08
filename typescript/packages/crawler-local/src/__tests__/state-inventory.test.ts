import { hash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { inspectStateInventory } from "../runtime/state-inventory.js";

const TEST_ROOTS: string[] = [];

afterEach(() => {
  for (const root of TEST_ROOTS) {
    rmSync(root, { force: true, recursive: true });
  }
  TEST_ROOTS.length = 0;
});

describe("state inventory", () => {
  it.each([
    { schemaVersion: 9, safeToOperate: false, state: "valid" },
    { schemaVersion: 10, safeToOperate: true, state: "valid" },
    { schemaVersion: 11, safeToOperate: true, state: "valid" },
    { schemaVersion: 12, safeToOperate: false, state: "invalid" },
  ])(
    "checks SQLite authority schema $schemaVersion",
    async ({ schemaVersion, safeToOperate, state }) => {
      const fixture = createFixture();
      const serialized = schedulerState(schemaVersion);
      fixture.ledger.saveSchedulerState(
        "provider-v10:sol",
        serialized,
        sha256(serialized),
        null,
        1,
      );

      const inventory = await inspectStateInventory(fixture);

      expect(inventory.codexScheduler).toMatchObject({
        authority: {
          digestValid: true,
          schemaVersion: state === "valid" ? schemaVersion : null,
          state,
        },
        fallback: state === "valid" ? "none" : "blocked",
        fallbackName: null,
        safeToOperate,
      });
      fixture.ledger.close();
    },
  );

  it("fails closed for missing or corrupt current database state", async () => {
    const fixture = createFixture();
    const inventory = await inspectStateInventory(fixture);
    expect(inventory.codexScheduler).toMatchObject({
      authority: { state: "missing" },
      safeToOperate: false,
    });
    fixture.ledger.close();

    const corrupt = {
      ledger: {
        loadSchedulerState: vi.fn((key: string) =>
          key === "provider-v10:sol"
            ? { digest: "0".repeat(64), serialized: schedulerState(10) }
            : null,
        ),
      },
      root: createRoot(),
    };
    const corruptInventory = await inspectStateInventory(corrupt);
    expect(corruptInventory.codexScheduler).toMatchObject({
      authority: { digestValid: false, state: "invalid" },
      fallback: "blocked",
      safeToOperate: false,
    });
  });

  it("reports the exact legacy fallback without treating it as current", async () => {
    const fixture = createFixture();
    const serialized = schedulerState(9);
    fixture.ledger.saveSchedulerState(
      "provider:sol",
      serialized,
      sha256(serialized),
      null,
      1,
    );

    const databaseInventory = await inspectStateInventory(fixture);
    expect(databaseInventory.codexScheduler).toMatchObject({
      fallback: "legacy_sqlite",
      fallbackName: "provider:sol",
      safeToOperate: false,
    });
    fixture.ledger.close();

    const fileRoot = createRoot();
    writeFileSync(join(fileRoot, "sol-scheduler.json.v8"), serialized);
    const fileInventory = await inspectStateInventory({
      ledger: { loadSchedulerState: () => null },
      root: fileRoot,
    });
    expect(fileInventory.codexScheduler).toMatchObject({
      fallback: "legacy_json",
      fallbackName: "sol-scheduler.json.v8",
      safeToOperate: false,
    });
  });

  it("reports only allowlisted inert basenames", async () => {
    const fixture = createFixture();
    writeFileSync(join(fixture.root, "PAID_WORK_PAUSED.deploy-170"), "");
    writeFileSync(join(fixture.root, "credential-secret.json"), "private");

    const inventory = await inspectStateInventory(fixture);

    expect(inventory.inertCandidates).toEqual(["PAID_WORK_PAUSED.deploy-170"]);
    expect(JSON.stringify(inventory)).not.toContain("credential-secret");
    expect(JSON.stringify(inventory)).not.toContain(fixture.root);
    fixture.ledger.close();
  });

  it("uses bounded root metadata without traversing attempts or artifacts", async () => {
    const root = createRoot();
    mkdirSync(join(root, "sol-attempts", "never-read"), { recursive: true });
    mkdirSync(join(root, "artifacts", "never-read"), { recursive: true });
    for (let index = 0; index < 300; index += 1) {
      writeFileSync(
        join(root, `unrelated-${String(index).padStart(3, "0")}`),
        "",
      );
    }
    const loader = vi.fn(() => null);

    const inventory = await inspectStateInventory({
      ledger: { loadSchedulerState: loader },
      root,
    });

    expect(inventory.rootEntries).toEqual({ inspected: 256, truncated: true });
    expect(loader).toHaveBeenCalledTimes(2);
    expect(inventory.surfaces).toHaveLength(16);
  });
});

function createFixture(): { readonly ledger: Ledger; readonly root: string } {
  const root = createRoot();
  return { ledger: Ledger.initialize(join(root, "ledger.sqlite3")), root };
}

function createRoot(): string {
  const root = mkdtempSync(join(tmpdir(), "saqi-state-inventory-"));
  TEST_ROOTS.push(root);
  return root;
}

function sha256(value: string): string {
  return hash("sha256", value, "hex");
}

function schedulerState(schemaVersion: number): string {
  return `${JSON.stringify({
    configDigest: "a".repeat(64),
    consecutiveErrors: 0,
    consecutiveRateLimits: 0,
    ewmaLatencyMs: null,
    quotaUntil: 0,
    rateLimitedUntil: 0,
    samples: 0,
    schemaVersion,
    selectedConcurrency: 2,
    successStreak: 0,
    updatedAt: 1,
  })}\n`;
}
