import { readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { PoemEnrichmentInput } from "@saqi/precedent-iso";
import { describe, expect, it } from "vitest";

import { SOL_ENRICHMENT_WORK_KIND } from "../enrichment/sol-coordinator";
import { SOL_PIPELINE_VERSION } from "../enrichment/sol-runner";
import { Ledger } from "../persistence/ledger";
import { inputHash } from "../persistence/work-key";
import { parseScraperOperationConfig as parseProductionOperationConfig } from "../runtime/operations-contract";
import { readRunLock } from "../runtime/run-lock.js";
import { UnifiedSupervisor } from "../runtime/supervisor";
import {
  type UnifiedRigPaths,
  UnifiedRigRuntime,
} from "../runtime/unified-rig";
import { importEmptyTestOperations } from "./support/import-empty-test-operations.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DIGEST = "e".repeat(64);
const AUTHOR_ID = "00000000-0000-4000-8000-000000000001";

function parseScraperOperationConfig(
  input: Record<string, unknown>,
): ReturnType<typeof parseProductionOperationConfig> {
  const resources =
    typeof input["resources"] === "object" && input["resources"] !== null
      ? input["resources"]
      : {};
  return parseProductionOperationConfig({
    ...input,
    resources: { enabled: false, ...resources },
  });
}

describe("unified runtime chaos boundaries", () => {
  it("replays a partially seeded baseline without duplicate Sol work", async () => {
    const root = directory();
    const authorsPath = join(root, "authors.ndjson");
    const poemsPath = join(root, "poems.ndjson");
    writeLines(authorsPath, [
      { id: AUTHOR_ID, name_arabic: "شاعر", slug: "author" },
    ]);
    writeLines(poemsPath, [poem(1), { invalid: true }]);
    const config = parseScraperOperationConfig({
      baseline: { authorsPath, batchSize: 1, enabled: true, poemsPath },
      collector: { enabled: false },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const first = runtime(root, config);
    await expect(first.createLanes(await first.preflight())).rejects.toThrow();
    first.close();

    writeLines(poemsPath, [poem(1), poem(2)]);
    const replay = runtime(root, config);
    await replay.createLanes(await replay.preflight());
    expect(statusTotal(await replay.status(), SOL_ENRICHMENT_WORK_KIND)).toBe(
      2,
    );
    replay.close();
  });

  it("releases a scheduler permit after repeated coordinator crashes", async () => {
    const root = directory();
    const authorsPath = join(root, "authors.ndjson");
    const poemsPath = join(root, "poems.ndjson");
    writeLines(authorsPath, [
      { id: AUTHOR_ID, name_arabic: "شاعر", slug: "author" },
    ]);
    writeLines(poemsPath, [poem(1)]);
    const config = parseScraperOperationConfig({
      baseline: { authorsPath, enabled: true, poemsPath },
      collector: { enabled: false },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true },
      stateDirectory: root,
    });
    let calls = 0;
    let runAt = Date.now() + 1_000;
    const fundedLedger = Ledger.initialize(join(root, "ledger.sqlite3"));
    fundedLedger.armSolPaidUsageBudget(12);
    fundedLedger.close();
    const rig = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => runAt,
      paths: paths(root),
      solCoordinatorFactory: ({ ledger }) => ({
        run: () => {
          calls += 1;
          return Promise.reject(new Error("synthetic SIGKILL boundary"));
        },
        seed: (input: PoemEnrichmentInput, priority = 0) =>
          ledger.seed({
            implementationVersion: SOL_PIPELINE_VERSION,
            input,
            inputHash: inputHash(input),
            kind: SOL_ENRICHMENT_WORK_KIND,
            priority,
            schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
          }),
      }),
    });
    const lanes = await rig.createLanes(await rig.preflight());
    const sol = lanes.find(({ name }) => name === "sol-1");
    if (!sol) throw new Error("Sol lane missing");
    await expect(sol.runOnce(new AbortController().signal)).rejects.toThrow();
    await rig.status();
    expect(
      JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
    ).toMatchObject({
      lanes: [expect.objectContaining({ name: "sol", state: "waiting" })],
    });
    runAt += 20_000;
    await expect(sol.runOnce(new AbortController().signal)).rejects.toThrow();
    expect(calls).toBe(2);
    rig.close();
  });

  it("quarantines corrupt optional status in the structured runtime status", async () => {
    const root = directory();
    const statusPath = join(root, "inventory.json");
    writeFileSync(statusPath, "{not-json", { mode: 0o600 });
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      inventory: { statusPath },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { enabled: true },
      stateDirectory: root,
    });
    await expect(runtime(root, config).status()).resolves.toMatchObject({
      authorInventory: { state: "invalid" },
    });
  });

  it("does not strand RUN.lock when log initialization or terminal status fails", async () => {
    const loggerRoot = directory();
    writeFileSync(join(loggerRoot, "logs"), "collision", { mode: 0o600 });
    const loggerConfig = parseScraperOperationConfig({
      schemaVersion: 1,
      stateDirectory: loggerRoot,
    });
    await expect(
      new UnifiedSupervisor({
        config: loggerConfig,
        configDigest: DIGEST,
        createLanes: () => [],
        paused: () => false,
      }).run(new AbortController().signal, { maximumCycles: 1 }),
    ).rejects.toThrow();
    await expect(readRunLock(join(loggerRoot, "RUN.lock"))).resolves.toBeNull();

    const statusRoot = directory();
    const statusConfig = parseScraperOperationConfig({
      schemaVersion: 1,
      stateDirectory: statusRoot,
    });
    await expect(
      new UnifiedSupervisor({
        config: statusConfig,
        configDigest: DIGEST,
        createLanes: () => [
          {
            close: () => undefined,
            name: "fake",
            runOnce: () =>
              Promise.resolve({ nextWakeAt: null, result: "idle" }),
          },
        ],
        paused: () => false,
        status: () => {
          throw new Error("synthetic status serialization fault");
        },
      }).run(new AbortController().signal, { maximumCycles: 1 }),
    ).rejects.toThrow("synthetic status serialization fault");
    await expect(readRunLock(join(statusRoot, "RUN.lock"))).resolves.toBeNull();
  });

  it("drains on a valid config generation change without aborting paid work", async () => {
    const root = directory();
    let desiredDigest = DIGEST;
    let runs = 0;
    const { promise: entered, resolve: markEntered } =
      Promise.withResolvers<AbortSignal>();
    const { promise: finish, resolve: finishWork } =
      Promise.withResolvers<undefined>();
    const supervisor = new UnifiedSupervisor({
      config: parseScraperOperationConfig({
        resources: { enabled: false },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "paid-provider",
          runOnce: async (signal) => {
            runs += 1;
            markEntered(signal);
            await finish;
            return { nextWakeAt: null, result: "succeeded" };
          },
        },
      ],
      desiredConfigDigest: () => desiredDigest,
      paused: () => false,
    });
    const hardStop = new AbortController();
    const running = supervisor.run(hardStop.signal);
    const providerSignal = await entered;

    desiredDigest = "f".repeat(64);
    finishWork(undefined);

    await expect(running).resolves.toMatchObject({
      cycles: 1,
      stopped: "config_changed",
    });
    expect(providerSignal.aborted).toBe(false);
    expect(hardStop.signal.aborted).toBe(false);
    expect(runs).toBe(1);
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
    expect(readEvents(root)).toContainEqual(
      expect.objectContaining({
        errorCode: "CONFIG_CHANGED",
        event: "config_drain",
        lane: "supervisor",
      }),
    );
  });

  it("fails closed before admission when the desired config is unreadable", async () => {
    const root = directory();
    let runs = 0;
    const supervisor = new UnifiedSupervisor({
      config: parseScraperOperationConfig({
        resources: { enabled: false },
        schemaVersion: 1,
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      createLanes: () => [
        {
          close: () => undefined,
          name: "paid-provider",
          runOnce: () => {
            runs += 1;
            return Promise.resolve({ nextWakeAt: null, result: "succeeded" });
          },
        },
      ],
      desiredConfigDigest: () => null,
      paused: () => false,
    });

    await expect(
      supervisor.run(new AbortController().signal),
    ).resolves.toMatchObject({ cycles: 0, stopped: "config_changed" });
    expect(runs).toBe(0);
    await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
    expect(readEvents(root)).toContainEqual(
      expect.objectContaining({
        errorCode: "CONFIG_INVALID",
        event: "config_drain",
        lane: "supervisor",
      }),
    );
  });
});

function directory(): string {
  const root = mkdtempSync(join(tmpdir(), "saqi-rig-chaos-"));
  Ledger.initialize(join(root, "ledger.sqlite3")).close();
  return root;
}

function paths(root: string): UnifiedRigPaths {
  return {
    artifacts: join(root, "artifacts"),
    database: join(root, "ledger.sqlite3"),
    paused: join(root, "PAUSED"),
    root,
    schedulerState: join(root, "sol-scheduler.json"),
    solAttempts: join(root, "sol-attempts"),
  };
}

function runtime(
  root: string,
  config: ReturnType<typeof parseScraperOperationConfig>,
): UnifiedRigRuntime {
  return new UnifiedRigRuntime({
    config,
    configDigest: DIGEST,
    paths: paths(root),
  });
}

function poem(index: number) {
  return {
    author_id: AUTHOR_ID,
    content_arabic: { content: [`بيت ${String(index)}`] },
    id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
    insights_missing: true,
    name_arabic: `قصيدة ${String(index)}`,
    slug: `work-${String(index)}`,
    translation_missing: true,
  };
}

function writeLines(path: string, values: readonly unknown[]): void {
  writeFileSync(
    path,
    `${values.map((value) => JSON.stringify(value)).join("\n")}\n`,
    { mode: 0o600 },
  );
}

function readEvents(root: string): Record<string, unknown>[] {
  const logs = join(root, "logs");
  return readFileSync(join(logs, "events.jsonl"), "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function statusTotal(value: unknown, kind: string): number | undefined {
  const parsed = value as {
    ledger?: { kindProgress?: { kind: string; total: number }[] };
  };
  return parsed.ledger?.kindProgress?.find((item) => item.kind === kind)?.total;
}
