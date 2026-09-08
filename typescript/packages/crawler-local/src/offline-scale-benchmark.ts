#!/usr/bin/env node

import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

import Database from "better-sqlite3";

import { seedAuthorManifests } from "./collection/collector.js";
import { parseCatalogInventory } from "./collection/inventory-reconciliation.js";
import { QuotaAwareSolLaneScheduler } from "./enrichment/sol-lane-scheduler.js";
import { Ledger } from "./persistence/ledger.js";
import { planProductionBaselineFiles } from "./persistence/production-baseline-planner.js";
import type { WorkDefinition } from "./persistence/schema.js";
import { inputHash } from "./persistence/work-key.js";
import { parseScraperOperationConfig } from "./runtime/operations-contract.js";
import { UnifiedSupervisor } from "./runtime/supervisor.js";

const DEFAULT_ROWS = 100_064;
const DEFAULT_AUTHORS = 1_303;
const HASH = "b".repeat(64);

interface Measurement {
  readonly cpuMs: number;
  readonly wallMs: number;
}

interface Threshold {
  readonly actual: number;
  readonly maximum: number;
  readonly pass: boolean;
  readonly unit: string;
}

async function main(commandArguments: readonly string[]): Promise<void> {
  const rows = integerOption(commandArguments, "--rows", DEFAULT_ROWS);
  const authors = integerOption(commandArguments, "--authors", DEFAULT_AUTHORS);
  const keep = commandArguments.includes("--keep");
  const root = await mkdtemp(join(tmpdir(), "saqi-offline-scale-"));
  const rssStart = process.memoryUsage().rss;
  let rssPeak = rssStart;
  const sampleRss = () => {
    rssPeak = Math.max(rssPeak, process.memoryUsage().rss);
  };
  const fdStart = await descriptorCount();
  try {
    const baseline = await benchmarkBaseline(root, rows, sampleRss);
    const authorSeed = benchmarkAuthorSeed(root, authors, sampleRss);
    const ledger = await benchmarkLedger(root, rows, sampleRss);
    const idle = await benchmarkIdleSupervisor(root);
    const sol = await benchmarkSol(root);
    sampleRss();
    const fdEnd = await descriptorCount();
    const thresholds = {
      baselineFirstWallMs: threshold(baseline.first.wallMs, 60_000, "ms"),
      baselineReplayWallMs: threshold(baseline.replay.wallMs, 60_000, "ms"),
      authorSeedWallMs: threshold(authorSeed.measurement.wallMs, 5_000, "ms"),
      fdGrowth: threshold(fdEnd - fdStart, 32, "descriptors"),
      idleCpuMs: threshold(idle.measurement.cpuMs, 5_000, "ms"),
      idleWakeErrorMs: threshold(idle.maximumWakeErrorMs, 1, "ms"),
      ledgerPageWallMs: threshold(ledger.fanoutPaging.wallMs, 15_000, "ms"),
      ledgerSeedWallMs: threshold(ledger.seed.wallMs, 60_000, "ms"),
      ledgerStatusWallMs: threshold(ledger.status.wallMs, 2_000, "ms"),
      logBytes: threshold(idle.logBytes, 2 * 1024 * 1024, "bytes"),
      rssGrowthBytes: threshold(rssPeak - rssStart, 768 * 1024 * 1024, "bytes"),
      statusBytes: threshold(idle.statusBytes, 64 * 1024, "bytes"),
      walBytes: threshold(ledger.walBytes, 128 * 1024 * 1024, "bytes"),
    } satisfies Record<string, Threshold>;
    const report = {
      environment: {
        architecture: process.arch,
        node: process.version,
        platform: process.platform,
      },
      inputs: { authors, rows },
      measurements: {
        authorSeed,
        baseline,
        descriptors: { end: fdEnd, growth: fdEnd - fdStart, start: fdStart },
        idle,
        ledger,
        memory: { rssGrowthBytes: rssPeak - rssStart, rssPeak, rssStart },
        sol,
      },
      passed: Object.values(thresholds).every(({ pass }) => pass),
      root: keep ? root : null,
      schemaId: "saqi.offline-scale-benchmark",
      schemaVersion: 1,
      thresholds,
    };
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    if (!report.passed) process.exitCode = 2;
  } finally {
    if (!keep) await rm(root, { force: true, recursive: true });
  }
}

async function benchmarkBaseline(
  root: string,
  rows: number,
  sampleRss: () => void,
) {
  const authorsPath = join(root, "authors.ndjson");
  const poemsPath = join(root, "poems.ndjson");
  const authorId = "00000000-0000-4000-8000-000000000001";
  await writeLines(authorsPath, [
    { id: authorId, name_arabic: "شاعر", slug: "benchmark-author" },
  ]);
  const stream = createWriteStream(poemsPath, {
    encoding: "utf8",
    mode: 0o600,
  });
  for (let index = 1; index <= rows; index += 1) {
    const line = `${JSON.stringify({
      author_id: authorId,
      content_arabic: { content: ["بيت"] },
      id: uuid(index + 1),
      insights_missing: false,
      name_arabic: `قصيدة ${String(index)}`,
      slug: `work-${String(index)}`,
      translation_missing: false,
    })}\n`;
    if (!stream.write(line)) {
      // eslint-disable-next-line no-await-in-loop -- Respect stream backpressure before writing the next row.
      await once(stream, "drain");
    }
  }
  stream.end();
  await once(stream, "finish");
  const ledger = Ledger.open(join(root, "baseline.sqlite3"));
  try {
    const first = await measureAsync(async () =>
      planProductionBaselineFiles({ authorsPath, ledger, poemsPath }),
    );
    sampleRss();
    const replay = await measureAsync(async () =>
      planProductionBaselineFiles({ authorsPath, ledger, poemsPath }),
    );
    sampleRss();
    return {
      first: first.measurement,
      planHashStable:
        first.value.report.planHash === replay.value.report.planHash,
      replay: replay.measurement,
      report: replay.value.report,
    };
  } finally {
    ledger.close();
  }
}

function benchmarkAuthorSeed(
  root: string,
  authors: number,
  sampleRss: () => void,
) {
  const ledger = Ledger.open(join(root, "authors.sqlite3"));
  try {
    const inventory = parseCatalogInventory(
      Array.from({ length: authors }, (_, index) => ({
        poemCount: index % 100,
        slug: `benchmark-${String(index + 1)}`,
      })),
    );
    const measured = measure(() =>
      seedAuthorManifests(ledger, inventory, 0, "benchmark-generation"),
    );
    sampleRss();
    return {
      inserted: measured.value.filter(({ inserted }) => inserted).length,
      measurement: measured.measurement,
      replayDuplicates: seedAuthorManifests(
        ledger,
        inventory,
        0,
        "benchmark-generation",
      ).filter(({ inserted }) => !inserted).length,
    };
  } finally {
    ledger.close();
  }
}

async function benchmarkLedger(
  root: string,
  rows: number,
  sampleRss: () => void,
) {
  const path = join(root, "scale.sqlite3");
  let ledger = Ledger.open(path);
  const seed = measure(() => {
    for (let offset = 0; offset < rows; offset += 1_000) {
      const definitions: WorkDefinition[] = [];
      for (
        let index = offset;
        index < Math.min(rows, offset + 1_000);
        index += 1
      ) {
        const input = { ordinal: index };
        definitions.push({
          implementationVersion: "benchmark-v1",
          input,
          inputHash: inputHash(input),
          kind: "benchmark-completed",
          priority: 0,
          schemaVersion: "benchmark@1",
        });
      }
      ledger.seedMany(definitions, 1_000);
    }
  }).measurement;
  sampleRss();
  ledger.close();
  const fixture = new Database(path);
  fixture.exec(`
    PRAGMA foreign_keys = ON;
    UPDATE work_item
       SET state = 'succeeded', output_artifact_hash = '${HASH}', updated_at = 2000
     WHERE kind = 'benchmark-completed';
    INSERT INTO work_event(event_id, work_key, attempt_id, lease_epoch, event_type, payload_json, created_at)
    SELECT 'benchmark-succeeded-' || work_key, work_key, NULL, NULL, 'succeeded', '{}', 2000
      FROM work_item WHERE kind = 'benchmark-completed';
  `);
  fixture.close();
  ledger = Ledger.open(path);
  try {
    const status = measure(() => ledger.status(2_001)).measurement;
    const pageScan = measure(() => {
      let cursor = 0;
      let pages = 0;
      let scanned = 0;
      for (;;) {
        const page = ledger.listSucceededAfter(
          cursor,
          ["benchmark-completed"],
          250,
          {
            implementationVersion: "benchmark-v1",
            schemaVersion: "benchmark@1",
          },
        );
        if (page.items.length > 0) pages += 1;
        scanned += page.items.length;
        if (page.cursor === cursor || page.items.length === 0) break;
        cursor = page.cursor;
      }
      return { pages, scanned };
    });
    sampleRss();
    ledger.close();
    const databaseStatus = await stat(path);
    const databaseBytes = databaseStatus.size;
    const walPath = `${path}-wal`;
    return {
      databaseBytes,
      fanoutPaging: {
        ...pageScan.measurement,
        ...pageScan.value,
      },
      seed,
      status,
      walBytes: await existsSize(walPath),
    };
  } finally {
    try {
      ledger.close();
    } catch {
      // Already closed after measurements.
    }
  }
}

async function benchmarkIdleSupervisor(root: string) {
  const stateDirectory = join(root, "idle-supervisor");
  const config = parseScraperOperationConfig({
    logging: { maximumBytes: 2 * 1024 * 1024, retainedFiles: 2 },
    restart: { errorBackoffMs: 1_000, idlePollMs: 1_000 },
    schemaVersion: 1,
    stateDirectory,
  });
  let now = 10_000;
  const sleeps: number[] = [];
  const supervisor = new UnifiedSupervisor({
    config,
    configDigest: HASH,
    createLanes: () => [
      {
        name: "idle",
        close: () => undefined,
        runOnce: () =>
          Promise.resolve({ nextWakeAt: now + 17, result: "idle" }),
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
  const { measurement } = await measureAsync(() =>
    supervisor.run(new AbortController().signal, { maximumCycles: 1_000 }),
  );
  const status = await stat(join(stateDirectory, "status.json"));
  return {
    cycles: 1_000,
    logBytes: await directoryBytes(join(stateDirectory, "logs")),
    maximumWakeErrorMs: Math.max(
      ...sleeps.map((value) => Math.abs(value - 17)),
    ),
    measurement,
    statusBytes: status.size,
  };
}

async function benchmarkSol(root: string) {
  const results: Record<string, unknown> = {};
  for (let ceiling = 1; ceiling <= 4; ceiling += 1) {
    const ledger = Ledger.initialize(
      join(root, `sol-${String(ceiling)}.sqlite3`),
    );
    try {
      let now = 1_000;
      const scheduler = new QuotaAwareSolLaneScheduler({
        ceiling,
        configDigest: HASH,
        now: () => now,
        promotionSuccesses: 2,
        stateKey: "benchmark:sol",
        stateStore: ledger,
      });
      for (;;) {
        // eslint-disable-next-line no-await-in-loop -- Each benchmark level must be learned before the next admission.
        const snapshot = await scheduler.snapshot(OPEN_GATES);
        if (snapshot.selectedConcurrency >= ceiling) break;
        // eslint-disable-next-line no-await-in-loop -- Scheduler persistence is part of the measured admission protocol.
        const permit = await scheduler.acquire(OPEN_GATES);
        if (!permit)
          throw new Error("Benchmark scheduler did not issue permit");
        now += 1;
        // eslint-disable-next-line no-await-in-loop -- Each promotion outcome must be durable before the next sample.
        await permit.complete({ kind: "success" }, now);
      }
      const tasks = 200;
      let cursor = 0;
      // eslint-disable-next-line no-await-in-loop -- Benchmark ceilings run independently to avoid cross-sample contention.
      const measured = await measureAsync(async () => {
        const outcomes = await Promise.allSettled(
          Array.from({ length: ceiling }, async () => {
            while (cursor < tasks) {
              cursor += 1;
              // eslint-disable-next-line no-await-in-loop -- Simulated workers preserve the production acquire-run-complete lifecycle.
              const permit = await scheduler.acquire(OPEN_GATES);
              if (!permit)
                throw new Error("Benchmark exceeded scheduler ceiling");
              // eslint-disable-next-line no-await-in-loop -- Each simulated worker completes one permit before acquiring another.
              await delay(2);
              now += 3;
              // eslint-disable-next-line no-await-in-loop -- A worker completes its permit before acquiring the next task.
              await permit.complete({ kind: "success" }, now);
            }
          }),
        );
        const failure = outcomes.find(
          (outcome) => outcome.status === "rejected",
        );
        if (failure?.status === "rejected") throw failure.reason;
      });
      results[String(ceiling)] = {
        completed: tasks,
        measurement: measured.measurement,
        tasksPerSecond: (tasks * 1_000) / measured.measurement.wallMs,
      };
    } finally {
      ledger.close();
    }
  }
  return results;
}

const OPEN_GATES = {
  circuitOpen: false,
  diskWritable: true,
  paused: false,
  quotaWaitUntil: null,
} as const;

function measure<T>(operation: () => T): {
  measurement: Measurement;
  value: T;
} {
  const wallStart = performance.now();
  const cpuStart = process.cpuUsage();
  const value = operation();
  const cpu = process.cpuUsage(cpuStart);
  return {
    measurement: {
      cpuMs: (cpu.user + cpu.system) / 1_000,
      wallMs: performance.now() - wallStart,
    },
    value,
  };
}

async function measureAsync<T>(
  operation: () => Promise<T>,
): Promise<{ measurement: Measurement; value: T }> {
  const wallStart = performance.now();
  const cpuStart = process.cpuUsage();
  const value = await operation();
  const cpu = process.cpuUsage(cpuStart);
  return {
    measurement: {
      cpuMs: (cpu.user + cpu.system) / 1_000,
      wallMs: performance.now() - wallStart,
    },
    value,
  };
}

function threshold(actual: number, maximum: number, unit: string): Threshold {
  return { actual, maximum, pass: actual <= maximum, unit };
}

function uuid(value: number): string {
  return `00000000-0000-4000-8000-${String(value).padStart(12, "0")}`;
}

async function writeLines(path: string, values: readonly unknown[]) {
  const stream = createWriteStream(path, { encoding: "utf8", mode: 0o600 });
  for (const value of values) stream.write(`${JSON.stringify(value)}\n`);
  stream.end();
  await once(stream, "finish");
}

function integerOption(
  commandArguments: readonly string[],
  name: string,
  fallback: number,
): number {
  const index = commandArguments.indexOf(name);
  if (index < 0) return fallback;
  const value = Number(commandArguments[index + 1]);
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`${name} must be a positive integer`);
  return value;
}

async function descriptorCount(): Promise<number> {
  try {
    const descriptors = await readdir("/dev/fd");
    return descriptors.length;
  } catch {
    return -1;
  }
}

async function existsSize(path: string): Promise<number> {
  try {
    const status = await stat(path);
    return status.size;
  } catch {
    return 0;
  }
}

async function directoryBytes(path: string): Promise<number> {
  let total = 0;
  const entries = await readdir(path, { withFileTypes: true });
  for (const entry of entries) {
    const child = join(path, entry.name);
    if (entry.isDirectory()) {
      // eslint-disable-next-line no-await-in-loop -- Serial recursion bounds concurrently open directories during large benchmarks.
      total += await directoryBytes(child);
    } else {
      // eslint-disable-next-line no-await-in-loop -- Serial stats bound concurrently open file descriptors during large benchmarks.
      total += await fileSize(child);
    }
  }
  return total;
}

async function fileSize(path: string): Promise<number> {
  const status = await stat(path);
  return status.size;
}

await main(process.argv.slice(2));
