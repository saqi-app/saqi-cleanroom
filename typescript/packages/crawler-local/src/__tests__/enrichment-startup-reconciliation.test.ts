import { tmpdir } from "node:os";
import { join } from "node:path";

import type { PoemEnrichmentInput } from "@saqi/precedent-iso";
import { describe, expect, it, vi } from "vitest";

import { reconcileExistingEnrichmentInputs } from "../enrichment/enrichment-startup-reconciliation.js";
import { SOL_ENRICHMENT_WORK_KIND } from "../enrichment/sol-coordinator.js";
import { SOL_PIPELINE_VERSION } from "../enrichment/sol-runner.js";
import { Ledger } from "../persistence/ledger.js";
import { inputHash } from "../persistence/work-key.js";
import { parseScraperOperationConfig } from "../runtime/operations-contract.js";
import { UnifiedRigRuntime } from "../runtime/unified-rig.js";
import { importEmptyTestOperations } from "./support/import-empty-test-operations.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

const HASH = "a".repeat(64);
const SCHEMA_VERSION = "saqi.poem-enrichment-input@1";

describe("startup provider reconciliation", () => {
  it("does not page the current profile when Codex is the only target", () => {
    const ledger = Ledger.open(":memory:");
    seedSol(ledger, input("poem-1"));
    const list = vi.spyOn(ledger, "listWorkDefinitionsAfter");

    expect(
      reconcileExistingEnrichmentInputs({ ledger, providers: ["sol"] }),
    ).toMatchObject({ scannedInputs: 0, seeded: 0 });
    expect(list).not.toHaveBeenCalled();
    ledger.close();
  });

  it("does not retranslate historical Sol inputs on a profile release", () => {
    const ledger = Ledger.open(":memory:");
    seedSol(ledger, input("legacy-poem"), 99, "sol-enrichment-v1");
    seedSol(ledger, input("v2-poem"), 99, "sol-word-gloss-v2");

    expect(
      reconcileExistingEnrichmentInputs({
        ledger,
        providers: ["sol"],
      }),
    ).toMatchObject({
      duplicates: 0,
      scannedInputs: 0,
      seeded: 0,
    });
    expect(
      reconcileExistingEnrichmentInputs({
        ledger,
        providers: ["sol"],
      }),
    ).toMatchObject({
      duplicates: 0,
      scannedInputs: 0,
      seeded: 0,
    });
    expect(
      ledger.status().kindProgress.map(({ kind, total }) => ({ kind, total })),
    ).toEqual([{ kind: SOL_ENRICHMENT_WORK_KIND, total: 2 }]);
    ledger.close();
  });

  it("does not scan or seed history even with a small reconciliation bound", () => {
    const ledger = Ledger.open(":memory:");
    seedSol(ledger, input("poem-1"), 0, "sol-enrichment-v1");
    seedSol(ledger, input("poem-2"), 0, "sol-enrichment-v1");
    expect(
      reconcileExistingEnrichmentInputs({
        batchSize: 1,
        ledger,
        maximumInputs: 1,
        providers: ["sol"],
      }),
    ).toMatchObject({ scannedInputs: 0, seeded: 0 });
    expect(
      reconcileExistingEnrichmentInputs({
        batchSize: 1,
        ledger,
        maximumInputs: 2,
        providers: ["sol"],
      }),
    ).toMatchObject({ duplicates: 0, scannedInputs: 0, seeded: 0 });
    ledger.close();
  });

  it("publishes reconciliation counts in runtime baseline and status", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-startup-reconcile-"));
    const database = join(root, "ledger.sqlite3");
    const ledger = Ledger.open(database);
    seedSol(ledger, input("poem-1"));
    ledger.close();
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true, initialConcurrency: 1 },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: "b".repeat(64),
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    await runtime.createLanes(await runtime.preflight());
    await expect(runtime.status()).resolves.toMatchObject({
      baseline: {
        enrichmentReconciliation: { scannedInputs: 0, seeded: 0 },
      },
      enrichmentReconciliation: { scannedInputs: 0, seeded: 0 },
    });
    runtime.close();
  });
});

function input(poemId: string): PoemEnrichmentInput {
  return {
    authorArabic: "المتنبي",
    linesArabic: ["على قدر أهل العزم تأتي العزائم"],
    poemId,
    schemaId: "saqi.poem-enrichment-input",
    schemaVersion: 1,
    sourceContentSha256: HASH,
    sourceRevisionId: HASH,
    titleArabic: "على قدر أهل العزم",
  };
}

function seedSol(
  ledger: Ledger,
  value: PoemEnrichmentInput,
  priority = 0,
  implementationVersion = SOL_PIPELINE_VERSION,
): void {
  ledger.seed({
    implementationVersion,
    input: value,
    inputHash: inputHash(value),
    kind: SOL_ENRICHMENT_WORK_KIND,
    priority,
    schemaVersion: SCHEMA_VERSION,
  });
}
