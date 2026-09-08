import { execFileSync } from "node:child_process";
import { hash } from "node:crypto";
import { existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { configureSource, currentSource } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger";
import { importEmptyTestOperations } from "./support/import-empty-test-operations.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const CLI = resolve(import.meta.dirname, "../cli.ts");

function runCli(commandArguments: readonly string[]): unknown {
  return JSON.parse(
    execFileSync(
      process.execPath,
      ["--import", "tsx", CLI, ...commandArguments],
      {
        encoding: "utf8",
        stdio: "pipe",
      },
    ),
  );
}

describe("crawler CLI command policy", () => {
  it.each(["pause", "resume", "pause-paid"])(
    "%s changes existing controls without source identity, migration, or budget changes",
    (command) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-pause-cli-"));
      const path = join(root, "ledger.sqlite3");
      const originalSource = currentSource();
      configureSource({
        name: "fixture-archive",
        origin: "https://archive.example",
      });
      try {
        const ledger = Ledger.initialize(path);
        ledger.pauseControls.set("global", true);
        ledger.pauseControls.set("paid", false);
        ledger.armSolPaidUsageBudget(3);
        ledger.close();
      } finally {
        configureSource(originalSource);
      }
      const db = new Database(path);
      try {
        const budgets = db.prepare("SELECT * FROM sol_paid_usage_budget").all();
        const reservations = db
          .prepare("SELECT * FROM sol_paid_usage_reservation")
          .all();
        const schema = db.prepare("SELECT version FROM local_schema").get();
        expect(runCli([command, "--state-dir", root])).toMatchObject({
          command,
          paused: command !== "resume",
          paidWorkPaused: command === "pause-paid",
        });
        expect(db.prepare("SELECT * FROM sol_paid_usage_budget").all()).toEqual(
          budgets,
        );
        expect(
          db.prepare("SELECT * FROM sol_paid_usage_reservation").all(),
        ).toEqual(reservations);
        expect(db.prepare("SELECT version FROM local_schema").get()).toEqual(
          schema,
        );
      } finally {
        db.close();
      }
    },
  );

  it.each(["pause", "resume", "pause-paid"])(
    "%s never creates a missing ledger",
    (command) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-pause-missing-"));
      expect(() => runCli([command, "--state-dir", root])).toThrow();
      expect(existsSync(join(root, "ledger.sqlite3"))).toBe(false);
    },
  );

  it.each(["missing-marker", "missing-paid-row", "zero-marker"])(
    "pause rejects %s without importing legacy flags",
    (fault) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-pause-invalid-"));
      const path = join(root, "ledger.sqlite3");
      const ledger = Ledger.initialize(path);
      ledger.pauseControls.read();
      ledger.close();
      const db = new Database(path);
      try {
        if (fault === "zero-marker") {
          db.exec(
            "UPDATE runtime_control SET enabled=0 WHERE control_key='legacy_pause_imported'",
          );
        } else {
          db.prepare("DELETE FROM runtime_control WHERE control_key=?").run(
            fault === "missing-marker"
              ? "legacy_pause_imported"
              : "paid_work_paused",
          );
        }
        const controls = db
          .prepare("SELECT * FROM runtime_control ORDER BY control_key")
          .all();
        expect(() => runCli(["pause", "--state-dir", root])).toThrow();
        expect(
          db
            .prepare("SELECT * FROM runtime_control ORDER BY control_key")
            .all(),
        ).toEqual(controls);
      } finally {
        db.close();
      }
    },
  );

  it("labels a maximum-runtime shutdown without changing drained compatibility", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-runtime-drain-cli-"));
    const configPath = join(root, "config.json");
    await importEmptyTestOperations(root);
    writeFileSync(
      configPath,
      JSON.stringify({
        collector: { enabled: false },
        resources: { enabled: false },
        retention: { enabled: false },
        schemaVersion: 1,
        startupReconciliation: { enabled: false },
        stateDirectory: root,
      }),
    );

    expect(
      runCli(["run", "--config", configPath, "--maximum-runtime-ms", "1000"]),
    ).toMatchObject({
      command: "run",
      result: {
        cycles: 1,
        drainReason: "maximum_runtime",
        stopped: "drained",
      },
    });
  }, 10_000);

  it("retires standalone collection in favor of the supervised runtime", () => {
    expect(() =>
      execFileSync(
        process.execPath,
        ["--import", "tsx", CLI, "run-collector"],
        { encoding: "utf8", stdio: "pipe" },
      ),
    ).toThrow(
      "run-collector was removed; use `saqi-crawler run --config FILE` so collection obeys RUN.lock and collector.enabled",
    );
  });

  it("retires unbounded render requeue in favor of reserved recovery", () => {
    expect(() =>
      execFileSync(
        process.execPath,
        ["--import", "tsx", CLI, "requeue-render-failures"],
        { encoding: "utf8", stdio: "pipe" },
      ),
    ).toThrow(
      "requeue-render-failures was removed; enable collector.recovery and use `saqi-crawler collector-recovery --action arm --config FILE` for bounded recovery",
    );
  });

  it("keeps doctor fast unless a deep integrity scan is explicit", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-doctor-cli-"));
    const ledger = Ledger.initialize(join(root, "ledger.sqlite3"));
    const scheduler = schedulerState();
    ledger.saveSchedulerState(
      "provider-v10:sol",
      scheduler,
      hash("sha256", scheduler, "hex"),
      null,
      1,
    );
    ledger.close();

    expect(runCli(["doctor", "--state-dir", root])).toMatchObject({
      report: { integrity: "ok", integrityScope: "schema" },
      stateInventory: {
        codexScheduler: { fallback: "none", safeToOperate: true },
      },
    });
    expect(
      runCli(["doctor", "--state-dir", root, "--deep-integrity"]),
    ).toMatchObject({
      report: { integrity: "ok", integrityScope: "full" },
    });
  }, 15_000);

  it("makes doctor fail closed when current scheduler authority is missing", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-doctor-state-cli-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();

    expect(() => runCli(["doctor", "--state-dir", root])).toThrow();
  }, 15_000);

  it("requires and durably arms an explicit paid Sol operation ceiling", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-paid-budget-cli-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    writeFileSync(join(root, "PAID_WORK_PAUSED"), "test fence\n");

    expect(() => runCli(["resume-paid", "--state-dir", root])).toThrow(
      "resume-paid requires --maximum-sol-operations",
    );
    expect(
      runCli([
        "resume-paid",
        "--state-dir",
        root,
        "--maximum-sol-operations",
        "6",
      ]),
    ).toMatchObject({
      paidUsageBudget: {
        maximumOperations: 6,
        remainingOperations: 6,
        state: "active",
      },
      paidWorkPaused: false,
    });
    expect(
      runCli([
        "resume-paid",
        "--state-dir",
        root,
        "--maximum-sol-operations",
        "6",
      ]),
    ).toMatchObject({
      paidUsageBudget: { remainingOperations: 6, state: "active" },
    });
    expect(
      runCli([
        "resume-paid",
        "--state-dir",
        root,
        "--maximum-sol-operations",
        "9",
      ]),
    ).toMatchObject({
      paidUsageBudget: {
        maximumOperations: 9,
        remainingOperations: 9,
        state: "active",
      },
    });
    expect(() =>
      runCli([
        "resume-paid",
        "--state-dir",
        root,
        "--maximum-sol-operations",
        "6",
      ]),
    ).toThrow("SOL_PAID_USAGE_BUDGET_CANNOT_DECREASE");
  }, 30_000);

  it("requires explicit confirmation before clearing a source stop", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-source-stop-cli-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();

    expect(() => runCli(["clear-source-stop", "--state-dir", root])).toThrow(
      "clear-source-stop requires --confirm",
    );
  });

  it("exposes browser-free durable recovery status and explicit arming", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-recovery-cli-"));
    const configPath = join(root, "config.json");
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    writeFileSync(
      configPath,
      JSON.stringify({
        collector: { recovery: { enabled: true } },
        schemaVersion: 1,
        sol: { enabled: false },
        stateDirectory: root,
      }),
    );

    expect(
      runCli(["collector-recovery", "--action", "status", "--state-dir", root]),
    ).toMatchObject({ action: "status", state: { phase: "disarmed" } });
    expect(
      runCli(["collector-recovery", "--action", "arm", "--config", configPath]),
    ).toMatchObject({ action: "arm", state: { phase: "ready" } });
    expect(
      runCli(["collector-recovery", "--action", "status", "--state-dir", root]),
    ).toMatchObject({ action: "status", state: { phase: "ready" } });
  }, 15_000);
});

function schedulerState(): string {
  return `${JSON.stringify({
    configDigest: "a".repeat(64),
    consecutiveErrors: 0,
    consecutiveRateLimits: 0,
    ewmaLatencyMs: null,
    quotaUntil: 0,
    rateLimitedUntil: 0,
    samples: 0,
    schemaVersion: 10,
    selectedConcurrency: 2,
    successStreak: 0,
    updatedAt: 1,
  })}\n`;
}
