import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { expect, test, vi } from "vitest";

import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const { run } = vi.hoisted(() => ({ run: vi.fn() }));
vi.mock(import("../enrichment/sol-coordinator.js"), async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    SolEnrichmentCoordinator: class extends actual.SolEnrichmentCoordinator {
      override run = run;
    },
  };
});

test.each(["success", "abort", "run-failure", "close-failure"])(
  "standalone %s releases only after settled work and successful close",
  async (mode) => {
    vi.resetModules();
    const { Ledger } = await import("../persistence/ledger.js");
    const { readRunLock } = await import("../runtime/run-lock.js");
    const root = trackedMkdtempSync(
      join(tmpdir(), "standalone-owner-lifecycle-"),
    );
    const path = join(root, "ledger.sqlite3");
    Ledger.initialize(path).close();
    const database = new Database(path);
    database.exec(
      "INSERT INTO runtime_control VALUES('sol_operation_import_complete',1),('global_paused',1),('paid_work_paused',1),('legacy_pause_imported',1); INSERT INTO sol_operation_import_receipt VALUES(1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,0,1)",
    );
    const controls = database
      .prepare("SELECT * FROM runtime_control ORDER BY control_key")
      .all();
    const budgets = database
      .prepare("SELECT * FROM sol_paid_usage_budget")
      .all();
    const argv = process.argv;
    const exitCode = process.exitCode;
    let abort: (() => void) | undefined;
    let settled = false;
    const once = process.once.bind(process);
    const onceSpy = vi
      .spyOn(process, "once")
      .mockImplementation((event, listener) => {
        if (event === "SIGTERM") {
          abort = () => listener();
          return process;
        }
        if (event === "SIGINT") return process;
        return once(event, listener);
      });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Preserve the original implementation and invoke it only with explicit close.call(this) below.
    const close = Ledger.prototype.close;
    const closeSpy = vi.spyOn(Ledger.prototype, "close");
    closeSpy.mockImplementation(function (this: InstanceType<typeof Ledger>) {
      expect(settled).toBe(true);
      if (mode === "close-failure") {
        close.call(this);
        throw new Error("injected close failure");
      }
      close.call(this);
    });
    const stdout = vi
      .spyOn(process.stdout, "write")
      .mockImplementation(() => true);
    const stderr = vi
      .spyOn(process.stderr, "write")
      .mockImplementation(() => true);
    run.mockImplementation(async (signal: AbortSignal) => {
      const owner = await readRunLock(join(root, "RUN.lock"));
      expect(owner?.pid).toBe(process.pid);
      if (mode === "abort") {
        abort?.();
        expect(signal.aborted).toBe(true);
      }
      settled = true;
      if (mode === "run-failure") throw new Error("injected run failure");
      return { claimed: 0, stopped: mode === "abort" ? "aborted" : "idle" };
    });
    try {
      process.argv = [
        process.execPath,
        "saqi-crawler",
        "run-enrichment",
        "--state-dir",
        root,
        "--max",
        "1",
      ];
      await import("../cli.js");
      expect(run).toHaveBeenCalledOnce();
      expect(closeSpy).toHaveBeenCalledOnce();
      if (mode === "close-failure")
        await expect(
          readRunLock(join(root, "RUN.lock")),
        ).resolves.toMatchObject({ pid: process.pid });
      else
        await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
      expect(
        database
          .prepare("SELECT * FROM runtime_control ORDER BY control_key")
          .all(),
      ).toEqual(controls);
      expect(
        database.prepare("SELECT * FROM sol_paid_usage_budget").all(),
      ).toEqual(budgets);
      if (mode.endsWith("failure"))
        expect(stderr).toHaveBeenCalledWith(
          expect.stringContaining(
            `injected ${mode === "run-failure" ? "run" : "close"} failure`,
          ),
        );
    } finally {
      process.argv = argv;
      process.exitCode = exitCode;
      onceSpy.mockRestore();
      closeSpy.mockRestore();
      stdout.mockRestore();
      stderr.mockRestore();
      run.mockReset();
      database.close();
    }
  },
);
