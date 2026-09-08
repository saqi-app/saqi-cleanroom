import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { PauseControls } from "../persistence/pause-controls.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

describe("SQLite pause controls", () => {
  it("never imports authority lost after opening existing controls", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-controls-lost-"));
    const path = join(root, "ledger.sqlite3");
    const ledger = Ledger.initialize(path);
    ledger.pauseControls.read();
    ledger.close();
    const database = new Database(path);
    try {
      PauseControls.withExisting(root, (controls) => {
        database.exec(
          "DELETE FROM runtime_control WHERE control_key='legacy_pause_imported'",
        );
        const before = database
          .prepare("SELECT * FROM runtime_control ORDER BY control_key")
          .all();
        expect(() => controls.read()).toThrow();
        expect(() => controls.set("global", true)).toThrow();
        expect(
          database
            .prepare("SELECT * FROM runtime_control ORDER BY control_key")
            .all(),
        ).toEqual(before);
      });
    } finally {
      database.close();
    }
  });

  it("commits pause and resume when the rollback mirror cannot be written or removed", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-controls-"));
    const ledger = Ledger.initialize(join(root, "ledger.sqlite3"));
    ledger.pauseControls.read();
    mkdirSync(join(root, "PAUSED"));
    expect(ledger.pauseControls.set("global", true).paused).toBe(true);
    expect(ledger.pauseControls.set("global", false).paused).toBe(false);
    expect(ledger.pauseControls.read().paused).toBe(false);
    ledger.close();
  });

  it("rejects incomplete SQLite controls instead of falling back to files", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-controls-"));
    const path = join(root, "ledger.sqlite3");
    const ledger = Ledger.initialize(path);
    ledger.pauseControls.read();
    const database = new Database(path);
    database
      .prepare(
        "DELETE FROM runtime_control WHERE control_key = 'paid_work_paused'",
      )
      .run();
    database.close();
    expect(() => ledger.pauseControls.read()).toThrow();
    ledger.close();
  });

  it("imports legacy pauses once and ignores later sentinel changes across reopen", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-controls-"));
    const path = join(root, "ledger.sqlite3");
    writeFileSync(join(root, "PAID_WORK_PAUSED"), "legacy fence");
    const ledger = Ledger.initialize(path);
    expect(ledger.pauseControls.read()).toEqual({
      paused: false,
      paidWorkPaused: true,
    });
    rmSync(join(root, "PAID_WORK_PAUSED"));
    expect(ledger.pauseControls.read().paidWorkPaused).toBe(true);
    ledger.pauseControls.set("paid", false);
    writeFileSync(join(root, "PAID_WORK_PAUSED"), "stale mirror");
    writeFileSync(join(root, "PAUSED"), "stale mirror");
    ledger.close();
    const reopened = Ledger.open(path);
    expect(reopened.pauseControls.read()).toEqual({
      paused: false,
      paidWorkPaused: false,
    });
    reopened.close();
  });

  it("does not arm or replenish a budget when changing pause state", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-controls-"));
    const ledger = Ledger.initialize(join(root, "ledger.sqlite3"));
    const budget = ledger.solPaidUsageBudgetStatus();
    ledger.pauseControls.set("global", true);
    ledger.pauseControls.set("paid", true);
    ledger.pauseControls.set("global", false);
    expect(ledger.pauseControls.read()).toEqual({
      paused: false,
      paidWorkPaused: true,
    });
    expect(ledger.solPaidUsageBudgetStatus()).toEqual(budget);
    ledger.close();
    expect(() => ledger.pauseControls.read()).toThrow();
  });
});
