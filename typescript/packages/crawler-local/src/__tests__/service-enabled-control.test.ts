import { execFileSync } from "node:child_process";
import { existsSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import {
  readServiceEnabled,
  writeServiceEnabled,
} from "../runtime/service-enabled-control.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

describe("SQLite managed-service control", () => {
  it("imports once and never accepts stale file edits as service authority", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-service-control-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    writeFileSync(join(root, "SERVICE_ENABLED"), "legacy");
    expect(readServiceEnabled(root)).toBe(true);
    rmSync(join(root, "SERVICE_ENABLED"));
    expect(readServiceEnabled(root)).toBe(true);
    writeServiceEnabled(root, false);
    writeFileSync(join(root, "SERVICE_ENABLED"), "stale");
    expect(readServiceEnabled(root)).toBe(false);
  });

  it("fails closed for missing database or incomplete imported control", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-service-control-"));
    expect(() => readServiceEnabled(root)).toThrow();
    expect(() => writeServiceEnabled(root, true)).toThrow();
    expect(existsSync(join(root, "ledger.sqlite3"))).toBe(false);
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    expect(readServiceEnabled(root)).toBe(false);
    const database = new Database(join(root, "ledger.sqlite3"));
    database
      .prepare(
        "DELETE FROM runtime_control WHERE control_key = 'service_enabled'",
      )
      .run();
    database.close();
    expect(() => readServiceEnabled(root)).toThrow();
  });

  it("disabled managed entry exits successfully before credential bootstrap or lanes", () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-service-control-"));
    const path = join(root, "config.json");
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    writeServiceEnabled(root, false);
    writeFileSync(
      path,
      JSON.stringify({ schemaVersion: 1, stateDirectory: root }),
    );
    const result: unknown = JSON.parse(
      execFileSync(
        process.execPath,
        [
          "--import",
          "tsx",
          resolve(import.meta.dirname, "../cli.ts"),
          "run-service",
          "--config",
          path,
        ],
        { encoding: "utf8", stdio: "pipe" },
      ),
    );
    expect(result).toMatchObject({
      serviceEnabled: false,
      result: { stopped: "disabled", cycles: 0 },
    });
    expect(existsSync(join(root, "RUN.lock"))).toBe(false);
    expect(existsSync(join(root, "status.json"))).toBe(false);
  });
});
