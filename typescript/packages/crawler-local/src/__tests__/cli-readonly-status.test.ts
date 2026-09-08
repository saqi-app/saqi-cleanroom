import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  currentSource,
  currentSourceAdapterProfile,
} from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { CURRENT_SCHEMA_VERSION } from "../persistence/migrations.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

const CLI = resolve(import.meta.dirname, "../cli.ts");

function status(root: string, source = currentSource()): unknown {
  return JSON.parse(
    execFileSync(
      process.execPath,
      ["--import", "tsx", CLI, "status", "--state-dir", root],
      {
        encoding: "utf8",
        stdio: "pipe",
        env: {
          ...process.env,
          SAQI_SOURCE_NAME: source.name,
          SAQI_SOURCE_BASE_URL: source.origin,
          SAQI_SOURCE_ADAPTER_CONFIG: JSON.stringify(
            currentSourceAdapterProfile(),
          ),
        },
      },
    ),
  );
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "saqi-status-readonly-"));
  const path = join(root, "ledger.sqlite3");
  Ledger.initialize(path).close();
  const database = new Database(path);
  database.exec(
    "INSERT INTO runtime_control VALUES('global_paused',1),('paid_work_paused',1),('legacy_pause_imported',1); INSERT INTO sol_paid_usage_budget VALUES('fixture-exhausted',3,3,'exhausted',1,1)",
  );
  database.close();
  return { root, path };
}

it("status inspects the current schema without budget or control changes", () => {
  const f = fixture();
  const before = readFileSync(f.path);
  expect(status(f.root)).toMatchObject({
    command: "status",
    paused: true,
    paidWorkPaused: true,
    runtimeOwnerIssue: null,
  });
  expect(readFileSync(f.path)).toEqual(before);
  const database = new Database(f.path, {
    readonly: true,
    fileMustExist: true,
  });
  try {
    expect(database.prepare("SELECT version FROM local_schema").get()).toEqual({
      version: CURRENT_SCHEMA_VERSION,
    });
    expect(
      database
        .prepare("SELECT state,reserved_operations FROM sol_paid_usage_budget")
        .get(),
    ).toEqual({ state: "exhausted", reserved_operations: 3 });
  } finally {
    database.close();
  }
});

it("source mismatch cannot mutate the inspected ledger", () => {
  const f = fixture();
  const before = readFileSync(f.path);
  expect(() =>
    status(f.root, {
      name: "different-source",
      origin: "https://different.example",
    }),
  ).toThrow("LOCAL_SOURCE_IDENTITY_MISMATCH");
  expect(readFileSync(f.path)).toEqual(before);
});

it("missing controls fail without importing pause files", () => {
  const f = fixture();
  const db = new Database(f.path);
  db.exec("DELETE FROM runtime_control");
  db.close();
  writeFileSync(join(f.root, "PAUSED"), "retained");
  const before = readFileSync(f.path);
  expect(() => status(f.root)).toThrow("readonly");
  expect(readFileSync(f.path)).toEqual(before);
  expect(readFileSync(join(f.root, "PAUSED"), "utf8")).toBe("retained");
});

it("status never creates a missing ledger", () => {
  const root = mkdtempSync(join(tmpdir(), "saqi-status-missing-"));
  expect(() => status(root)).toThrow("Ledger does not exist");
  expect(existsSync(join(root, "ledger.sqlite3"))).toBe(false);
});
