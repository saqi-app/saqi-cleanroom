import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { SqliteQueryValidationError } from "../persistence/sqlite-query.js";
import { inputHash } from "../persistence/work-key.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

function fixture() {
  const root = trackedMkdtempSync(join(tmpdir(), "saqi-provider-row-parsing-"));
  const path = join(root, "ledger.sqlite3");
  const ledger = Ledger.initialize(path);
  const database = new Database(path);
  database.pragma("ignore_check_constraints = ON");
  return { database, ledger };
}

describe("provider ledger row validation", () => {
  it("rejects malformed budget state before rearming and sanitizes diagnostics", () => {
    const { database, ledger } = fixture();
    try {
      ledger.armSolPaidUsageBudget(6, false, 100);
      database
        .prepare("UPDATE sol_paid_usage_budget SET state = ?")
        .run("PRIVATE_BUDGET_STATE");
      const before = database
        .prepare("SELECT * FROM sol_paid_usage_budget")
        .all();
      expect(() => ledger.armSolPaidUsageBudget(3, true, 200)).toThrow(
        SqliteQueryValidationError,
      );
      expect(() => ledger.armSolPaidUsageBudget(3, true, 200)).toThrow(
        "SQLITE_ROW_VALIDATION_FAILED:Ledger.armSolPaidUsageBudget",
      );
      try {
        ledger.solPaidUsageBudgetStatus();
      } catch (error) {
        expect(error).toBeInstanceOf(SqliteQueryValidationError);
        expect(String(error)).not.toContain("PRIVATE_BUDGET_STATE");
      }
      expect(
        database.prepare("SELECT * FROM sol_paid_usage_budget").all(),
      ).toEqual(before);
    } finally {
      ledger.close();
      database.close();
    }
  });

  it.each([
    "maximum_operations = 0",
    "maximum_operations = 9007199254740992",
    "reserved_operations = -1",
    "reserved_operations = 7",
  ])("rejects %s before reserving paid work", (corruption) => {
    const { database, ledger } = fixture();
    try {
      ledger.armSolPaidUsageBudget(6, false, 100);
      const input = { poem: 1 };
      ledger.seed(
        {
          implementationVersion: "test-v1",
          input,
          inputHash: inputHash(input),
          kind: "author-manifest",
          priority: 0,
          schemaVersion: "1",
        },
        100,
      );
      const claim = ledger.claim("test", 101, 10_000);
      if (!claim) throw new Error("Expected test claim");
      database.exec(`UPDATE sol_paid_usage_budget SET ${corruption}`);
      const before = database
        .prepare("SELECT * FROM sol_paid_usage_budget")
        .all();
      expect(() => ledger.solPaidUsageBudgetStatus()).toThrow(
        SqliteQueryValidationError,
      );
      expect(() => ledger.reserveSolPaidClaim(claim, 102)).toThrow(
        SqliteQueryValidationError,
      );
      expect(
        database.prepare("SELECT * FROM sol_paid_usage_budget").all(),
      ).toEqual(before);
      expect(
        database.prepare("SELECT * FROM sol_paid_usage_reservation").all(),
      ).toEqual([]);
    } finally {
      ledger.close();
      database.close();
    }
  });

  it("preserves SQL-valid historical caps and identifiers without adding a divisibility restriction", () => {
    const { database, ledger } = fixture();
    try {
      database
        .prepare(
          "INSERT INTO sol_paid_usage_budget VALUES(?, 7, 1, 'closed', 100, 100)",
        )
        .run("historical-budget");
      expect(ledger.solPaidUsageBudgetStatus()).toEqual({
        budgetId: "historical-budget",
        maximumOperations: 7,
        remainingOperations: 6,
        reservedOperations: 1,
        state: "closed",
      });
      expect(ledger.armSolPaidUsageBudget(3, true, 200)).toMatchObject({
        maximumOperations: 3,
        state: "active",
      });
    } finally {
      ledger.close();
      database.close();
    }
  });

  it("parses complete scheduler rows and never exposes their serialized contents on failure", () => {
    const { database, ledger } = fixture();
    try {
      const serialized = '{"private":"PRIVATE_SCHEDULER_CONTENT"}';
      expect(ledger.loadSchedulerState("missing")).toBeNull();
      ledger.saveSchedulerState("test", serialized, "a".repeat(64), null, 100);
      expect(ledger.loadSchedulerState("test")).toEqual({
        digest: "a".repeat(64),
        serialized,
      });
      database
        .prepare("UPDATE scheduler_state SET state_digest = ?")
        .run("PRIVATE_INVALID_DIGEST");
      expect(() => ledger.loadSchedulerState("test")).toThrow(
        SqliteQueryValidationError,
      );
      try {
        ledger.loadSchedulerState("test");
      } catch (error) {
        expect(String(error)).not.toContain("PRIVATE_INVALID_DIGEST");
        expect(String(error)).not.toContain("PRIVATE_SCHEDULER_CONTENT");
      }
    } finally {
      ledger.close();
      database.close();
    }
  });
});
