import { randomUUID } from "node:crypto";

import Database from "better-sqlite3";
import { expect, test, vi } from "vitest";

import { RUNTIME_OWNER_MIGRATION_SQL } from "../persistence/runtime-owner-schema.js";
import {
  RuntimeOwnerBusyError,
  RuntimeOwnerStore,
} from "../persistence/runtime-owner-store.js";

function fixture(run: (database: Database.Database) => void) {
  const database = new Database(":memory:");
  try {
    database.exec(
      "CREATE TABLE local_schema(singleton INTEGER PRIMARY KEY, version INTEGER); INSERT INTO local_schema VALUES(1,36)",
    );
    database.exec(RUNTIME_OWNER_MIGRATION_SQL);
    run(database);
  } finally {
    database.close();
  }
}
function record(pid = process.pid) {
  return {
    configDigest: "a".repeat(64),
    pid,
    runId: randomUUID(),
    schemaVersion: 1 as const,
    startedAt: new Date().toISOString(),
  };
}

test("monotonic owner claims reject live or ambiguous prior PID and release exactly", () =>
  fixture((database) => {
    const alive = vi.fn(() => false);
    const store = new RuntimeOwnerStore(database, alive);
    expect(store.read()).toBeNull();
    const first = store.claim(record(), "supervisor", true);
    expect(first.epoch).toBe(1);
    expect(() => store.claim(record(), "supervisor", true)).toThrow(
      RuntimeOwnerBusyError,
    );
    expect(store.release({ ...first, record: record() })).toBe(false);
    expect(store.read()).toEqual(first);
    expect(store.release(first)).toBe(true);
    expect(store.read()).toBeNull();
    expect(store.claim(record(), "supervisor", true).epoch).toBe(2);
  }));

test("dead owner is replaced atomically and stale release cannot remove replacement", () =>
  fixture((database) => {
    const store = new RuntimeOwnerStore(database, () => true);
    const first = store.claim(record(42), "supervisor", true);
    const replacement = store.claim(record(), "supervisor", true);
    expect(replacement.epoch).toBe(first.epoch + 1);
    expect(store.release(first)).toBe(false);
    expect(store.read()).toEqual(replacement);
  }));

test("strict maintenance acquisition refuses even a definitely dead held owner", () =>
  fixture((database) => {
    const dead = vi.fn(() => true);
    const store = new RuntimeOwnerStore(database, dead);
    const first = store.claim(record(42), "supervisor", true);
    expect(() => store.claim(record(), "maintenance", false)).toThrow(
      RuntimeOwnerBusyError,
    );
    expect(() => store.claim(record(), "maintenance", true)).toThrow(
      RuntimeOwnerBusyError,
    );
    expect(dead).not.toHaveBeenCalled();
    expect(store.read()).toEqual(first);
  }));

test("permission errors and PID ambiguity never authorize reclaim", () =>
  fixture((database) => {
    const store = new RuntimeOwnerStore(database);
    const first = store.claim(record(), "supervisor", true);
    const probe = vi.spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("Permission denied"), { code: "EPERM" });
    });
    try {
      expect(() => store.claim(record(), "supervisor", true)).toThrow(
        RuntimeOwnerBusyError,
      );
      expect(store.release(first, true)).toBe(false);
      expect(store.read()).toEqual(first);
    } finally {
      probe.mockRestore();
    }
  }));

test("dead-owner release and process actions require current identity", () =>
  fixture((database) => {
    const store = new RuntimeOwnerStore(database, () => false);
    const owner = store.claim(record(), "supervisor", true);
    expect(store.release(owner, true)).toBe(false);
    const action = vi.fn();
    expect(store.withCurrentOwner(owner.record.pid, randomUUID(), action)).toBe(
      false,
    );
    expect(action).not.toHaveBeenCalled();
    expect(
      store.withCurrentOwner(owner.record.pid, owner.record.runId, action),
    ).toBe(true);
    expect(action).toHaveBeenCalledOnce();
  }));

test.each(["missing row", "wrong version", "malformed owner"])(
  "rejects %s authority instead of treating it as unlocked",
  (kind) =>
    fixture((database) => {
      if (kind === "missing row")
        database.exec(
          "DROP TRIGGER runtime_owner_no_delete; DELETE FROM runtime_owner",
        );
      else if (kind === "wrong version")
        database.exec("UPDATE local_schema SET version=34");
      else
        database.exec(
          "UPDATE runtime_owner SET epoch=1, held=1, owner_kind='supervisor',pid=42,run_id='invalid',config_digest='bad',started_at='bad'",
        );
      const store = new RuntimeOwnerStore(database);
      expect(() => store.read()).toThrow();
      expect(() => store.claim(record(), "supervisor", true)).toThrow();
    }),
);
