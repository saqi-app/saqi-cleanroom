import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { expect, test, vi } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { RuntimeOwnerRecordSchema } from "../persistence/runtime-owner-schema.js";
import { RuntimeOwnerStore } from "../persistence/runtime-owner-store.js";
import {
  acquireRunLock,
  actOnRunOwner,
  readRunLock,
  RunLockBusyError,
} from "../runtime/run-lock.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const DIGEST = "a".repeat(64);
test("stale stop identity cannot signal a replacement owner", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "run-owner-signal-fence-"));
  Ledger.initialize(join(root, "ledger.sqlite3")).close();
  const path = join(root, "RUN.lock");
  const previous = await acquireRunLock(path, DIGEST);
  await previous.release();
  const replacement = await acquireRunLock(path, DIGEST);
  const signal = vi.fn(() => undefined);
  try {
    await expect(
      actOnRunOwner(path, previous.record.pid, previous.record.runId, signal),
    ).resolves.toBe(false);
    expect(signal).not.toHaveBeenCalled();
    await expect(readRunLock(path)).resolves.toEqual(replacement.record);
    await expect(
      actOnRunOwner(
        path,
        replacement.record.pid,
        replacement.record.runId,
        signal,
      ),
    ).resolves.toBe(true);
    expect(signal).toHaveBeenCalledOnce();
  } finally {
    await replacement.release();
  }
});
const DEAD_OWNER = JSON.stringify({
  configDigest: DIGEST,
  pid: 2_147_483_647,
  runId: "2d39ce36-2dc8-4be6-b23c-70b12ee30db6",
  schemaVersion: 1,
  startedAt: "2026-08-25T00:00:00.000Z",
});

test("acquisition refuses a FIFO without waiting for a writer", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "run-lock-fifo-"));
  const path = join(root, "RUN.lock");
  execFileSync("mkfifo", [path], { timeout: 1000 });
  await expect(acquireRunLock(path, DIGEST)).rejects.toThrow(
    "RUNTIME_OWNER_LEGACY_LOCK_PRESENT",
  );
  expect(existsSync(path)).toBe(true);
}, 1000);

test("acquisition refuses a symlink to valid dead-owner evidence without removing either path", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "run-lock-symlink-"));
  const target = join(root, "retained.json");
  const path = join(root, "RUN.lock");
  writeFileSync(target, DEAD_OWNER);
  symlinkSync(target, path);
  await expect(acquireRunLock(path, DIGEST)).rejects.toThrow(
    "RUNTIME_OWNER_LEGACY_LOCK_PRESENT",
  );
  expect(existsSync(path)).toBe(true);
  expect(readFileSync(target, "utf8")).toBe(DEAD_OWNER);
});

test.each(["oversized", "malformed", "foreign-schema", "directory"])(
  "acquisition refuses %s lock evidence without deleting it",
  async (kind) => {
    const root = trackedMkdtempSync(join(tmpdir(), "run-lock-invalid-"));
    const path = join(root, "RUN.lock");
    if (kind === "directory") mkdirSync(path);
    else
      writeFileSync(
        path,
        kind === "oversized"
          ? `${DEAD_OWNER}${" ".repeat(4096)}`
          : kind === "malformed"
            ? "{"
            : DEAD_OWNER.replace('"schemaVersion":1', '"schemaVersion":2'),
      );
    await expect(acquireRunLock(path, DIGEST)).rejects.toThrow(
      "RUNTIME_OWNER_LEGACY_LOCK_PRESENT",
    );
    expect(existsSync(path)).toBe(true);
  },
);

test("bounded recovery preserves live owners and recovers a valid dead owner", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "run-lock-owner-"));
  const path = join(root, "RUN.lock");
  const ledgerPath = join(root, "ledger.sqlite3");
  Ledger.initialize(ledgerPath).close();
  const database = new Database(ledgerPath);
  try {
    new RuntimeOwnerStore(database).claim(
      RuntimeOwnerRecordSchema.parse(JSON.parse(DEAD_OWNER)),
      "supervisor",
      true,
    );
  } finally {
    database.close();
  }
  const owner = await acquireRunLock(path, DIGEST);
  try {
    await expect(acquireRunLock(path, DIGEST)).rejects.toThrow(
      RunLockBusyError,
    );
    await expect(readRunLock(path)).resolves.toEqual(owner.record);
  } finally {
    await owner.release();
  }
  expect(existsSync(path)).toBe(false);
  await expect(readRunLock(path)).resolves.toBeNull();
});
