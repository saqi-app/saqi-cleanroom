import { randomUUID } from "node:crypto";
import { lstat } from "node:fs/promises";
import { dirname, join } from "node:path";

import Database from "better-sqlite3";

import {
  type RuntimeOwnerRecord,
  RuntimeOwnerRecordSchema,
} from "../persistence/runtime-owner-schema.js";
import {
  type RuntimeOwnerFence,
  RuntimeOwnerStore,
} from "../persistence/runtime-owner-store.js";

// Keep the public snapshot shape stable for existing diagnostic consumers.
export type RunLockRecord = RuntimeOwnerRecord;
export { RuntimeOwnerBusyError as RunLockBusyError } from "../persistence/runtime-owner-store.js";
export interface RunLock {
  readonly path: string;
  readonly record: RunLockRecord;
  release(): Promise<void>;
}

async function withOwner<T>(
  legacyPath: string,
  readonly: boolean,
  operation: (store: RuntimeOwnerStore) => T,
): Promise<T> {
  // Legacy files are never fallback authority, even with a dead-looking PID.
  const legacy = await lstat(legacyPath).catch((error: unknown) => {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return null;
    throw error;
  });
  if (legacy !== null) throw new Error("RUNTIME_OWNER_LEGACY_LOCK_PRESENT");
  const path = join(dirname(legacyPath), "ledger.sqlite3");
  const metadata = await lstat(path);
  if (!metadata.isFile() || metadata.isSymbolicLink())
    throw new Error("RUNTIME_OWNER_UNSAFE_LEDGER");
  const database = new Database(path, {
    readonly,
    fileMustExist: true,
    timeout: 5000,
  });
  try {
    // eslint-disable-next-line @sarj/require-sql-access-class -- This connection factory sets durability before injecting the connection into RuntimeOwnerStore; all ownership reads/writes remain in that store.
    if (!readonly) database.pragma("synchronous=FULL");
    return operation(new RuntimeOwnerStore(database));
  } finally {
    database.close();
  }
}

export function readRunLock(path: string): Promise<null | RunLockRecord> {
  return withOwner(path, true, (store) => store.read()?.record ?? null);
}

/** Diagnostics only: unavailable authority must never be interpreted as free. */
export async function inspectRunOwner(path: string) {
  try {
    return { lock: await readRunLock(path), issue: null };
  } catch (error) {
    return {
      lock: null,
      issue:
        error instanceof Error &&
        error.message === "RUNTIME_OWNER_LEGACY_LOCK_PRESENT"
          ? "RUNTIME_OWNER_LEGACY_LOCK_PRESENT"
          : "RUNTIME_OWNER_AUTHORITY_UNAVAILABLE",
    };
  }
}

export async function acquireRunLock(
  path: string,
  configDigest: string,
  now = new Date(),
  options: { readonly recoverStale?: boolean } = {},
): Promise<RunLock> {
  const record = RuntimeOwnerRecordSchema.parse({
    configDigest,
    pid: process.pid,
    runId: randomUUID(),
    schemaVersion: 1,
    startedAt: now.toISOString(),
  });
  const fence = await withOwner(path, false, (store) =>
    store.claim(
      record,
      options.recoverStale === false ? "maintenance" : "supervisor",
      options.recoverStale !== false,
    ),
  );
  let released = false;
  return {
    path: join(dirname(path), "ledger.sqlite3"),
    record,
    async release() {
      if (released) return;
      if (!(await withOwner(path, false, (store) => store.release(fence))))
        throw new Error("Runtime owner changed; refusing release");
      released = true;
    },
  };
}

export function releaseDeadRunOwner(
  path: string,
  pid: number,
  runId: string,
): Promise<boolean> {
  return withOwner(path, false, (store) => {
    const current: null | RuntimeOwnerFence = store.read();
    if (current?.record.pid !== pid || current.record.runId !== runId)
      return false;
    return store.release(current, true);
  });
}

export function actOnRunOwner(
  path: string,
  pid: number,
  runId: string,
  action: () => undefined,
): Promise<boolean> {
  return withOwner(path, false, (store) =>
    store.withCurrentOwner(pid, runId, action),
  );
}
