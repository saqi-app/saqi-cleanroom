import { hash } from "node:crypto";
import { lstat, opendir, readFile } from "node:fs/promises";
import { basename, join } from "node:path";

import {
  CURRENT_SCHEDULER_STATE_SCHEMA_VERSION,
  schedulerStateSchemaVersion,
} from "../enrichment/sol-lane-scheduler.js";

interface SchedulerStateReader {
  loadSchedulerState(
    stateKey: string,
  ): { readonly digest: string; readonly serialized: string } | null;
}

type AuthorityState = "invalid" | "missing" | "valid";

interface BoundedRootEntries {
  readonly inspected: number;
  readonly names: readonly string[];
  readonly truncated: boolean;
}

interface StateInventorySchedulerRecord {
  readonly digestValid: boolean | null;
  readonly key: string;
  readonly schemaVersion: null | number;
  readonly state: AuthorityState;
}

interface StateInventorySurface {
  readonly category:
    | "artifact"
    | "authoritative_sqlite"
    | "control"
    | "derived"
    | "evidence"
    | "immutable_cache"
    | "lock";
  readonly name: string;
  readonly present: boolean;
}

export interface StateInventory {
  readonly codexScheduler: {
    readonly authority: StateInventorySchedulerRecord;
    readonly fallback: "blocked" | "legacy_json" | "legacy_sqlite" | "none";
    readonly fallbackName: null | string;
    readonly safeToOperate: boolean;
  };
  readonly inertCandidates: readonly string[];
  readonly rootEntries: {
    readonly inspected: number;
    readonly truncated: boolean;
  };
  readonly schemaId: "saqi.state-inventory";
  readonly schemaVersion: 1;
  readonly surfaces: readonly StateInventorySurface[];
}

const MAXIMUM_ROOT_ENTRIES = 256;
const MAXIMUM_SCHEDULER_BYTES = 64 * 1024;
const CURRENT_SOL_KEY = "provider-v10:sol";
const LEGACY_SOL_KEY = "provider:sol";
const SURFACES = [
  ["ledger.sqlite3", "authoritative_sqlite"],
  ["production-resolution-demand.sqlite3", "authoritative_sqlite"],
  ["production-resolution.sqlite3", "immutable_cache"],
  ["artifacts", "artifact"],
  ["sol-attempts", "evidence"],
  ["PAUSED", "derived"],
  ["PAID_WORK_PAUSED", "derived"],
  ["SERVICE_ENABLED", "derived"],
  ["MONITOR_AUTOSTART_ENABLED", "derived"],
  ["MONITOR_AUTOSTART_DISABLED", "derived"],
  ["RUN.lock", "evidence"],
  ["CONTROL.lock", "lock"],
  ["status.json", "derived"],
  ["health/latest.json", "derived"],
  ["health/provider-execution-latest.json", "derived"],
  ["signals/sol-quota.json", "derived"],
] as const satisfies readonly (readonly [
  string,
  StateInventorySurface["category"],
])[];

const LEGACY_SCHEDULER_FILES = [
  "sol-scheduler.json.v10",
  "sol-scheduler.json.v8",
  "sol-scheduler.json",
] as const;

const INERT_FIXED_NAMES: ReadonlySet<string> = new Set([
  "sol-scheduler.json.v9",
]);

export async function inspectStateInventory(options: {
  readonly ledger: SchedulerStateReader;
  readonly root: string;
}): Promise<StateInventory> {
  const rootEntries = await boundedRootEntries(options.root);
  const authority = inspectSchedulerRecord(options.ledger, CURRENT_SOL_KEY);
  const legacyDatabase = inspectSchedulerRecord(options.ledger, LEGACY_SOL_KEY);
  const legacyFile = await firstSchedulerFile(options.root);
  const fallback = schedulerFallback(authority, legacyDatabase, legacyFile);
  const surfaces = await Promise.all(
    SURFACES.map(async ([name, category]) => ({
      category,
      name: boundedName(name),
      present: await pathExists(join(options.root, name)),
    })),
  );
  return {
    codexScheduler: {
      authority,
      fallback: fallback.kind,
      fallbackName: fallback.name,
      safeToOperate:
        // Schema 10 is the existing SQLite authority and upgrades in place;
        // requiring only the latest version would prevent that startup upgrade.
        authority.state === "valid" &&
        authority.schemaVersion !== null &&
        authority.schemaVersion >= 10 &&
        authority.schemaVersion <= CURRENT_SCHEDULER_STATE_SCHEMA_VERSION,
    },
    inertCandidates: inertCandidates(rootEntries.names),
    rootEntries: {
      inspected: rootEntries.inspected,
      truncated: rootEntries.truncated,
    },
    schemaId: "saqi.state-inventory",
    schemaVersion: 1,
    surfaces,
  };
}

function inspectSchedulerRecord(
  ledger: SchedulerStateReader,
  key: string,
): StateInventorySchedulerRecord {
  try {
    const record = ledger.loadSchedulerState(key);
    if (!record) return missingRecord(key);
    const digestValid = sha256(record.serialized) === record.digest;
    const schemaVersion = schedulerStateSchemaVersion(record.serialized);
    return {
      digestValid,
      key,
      schemaVersion,
      state: digestValid && schemaVersion !== null ? "valid" : "invalid",
    };
  } catch {
    return { digestValid: null, key, schemaVersion: null, state: "invalid" };
  }
}

function missingRecord(key: string): StateInventorySchedulerRecord {
  return { digestValid: null, key, schemaVersion: null, state: "missing" };
}

async function firstSchedulerFile(
  root: string,
): Promise<{ readonly name: string; readonly state: AuthorityState } | null> {
  const results = await Promise.all(
    LEGACY_SCHEDULER_FILES.map(async (name) => ({
      name,
      state: await schedulerFileState(join(root, name)),
    })),
  );
  return results.find((result) => result.state !== "missing") ?? null;
}

function schedulerFallback(
  authority: StateInventorySchedulerRecord,
  legacyDatabase: StateInventorySchedulerRecord,
  legacyFile: { readonly name: string; readonly state: AuthorityState } | null,
): {
  readonly kind: StateInventory["codexScheduler"]["fallback"];
  readonly name: null | string;
} {
  if (authority.state === "valid") return { kind: "none", name: null };
  if (authority.state === "invalid") return { kind: "blocked", name: null };
  if (legacyDatabase.state === "valid") {
    return { kind: "legacy_sqlite", name: LEGACY_SOL_KEY };
  }
  if (legacyDatabase.state === "invalid")
    return { kind: "blocked", name: null };
  if (legacyFile?.state === "valid") {
    return { kind: "legacy_json", name: boundedName(legacyFile.name) };
  }
  return {
    kind: legacyFile ? "blocked" : "none",
    name: legacyFile?.name ?? null,
  };
}

async function schedulerFileState(path: string): Promise<AuthorityState> {
  try {
    const metadata = await lstat(path);
    if (!metadata.isFile() || metadata.size > MAXIMUM_SCHEDULER_BYTES) {
      return "invalid";
    }
    const serialized = await readFile(path, "utf8");
    return schedulerStateSchemaVersion(serialized) === null
      ? "invalid"
      : "valid";
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return "missing";
    }
    return "invalid";
  }
}

async function boundedRootEntries(root: string): Promise<BoundedRootEntries> {
  const directory = await opendir(root);
  const names: string[] = [];
  let truncated = false;
  for await (const entry of directory) {
    if (names.length === MAXIMUM_ROOT_ENTRIES) {
      truncated = true;
      break;
    }
    names.push(entry.name);
  }
  return {
    inspected: names.length,
    names,
    truncated,
  };
}

function inertCandidates(entries: readonly string[]): readonly string[] {
  return entries
    .filter(
      (name) =>
        INERT_FIXED_NAMES.has(name) ||
        /^RUN\.lock\.stale\.[\w.-]{1,96}$/.test(name) ||
        /^CONTROL\.lock\.stale\.[\w.-]{1,96}$/.test(name) ||
        /^PAID_WORK_PAUSED\.deploy-[\w.-]{1,96}$/.test(name),
    )
    .map(boundedName)
    .toSorted();
}

function boundedName(path: string): string {
  return basename(path).slice(0, 128);
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return false;
    }
    return false;
  }
}

function sha256(value: string): string {
  return hash("sha256", value, "hex");
}
