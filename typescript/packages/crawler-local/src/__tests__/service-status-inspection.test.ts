import {
  existsSync,
  readFileSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { expect, it } from "vitest";

import {
  controlLaunchdService,
  type LaunchdControlSnapshot,
  retainedSourceStatus,
} from "../runtime/launchd-control.js";
import { operationConfigDigestForSource } from "../runtime/operations-contract.js";
import { loadServiceStatusInspection } from "../runtime/service-status-inspection.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const SOURCE = { name: "fixture-source", origin: "https://fixture.example" };
const unavailable = async () => {
  throw new Error("fixture Keychain unavailable");
};
function fixture() {
  const root = mkdtempSync(join(tmpdir(), "saqi-retained-status-"));
  const configPath = join(root, "config.json");
  const ledgerPath = join(root, "ledger.sqlite3");
  writeFileSync(
    configPath,
    JSON.stringify({ schemaVersion: 1, stateDirectory: root }),
  );
  const db = new Database(ledgerPath);
  db.exec(`CREATE TABLE local_schema(singleton INTEGER PRIMARY KEY,version INTEGER);
    INSERT INTO local_schema VALUES(1,33);
    CREATE TABLE local_source_identity(singleton INTEGER PRIMARY KEY,source_name TEXT,source_origin TEXT);
    CREATE TABLE runtime_control(control_key TEXT PRIMARY KEY,enabled INTEGER);
    INSERT INTO runtime_control VALUES('service_enabled',0),('legacy_service_imported',1),('legacy_concurrency_imported',1);
    CREATE TABLE runtime_provider_concurrency(provider TEXT PRIMARY KEY,target INTEGER,initial INTEGER,revision INTEGER);
    INSERT INTO runtime_provider_concurrency VALUES('sol',4,2,1);`);
  db.prepare("INSERT INTO local_source_identity VALUES(1,?,?)").run(
    SOURCE.name,
    SOURCE.origin,
  );
  db.close();
  return { root, configPath, ledgerPath };
}

it("uses retained identity with unchanged ledger and current desired concurrency when Keychain fails", async () => {
  const f = fixture();
  const before = readFileSync(f.ledgerPath);
  const inspected = await loadServiceStatusInspection(
    f.configPath,
    unavailable,
  );
  expect(inspected.sourceIdentityStatus).toBe("retained_unverified");
  expect(inspected.serviceEnabled).toBe(false);
  expect(inspected.loaded.config.sol.concurrency).toBe(4);
  expect(inspected.loaded.configDigest).toBe(
    operationConfigDigestForSource(inspected.loaded.config, SOURCE),
  );
  expect(readFileSync(f.ledgerPath)).toEqual(before);
  writeFileSync(
    f.configPath,
    JSON.stringify({
      schemaVersion: 1,
      stateDirectory: f.root,
      collector: { enabled: false },
    }),
  );
  const changed = await loadServiceStatusInspection(f.configPath, unavailable);
  expect(changed.loaded.configDigest).not.toBe(inspected.loaded.configDigest);
});

it("configured source remains authoritative and cannot be silently replaced by retained identity", async () => {
  const f = fixture();
  const configured = {
    name: "new-source",
    origin: "https://new.example",
    profile: DEFAULT_SOURCE_ADAPTER_PROFILE,
  };
  const result = await loadServiceStatusInspection(
    f.configPath,
    async () => configured,
  );
  expect(result.sourceIdentityStatus).toBe("configured");
  expect(result.loaded.configDigest).toBe(
    operationConfigDigestForSource(result.loaded.config, configured),
  );
  expect(result.loaded.configDigest).not.toBe(
    operationConfigDigestForSource(result.loaded.config, SOURCE),
  );
});

it("missing and symlinked ledgers fail closed without creating state", async () => {
  const f = fixture();
  const target = fixture();
  unlinkSync(f.ledgerPath);
  await expect(
    loadServiceStatusInspection(f.configPath, unavailable),
  ).rejects.toThrow();
  expect(existsSync(f.ledgerPath)).toBe(false);
  const before = readFileSync(target.ledgerPath);
  symlinkSync(target.ledgerPath, f.ledgerPath);
  await expect(
    loadServiceStatusInspection(f.configPath, unavailable),
  ).rejects.toThrow("STATUS_UNSAFE_LEDGER");
  expect(readFileSync(target.ledgerPath)).toEqual(before);
});

it("invalid retained identity fails without repairing or migrating the ledger", async () => {
  const f = fixture();
  const database = new Database(f.ledgerPath);
  database.exec(
    "UPDATE local_source_identity SET source_origin = 'not-an-origin'",
  );
  database.close();
  const before = readFileSync(f.ledgerPath);
  await expect(
    loadServiceStatusInspection(f.configPath, unavailable),
  ).rejects.toThrow();
  expect(readFileSync(f.ledgerPath)).toEqual(before);
});

it("rejects oversized status configuration before reading managed credentials", async () => {
  const f = fixture();
  writeFileSync(f.configPath, " ".repeat(1024 * 1024 + 1));
  const before = readFileSync(f.ledgerPath);
  await expect(
    loadServiceStatusInspection(f.configPath, unavailable),
  ).rejects.toThrow("STATUS_CONFIG_INVALID_SIZE_OR_TYPE");
  expect(readFileSync(f.ledgerPath)).toEqual(before);
});

it("retained status cannot enable start/restart and preserves stronger failures", async () => {
  const f = fixture();
  const inspection = await loadServiceStatusInspection(
    f.configPath,
    unavailable,
  );
  const snapshot: LaunchdControlSnapshot = {
    action: "status",
    actualState: "running",
    allowedActions: ["start", "restart", "stop"],
    configDigest: inspection.loaded.configDigest,
    desiredConfigDigest: inspection.loaded.configDigest,
    desiredRuntimeCommit: null,
    detail: null,
    loadedConfigDigest: null,
    observedAt: new Date().toISOString(),
    ownerPid: null,
    runId: null,
    runtimeCommit: null,
    schemaId: "saqi.launchd-control",
    schemaVersion: 1,
    serviceEnabled: false,
  };
  expect(retainedSourceStatus(snapshot)).toMatchObject({
    actualState: "running_outdated",
    allowedActions: ["stop"],
    detail: "SOURCE_IDENTITY_UNVERIFIED",
    sourceIdentityStatus: "retained_unverified",
  });
  expect(
    retainedSourceStatus({
      ...snapshot,
      actualState: "fenced",
      detail: "SERVICE_PID_MISMATCH",
    }),
  ).toMatchObject({
    actualState: "fenced",
    detail: "SERVICE_PID_MISMATCH",
    sourceIdentityStatus: "retained_unverified",
  });
  await expect(
    controlLaunchdService({
      action: "restart",
      configPath: f.configPath,
      statusInspection: inspection,
    }),
  ).rejects.toThrow("STATUS_INSPECTION_CANNOT_CONTROL_SERVICE");
});
