import { constants } from "node:fs";
import { lstat, open } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { SourceConfigurationSchema } from "@saqi/precedent-iso";
import {
  SourceAdapterProfileV1Schema,
  type SourceConfiguration,
} from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { z } from "zod";

import { controlLaunchdService } from "./launchd-control.js";
import { loadScraperOperationConfigFromInput } from "./operations-contract.js";

const ConfigRootSchema = z.looseObject({
  stateDirectory: z.string().min(1).max(4096),
});
const MAXIMUM_CONFIG_BYTES = 1024 * 1024;

const RetainedSchema = z.strictObject({
  name: z.string(),
  origin: z.string(),
  schemaVersion: z.int().min(29),
  serviceEnabled: z.literal([0, 1]),
  serviceImported: z.literal(1),
});

class RetainedServiceStatusStore {
  readonly #database: Database.Database;
  constructor(database: Database.Database) {
    this.#database = database;
  }

  read() {
    this.#database.pragma("busy_timeout = 1000");
    return RetainedSchema.parse(
      this.#database
        .prepare(
          `SELECT
      source_name AS name, source_origin AS origin,
      (SELECT version FROM local_schema WHERE singleton = 1) AS schemaVersion,
      (SELECT enabled FROM runtime_control WHERE control_key = 'service_enabled') AS serviceEnabled,
      (SELECT enabled FROM runtime_control WHERE control_key = 'legacy_service_imported') AS serviceImported
      FROM local_source_identity WHERE singleton = 1`,
        )
        .get(),
    );
  }
}

/** Trusted local state directory only; never creates or migrates a ledger. */
export async function loadServiceStatusInspection(
  configPath: string,
  loadManagedSource: () => Promise<SourceConfiguration>,
) {
  const input = await readStatusConfig(configPath);
  const root = resolve(
    dirname(resolve(configPath)),
    ConfigRootSchema.parse(input).stateDirectory,
  );
  const ledgerPath = join(root, "ledger.sqlite3");
  const entry = await lstat(ledgerPath);
  if (!entry.isFile() || entry.isSymbolicLink())
    throw new Error("STATUS_UNSAFE_LEDGER");
  const database = new Database(ledgerPath, {
    readonly: true,
    fileMustExist: true,
  });
  let retained: z.infer<typeof RetainedSchema>;
  try {
    retained = new RetainedServiceStatusStore(database).read();
  } finally {
    database.close();
  }
  const persistedSource = SourceConfigurationSchema.parse({
    name: retained.name,
    origin: retained.origin,
  });
  let configuredSource: SourceConfiguration | undefined;
  try {
    const managed = await loadManagedSource();
    configuredSource = {
      ...SourceConfigurationSchema.parse({
        name: managed.name,
        origin: managed.origin,
      }),
      profile: SourceAdapterProfileV1Schema.parse(managed.profile),
    };
  } catch {
    // Retained identity permits inspection, never source admission or control.
  }
  const sourceIdentityStatus =
    configuredSource === undefined ? "retained_unverified" : "configured";
  return {
    loaded: loadScraperOperationConfigFromInput(
      input,
      configPath,
      configuredSource ?? persistedSource,
    ),
    serviceEnabled: retained.serviceEnabled === 1,
    sourceIdentityStatus,
  } satisfies NonNullable<
    Parameters<typeof controlLaunchdService>[0]["statusInspection"]
  >;
}

export async function inspectServiceStatus(
  configPath: string,
  label: string | undefined,
  loadManagedSource: () => Promise<SourceConfiguration>,
) {
  return controlLaunchdService({
    action: "status",
    configPath,
    ...(label === undefined ? {} : { label }),
    statusInspection: await loadServiceStatusInspection(
      configPath,
      loadManagedSource,
    ),
  });
}

async function readStatusConfig(path: string): Promise<unknown> {
  const file = await open(
    path,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
  );
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size > MAXIMUM_CONFIG_BYTES)
      throw new Error("STATUS_CONFIG_INVALID_SIZE_OR_TYPE");
    const bytes = Buffer.alloc(MAXIMUM_CONFIG_BYTES + 1);
    const result = await file.read(bytes, 0, bytes.length, 0);
    if (result.bytesRead !== metadata.size)
      throw new Error("STATUS_CONFIG_CHANGED_OR_OVERSIZED");
    return JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(
        bytes.subarray(0, result.bytesRead),
      ),
    );
  } finally {
    await file.close();
  }
}
