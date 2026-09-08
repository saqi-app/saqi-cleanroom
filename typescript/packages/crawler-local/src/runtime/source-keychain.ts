import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { SourceConfigurationSchema } from "@saqi/precedent-iso";
import {
  SourceAdapterProfileV1Schema,
  type SourceConfiguration,
} from "@saqi/source-adapter";

import {
  loadScraperOperationConfig,
  operationConfigDigestForSource,
} from "./operations-contract.js";

const executeFile = promisify(execFile);
const KEYCHAIN_ACCOUNT = "saqi-publication";
const SOURCE_BASE_URL_SERVICE = "saqi-source-base-url";
const SOURCE_ADAPTER_SERVICE = "saqi-source-adapter-v1";
const SOURCE_NAME_SERVICE = "saqi-source-name";

export type KeychainSecretReader = (
  account: string,
  service: string,
) => Promise<string>;

/**
 * Loads the launchd-only source identity without placing secret values in the
 * plist, command line, or diagnostics. Any missing or invalid value fails with
 * one stable, value-free error code.
 */
export async function loadLaunchdSourceConfiguration(
  readSecret: KeychainSecretReader = readKeychainSecret,
): Promise<SourceConfiguration> {
  try {
    const [name, origin, rawProfile] = await Promise.all([
      readSecret(KEYCHAIN_ACCOUNT, SOURCE_NAME_SERVICE),
      readSecret(KEYCHAIN_ACCOUNT, SOURCE_BASE_URL_SERVICE),
      readSecret(KEYCHAIN_ACCOUNT, SOURCE_ADAPTER_SERVICE),
    ]);
    return {
      ...SourceConfigurationSchema.parse({
        name: name.trim(),
        origin: origin.trim(),
      }),
      profile: SourceAdapterProfileV1Schema.parse(JSON.parse(rawProfile)),
    };
  } catch {
    throw new Error("SOURCE_KEYCHAIN_UNAVAILABLE");
  }
}

/** Computes desired identity from a fresh Keychain read without mutating the active source. */
export async function loadLaunchdDesiredConfigDigest(
  configPath: string,
  readSecret: KeychainSecretReader = readKeychainSecret,
): Promise<string> {
  const [loaded, source] = await Promise.all([
    loadScraperOperationConfig(configPath),
    loadLaunchdSourceConfiguration(readSecret),
  ]);
  return operationConfigDigestForSource(loaded.config, source);
}

async function readKeychainSecret(
  account: string,
  service: string,
): Promise<string> {
  const { stdout } = await executeFile(
    "/usr/bin/security",
    ["find-generic-password", "-a", account, "-s", service, "-w"],
    { encoding: "utf8", maxBuffer: 4_096 },
  );
  return stdout;
}
