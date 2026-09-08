import { constants } from "node:fs";
import { open, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

import { z } from "zod";

import { sha256 } from "../persistence/work-key.js";
import { schedulerStateDigest } from "./sol-lane-scheduler.js";
import type { EnrichmentProvider } from "./sol-runner.js";

const MAXIMUM_CREDENTIAL_BYTES = 1024 * 1024;
const MAXIMUM_STABLE_OBSERVATION_ATTEMPTS = 3;
const CodexAccountIdentitySchema = z.looseObject({
  auth_mode: z.string().min(1).optional(),
  tokens: z.looseObject({
    access_token: z.string().min(1),
    account_id: z.string().min(1),
  }),
});

const CODEX_ENVIRONMENT_KEYS = ["OPENAI_API_KEY"] as const;

export type ProviderCredentialSnapshot =
  | {
      readonly accountGeneration: string;
      readonly materialGeneration: string;
      readonly state: "observed";
    }
  | { readonly state: "absent" }
  | { readonly state: "transient_unavailable" };

export interface ProviderCredentialObservationHooks {
  readonly afterDescriptorRead?: (context: {
    readonly attempt: number;
    readonly path: string;
  }) => Promise<void> | void;
  readonly beforeAttempt?: (context: {
    readonly attempt: number;
    readonly path: string;
  }) => Promise<void> | void;
}

type CredentialFileSnapshot =
  | {
      readonly accountIdentity: null | string;
      readonly identity: CredentialFileIdentity;
      readonly state: "observed";
    }
  | { readonly state: "absent" }
  | { readonly state: "transient_unavailable" };

interface CredentialFileIdentity {
  readonly changedAtNs: string;
  readonly contentHash: string;
  readonly device: string;
  readonly inode: string;
  readonly modifiedAtNs: string;
  readonly pathHash: string;
  readonly size: string;
}

/**
 * Observes both provider account identity and authentication material without
 * returning either raw value. Account identity fences account-scoped pressure;
 * material identity invalidates login caches after an in-account token refresh.
 */
export async function providerCredentialSnapshot(
  provider: EnrichmentProvider,
  environment: Readonly<NodeJS.ProcessEnv>,
  homeDirectory = homedir(),
  hooks: ProviderCredentialObservationHooks = {},
): Promise<ProviderCredentialSnapshot> {
  const environmentIdentity = CODEX_ENVIRONMENT_KEYS.flatMap((key) => {
    const value = environment[key];
    return value === undefined ? [] : [{ key, valueHash: sha256(value) }];
  });
  const credentialSnapshots = await Promise.all(
    credentialPaths(environment, homeDirectory).map((path) =>
      credentialFileSnapshot(path, hooks),
    ),
  );
  if (
    credentialSnapshots.some(
      (snapshot) => snapshot.state === "transient_unavailable",
    )
  )
    return { state: "transient_unavailable" };
  const observedFiles = credentialSnapshots.flatMap((snapshot) =>
    snapshot.state === "observed" ? [snapshot] : [],
  );
  const observed = observedFiles[0];
  if (observed?.accountIdentity === null || observed === undefined)
    return environmentIdentity.length === 0
      ? { state: "absent" }
      : { state: "transient_unavailable" };
  return {
    accountGeneration: observed.accountIdentity,
    materialGeneration: schedulerStateDigest({
      environmentIdentity,
      fileIdentity: observedFiles.map(({ identity }) => identity),
      provider,
    }),
    state: "observed",
  };
}

/** Compatibility adapter for the account-scoped scheduler. */
export async function providerCredentialGeneration(
  provider: EnrichmentProvider,
  environment: Readonly<NodeJS.ProcessEnv>,
  homeDirectory = homedir(),
): Promise<null | string> {
  const snapshot = await providerCredentialSnapshot(
    provider,
    environment,
    homeDirectory,
  );
  return snapshot.state === "observed" ? snapshot.accountGeneration : null;
}

async function credentialFileSnapshot(
  path: string,
  hooks: ProviderCredentialObservationHooks,
): Promise<CredentialFileSnapshot> {
  for (
    let attempt = 0;
    attempt < MAXIMUM_STABLE_OBSERVATION_ATTEMPTS;
    attempt += 1
  ) {
    // eslint-disable-next-line no-await-in-loop -- Each bounded pass reopens the pathname after transient credential-writer churn.
    await hooks.beforeAttempt?.({ attempt, path });
    // eslint-disable-next-line no-await-in-loop -- Credential versions must be observed serially to reject partial or replaced files.
    const snapshot = await credentialFileSnapshotOnce(path, attempt, hooks);
    if (snapshot.state !== "transient_unavailable") return snapshot;
    if (attempt + 1 < MAXIMUM_STABLE_OBSERVATION_ATTEMPTS)
      // eslint-disable-next-line no-await-in-loop -- Yield once so an atomic/truncate writer can complete before the next bounded filesystem-only observation.
      await immediate();
  }
  return { state: "transient_unavailable" };
}

async function credentialFileSnapshotOnce(
  path: string,
  attempt: number,
  hooks: ProviderCredentialObservationHooks,
): Promise<CredentialFileSnapshot> {
  let handle;
  try {
    // Validate the opened descriptor without waiting for a writer if auth.json
    // was replaced by a FIFO. Regular files retain normal read semantics.
    handle = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
  } catch (error) {
    return isMissingFileError(error)
      ? { state: "absent" }
      : { state: "transient_unavailable" };
  }
  let observation: null | StableCredentialFile;
  try {
    observation = await readStableCredentialFile(handle);
    await hooks.afterDescriptorRead?.({ attempt, path });
    if (observation !== null) {
      const pathMetadata = await stat(path, { bigint: true });
      if (
        !pathMetadata.isFile() ||
        !sameFileVersion(observation.metadata, pathMetadata)
      )
        observation = null;
    }
  } catch {
    return { state: "transient_unavailable" };
  } finally {
    await handle.close().catch(() => undefined);
  }
  if (observation === null) return { state: "transient_unavailable" };
  const { content, metadata } = observation;
  const accountIdentity = codexAccountIdentity(content);
  if (accountIdentity === null) return { state: "transient_unavailable" };
  return {
    accountIdentity,
    identity: {
      changedAtNs: metadata.ctimeNs.toString(),
      contentHash: sha256(content),
      device: metadata.dev.toString(),
      inode: metadata.ino.toString(),
      modifiedAtNs: metadata.mtimeNs.toString(),
      pathHash: sha256(path),
      size: metadata.size.toString(),
    },
    state: "observed",
  };
}

function codexAccountIdentity(content: Buffer): null | string {
  try {
    const input: unknown = JSON.parse(content.toString("utf8"));
    const parsed = CodexAccountIdentitySchema.safeParse(input);
    if (!parsed.success) return null;
    return schedulerStateDigest({
      accountIdHash: sha256(parsed.data.tokens.account_id),
      authMode: parsed.data.auth_mode ?? "unknown",
      provider: "sol",
    });
  } catch {
    return null;
  }
}

function credentialPaths(
  environment: Readonly<NodeJS.ProcessEnv>,
  homeDirectory: string,
): readonly string[] {
  return [
    join(
      resolve(environment["CODEX_HOME"] ?? join(homeDirectory, ".codex")),
      "auth.json",
    ),
  ];
}

function isMissingFileError(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

interface ComparableFileVersion {
  readonly ctimeNs: bigint;
  readonly dev: bigint;
  readonly ino: bigint;
  readonly mtimeNs: bigint;
  readonly size: bigint;
}

interface StableCredentialFile {
  readonly content: Buffer;
  readonly metadata: ComparableFileVersion;
}

async function readStableCredentialFile(
  handle: Awaited<ReturnType<typeof open>>,
): Promise<null | StableCredentialFile> {
  const before = await handle.stat({ bigint: true });
  if (
    !before.isFile() ||
    before.size < 0n ||
    before.size > BigInt(MAXIMUM_CREDENTIAL_BYTES)
  )
    return null;
  const content = await handle.readFile();
  const after = await handle.stat({ bigint: true });
  return sameFileVersion(before, after) &&
    BigInt(content.byteLength) === after.size
    ? { content, metadata: after }
    : null;
}

function sameFileVersion(
  left: ComparableFileVersion,
  right: ComparableFileVersion,
): boolean {
  return (
    left.dev === right.dev &&
    left.ino === right.ino &&
    left.size === right.size &&
    left.mtimeNs === right.mtimeNs &&
    left.ctimeNs === right.ctimeNs
  );
}

function immediate(): Promise<void> {
  return new Promise((resolvePromise) => setImmediate(resolvePromise));
}
