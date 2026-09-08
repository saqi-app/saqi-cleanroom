import { execFileSync, spawnSync } from "node:child_process";
import { createHash, hash as oneShotHash, randomUUID } from "node:crypto";
// eslint-disable-next-line @sarj/prefer-node-fs-promises -- The synchronous release-build hook removes build-only inputs before the caller seals the dependency closure.
import { constants, rmSync } from "node:fs";
import {
  access,
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readdir,
  readFile,
  readlink,
  realpath,
  rename,
  rm,
  symlink,
  unlink,
} from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";

import { z } from "zod";

const CommitSchema = z.string().regex(/^[0-9a-f]{40}$/);
const COMMIT_PEEL_SUFFIX = "^{commit}";
const MANIFEST_NAME = "runtime-release.json";
const LOCK_NAME = ".install.lock";
const INVALID_LOCK_STALE_MS = 3_600_000;
const RETENTION_VERIFICATION_CONCURRENCY = 4;
const ManifestSchema = z.strictObject({
  closureBytes: z.int().nonnegative(),
  closureEntries: z.int().positive(),
  closureSha256: z.string().regex(/^[0-9a-f]{64}$/),
  commit: CommitSchema,
  nodeVersion: z.string().regex(/^v\d+\.\d+\.\d+/),
  schemaId: z.literal("saqi.runtime-release"),
  schemaVersion: z.literal(2),
});
type RuntimeReleaseManifest = z.infer<typeof ManifestSchema>;

export interface RuntimeReleaseIdentity extends RuntimeReleaseManifest {
  readonly manifestPath: string;
}
const InstallLockSchema = z.strictObject({
  nonce: z.uuid(),
  pid: z.int().positive(),
  schemaId: z.literal("saqi.runtime-install-lock"),
  schemaVersion: z.literal(1),
  startedAt: z.int().nonnegative(),
});

export interface RuntimeInstallOptions {
  readonly commit: string;
  readonly releaseRoot: string;
  readonly repository: string;
}

export async function readRuntimeReleaseIdentity(
  executablePath: string,
): Promise<null | RuntimeReleaseIdentity> {
  if (executablePath.trim().length === 0) return null;
  let directory = dirname(await realpath(executablePath));
  for (;;) {
    const manifestPath = join(directory, MANIFEST_NAME);
    // eslint-disable-next-line no-await-in-loop -- Ancestor manifests must be checked from nearest to farthest.
    if (await pathEntryExists(manifestPath)) {
      // eslint-disable-next-line no-await-in-loop -- The discovered release directory must be validated before use.
      const directoryInformation = await lstat(directory);
      if ((directoryInformation.mode & 0o222) !== 0)
        throw new Error("RUNTIME_RELEASE_NOT_IMMUTABLE");
      // eslint-disable-next-line no-await-in-loop -- The discovered manifest must be validated before use.
      const manifestInformation = await lstat(manifestPath);
      if ((manifestInformation.mode & 0o222) !== 0)
        throw new Error("RUNTIME_MANIFEST_WRITABLE");
      // eslint-disable-next-line no-await-in-loop -- Only the nearest valid manifest identifies the runtime.
      const manifestContents = await readFile(manifestPath, "utf8");
      return {
        ...ManifestSchema.parse(JSON.parse(manifestContents)),
        manifestPath,
      };
    }
    const parent = dirname(directory);
    if (parent === directory) return null;
    directory = parent;
  }
}

export interface RuntimeInstallDependencies {
  readonly failpoint?: (name: RuntimeInstallFailpoint) => void;
  readonly prepareRelease?: (checkout: string) => Promise<void> | void;
  readonly run?: RuntimeBuildRunner;
}

type RuntimeBuildRunner = (
  command: string,
  argumentsList: readonly string[],
  cwd: string,
) => void;

type RuntimeInstallFailpoint =
  "current-switched" | "previous-switched" | "release-published";

export interface RuntimeInstallResult {
  readonly commit: string;
  readonly current: string;
  readonly previous: null | string;
  readonly release: string;
  readonly reused: boolean;
}

export interface RuntimeReleaseRetentionOptions {
  /** Apply a freshly computed plan. Omitted or false is always a dry run. */
  readonly apply?: boolean;
  /** Required when apply is true, and bounds removals even if more are eligible. */
  readonly maximumDeletions?: number;
  readonly releaseRoot: string;
  readonly repository: string;
  /** Number of additional, Git-ref-anchored rollback releases to preserve. */
  readonly retainRollbackCount?: number;
}

interface RuntimeReleaseRetentionEntry {
  readonly commit: string;
  readonly disposition: "delete" | "preserve";
  readonly path: string;
  readonly reason:
    | "active_ref"
    | "malformed_or_unknown"
    | "newest_rollback"
    | "retention_candidate"
    | "unanchored"
    | "verification_failed";
}

export interface RuntimeReleaseRetentionResult {
  readonly applied: boolean;
  readonly deleted: readonly string[];
  readonly entries: readonly RuntimeReleaseRetentionEntry[];
  readonly maximumDeletions: null | number;
  readonly retainRollbackCount: number;
}

interface ClosureDigest {
  readonly bytes: number;
  readonly entries: number;
  readonly sha256: string;
  readonly writableEntries: number;
}

export async function installRuntime(
  raw: RuntimeInstallOptions,
  dependencies: RuntimeInstallDependencies = {},
): Promise<RuntimeInstallResult> {
  requireSupportedNode();
  const repository = resolve(raw.repository);
  const requestedReleaseRoot = resolve(raw.releaseRoot);
  const commit = CommitSchema.parse(raw.commit);
  requireCleanRepository(repository);
  const resolvedCommit = git(repository, [
    "rev-parse",
    "--verify",
    `${commit}${COMMIT_PEEL_SUFFIX}`,
  ]).trim();
  if (resolvedCommit !== commit) throw new Error("RUNTIME_COMMIT_NOT_EXACT");

  const releaseRoot = await prepareReleaseRoot(requestedReleaseRoot);
  return withInstallLock(releaseRoot, () =>
    installLocked(repository, releaseRoot, commit, dependencies),
  );
}

/**
 * Plans or applies bounded runtime retention while holding the same lock used
 * by installation. Anything that cannot be positively identified as a sealed,
 * Git-ref-anchored release is preserved.
 */
export async function retainRuntimeReleases(
  raw: RuntimeReleaseRetentionOptions,
): Promise<RuntimeReleaseRetentionResult> {
  requireSupportedNode();
  const repository = resolve(raw.repository);
  const releaseRoot = await prepareReleaseRoot(resolve(raw.releaseRoot));
  const retainRollbackCount = z
    .int()
    .min(3)
    .parse(raw.retainRollbackCount ?? 3);
  const maximumDeletions =
    raw.maximumDeletions === undefined
      ? null
      : z.int().positive().max(100).parse(raw.maximumDeletions);
  if (raw.apply === true && maximumDeletions === null) {
    throw new Error("RUNTIME_RETENTION_APPLY_REQUIRES_MAXIMUM_DELETIONS");
  }
  return withInstallLock(releaseRoot, () =>
    retainRuntimeReleasesLocked({
      apply: raw.apply === true,
      maximumDeletions,
      releaseRoot,
      repository,
      retainRollbackCount,
    }),
  );
}

async function installLocked(
  repository: string,
  releaseRoot: string,
  commit: string,
  dependencies: RuntimeInstallDependencies,
): Promise<RuntimeInstallResult> {
  const releases = await prepareReleasesDirectory(releaseRoot);
  const release = join(releases, commit);
  const reused = await pathEntryExists(release);
  if (reused) {
    await requirePlainDirectory(release, "RUNTIME_RELEASE_NOT_DIRECTORY");
    await requireDirectRealChild(
      releases,
      release,
      "RUNTIME_RELEASE_PATH_INVALID",
    );
    await verifyRelease(release, commit);
  } else {
    const staging = await mkdtemp(join(releases, `.staging-${commit}-`));
    await requireDirectRealChild(
      releases,
      staging,
      "RUNTIME_STAGING_PATH_INVALID",
    );
    try {
      await exportCommit(repository, commit, staging);
      if (dependencies.prepareRelease)
        await dependencies.prepareRelease(staging);
      else prepareRelease(staging, dependencies.run ?? run);
      await sealRelease(staging, commit);
      await verifyRelease(staging, commit);
      await rename(staging, release);
      await fsyncDirectory(releases);
      dependencies.failpoint?.("release-published");
    } catch (error) {
      await removeStaging(releases, staging);
      throw error;
    }
  }

  const currentLink = join(releaseRoot, "current");
  const previousLink = join(releaseRoot, "previous");
  const target = join("releases", commit);
  const oldTarget = await readOptionalReleaseLink(currentLink, releaseRoot);
  if (oldTarget !== null && oldTarget !== target) {
    await atomicSymlink(previousLink, oldTarget, releaseRoot);
    dependencies.failpoint?.("previous-switched");
  }
  await atomicSymlink(currentLink, target, releaseRoot);
  dependencies.failpoint?.("current-switched");
  const currentRelease = await realpath(currentLink);
  await requireDirectRealChild(
    releases,
    currentRelease,
    "RUNTIME_RELEASE_LINK_INVALID",
  );
  await verifyRelease(currentRelease, commit);

  return {
    commit,
    current: currentLink,
    previous:
      oldTarget === null || oldTarget === target
        ? await readOptionalReleaseLink(previousLink, releaseRoot)
        : oldTarget,
    release,
    reused,
  };
}

interface VerifiedRetentionRelease {
  readonly commit: string;
  readonly committedAt: number;
  readonly historyDepth: number;
  readonly path: string;
}

type InspectedRetentionRelease =
  | Readonly<{ entry: RuntimeReleaseRetentionEntry }>
  | Readonly<{ verified: VerifiedRetentionRelease }>;

async function retainRuntimeReleasesLocked(options: {
  readonly apply: boolean;
  readonly maximumDeletions: null | number;
  readonly releaseRoot: string;
  readonly repository: string;
  readonly retainRollbackCount: number;
}): Promise<RuntimeReleaseRetentionResult> {
  const releases = await prepareReleasesDirectory(options.releaseRoot);
  const activeTargets = new Set<string>();
  for (const name of ["current", "previous"]) {
    // eslint-disable-next-line no-await-in-loop -- Active links must be captured under the installation lock.
    const target = await readOptionalReleaseLink(
      join(options.releaseRoot, name),
      options.releaseRoot,
    );
    if (target !== null) activeTargets.add(target);
  }

  const entries: RuntimeReleaseRetentionEntry[] = [];
  const verified: VerifiedRetentionRelease[] = [];
  const releaseNames = await readdir(releases);
  const names = releaseNames.toSorted(codeUnitCompare);
  for (const name of names) {
    const path = join(releases, name);
    if (!CommitSchema.safeParse(name).success) {
      entries.push({
        commit: name,
        disposition: "preserve",
        path,
        reason: "malformed_or_unknown",
      });
    } else if (activeTargets.has(join("releases", name))) {
      entries.push({
        commit: name,
        disposition: "preserve",
        path,
        reason: "active_ref",
      });
    }
  }

  const inspectableNames = names.filter(
    (name) =>
      CommitSchema.safeParse(name).success &&
      !activeTargets.has(join("releases", name)),
  );
  for (
    let index = 0;
    index < inspectableNames.length;
    index += RETENTION_VERIFICATION_CONCURRENCY
  ) {
    // eslint-disable-next-line no-await-in-loop -- Batches bound hashing, process, and memory pressure while independent releases verify concurrently.
    const inspected = await Promise.all(
      inspectableNames
        .slice(index, index + RETENTION_VERIFICATION_CONCURRENCY)
        .map((name) =>
          inspectRetentionRelease(releases, options.repository, name),
        ),
    );
    for (const result of inspected) {
      if ("entry" in result) entries.push(result.entry);
      else verified.push(result.verified);
    }
  }

  verified.sort(
    (left, right) =>
      right.committedAt - left.committedAt ||
      right.historyDepth - left.historyDepth ||
      codeUnitCompare(right.commit, left.commit),
  );
  for (const [index, release] of verified.entries()) {
    entries.push({
      commit: release.commit,
      disposition: index < options.retainRollbackCount ? "preserve" : "delete",
      path: release.path,
      reason:
        index < options.retainRollbackCount
          ? "newest_rollback"
          : "retention_candidate",
    });
  }
  entries.sort((left, right) => codeUnitCompare(left.commit, right.commit));

  const candidates = entries
    .filter(
      (
        entry,
      ): entry is RuntimeReleaseRetentionEntry & {
        disposition: "delete";
      } => entry.disposition === "delete",
    )
    .toSorted((left, right) => codeUnitCompare(left.commit, right.commit));
  const selected =
    options.maximumDeletions === null
      ? []
      : candidates.slice(0, options.maximumDeletions);
  const deleted: string[] = [];
  if (options.apply) {
    for (const entry of selected) {
      // Git refs can move independently of the install lock. A candidate that
      // lost its durable anchor after planning is preserved, never deleted.
      if (durableRefCommitMetadata(options.repository, entry.commit) === null)
        continue;
      // eslint-disable-next-line no-await-in-loop -- Active refs are re-read immediately before each destructive rename.
      if (await releaseIsActive(options.releaseRoot, entry.commit)) continue;
      // eslint-disable-next-line no-await-in-loop -- The exact candidate boundary must still be intact immediately before removal.
      await requireDirectRealChild(
        releases,
        entry.path,
        "RUNTIME_RELEASE_PATH_INVALID",
      );
      // eslint-disable-next-line no-await-in-loop -- A final closure check prevents deleting a candidate changed after planning.
      await verifyRelease(entry.path, entry.commit);
      const tombstone = join(
        releases,
        `.pruning-${entry.commit}-${randomUUID()}`,
      );
      // eslint-disable-next-line no-await-in-loop -- Rename atomically removes the release from the usable namespace before recursive deletion.
      await rename(entry.path, tombstone);
      // eslint-disable-next-line no-await-in-loop -- Each namespace mutation is made durable before data removal.
      await fsyncDirectory(releases);
      // eslint-disable-next-line no-await-in-loop -- Only the exact, already-renamed tombstone is made writable.
      await makeTreeOwnerWritable(tombstone);
      // eslint-disable-next-line no-await-in-loop -- Bounded candidates are removed serially for deterministic failure recovery.
      await rm(tombstone, { force: true, recursive: true });
      // eslint-disable-next-line no-await-in-loop -- Each completed deletion is durable before the next candidate begins.
      await fsyncDirectory(releases);
      deleted.push(entry.commit);
    }
  }

  return {
    applied: options.apply,
    deleted,
    entries,
    maximumDeletions: options.maximumDeletions,
    retainRollbackCount: options.retainRollbackCount,
  };
}

async function inspectRetentionRelease(
  releases: string,
  repository: string,
  commit: string,
): Promise<InspectedRetentionRelease> {
  const path = join(releases, commit);
  try {
    await requireDirectRealChild(
      releases,
      path,
      "RUNTIME_RELEASE_PATH_INVALID",
    );
    await verifyRelease(path, commit);
  } catch {
    return {
      entry: {
        commit,
        disposition: "preserve",
        path,
        reason: "verification_failed",
      },
    };
  }
  const metadata = durableRefCommitMetadata(repository, commit);
  if (metadata === null) {
    return {
      entry: {
        commit,
        disposition: "preserve",
        path,
        reason: "unanchored",
      },
    };
  }
  return { verified: { commit, path, ...metadata } };
}

function durableRefCommitMetadata(
  repository: string,
  commit: string,
): { readonly committedAt: number; readonly historyDepth: number } | null {
  try {
    const ref = git(repository, [
      "for-each-ref",
      `--contains=${commit}`,
      "--format=%(refname)",
      "refs/heads",
      "refs/remotes",
      "refs/tags",
    ]).trim();
    if (ref === "") return null;
    const committedAt = Number(
      git(repository, ["show", "-s", "--format=%ct", commit]).trim(),
    );
    const historyDepth = Number(
      git(repository, ["rev-list", "--count", commit]).trim(),
    );
    if (
      !Number.isSafeInteger(committedAt) ||
      committedAt < 0 ||
      !Number.isSafeInteger(historyDepth) ||
      historyDepth < 1
    ) {
      return null;
    }
    return { committedAt, historyDepth };
  } catch {
    return null;
  }
}

async function releaseIsActive(
  releaseRoot: string,
  commit: string,
): Promise<boolean> {
  for (const name of ["current", "previous"]) {
    // eslint-disable-next-line no-await-in-loop -- Both durable refs must be checked at one deletion boundary.
    const target = await readOptionalReleaseLink(
      join(releaseRoot, name),
      releaseRoot,
    );
    if (target === join("releases", commit)) return true;
  }
  return false;
}

function prepareRelease(checkout: string, runner: RuntimeBuildRunner): void {
  const typescript = join(checkout, "typescript");
  runner("corepack", ["yarn", "install", "--immutable"], typescript);
  runner(
    "corepack",
    ["yarn", "workspace", "@saqi/source-adapter", "build"],
    typescript,
  );
  runner(
    "corepack",
    ["yarn", "workspace", "@saqi/precedent-iso", "build"],
    typescript,
  );
  runner(
    "corepack",
    ["yarn", "workspace", "@saqi/crawler-local", "build"],
    typescript,
  );
  runner(
    "corepack",
    ["yarn", "workspaces", "focus", "@saqi/crawler-local", "--production"],
    typescript,
  );
  // The focused node_modules tree is the runtime dependency closure. Yarn's
  // download cache and install-state file are build inputs only; sealing them
  // into every immutable release wastes hundreds of megabytes and can fence
  // live artifact writes under disk pressure.
  rmSync(join(typescript, ".yarn", "cache"), { force: true, recursive: true });
  rmSync(join(typescript, ".yarn", "install-state.gz"), { force: true });
}

async function exportCommit(
  repository: string,
  commit: string,
  staging: string,
): Promise<void> {
  const archive = join(staging, ".release.tar");
  execFileSync(
    "git",
    ["archive", "--format=tar", `--output=${archive}`, commit],
    { cwd: repository, stdio: "pipe" },
  );
  execFileSync("tar", ["-xf", archive, "-C", staging], { stdio: "pipe" });
  await rm(archive);
}

async function sealRelease(release: string, commit: string): Promise<void> {
  await requireExecutableCli(releaseCli(release));
  await makeTreeReadOnly(release, false);
  const closure = await digestClosure(release);
  if (closure.writableEntries !== 0)
    throw new Error("RUNTIME_RELEASE_NOT_IMMUTABLE");
  const manifest = ManifestSchema.parse({
    closureBytes: closure.bytes,
    closureEntries: closure.entries,
    closureSha256: closure.sha256,
    commit,
    nodeVersion: process.version,
    schemaId: "saqi.runtime-release",
    schemaVersion: 2,
  });
  await writeDurableFile(
    join(release, MANIFEST_NAME),
    `${JSON.stringify(manifest, null, 2)}\n`,
    0o444,
  );
  await fsyncDirectory(release);
  const releaseInformation = await lstat(release);
  await chmod(release, releaseInformation.mode & ~0o222);
  await fsyncDirectory(release);
}

async function verifyRelease(release: string, commit: string): Promise<void> {
  await requirePlainDirectory(release, "RUNTIME_RELEASE_NOT_DIRECTORY");
  const releaseInformation = await lstat(release);
  if ((releaseInformation.mode & 0o222) !== 0) {
    throw new Error("RUNTIME_RELEASE_NOT_IMMUTABLE");
  }
  const manifestPath = join(release, MANIFEST_NAME);
  const manifest = ManifestSchema.parse(
    JSON.parse(await readFile(manifestPath, "utf8")),
  );
  if (manifest.commit !== commit)
    throw new Error("RUNTIME_MANIFEST_COMMIT_MISMATCH");
  if (nodeMajor(manifest.nodeVersion) < 24) {
    throw new Error("RUNTIME_MANIFEST_NODE_UNSUPPORTED");
  }
  const manifestInformation = await lstat(manifestPath);
  if ((manifestInformation.mode & 0o222) !== 0) {
    throw new Error("RUNTIME_MANIFEST_WRITABLE");
  }
  const closure = await digestClosure(release);
  if (closure.writableEntries !== 0) {
    throw new Error("RUNTIME_RELEASE_NOT_IMMUTABLE");
  }
  if (
    closure.sha256 !== manifest.closureSha256 ||
    closure.entries !== manifest.closureEntries ||
    closure.bytes !== manifest.closureBytes
  ) {
    throw new Error("RUNTIME_EXECUTABLE_CLOSURE_MISMATCH");
  }
  const cli = releaseCli(release);
  await requireExecutableCli(cli);
  const result = spawnSync(cli, ["--help"], {
    encoding: "utf8",
    timeout: 30_000,
  });
  if (result.status !== 0) {
    throw new Error(
      `RUNTIME_CLI_SMOKE_FAILED: ${result.error?.message ?? result.stderr.trim()}`,
    );
  }
}

async function digestClosure(root: string): Promise<ClosureDigest> {
  const hash = createHash("sha256");
  let bytes = 0;
  let entries = 0;
  let writableEntries = 0;
  const visit = async (directory: string, prefix: string): Promise<void> => {
    const directoryEntries = await readdir(directory);
    const names = directoryEntries.toSorted(codeUnitCompare);
    for (const name of names) {
      if (prefix === "" && name === MANIFEST_NAME) continue;
      const path = join(directory, name);
      const logicalPath = prefix === "" ? name : `${prefix}/${name}`;
      // eslint-disable-next-line no-await-in-loop -- Closure hashing must preserve deterministic path order.
      const information = await lstat(path);
      entries += 1;
      if (!information.isSymbolicLink() && (information.mode & 0o222) !== 0) {
        writableEntries += 1;
      }
      if (information.isSymbolicLink()) {
        // eslint-disable-next-line no-await-in-loop -- Closure hashing must preserve deterministic path order.
        const target = await readlink(path);
        // eslint-disable-next-line no-await-in-loop -- Closure hashing must preserve deterministic path order.
        const resolvedTarget = await realpath(path);
        // eslint-disable-next-line no-await-in-loop -- Each link must be containment-checked before hashing.
        await requireRealContained(
          root,
          resolvedTarget,
          "RUNTIME_SYMLINK_ESCAPE",
        );
        hash.update(`L\0${logicalPath}\0${target}\0`);
      } else if (information.isDirectory()) {
        hash.update(`D\0${logicalPath}\0${String(information.mode & 0o777)}\0`);
        // eslint-disable-next-line no-await-in-loop -- Recursive hashing must preserve deterministic path order.
        await visit(path, logicalPath);
      } else if (information.isFile()) {
        // eslint-disable-next-line no-await-in-loop -- Closure hashing must preserve deterministic path order.
        const content = await readFile(path);
        bytes += content.byteLength;
        hash.update(
          `F\0${logicalPath}\0${String(information.mode & 0o777)}\0${String(content.byteLength)}\0`,
        );
        hash.update(oneShotHash("sha256", content, "hex"));
        hash.update("\0");
      } else {
        throw new Error("RUNTIME_CLOSURE_ENTRY_UNSUPPORTED");
      }
    }
  };
  await visit(root, "");
  return { bytes, entries, sha256: hash.digest("hex"), writableEntries };
}

async function makeTreeReadOnly(
  root: string,
  includeRoot = true,
): Promise<void> {
  const names = await readdir(root);
  for (const name of names) {
    const path = join(root, name);
    // eslint-disable-next-line no-await-in-loop -- Tree permissions are sealed depth-first for safe recovery.
    const information = await lstat(path);
    if (information.isSymbolicLink()) continue;
    if (information.isDirectory()) {
      // eslint-disable-next-line no-await-in-loop -- Tree permissions are sealed depth-first for safe recovery.
      await makeTreeReadOnly(path);
    } else if (information.isFile()) {
      // eslint-disable-next-line no-await-in-loop -- Tree permissions are sealed depth-first for safe recovery.
      await chmod(path, information.mode & ~0o222);
    } else throw new Error("RUNTIME_CLOSURE_ENTRY_UNSUPPORTED");
  }
  if (includeRoot) {
    const rootInformation = await lstat(root);
    await chmod(root, rootInformation.mode & ~0o222);
  }
}

async function requireExecutableCli(cli: string): Promise<void> {
  await access(cli, constants.R_OK | constants.X_OK);
  const information = await lstat(cli);
  if (!information.isFile()) throw new Error("RUNTIME_CLI_NOT_FILE");
  if ((information.mode & 0o111) === 0) {
    throw new Error("RUNTIME_CLI_NOT_EXECUTABLE");
  }
  const contents = await readFile(cli, "utf8");
  if (contents.split("\n", 1)[0] !== "#!/usr/bin/env node") {
    throw new Error("RUNTIME_CLI_SHEBANG_INVALID");
  }
}

function requireCleanRepository(repository: string): void {
  const status = git(repository, [
    "status",
    "--porcelain=v1",
    "--untracked-files=all",
  ]);
  if (status.trim() !== "") throw new Error("RUNTIME_SOURCE_DIRTY");
}

function requireSupportedNode(): void {
  if (nodeMajor(process.version) < 24)
    throw new Error("RUNTIME_NODE_UNSUPPORTED");
}

function nodeMajor(version: string): number {
  const match = /^v(\d+)\./.exec(version);
  return match ? Number(match[1]) : 0;
}

function releaseCli(release: string): string {
  return join(
    release,
    "typescript",
    "packages",
    "crawler-local",
    "dist",
    "cli.js",
  );
}

function git(repository: string, argumentsList: readonly string[]): string {
  return execFileSync("git", [...argumentsList], {
    cwd: repository,
    encoding: "utf8",
    maxBuffer: 1_048_576,
  });
}

function run(
  command: string,
  argumentsList: readonly string[],
  cwd: string,
): void {
  execFileSync(command, [...argumentsList], { cwd, stdio: "inherit" });
}

async function prepareReleaseRoot(requested: string): Promise<string> {
  const requestedExists = await pathEntryExists(requested);
  const requestedInformation = requestedExists ? await lstat(requested) : null;
  if (requestedInformation?.isSymbolicLink()) {
    throw new Error("RUNTIME_RELEASE_ROOT_SYMLINK");
  }
  await mkdir(requested, { recursive: true, mode: 0o755 });
  await requirePlainDirectory(requested, "RUNTIME_RELEASE_ROOT_INVALID");
  return realpath(requested);
}

async function prepareReleasesDirectory(releaseRoot: string): Promise<string> {
  const releases = join(releaseRoot, "releases");
  const releasesExist = await pathEntryExists(releases);
  const releasesInformation = releasesExist ? await lstat(releases) : null;
  if (releasesInformation?.isSymbolicLink()) {
    throw new Error("RUNTIME_RELEASES_SYMLINK");
  }
  await mkdir(releases, { mode: 0o755, recursive: true });
  await requirePlainDirectory(releases, "RUNTIME_RELEASES_INVALID");
  await requireDirectRealChild(
    releaseRoot,
    releases,
    "RUNTIME_RELEASES_PATH_INVALID",
  );
  await fsyncDirectory(releaseRoot);
  return realpath(releases);
}

async function requirePlainDirectory(
  path: string,
  code: string,
): Promise<void> {
  const information = await lstat(path);
  if (!information.isDirectory() || information.isSymbolicLink()) {
    throw new Error(code);
  }
}

async function requireDirectRealChild(
  parent: string,
  child: string,
  code: string,
): Promise<void> {
  const realParent = await realpath(parent);
  const realChild = await realpath(child);
  if (dirname(realChild) !== realParent) throw new Error(code);
}

async function requireRealContained(
  root: string,
  candidate: string,
  code: string,
): Promise<void> {
  const realRoot = await realpath(root);
  const realCandidate = await realpath(candidate);
  const suffix = relative(realRoot, realCandidate);
  if (suffix === "" || suffix.startsWith("..") || suffix.startsWith(sep)) {
    throw new Error(code);
  }
}

async function readOptionalReleaseLink(
  link: string,
  releaseRoot: string,
): Promise<null | string> {
  if (!(await pathEntryExists(link))) return null;
  const information = await lstat(link);
  if (!information.isSymbolicLink()) {
    throw new Error("RUNTIME_RELEASE_LINK_INVALID");
  }
  const target = await readlink(link);
  requireContainedTarget(releaseRoot, target);
  const resolvedTarget = await realpath(link);
  await requireDirectRealChild(
    join(releaseRoot, "releases"),
    resolvedTarget,
    "RUNTIME_RELEASE_LINK_INVALID",
  );
  return target;
}

async function atomicSymlink(
  link: string,
  target: string,
  releaseRoot: string,
): Promise<void> {
  requireContainedTarget(releaseRoot, target);
  const parent = dirname(link);
  if ((await realpath(parent)) !== releaseRoot) {
    throw new Error("RUNTIME_RELEASE_LINK_INVALID");
  }
  const temporary = join(parent, `.link-${randomUUID()}`);
  await symlink(target, temporary, "dir");
  await fsyncDirectory(parent);
  try {
    await rename(temporary, link);
    await fsyncDirectory(parent);
  } catch (error) {
    await unlink(temporary);
    await fsyncDirectory(parent);
    throw error;
  }
}

function requireContainedTarget(releaseRoot: string, target: string): void {
  if (target.startsWith(sep)) throw new Error("RUNTIME_RELEASE_LINK_INVALID");
  const destination = resolve(releaseRoot, target);
  const releases = resolve(releaseRoot, "releases");
  const suffix = relative(releases, destination);
  if (suffix.startsWith("..") || suffix === "" || suffix.includes(sep)) {
    throw new Error("RUNTIME_RELEASE_LINK_INVALID");
  }
}

async function withInstallLock<T>(
  releaseRoot: string,
  operation: () => Promise<T>,
): Promise<T> {
  const lock = await acquireInstallLock(releaseRoot);
  try {
    return await operation();
  } finally {
    await releaseInstallLock(releaseRoot, lock);
  }
}

async function acquireInstallLock(
  releaseRoot: string,
): Promise<z.infer<typeof InstallLockSchema>> {
  const lockPath = join(releaseRoot, LOCK_NAME);
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const lock = InstallLockSchema.parse({
      nonce: randomUUID(),
      pid: process.pid,
      schemaId: "saqi.runtime-install-lock",
      schemaVersion: 1,
      startedAt: Date.now(),
    });
    try {
      // eslint-disable-next-line no-await-in-loop -- Exclusive installation lock attempts must remain serialized.
      await writeDurableFile(lockPath, `${JSON.stringify(lock)}\n`, 0o600);
      // eslint-disable-next-line no-await-in-loop -- The lock directory must be durable before ownership is returned.
      await fsyncDirectory(releaseRoot);
      return lock;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
      // eslint-disable-next-line no-await-in-loop -- Stale lock recovery must complete before retrying acquisition.
      await recoverStaleLock(releaseRoot, lockPath);
    }
  }
  throw new Error("RUNTIME_INSTALL_LOCKED");
}

async function recoverStaleLock(
  releaseRoot: string,
  lockPath: string,
): Promise<void> {
  let information: Awaited<ReturnType<typeof lstat>>;
  try {
    information = await lstat(lockPath);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return;
    throw error;
  }
  if (information.isSymbolicLink()) {
    throw new Error("RUNTIME_INSTALL_LOCK_INVALID");
  }
  let stale = false;
  try {
    const lock = InstallLockSchema.parse(
      JSON.parse(await readFile(lockPath, "utf8")),
    );
    stale = !pidExists(lock.pid);
  } catch {
    stale = Date.now() - information.mtimeMs >= INVALID_LOCK_STALE_MS;
  }
  if (!stale) throw new Error("RUNTIME_INSTALL_LOCKED");
  const stalePath = join(releaseRoot, `.stale-lock-${randomUUID()}`);
  try {
    await rename(lockPath, stalePath);
    await fsyncDirectory(releaseRoot);
    await unlink(stalePath);
    await fsyncDirectory(releaseRoot);
  } catch (error) {
    if (errorCode(error) !== "ENOENT") throw error;
  }
}

async function releaseInstallLock(
  releaseRoot: string,
  expected: z.infer<typeof InstallLockSchema>,
): Promise<void> {
  const lockPath = join(releaseRoot, LOCK_NAME);
  try {
    const current = InstallLockSchema.parse(
      JSON.parse(await readFile(lockPath, "utf8")),
    );
    if (current.nonce !== expected.nonce) return;
    await unlink(lockPath);
    await fsyncDirectory(releaseRoot);
  } catch (error) {
    if (errorCode(error) !== "ENOENT") throw error;
  }
}

function pidExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return errorCode(error) === "EPERM";
  }
}

async function writeDurableFile(
  path: string,
  content: string,
  mode: number,
): Promise<void> {
  const handle = await open(
    path,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY,
    mode,
  );
  try {
    await handle.writeFile(content, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, constants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function pathEntryExists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

function codeUnitCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function errorCode(error: unknown): null | string {
  return typeof error === "object" && error !== null && "code" in error
    ? String(error.code)
    : null;
}

async function removeStaging(releases: string, staging: string): Promise<void> {
  const suffix = relative(releases, staging);
  if (!suffix.startsWith(".staging-") || suffix.includes(sep)) {
    throw new Error("RUNTIME_STAGING_PATH_INVALID");
  }
  if (!(await pathEntryExists(staging))) return;
  await requireDirectRealChild(
    releases,
    staging,
    "RUNTIME_STAGING_PATH_INVALID",
  );
  await makeTreeOwnerWritable(staging);
  await rm(staging, { force: true, recursive: true });
  await fsyncDirectory(releases);
}

async function makeTreeOwnerWritable(root: string): Promise<void> {
  if (!(await pathEntryExists(root))) return;
  const information = await lstat(root);
  if (information.isSymbolicLink()) return;
  await chmod(root, information.mode | 0o700);
  if (!information.isDirectory()) return;
  const names = await readdir(root);
  for (const name of names) {
    // eslint-disable-next-line no-await-in-loop -- Recovery permissions are restored depth-first before removal.
    await makeTreeOwnerWritable(join(root, name));
  }
}
