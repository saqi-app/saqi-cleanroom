import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
  access,
  link,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  statfs,
  unlink,
} from "node:fs/promises";
import { join } from "node:path";

import { sha256 } from "./work-key.js";

export interface Artifact {
  readonly bytes: number;
  readonly hash: string;
  readonly path: string;
}

export interface ArtifactVerification {
  readonly actualHash: null | string;
  readonly expectedHash: string;
  readonly ok: boolean;
  readonly path: string;
}

export interface ArtifactStoreOptions {
  readonly minimumFreeBytes?: number;
  readonly staleTemporaryAgeMs?: number;
}

export interface TemporaryCleanupReport {
  readonly bytesRemoved: number;
  readonly filesRemoved: number;
  readonly scanned: number;
  readonly truncated: boolean;
}

export interface StorageCapacity {
  readonly availableBytes: number;
  readonly minimumFreeBytes: number;
  readonly writable: boolean;
}

/** Durable content-addressed storage boundary consumed by pipeline services. */
export interface ArtifactStorage {
  assertWritableCapacity(requiredBytes?: number): Promise<StorageCapacity>;
  capacity(requiredBytes?: number): Promise<StorageCapacity>;
  readonly capacitySnapshot: StorageCapacity;
  path(hash: string): string;
  put(value: string | Uint8Array): Promise<Artifact>;
  read(hash: string): Promise<Buffer>;
  readonly temporaryCleanup: Promise<TemporaryCleanupReport>;
  verify(hash: string): Promise<ArtifactVerification>;
  verifyAll(): Promise<readonly ArtifactVerification[]>;
}

export async function verifyArtifactInventory(
  store: Pick<ArtifactStorage, "verify" | "verifyAll">,
  referencedHashes: readonly string[],
) {
  const artifacts = await store.verifyAll();
  const verified = new Map(
    artifacts.map((artifact) => [artifact.expectedHash, artifact]),
  );
  const missingOrCorruptReferences: ArtifactVerification[] = [];
  for (const hash of referencedHashes) {
    let result = verified.get(hash);
    if (!result) {
      // eslint-disable-next-line no-await-in-loop -- Only unlisted references need another read; serialize them to bound memory and open files even for a missing corpus.
      result = await store.verify(hash);
      verified.set(hash, result);
    }
    if (!result.ok) missingOrCorruptReferences.push(result);
  }
  return {
    checked: artifacts.length,
    corrupt: artifacts.filter((artifact) => !artifact.ok),
    missingOrCorruptReferences,
    referenced: referencedHashes.length,
  };
}

export class DiskPressureError extends Error {
  readonly availableBytes: number;
  readonly minimumFreeBytes: number;

  constructor(availableBytes: number, minimumFreeBytes: number) {
    super(
      `ARTIFACT_STORE_DISK_PRESSURE: ${String(availableBytes)} bytes available; ${String(minimumFreeBytes)} bytes reserved`,
    );
    this.name = "DiskPressureError";
    this.availableBytes = availableBytes;
    this.minimumFreeBytes = minimumFreeBytes;
  }
}

const DEFAULT_MINIMUM_FREE_BYTES = 2 * 1024 * 1024 * 1024;
const DEFAULT_STALE_TEMPORARY_AGE_MS = 24 * 60 * 60_000;
const MAXIMUM_TEMPORARIES_SCANNED = 10_000;
const VERIFY_BUFFER_BYTES = 1024 * 1024;

export class ArtifactStore implements ArtifactStorage {
  readonly #root: string;
  readonly #minimumFreeBytes: number;
  readonly #temporaryCleanup: Promise<TemporaryCleanupReport>;
  #capacitySnapshot: StorageCapacity;

  constructor(root: string, options: ArtifactStoreOptions = {}) {
    this.#root = root;
    this.#minimumFreeBytes =
      options.minimumFreeBytes ?? DEFAULT_MINIMUM_FREE_BYTES;
    this.#capacitySnapshot = {
      availableBytes: 0,
      minimumFreeBytes: this.#minimumFreeBytes,
      writable: false,
    };
    if (
      !Number.isSafeInteger(this.#minimumFreeBytes) ||
      this.#minimumFreeBytes < 0
    ) {
      throw new Error(
        "Artifact minimumFreeBytes must be a nonnegative integer",
      );
    }
    const staleTemporaryAgeMs =
      options.staleTemporaryAgeMs ?? DEFAULT_STALE_TEMPORARY_AGE_MS;
    if (!Number.isSafeInteger(staleTemporaryAgeMs) || staleTemporaryAgeMs < 0)
      throw new Error("Artifact staleTemporaryAgeMs must be nonnegative");
    this.#temporaryCleanup = mkdir(root, { mode: 0o700, recursive: true }).then(
      async () => cleanupStaleArtifactTemporaries(root, staleTemporaryAgeMs),
    );
  }

  get temporaryCleanup(): Promise<TemporaryCleanupReport> {
    return this.#temporaryCleanup;
  }

  get capacitySnapshot(): StorageCapacity {
    return this.#capacitySnapshot;
  }

  async put(value: string | Uint8Array): Promise<Artifact> {
    await this.#temporaryCleanup;
    const bytes =
      typeof value === "string" ? Buffer.from(value) : Buffer.from(value);
    const hash = sha256(bytes);
    const directory = join(this.#root, hash.slice(0, 2));
    const path = this.path(hash);
    await mkdir(directory, { mode: 0o700, recursive: true });
    if (await pathExists(path)) {
      const verification = await this.verify(hash);
      if (!verification.ok)
        throw new Error(`Existing artifact is corrupt: ${hash}`);
      return { bytes: bytes.length, hash, path };
    }
    await this.assertWritableCapacity(bytes.length);

    const temporary = join(
      directory,
      `.${hash}.${String(process.pid)}.${randomUUID()}.tmp`,
    );
    let file: Awaited<ReturnType<typeof open>> | null = null;
    try {
      file = await open(temporary, "wx", 0o600);
      await file.writeFile(bytes);
      await file.sync();
      await file.close();
      file = null;
      try {
        await link(temporary, path);
      } catch (error) {
        const code = errorCode(error);
        if (code !== "EEXIST") throw error;
      }
      const directoryHandle = await open(directory, constants.O_RDONLY);
      try {
        await directoryHandle.sync();
      } finally {
        await directoryHandle.close();
      }
      const verification = await this.verify(hash);
      if (!verification.ok)
        throw new Error(`Artifact publication failed verification: ${hash}`);
      return { bytes: bytes.length, hash, path };
    } finally {
      await file?.close();
      await unlinkIfExists(temporary);
    }
  }

  async capacity(requiredBytes = 0): Promise<StorageCapacity> {
    await this.#temporaryCleanup;
    if (!Number.isSafeInteger(requiredBytes) || requiredBytes < 0) {
      throw new Error("Required artifact bytes must be a nonnegative integer");
    }
    const filesystem = await statfs(this.#root);
    const availableBytes = filesystem.bavail * filesystem.bsize;
    const writable =
      Number.isSafeInteger(availableBytes) &&
      availableBytes >= this.#minimumFreeBytes + requiredBytes;
    this.#capacitySnapshot = {
      availableBytes,
      minimumFreeBytes: this.#minimumFreeBytes,
      writable,
    };
    return this.#capacitySnapshot;
  }

  async assertWritableCapacity(requiredBytes = 0): Promise<StorageCapacity> {
    const capacity = await this.capacity(requiredBytes);
    if (!capacity.writable) {
      throw new DiskPressureError(
        capacity.availableBytes,
        capacity.minimumFreeBytes,
      );
    }
    return capacity;
  }

  async read(hash: string): Promise<Buffer> {
    await this.#temporaryCleanup;
    const path = this.path(hash);
    let bytes: Buffer;
    try {
      bytes = await readFile(path);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
      throw new Error(`Artifact is missing or corrupt: ${hash}`);
    }
    if (sha256(bytes) !== hash)
      throw new Error(`Artifact is missing or corrupt: ${hash}`);
    return bytes;
  }

  path(hash: string): string {
    requireHash(hash);
    return join(this.#root, hash.slice(0, 2), hash.slice(2));
  }

  async verify(hash: string): Promise<ArtifactVerification> {
    await this.#temporaryCleanup;
    const path = this.path(hash);
    if (!(await pathExists(path)))
      return { actualHash: null, expectedHash: hash, ok: false, path };
    let actualHash: null | string;
    try {
      actualHash = await hashFile(path);
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        actualHash = null;
      } else {
        throw error;
      }
    }
    return { actualHash, expectedHash: hash, ok: actualHash === hash, path };
  }

  async verifyAll(): Promise<readonly ArtifactVerification[]> {
    await this.#temporaryCleanup;
    if (!(await pathExists(this.#root))) return [];
    const results: ArtifactVerification[] = [];
    const prefixEntries = await readdir(this.#root, { withFileTypes: true });
    for (const prefixEntry of prefixEntries.toSorted((left, right) =>
      left.name.localeCompare(right.name),
    )) {
      const prefix = prefixEntry.name;
      if (!prefixEntry.isDirectory() || !/^[a-f\d]{2}$/.test(prefix)) continue;
      const directory = join(this.#root, prefix);
      // eslint-disable-next-line no-await-in-loop -- Prefix directories are traversed in stable order to keep verification bounded and deterministic.
      const suffixEntries = await readdir(directory, { withFileTypes: true });
      for (const suffixEntry of suffixEntries.toSorted((left, right) =>
        left.name.localeCompare(right.name),
      )) {
        const suffix = suffixEntry.name;
        if (!suffixEntry.isFile()) continue;
        if (!/^[a-f\d]{62}$/.test(suffix)) continue;
        // eslint-disable-next-line no-await-in-loop -- Verification is intentionally serialized to bound file descriptors and memory.
        results.push(await this.verify(prefix + suffix));
      }
    }
    return results;
  }
}

/** Removes only old, unpublished CAS staging files. They are never ledger-
 * referenced and their names encode the exact final content hash. */
export async function cleanupStaleArtifactTemporaries(
  root: string,
  minimumAgeMs = DEFAULT_STALE_TEMPORARY_AGE_MS,
  now = Date.now(),
): Promise<TemporaryCleanupReport> {
  let bytesRemoved = 0;
  let filesRemoved = 0;
  let scanned = 0;
  let truncated = false;
  const prefixes = await readCleanupDirectory(root);
  for (const prefix of prefixes) {
    if (!prefix.isDirectory() || !/^[a-f\d]{2}$/.test(prefix.name)) continue;
    const directory = join(root, prefix.name);
    // eslint-disable-next-line no-await-in-loop -- Cleanup walks one bounded CAS prefix at a time to limit filesystem pressure.
    const entries = await readCleanupDirectory(directory);
    for (const entry of entries) {
      if (scanned >= MAXIMUM_TEMPORARIES_SCANNED) {
        truncated = true;
        return { bytesRemoved, filesRemoved, scanned, truncated };
      }
      if (!entry.isFile()) continue;
      if (!/^\.[a-f\d]{64}\.\d+\.[\da-f-]{36}\.tmp$/.test(entry.name)) continue;
      scanned += 1;
      const path = join(directory, entry.name);
      try {
        // eslint-disable-next-line no-await-in-loop -- Each candidate must be restatted immediately before its guarded deletion.
        const metadata = await lstat(path);
        if (!metadata.isFile() || now - metadata.mtimeMs < minimumAgeMs)
          continue;
        // eslint-disable-next-line no-await-in-loop -- Delete only the candidate just validated before advancing the bounded scan.
        await unlink(path);
        bytesRemoved += metadata.size;
        filesRemoved += 1;
      } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
      }
    }
  }
  return { bytesRemoved, filesRemoved, scanned, truncated };
}

async function readCleanupDirectory(path: string) {
  try {
    return await readdir(path, { withFileTypes: true });
  } catch (error) {
    if (errorCode(error) !== "ENOENT") throw error;
    return [];
  }
}

function errorCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !("code" in error)) return undefined;
  return typeof error.code === "string" ? error.code : undefined;
}

async function hashFile(path: string): Promise<string> {
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(VERIFY_BUFFER_BYTES);
  const file = await open(path, constants.O_RDONLY);
  try {
    let bytesRead: number;
    do {
      // eslint-disable-next-line no-await-in-loop -- A single file handle must be hashed sequentially in byte order.
      ({ bytesRead } = await file.read(buffer, 0, buffer.length, null));
      if (bytesRead > 0) hash.update(buffer.subarray(0, bytesRead));
    } while (bytesRead > 0);
  } finally {
    await file.close();
  }
  return hash.digest("hex");
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch (error) {
    if (errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

async function unlinkIfExists(path: string): Promise<void> {
  try {
    await unlink(path);
  } catch (error) {
    if (errorCode(error) !== "ENOENT") throw error;
  }
}

function requireHash(hash: string): void {
  if (!/^[a-f\d]{64}$/.test(hash))
    throw new Error("Invalid SHA-256 artifact hash");
}
