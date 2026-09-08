import { randomUUID } from "node:crypto";
// eslint-disable-next-line @sarj/prefer-node-fs-promises -- Retention synchronously inspects and commits filesystem evidence while holding the paid-attempt reservation.
import {
  closeSync,
  constants,
  type Dirent,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmdirSync,
  statfsSync,
  type Stats,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { lstat, readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { gunzip, gunzipSync, gzip } from "node:zlib";

import { z } from "zod";

import { canonicalJson, sha256 } from "../persistence/work-key.js";

const ATTEMPT_ID =
  /^[\da-f]{8}-[\da-f]{4}-[1-8][\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/i;
const ARCHIVE_NAME = "diagnostics.archive.json.gz";
const EMERGENCY_FREE_BYTES = 16 * 1024 * 1024;
const EMERGENCY_MINIMUM_NET_RECLAIM_BYTES = 1024 * 1024;
const QUARANTINE = "diagnostics.quarantine";
const PURGE_PENDING = "diagnostics.purge.pending";
const DIAGNOSTICS = ["events.jsonl", "stderr.log", "stdout.log"] as const;
const ArchiveSchema = z
  .strictObject({
    attemptId: z.string().regex(ATTEMPT_ID),
    files: z.array(
      z.strictObject({
        bytes: z.int().nonnegative(),
        contentBase64: z.string(),
        name: z.enum(DIAGNOSTICS),
        sha256: z.string().regex(/^[\da-f]{64}$/),
      }),
    ),
    schemaId: z.literal("saqi.sol-attempt-diagnostics-archive"),
    schemaVersion: z.literal(1),
  })
  .superRefine((archive, context) => {
    if (
      new Set(archive.files.map(({ name }) => name)).size !==
      archive.files.length
    ) {
      context.addIssue({
        code: "custom",
        message: "Archive file names must be unique",
      });
    }
  });
type DiagnosticsArchive = z.infer<typeof ArchiveSchema>;
const AttemptManifestSchema = z.record(z.string(), z.unknown());
const gunzipAsync = promisify(gunzip);
const gzipAsync = promisify(gzip);

export interface SolRetentionEligibility {
  readonly eligibleAttemptIds: ReadonlySet<string>;
  readonly nextEligibleAt: null | number;
  readonly protectedAttemptIds: ReadonlySet<string>;
  readonly withAttemptReservation: (
    attemptId: string,
    operation: () => void,
  ) => boolean;
}

export interface SolRetentionPolicy {
  readonly attemptIds?: readonly string[];
  readonly maximumAttempts: number;
  readonly maximumInputBytes: number;
  readonly maximumScannedAttempts?: number;
  readonly minimumFreeBytes: number;
  readonly purgeArchivedDiagnostics?: boolean;
  readonly scanCursor?: null | string;
  readonly scanWindowComplete?: boolean;
}

export interface SolRetentionCandidate {
  readonly archiveBytes: number;
  readonly attemptId: string;
  readonly diagnosticBytes: number;
  readonly files: readonly (typeof DIAGNOSTICS)[number][];
  readonly quarantinedBytes: number;
  readonly reclaimableAfterExplicitPurgeBytes: number;
}

export interface SolRetentionAttemptScan {
  readonly attemptIds: readonly string[];
  readonly cursor: null | string;
  readonly done: boolean;
}

export interface SolRetentionReport {
  readonly applied: boolean;
  readonly archivedAttempts: number;
  readonly candidates: readonly SolRetentionCandidate[];
  readonly projectedArchiveBytes: number;
  readonly quarantinedBytes: number;
  readonly reclaimableAfterExplicitPurgeBytes: number;
  readonly scanComplete: boolean;
  readonly scanCursor: null | string;
  readonly scannedAttempts: number;
  readonly skippedProtectedOrUnclassified: number;
}

/**
 * Archives only diagnostic streams. Inputs, outputs, manifests, result files,
 * operation-index intents and CAS artifacts are never moved or rewritten.
 * Raw diagnostics are moved to a recoverable quarantine after archive
 * verification. Physical deletion normally requires an explicit operator
 * action; below the configured disk reserve, already-archived diagnostics are
 * purged automatically so retention cannot deadlock behind the pressure it is
 * meant to relieve.
 */
// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete retention engine owns filesystem reservations; eligibility and policy are the independent contracts.
export class SolAttemptRetention {
  #attemptIds: null | readonly string[] = null;
  readonly #root: string;

  constructor(attemptRoot: string) {
    this.#root = resolve(attemptRoot);
  }

  async scanAttemptIds(
    cursor: null | string,
    limit: number,
  ): Promise<SolRetentionAttemptScan> {
    if (cursor !== null && !ATTEMPT_ID.test(cursor))
      throw new Error("Invalid retention scan cursor");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
      throw new Error("Invalid retention scan limit");
    if (cursor === null || this.#attemptIds === null) {
      const rootEntries = await readdirOrEmpty(this.#root);
      this.#attemptIds = rootEntries
        .filter((entry) => entry.isDirectory() && ATTEMPT_ID.test(entry.name))
        .map(({ name }) => name)
        .toSorted((left, right) => left.localeCompare(right));
    }
    const entries = this.#attemptIds;
    const firstIndex =
      cursor === null
        ? 0
        : entries.findIndex((attemptId) => attemptId.localeCompare(cursor) > 0);
    if (firstIndex < 0) return { attemptIds: [], cursor: null, done: true };
    const attemptIds = entries.slice(firstIndex, firstIndex + limit);
    const done = firstIndex + attemptIds.length >= entries.length;
    return {
      attemptIds,
      cursor: done ? null : (attemptIds.at(-1) ?? null),
      done,
    };
  }

  async plan(
    eligibility: SolRetentionEligibility,
    policy: SolRetentionPolicy,
  ): Promise<SolRetentionReport> {
    validatePolicy(policy);
    const candidates: SolRetentionCandidate[] = [];
    let accumulated = 0;
    let scannedAttempts = 0;
    let skippedProtectedOrUnclassified = 0;
    const rootEntries = policy.attemptIds
      ? null
      : await readdirOrEmpty(this.#root);
    const entries =
      policy.attemptIds ??
      (rootEntries ?? [])
        .filter((entry) => entry.isDirectory() && ATTEMPT_ID.test(entry.name))
        .map(({ name }) => name)
        .toSorted((left, right) => left.localeCompare(right));
    let firstIndex = 0;
    if (
      !policy.attemptIds &&
      policy.scanCursor !== null &&
      policy.scanCursor !== undefined
    ) {
      firstIndex = entries.findIndex(
        (attemptId) => attemptId.localeCompare(policy.scanCursor ?? "") > 0,
      );
    }
    const window =
      firstIndex < 0
        ? []
        : entries.slice(
            firstIndex,
            firstIndex + (policy.maximumScannedAttempts ?? entries.length),
          );
    const processedAttemptIds: string[] = [];
    for (const attemptId of window) {
      if (candidates.length >= policy.maximumAttempts) break;
      scannedAttempts += 1;
      processedAttemptIds.push(attemptId);
      if (
        eligibility.protectedAttemptIds.has(attemptId) ||
        !eligibility.eligibleAttemptIds.has(attemptId)
      ) {
        skippedProtectedOrUnclassified += 1;
        continue;
      }
      // eslint-disable-next-line no-await-in-loop -- ordered candidates must preserve deterministic byte and attempt limits
      const candidate = await this.#scanCandidate(
        attemptId,
        policy.purgeArchivedDiagnostics ?? false,
      );
      if (!candidate) continue;
      if (candidate.diagnosticBytes > policy.maximumInputBytes) continue;
      if (
        accumulated > 0 &&
        accumulated + candidate.diagnosticBytes > policy.maximumInputBytes
      )
        break;
      accumulated += candidate.diagnosticBytes;
      candidates.push(candidate);
    }
    const quarantinedByteCounts = await mapWithConcurrency(
      processedAttemptIds,
      32,
      (attemptId) => this.#quarantinedBytes(attemptId),
    );
    const quarantinedBytes = quarantinedByteCounts.reduce(
      (sum, bytes) => sum + bytes,
      0,
    );
    const tail = processedAttemptIds.at(-1);
    const scanComplete =
      !tail ||
      (firstIndex + processedAttemptIds.length >= entries.length &&
        (policy.attemptIds === undefined ||
          (policy.scanWindowComplete ?? false)));
    return report(
      false,
      candidates,
      scannedAttempts,
      skippedProtectedOrUnclassified,
      quarantinedBytes,
      scanComplete,
      scanComplete ? null : tail,
    );
  }

  async apply(
    eligibility: SolRetentionEligibility,
    policy: SolRetentionPolicy,
  ): Promise<SolRetentionReport> {
    validatePolicy(policy);
    let emergencyPurge = false;
    if (!policy.purgeArchivedDiagnostics) {
      const filesystem = statfsSync(this.#root);
      const available = filesystem.bavail * filesystem.bsize;
      if (!Number.isSafeInteger(available))
        throw new Error("SOL_RETENTION_DISK_PRESSURE");
      emergencyPurge = available < policy.minimumFreeBytes;
    }
    const purgeArchivedDiagnostics =
      (policy.purgeArchivedDiagnostics ?? false) || emergencyPurge;
    const effectivePolicy = emergencyPurge
      ? { ...policy, purgeArchivedDiagnostics: true }
      : policy;
    let planned = await this.plan(eligibility, effectivePolicy);
    if (emergencyPurge) {
      // Never allocate an archive below reserve. Only verified archives have
      // archiveBytes=0; their raw diagnostic quarantine is redundant and can
      // be removed without weakening durable recovery evidence.
      const emergencyCandidates = planned.candidates.filter(
        ({ archiveBytes, diagnosticBytes }) =>
          archiveBytes === 0 && diagnosticBytes > 0,
      );
      if (
        emergencyCandidates.length === 0 &&
        planned.candidates.some(({ archiveBytes }) => archiveBytes > 0)
      ) {
        throw new Error("SOL_RETENTION_DISK_PRESSURE");
      }
      planned = report(
        false,
        emergencyCandidates,
        planned.scannedAttempts,
        planned.skippedProtectedOrUnclassified,
        planned.quarantinedBytes,
        planned.scanComplete,
        planned.scanCursor,
      );
    }
    if (planned.candidates.length === 0) return { ...planned, applied: true };
    if (!purgeArchivedDiagnostics) {
      const filesystem = statfsSync(this.#root);
      const available = filesystem.bavail * filesystem.bsize;
      if (
        !Number.isSafeInteger(available) ||
        available < policy.minimumFreeBytes ||
        available - policy.minimumFreeBytes < planned.projectedArchiveBytes
      )
        throw new Error("SOL_RETENTION_DISK_PRESSURE");
    }
    let archivedAttempts = 0;
    for (const expected of planned.candidates) {
      const current = this.#candidate(
        expected.attemptId,
        purgeArchivedDiagnostics,
      );
      if (!current || canonicalJson(current) !== canonicalJson(expected))
        throw new Error(`Retention candidate changed: ${expected.attemptId}`);
      const archivePath = join(this.#root, current.attemptId, ARCHIVE_NAME);
      const requiredArchiveBytes = existsSync(archivePath)
        ? 0
        : current.archiveBytes;
      let recoveringBelowReserve = false;
      if (purgeArchivedDiagnostics) {
        const filesystem = statfsSync(this.#root);
        const available = filesystem.bavail * filesystem.bsize;
        if (!Number.isSafeInteger(available))
          throw new Error("SOL_RETENTION_DISK_PRESSURE");
        const hasNormalReserve =
          available >= policy.minimumFreeBytes &&
          available - policy.minimumFreeBytes >= requiredArchiveBytes;
        const canPrepareBelowReserve =
          available >= EMERGENCY_FREE_BYTES &&
          available - EMERGENCY_FREE_BYTES >= requiredArchiveBytes;
        recoveringBelowReserve = !hasNormalReserve;
        if (recoveringBelowReserve && requiredArchiveBytes > 0)
          throw new Error("SOL_RETENTION_DISK_PRESSURE");
        if (!hasNormalReserve && !canPrepareBelowReserve)
          throw new Error("SOL_RETENTION_DISK_PRESSURE");
      }
      // eslint-disable-next-line no-await-in-loop -- bounded candidates must publish serially
      const prepared = await this.#prepareArchive(current);
      try {
        if (recoveringBelowReserve) {
          const preparedBytes = prepared ? lstatSync(prepared).size : 0;
          const filesystem = statfsSync(this.#root);
          const available = filesystem.bavail * filesystem.bsize;
          const postPurgeAvailable = available + current.diagnosticBytes;
          if (
            !Number.isSafeInteger(available) ||
            current.diagnosticBytes <= preparedBytes ||
            !Number.isSafeInteger(postPurgeAvailable) ||
            postPurgeAvailable <
              EMERGENCY_FREE_BYTES + EMERGENCY_MINIMUM_NET_RECLAIM_BYTES
          )
            throw new Error("SOL_RETENTION_DISK_PRESSURE");
        }
        if (
          !eligibility.withAttemptReservation(current.attemptId, () => {
            const finalCandidate = this.#candidate(
              current.attemptId,
              purgeArchivedDiagnostics,
            );
            if (
              !finalCandidate ||
              canonicalJson(finalCandidate) !== canonicalJson(current)
            )
              throw new Error(
                `Retention candidate changed: ${current.attemptId}`,
              );
            this.#publishArchiveAndQuarantine(
              current,
              prepared,
              purgeArchivedDiagnostics,
            );
          })
        )
          throw new Error(
            `SOL_RETENTION_ATTEMPT_NO_LONGER_ELIGIBLE: ${current.attemptId}`,
          );
      } finally {
        if (prepared && existsSync(prepared)) unlinkSync(prepared);
      }
      archivedAttempts += 1;
    }
    const candidateQuarantinedBytes = planned.candidates.reduce(
      (sum, candidate) => sum + candidate.quarantinedBytes,
      0,
    );
    const newlyQuarantinedBytes = planned.candidates.reduce(
      (sum, candidate) =>
        sum + candidate.diagnosticBytes - candidate.quarantinedBytes,
      0,
    );
    const remainingQuarantinedBytes = purgeArchivedDiagnostics
      ? planned.quarantinedBytes - candidateQuarantinedBytes
      : planned.quarantinedBytes + newlyQuarantinedBytes;
    return {
      ...planned,
      applied: true,
      archivedAttempts,
      reclaimableAfterExplicitPurgeBytes: remainingQuarantinedBytes,
      quarantinedBytes: remainingQuarantinedBytes,
    };
  }

  async #prepareArchive(
    candidate: SolRetentionCandidate,
  ): Promise<null | string> {
    const directory = join(this.#root, candidate.attemptId);
    const archivePath = join(directory, ARCHIVE_NAME);
    if (existsSync(archivePath)) {
      verifyArchive(archivePath, candidate.attemptId, candidate.files);
      return null;
    }
    const encoded = await gzipAsync(
      this.#archivePayload(candidate.attemptId, candidate.files),
      { level: 9 },
    );
    if (encoded.length > candidate.archiveBytes)
      throw new Error(`Retention projection exceeded: ${candidate.attemptId}`);
    const temporary = join(
      directory,
      `.${ARCHIVE_NAME}.${String(process.pid)}.${randomUUID()}.tmp`,
    );
    const descriptor = openSync(
      temporary,
      constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY,
      0o600,
    );
    try {
      writeFileSync(descriptor, encoded);
      fsyncSync(descriptor);
    } finally {
      closeSync(descriptor);
    }
    verifyArchive(temporary, candidate.attemptId, candidate.files);
    return temporary;
  }

  #publishArchiveAndQuarantine(
    candidate: SolRetentionCandidate,
    preparedArchive: null | string,
    purgeArchivedDiagnostics: boolean,
  ): void {
    const directory = join(this.#root, candidate.attemptId);
    const archivePath = join(directory, ARCHIVE_NAME);
    if (!existsSync(archivePath)) {
      if (!preparedArchive)
        throw new Error(`Retention archive missing: ${candidate.attemptId}`);
      renameSync(preparedArchive, archivePath);
      fsyncDirectory(directory);
    }
    const archive = verifyArchive(
      archivePath,
      candidate.attemptId,
      candidate.files,
    );
    if (purgeArchivedDiagnostics) {
      this.#verifyPurgeLayout(directory, archive);
    }
    const quarantine = join(directory, QUARANTINE);
    const purgePending = join(directory, PURGE_PENDING);
    if (existsSync(purgePending) && existsSync(quarantine))
      throw new Error(`Retention purge collision: ${candidate.attemptId}`);
    if (!existsSync(purgePending)) {
      if (existsSync(quarantine))
        requirePlainDirectory(quarantine, "Retention quarantine invalid");
      else mkdirSync(quarantine, { mode: 0o700 });
    }
    for (const name of candidate.files) {
      const source = join(directory, name);
      const destination = join(quarantine, name);
      if (existsSync(source)) {
        if (existsSync(purgePending))
          throw new Error(
            `Retention purge already pending: ${candidate.attemptId}`,
          );
        if (existsSync(destination))
          throw new Error(
            `Retention quarantine collision: ${candidate.attemptId}/${name}`,
          );
        renameSync(source, destination);
      }
    }
    if (existsSync(quarantine)) fsyncDirectory(quarantine);
    fsyncDirectory(directory);
    if (purgeArchivedDiagnostics) {
      this.#verifyPurgeLayout(directory, archive);
      if (!existsSync(purgePending)) {
        renameSync(quarantine, purgePending);
        fsyncDirectory(directory);
      }
      for (const name of candidate.files) {
        const path = join(purgePending, name);
        if (!existsSync(path)) continue;
        const expected = archive.files.find((file) => file.name === name);
        if (!expected)
          throw new Error(`Retention archive file mismatch: ${name}`);
        requirePurgeableFile(path, `Retention purge entry invalid: ${name}`);
        verifyDiagnosticMatchesArchive(path, expected, archive.attemptId);
        unlinkSync(path);
        fsyncDirectory(purgePending);
      }
      rmdirSync(purgePending);
      fsyncDirectory(directory);
    }
  }

  async #quarantinedBytes(attemptId: string): Promise<number> {
    const sizes = await Promise.all(
      [QUARANTINE, PURGE_PENDING].flatMap((container) =>
        DIAGNOSTICS.map(async (name) => {
          const metadata = await metadataOrNull(
            join(this.#root, attemptId, container, name),
          );
          return metadata?.isFile() === true ? metadata.size : 0;
        }),
      ),
    );
    return sizes.reduce((sum, size) => sum + size, 0);
  }

  async #scanCandidate(
    attemptId: string,
    purgeArchivedDiagnostics: boolean,
  ): Promise<null | SolRetentionCandidate> {
    const directory = join(this.#root, attemptId);
    await requirePlainDirectoryAsync(
      directory,
      "Retention attempt directory invalid",
    );
    const manifestPath = join(directory, "manifest.json");
    const manifestMetadata = await metadataOrNull(manifestPath);
    if (!manifestMetadata) return null;
    if (!manifestMetadata.isFile())
      throw new Error("Retention manifest invalid");
    const manifest = AttemptManifestSchema.parse(
      JSON.parse(await readFile(manifestPath, "utf8")),
    );
    if (manifest["attemptId"] !== attemptId) return null;
    const rootFileMetadata = await Promise.all(
      DIAGNOSTICS.map(async (name) => ({
        metadata: await metadataOrNull(join(directory, name)),
        name,
      })),
    );
    const rootFiles = rootFileMetadata
      .filter(({ metadata }) => metadata?.isFile() === true)
      .map(({ name }) => name);
    const archivePath = join(directory, ARCHIVE_NAME);
    const archive = (await metadataOrNull(archivePath))
      ? await verifyArchiveAsync(archivePath, attemptId)
      : null;
    if (purgeArchivedDiagnostics && archive) {
      await this.#scanVerifyPurgeLayout(directory, archive);
    }
    const diagnosticPaths = archive
      ? await Promise.all(
          archive.files.map(async ({ name }) => ({
            name,
            path: await this.#scanDiagnosticPath(directory, name),
          })),
        )
      : await Promise.all(
          DIAGNOSTICS.map(async (name) => ({
            name,
            path: await this.#scanDiagnosticPath(directory, name),
          })),
        );
    const files =
      archive?.files.map(({ name }) => name) ??
      diagnosticPaths
        .filter(({ path }) => path !== null)
        .map(({ name }) => name);
    if (files.length === 0) return null;
    if (rootFiles.length === 0 && archive && !purgeArchivedDiagnostics)
      return null;
    if (
      purgeArchivedDiagnostics &&
      archive &&
      !(await metadataOrNull(join(directory, QUARANTINE))) &&
      !(await metadataOrNull(join(directory, PURGE_PENDING))) &&
      rootFiles.length === 0
    )
      return null;
    const diagnosticSizes = await Promise.all(
      diagnosticPaths.map(async ({ path }) => {
        if (!path) return 0;
        const metadata = await metadataOrNull(path);
        return metadata?.size ?? 0;
      }),
    );
    const diagnosticBytes = diagnosticSizes.reduce(
      (sum, size) => sum + size,
      0,
    );
    const quarantinedBytes = await this.#quarantinedBytes(attemptId);
    return {
      archiveBytes: archive
        ? 0
        : projectedArchiveBytes(diagnosticBytes, files.length),
      attemptId,
      diagnosticBytes,
      files,
      quarantinedBytes,
      reclaimableAfterExplicitPurgeBytes: diagnosticBytes,
    };
  }

  async #scanDiagnosticPath(
    directory: string,
    name: (typeof DIAGNOSTICS)[number],
  ): Promise<null | string> {
    const paths = [
      join(directory, name),
      join(directory, QUARANTINE, name),
      join(directory, PURGE_PENDING, name),
    ];
    const inspectedPaths = await Promise.all(
      paths.map(async (path) => ({
        metadata: await metadataOrNull(path),
        path,
      })),
    );
    const existing = inspectedPaths.filter(({ metadata }) => metadata !== null);
    if (existing.length > 1)
      throw new Error(`Retention diagnostic collision: ${name}`);
    const [match] = existing;
    if (!match) return null;
    if (!match.metadata?.isFile())
      throw new Error(`Retention diagnostic invalid: ${name}`);
    return match.path;
  }

  async #scanVerifyPurgeLayout(
    directory: string,
    archive: DiagnosticsArchive,
  ): Promise<void> {
    const archived = new Map<string, DiagnosticsArchive["files"][number]>(
      archive.files.map((file) => [file.name, file]),
    );
    for (const containerName of [QUARANTINE, PURGE_PENDING] as const) {
      const container = join(directory, containerName);
      // eslint-disable-next-line no-await-in-loop -- each safety check must finish before proceeding to the next purge container
      const metadata = await metadataOrNull(container);
      if (!metadata) continue;
      if (!metadata.isDirectory())
        throw new Error("Retention purge directory invalid");
      // eslint-disable-next-line no-await-in-loop -- each safety check must finish before proceeding to the next purge container
      const entries = await readdir(container, { withFileTypes: true });
      for (const entry of entries) {
        const expected = archived.get(entry.name);
        if (!expected || !entry.isFile())
          throw new Error(`Retention purge entry invalid: ${entry.name}`);
        const path = join(container, entry.name);
        // eslint-disable-next-line no-await-in-loop -- safety verification is deliberately sequential and fail-closed
        await requirePurgeableFileAsync(
          path,
          `Retention purge entry invalid: ${entry.name}`,
        );
        // eslint-disable-next-line no-await-in-loop -- safety verification is deliberately sequential and fail-closed
        await verifyDiagnosticMatchesArchiveAsync(
          path,
          expected,
          archive.attemptId,
        );
      }
    }
    for (const name of DIAGNOSTICS) {
      const path = join(directory, name);
      // eslint-disable-next-line no-await-in-loop -- safety verification is deliberately sequential and fail-closed
      if (!(await metadataOrNull(path))) continue;
      // eslint-disable-next-line no-await-in-loop -- safety verification is deliberately sequential and fail-closed
      await requirePurgeableFileAsync(
        path,
        `Retention diagnostic invalid: ${name}`,
      );
      const expected = archived.get(name);
      if (!expected)
        throw new Error(`Retention archive file mismatch: ${name}`);
      // eslint-disable-next-line no-await-in-loop -- safety verification is deliberately sequential and fail-closed
      await verifyDiagnosticMatchesArchiveAsync(
        path,
        expected,
        archive.attemptId,
      );
    }
  }

  #candidate(
    attemptId: string,
    purgeArchivedDiagnostics: boolean,
  ): null | SolRetentionCandidate {
    const directory = join(this.#root, attemptId);
    requirePlainDirectory(directory, "Retention attempt directory invalid");
    const manifestPath = join(directory, "manifest.json");
    if (!existsSync(manifestPath)) return null;
    requirePlainFile(manifestPath, "Retention manifest invalid");
    const manifest = AttemptManifestSchema.parse(
      JSON.parse(readFileSync(manifestPath, "utf8")),
    );
    if (manifest["attemptId"] !== attemptId) return null;
    const rootFiles = DIAGNOSTICS.filter((name) => {
      const path = join(directory, name);
      return existsSync(path) && lstatSync(path).isFile();
    });
    const archivePath = join(directory, ARCHIVE_NAME);
    const archive = existsSync(archivePath)
      ? verifyArchive(archivePath, attemptId)
      : null;
    if (purgeArchivedDiagnostics && archive) {
      this.#verifyPurgeLayout(directory, archive);
    }
    const files =
      archive?.files.map(({ name }) => name) ??
      DIAGNOSTICS.filter((name) => this.#diagnosticPath(directory, name));
    if (files.length === 0) return null;
    if (rootFiles.length === 0 && archive && !purgeArchivedDiagnostics) {
      return null;
    }
    if (
      purgeArchivedDiagnostics &&
      archive &&
      !existsSync(join(directory, QUARANTINE)) &&
      !existsSync(join(directory, PURGE_PENDING)) &&
      rootFiles.length === 0
    ) {
      return null;
    }
    const diagnosticBytes = files.reduce((sum, name) => {
      const path = this.#diagnosticPath(directory, name);
      return sum + (path ? lstatSync(path).size : 0);
    }, 0);
    const quarantinedBytes = files.reduce(
      (sum, name) =>
        sum +
        [QUARANTINE, PURGE_PENDING].reduce((containerSum, container) => {
          const path = join(directory, container, name);
          return (
            containerSum +
            (existsSync(path) && lstatSync(path).isFile()
              ? lstatSync(path).size
              : 0)
          );
        }, 0),
      0,
    );
    const archiveBytes = archive
      ? 0
      : projectedArchiveBytes(diagnosticBytes, files.length);
    return {
      archiveBytes,
      attemptId,
      diagnosticBytes,
      files,
      quarantinedBytes,
      reclaimableAfterExplicitPurgeBytes: diagnosticBytes,
    };
  }

  #diagnosticPath(
    directory: string,
    name: (typeof DIAGNOSTICS)[number],
  ): null | string {
    const rootPath = join(directory, name);
    const quarantinePath = join(directory, QUARANTINE, name);
    const purgePendingPath = join(directory, PURGE_PENDING, name);
    const matches = [rootPath, quarantinePath, purgePendingPath].filter(
      (path) => existsSync(path),
    );
    if (matches.length > 1)
      throw new Error(`Retention diagnostic collision: ${name}`);
    const [path] = matches;
    if (!path) return null;
    requirePlainFile(path, `Retention diagnostic invalid: ${name}`);
    return path;
  }

  #verifyPurgeLayout(directory: string, archive: DiagnosticsArchive): void {
    const archived = new Map<string, DiagnosticsArchive["files"][number]>(
      archive.files.map((file) => [file.name, file]),
    );
    for (const containerName of [QUARANTINE, PURGE_PENDING] as const) {
      const container = join(directory, containerName);
      if (!existsSync(container)) continue;
      requirePlainDirectory(container, "Retention purge directory invalid");
      for (const entry of readdirSync(container, { withFileTypes: true })) {
        const expected = archived.get(entry.name);
        if (!expected || !entry.isFile())
          throw new Error(`Retention purge entry invalid: ${entry.name}`);
        const path = join(container, entry.name);
        requirePurgeableFile(
          path,
          `Retention purge entry invalid: ${entry.name}`,
        );
        verifyDiagnosticMatchesArchive(path, expected, archive.attemptId);
      }
    }
    for (const name of DIAGNOSTICS) {
      const path = join(directory, name);
      if (!existsSync(path)) continue;
      requirePurgeableFile(path, `Retention diagnostic invalid: ${name}`);
      const expected = archived.get(name);
      if (!expected)
        throw new Error(`Retention archive file mismatch: ${name}`);
      verifyDiagnosticMatchesArchive(path, expected, archive.attemptId);
    }
  }

  #archivePayload(
    attemptId: string,
    files: readonly (typeof DIAGNOSTICS)[number][],
  ): Buffer {
    const directory = join(this.#root, attemptId);
    const contents = files.toSorted().map((name) => {
      const path = this.#diagnosticPath(directory, name);
      if (!path)
        throw new Error(`Diagnostic disappeared: ${attemptId}/${name}`);
      const content = readFileSync(path);
      return {
        bytes: content.length,
        contentBase64: content.toString("base64"),
        name,
        sha256: sha256(content),
      };
    });
    return Buffer.from(
      canonicalJson({
        attemptId,
        files: contents,
        schemaId: "saqi.sol-attempt-diagnostics-archive",
        schemaVersion: 1,
      }),
    );
  }
}

function projectedArchiveBytes(
  diagnosticBytes: number,
  fileCount: number,
): number {
  const base64Bytes = 4 * Math.ceil(diagnosticBytes / 3);
  const uncompressedBytes = base64Bytes + 512 + fileCount * 512;
  return (
    uncompressedBytes +
    Math.ceil(uncompressedBytes / 8) +
    Math.ceil(uncompressedBytes / 64) +
    64
  );
}

function report(
  applied: boolean,
  candidates: readonly SolRetentionCandidate[],
  scannedAttempts: number,
  skippedProtectedOrUnclassified: number,
  quarantinedBytes: number,
  scanComplete: boolean,
  scanCursor: null | string,
): SolRetentionReport {
  return {
    applied,
    archivedAttempts: 0,
    candidates,
    projectedArchiveBytes: candidates.reduce(
      (sum, item) => sum + item.archiveBytes,
      0,
    ),
    quarantinedBytes,
    reclaimableAfterExplicitPurgeBytes:
      quarantinedBytes +
      candidates.reduce(
        (sum, item) =>
          sum + item.reclaimableAfterExplicitPurgeBytes - item.quarantinedBytes,
        0,
      ),
    scanComplete,
    scanCursor,
    scannedAttempts,
    skippedProtectedOrUnclassified,
  };
}

function verifyArchive(
  path: string,
  attemptId: string,
  expectedFiles?: readonly string[],
): DiagnosticsArchive {
  requirePurgeableFile(path, "Retention archive invalid");
  const input: unknown = JSON.parse(
    gunzipSync(readFileSync(path)).toString("utf8"),
  );
  const archive = ArchiveSchema.parse(input);
  if (archive.attemptId !== attemptId)
    throw new Error("Retention archive attempt mismatch");
  if (
    expectedFiles &&
    canonicalJson(archive.files.map(({ name }) => name)) !==
      canonicalJson(expectedFiles.toSorted())
  )
    throw new Error("Retention archive file mismatch");
  for (const file of archive.files) {
    const content = Buffer.from(file.contentBase64, "base64");
    if (content.length !== file.bytes || sha256(content) !== file.sha256)
      throw new Error(
        `Retention archive content mismatch: ${attemptId}/${file.name}`,
      );
  }
  return archive;
}

async function verifyArchiveAsync(
  path: string,
  attemptId: string,
  expectedFiles?: readonly string[],
): Promise<DiagnosticsArchive> {
  await requirePurgeableFileAsync(path, "Retention archive invalid");
  const compressed = await readFile(path);
  const decompressed = await gunzipAsync(compressed);
  const input: unknown = JSON.parse(decompressed.toString("utf8"));
  const archive = ArchiveSchema.parse(input);
  if (archive.attemptId !== attemptId)
    throw new Error("Retention archive attempt mismatch");
  if (
    expectedFiles &&
    canonicalJson(archive.files.map(({ name }) => name)) !==
      canonicalJson(expectedFiles.toSorted())
  )
    throw new Error("Retention archive file mismatch");
  for (const file of archive.files) {
    const content = Buffer.from(file.contentBase64, "base64");
    if (content.length !== file.bytes || sha256(content) !== file.sha256)
      throw new Error(
        `Retention archive content mismatch: ${attemptId}/${file.name}`,
      );
  }
  return archive;
}

function verifyDiagnosticMatchesArchive(
  path: string,
  expected: DiagnosticsArchive["files"][number],
  attemptId: string,
): void {
  const content = readFileSync(path);
  if (content.length !== expected.bytes || sha256(content) !== expected.sha256)
    throw new Error(
      `Retention diagnostic changed: ${attemptId}/${expected.name}`,
    );
}

async function verifyDiagnosticMatchesArchiveAsync(
  path: string,
  expected: DiagnosticsArchive["files"][number],
  attemptId: string,
): Promise<void> {
  const content = await readFile(path);
  if (content.length !== expected.bytes || sha256(content) !== expected.sha256)
    throw new Error(
      `Retention diagnostic changed: ${attemptId}/${expected.name}`,
    );
}

function requirePlainDirectory(path: string, message: string): void {
  if (!existsSync(path) || !lstatSync(path).isDirectory())
    throw new Error(message);
}

async function requirePlainDirectoryAsync(
  path: string,
  message: string,
): Promise<void> {
  const metadata = await metadataOrNull(path);
  if (metadata?.isDirectory() !== true) throw new Error(message);
}

function requirePlainFile(path: string, message: string): void {
  if (!existsSync(path) || !lstatSync(path).isFile()) throw new Error(message);
}

function requirePurgeableFile(path: string, message: string): void {
  requirePlainFile(path, message);
  const metadata = lstatSync(path);
  if (metadata.nlink !== 1 || metadata.blocks * 512 < metadata.size)
    throw new Error(message);
}

async function requirePurgeableFileAsync(
  path: string,
  message: string,
): Promise<void> {
  const metadata = await metadataOrNull(path);
  if (
    !metadata?.isFile() ||
    metadata.nlink !== 1 ||
    metadata.blocks * 512 < metadata.size
  )
    throw new Error(message);
}

async function metadataOrNull(path: string): Promise<null | Stats> {
  try {
    return await lstat(path);
  } catch (error) {
    if (isEnoent(error)) return null;
    throw error;
  }
}

async function readdirOrEmpty(path: string): Promise<Dirent[]> {
  try {
    return await readdir(path, { withFileTypes: true });
  } catch (error) {
    if (isEnoent(error)) return [];
    throw error;
  }
}

function isEnoent(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    Reflect.get(error, "code") === "ENOENT"
  );
}

async function mapWithConcurrency<T, U>(
  values: readonly T[],
  concurrency: number,
  operation: (value: T) => Promise<U>,
): Promise<U[]> {
  const output: U[] = [];
  let cursor = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      while (cursor < values.length) {
        const index = cursor;
        cursor += 1;
        const value = values[index];
        if (value === undefined)
          throw new Error("Retention scan cursor exceeded input");
        // eslint-disable-next-line no-await-in-loop -- bounded workers intentionally claim one filesystem scan at a time
        output[index] = await operation(value);
      }
    }),
  );
  return output;
}

function fsyncDirectory(path: string): void {
  const descriptor = openSync(path, constants.O_RDONLY);
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

function validatePolicy(policy: SolRetentionPolicy): void {
  if (
    policy.attemptIds &&
    (policy.attemptIds.length > 1_000 ||
      policy.attemptIds.some((attemptId) => !ATTEMPT_ID.test(attemptId)))
  ) {
    throw new Error("Invalid retention policy: attemptIds");
  }
  for (const [name, value] of Object.entries({
    maximumAttempts: policy.maximumAttempts,
    maximumInputBytes: policy.maximumInputBytes,
    ...(policy.maximumScannedAttempts === undefined
      ? {}
      : { maximumScannedAttempts: policy.maximumScannedAttempts }),
    minimumFreeBytes: policy.minimumFreeBytes,
  }))
    if (
      !Number.isSafeInteger(value) ||
      value < (name === "minimumFreeBytes" ? 0 : 1)
    )
      throw new Error(`Invalid retention policy: ${name}`);
  if (
    policy.scanCursor !== undefined &&
    policy.scanCursor !== null &&
    !ATTEMPT_ID.test(policy.scanCursor)
  ) {
    throw new Error("Invalid retention policy: scanCursor");
  }
}
