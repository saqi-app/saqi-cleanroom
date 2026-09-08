import { constants } from "node:fs";
import { lstat, open, opendir } from "node:fs/promises";
import { join } from "node:path";

import { z } from "zod";

import { parseLegacySolIntent } from "./sol-legacy-intent.js";
import {
  LegacyArtifactReconciliationSchema,
  LegacyContinuationObservationSchema,
  LegacyFractionalSessionSchema,
  LegacySessionTimestampSchema,
} from "./sol-legacy-observation-schema.js";
import {
  LegacySolSessionSchema,
  LegacySolTerminalSchema,
  LegacySolTurnSchema,
  type SolImportedAttempt,
  SolImportedAttemptSchema,
  SolLegacyObservationsSchema,
  type SolLegacyScan,
} from "./sol-operation-import-schema.js";
import { SolReconciliationSchema } from "./sol-operation-observation-schema.js";
import { canonicalJson, sha256 } from "./work-key.js";

const MAX_RECORDS = 100_000;
const MAX_SOURCE_BYTES = 512 * 1024 * 1024;
const MAX_FILE_BYTES = 512 * 1024;
const MAX_OBSERVATION_BYTES = 16 * 1024;
const JsonObjectSchema = z.record(z.string(), z.unknown());
const EventTextSchema = z.string();
const TurnEventSchema = z.object({ type: z.literal("turn.started") });
const LimitsSchema = z.strictObject({
  records: z.int().positive().max(MAX_RECORDS).default(MAX_RECORDS),
  bytes: z.int().positive().max(MAX_SOURCE_BYTES).default(MAX_SOURCE_BYTES),
});
const ThreadSchema = z.object({
  type: z.literal("thread.started"),
  thread_id: z.uuid(),
});
const IndexNameSchema = z.string().regex(/^[a-f\d]{64}\.json$/);
interface LegacyReaderPort {
  scan(): Promise<SolLegacyScan>;
}
interface SourceFile {
  readonly digest: string;
  readonly modifiedAt: number;
  readonly value: unknown;
}

/** Frozen legacy reader: copies original operation keys; never rekeys old model/prompt profiles. */
export class SolOperationLegacyReader implements LegacyReaderPort {
  readonly #root: string;
  readonly #limits: z.infer<typeof LimitsSchema>;
  #bytes = 0;
  readonly #digests: {
    path: string;
    digest: string;
    modifiedAt: null | number;
  }[] = [];

  constructor(
    attemptRoot: string,
    limits: { records?: number; bytes?: number } = {},
  ) {
    this.#root = attemptRoot;
    this.#limits = LimitsSchema.parse(limits);
  }

  async scan(): Promise<SolLegacyScan> {
    this.#bytes = 0;
    this.#digests.length = 0;
    await this.#assertDirectory(this.#root);
    const index = join(this.#root, "operation-index");
    await this.#assertDirectory(index);
    const names = await this.#indexNames(index);
    const records: SolImportedAttempt[] = [];
    let legacyIntentNormalizations = 0;
    const legacyObservations = {
      continuations: 0,
      artifactReconciliations: 0,
      fractionalSessionTimestamps: 0,
    };
    const quarantine = {
      records: 0,
      missingExitCode: 0,
      signaled: 0,
      turnStarted: 0,
    };
    const attempts = new Set<string>();
    for (const name of names.toSorted()) {
      // eslint-disable-next-line no-await-in-loop -- Bounded sequential IO avoids opening the complete attempt inventory at once.
      const record = await this.#record(name).catch((error: unknown) => {
        const code =
          error instanceof Error && error.message.startsWith("SOL_IMPORT_")
            ? error.message
            : "SOL_IMPORT_RECORD_INVALID";
        throw new Error(`${code}:${name}`);
      });
      if (attempts.has(record.attemptId))
        throw new Error("SOL_IMPORT_DUPLICATE_ATTEMPT");
      attempts.add(record.attemptId);
      records.push(record);
      legacyObservations.continuations += Number(
        record.observations.legacyContinuationEvidence !== undefined,
      );
      legacyObservations.artifactReconciliations += Number(
        record.observations.legacyArtifactReconciliationEvidence !== undefined,
      );
      legacyObservations.fractionalSessionTimestamps += Number(
        record.observations.legacySessionTimestamp !== undefined,
      );
      if (record.observations.legacyIntentNormalization !== undefined)
        legacyIntentNormalizations += 1;
      const classification = record.observations.legacyImportQuarantine;
      if (classification !== undefined) {
        quarantine.records += 1;
        quarantine.missingExitCode += Number(
          classification.reasons.missingExitCode,
        );
        quarantine.signaled += Number(classification.reasons.signaled);
        quarantine.turnStarted += Number(classification.reasons.turnStarted);
      }
    }
    const finalNames = await this.#indexNames(index);
    if (
      canonicalJson(names.toSorted()) !== canonicalJson(finalNames.toSorted())
    ) {
      throw new Error("SOL_IMPORT_INDEX_CHANGED");
    }
    return {
      legacyObservations,
      legacyIntentNormalizations,
      quarantine,
      records,
      sourceBytes: this.#bytes,
      sourceDigest: sha256(
        canonicalJson(
          this.#digests.toSorted((a, b) => a.path.localeCompare(b.path)),
        ),
      ),
    };
  }

  async #indexNames(index: string): Promise<string[]> {
    const directory = await opendir(index);
    const names: string[] = [];
    for await (const entry of directory) {
      if (entry.name.includes(".lock"))
        throw new Error("SOL_IMPORT_LEGACY_LOCK_PRESENT");
      if (!entry.isFile()) throw new Error("SOL_IMPORT_UNSAFE_INDEX_ENTRY");
      names.push(IndexNameSchema.parse(entry.name));
      if (names.length > this.#limits.records)
        throw new Error("SOL_IMPORT_RECORD_LIMIT");
    }
    return names;
  }

  async #record(name: string): Promise<SolImportedAttempt> {
    const indexFile = await this.#read(join("operation-index", name));
    if (indexFile === null) throw new Error("SOL_IMPORT_INDEX_DISAPPEARED");
    const parsedIntent = parseLegacySolIntent(
      indexFile.value,
      name.slice(0, -5),
    );
    const intent = parsedIntent.intent;
    await this.#assertDirectory(join(this.#root, intent.attemptId));
    const manifest = await this.#read(join(intent.attemptId, "manifest.json"));
    if (
      manifest === null ||
      canonicalJson(manifest.value) !== canonicalJson(indexFile.value) ||
      canonicalJson(
        parseLegacySolIntent(manifest.value, name.slice(0, -5)).intent,
      ) !== canonicalJson(intent)
    ) {
      throw new Error("SOL_IMPORT_MANIFEST_MISMATCH");
    }
    const input = await this.#read(join(intent.attemptId, "input.json"));
    if (
      input === null ||
      sha256(canonicalJson(JsonObjectSchema.parse(input.value))) !==
        intent.inputHash
    ) {
      throw new Error("SOL_IMPORT_INPUT_HASH_MISMATCH");
    }
    const terminalRaw = await this.#read(
      join(intent.attemptId, "invocation-terminal.json"),
    );
    const terminal =
      terminalRaw === null
        ? null
        : LegacySolTerminalSchema.parse(terminalRaw.value);
    const turnRaw = await this.#read(
      join(intent.attemptId, "turn-started.json"),
    );
    const turn =
      turnRaw === null ? null : LegacySolTurnSchema.parse(turnRaw.value);
    const evidence = await this.#session(intent.attemptId, turn === null);
    const session = evidence.session;
    const turnStartedAt = turn?.observedAt ?? evidence.turnStartedAt;
    if (
      terminal &&
      ["known_success", "known_invalid"].includes(terminal.state) &&
      (terminal.exitCode !== 0 || terminal.signal !== null)
    )
      throw new Error("SOL_IMPORT_SUCCESS_EVIDENCE_INVALID");
    const quarantineRejection =
      terminal?.state === "known_rejection" &&
      (terminal.exitCode === null ||
        terminal.signal !== null ||
        turnStartedAt !== null);
    const observations = await this.#observations(intent.attemptId);
    if (evidence.legacySessionTimestamp !== undefined)
      observations.legacySessionTimestamp = evidence.legacySessionTimestamp;
    if (parsedIntent.provenance !== undefined)
      observations.legacyIntentNormalization = parsedIntent.provenance;
    if (quarantineRejection) {
      observations.legacyImportQuarantine = {
        strategy: "legacy_rejection_quarantine_v1",
        originalState: "known_rejection",
        reasons: {
          missingExitCode: terminal.exitCode === null,
          signaled: terminal.signal !== null,
          turnStarted: turnStartedAt !== null,
        },
      };
    }
    if (Buffer.byteLength(canonicalJson(observations)) > MAX_OBSERVATION_BYTES)
      throw new Error("SOL_IMPORT_OBSERVATION_LIMIT");
    for (const cleanup of [observations.cleanup, observations.cleanupPending]) {
      if (cleanup !== null && cleanup.sessionId !== session?.sessionId) {
        throw new Error("SOL_IMPORT_CLEANUP_SESSION_MISMATCH");
      }
    }
    return SolImportedAttemptSchema.parse({
      ...intent,
      operationKey: name.slice(0, -5),
      createdAt: Math.floor(
        Math.min(
          manifest.modifiedAt,
          terminal?.finishedAt ?? Infinity,
          turnStartedAt ?? Infinity,
          session?.observedAt ?? Infinity,
        ),
      ),
      state:
        terminal?.state === "completed" || quarantineRejection
          ? "unknown"
          : (terminal?.state ?? "intent"),
      exitCode: terminal?.exitCode ?? null,
      signal: terminal?.signal ?? null,
      finishedAt: terminal?.finishedAt ?? null,
      turnStartedAt,
      sessionId: session?.sessionId ?? null,
      sessionObservedAt: session?.observedAt ?? null,
      observations,
    });
  }

  async #observations(attemptId: string) {
    const credential = await this.#read(
      join(attemptId, "credential-observation.json"),
    );
    const cleanup = await this.#read(join(attemptId, "session-cleanup.json"));
    const pending = await this.#read(
      join(attemptId, "session-cleanup-pending.json"),
    );
    const reconciliation = await this.#read(
      join(attemptId, "reconciliation-terminal.json"),
    );
    const currentReconciliation =
      reconciliation === null
        ? null
        : SolReconciliationSchema.safeParse(reconciliation.value);
    const historicalReconciliation =
      reconciliation !== null && currentReconciliation?.success === false
        ? legacyReconciliationEvidence(reconciliation)
        : {};
    const result = SolLegacyObservationsSchema.parse({
      ...historicalReconciliation,
      credential: credential?.value ?? null,
      cleanup: cleanup?.value ?? null,
      cleanupPending: pending?.value ?? null,
      reconciliation: currentReconciliation?.success
        ? currentReconciliation.data
        : null,
    });
    if (Buffer.byteLength(canonicalJson(result)) > MAX_OBSERVATION_BYTES)
      throw new Error("SOL_IMPORT_OBSERVATION_LIMIT");
    return result;
  }

  async #session(attemptId: string, inspectTurn: boolean) {
    const observation = await this.#read(join(attemptId, "codex-session.json"));
    const currentSession =
      observation === null
        ? null
        : LegacySolSessionSchema.safeParse(observation.value);
    const legacySessionTimestamp =
      observation !== null && currentSession?.success === false
        ? LegacySessionTimestampSchema.parse({
            strategy: "legacy_mtime_ms_floor_v1",
            sourceRevision: "19018523d8473a1ea3f124fbe94d833ff0f49a00",
            sourceSha256: observation.digest,
            original: LegacyFractionalSessionSchema.parse(observation.value),
          })
        : undefined;
    let session = currentSession?.success
      ? currentSession.data
      : legacySessionTimestamp === undefined
        ? null
        : {
            sessionId: legacySessionTimestamp.original.sessionId,
            observedAt: Math.floor(legacySessionTimestamp.original.observedAt),
          };
    let turnStartedAt: null | number = null;
    if (session !== null && !inspectTurn)
      return { session, turnStartedAt, legacySessionTimestamp };
    const events = await this.#read(join(attemptId, "events.jsonl"), true);
    if (events === null)
      return { session, turnStartedAt, legacySessionTimestamp };
    for (const line of EventTextSchema.parse(events.value).split("\n")) {
      let raw: unknown;
      try {
        raw = JSON.parse(line);
      } catch {
        continue;
      }
      const thread = ThreadSchema.safeParse(raw);
      if (thread.success && session === null)
        session = {
          sessionId: thread.data.thread_id,
          observedAt: Math.floor(events.modifiedAt),
        };
      if (TurnEventSchema.safeParse(raw).success)
        turnStartedAt = Math.floor(events.modifiedAt);
    }
    return { session, turnStartedAt, legacySessionTimestamp };
  }

  async #assertDirectory(path: string): Promise<void> {
    const stat = await lstat(path);
    if (!stat.isDirectory() || stat.isSymbolicLink())
      throw new Error("SOL_IMPORT_UNSAFE_DIRECTORY");
  }

  async #read(relative: string, text = false): Promise<null | SourceFile> {
    let handle;
    try {
      handle = await open(
        join(this.#root, relative),
        constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
      );
    } catch (error) {
      if (
        error instanceof Error &&
        "code" in error &&
        error.code === "ENOENT"
      ) {
        this.#digests.push({
          path: relative,
          digest: "absent",
          modifiedAt: null,
        });
        return null;
      }
      throw error;
    }
    try {
      const before = await handle.stat();
      const limit = text ? 2 * 1024 * 1024 : MAX_FILE_BYTES;
      if (!before.isFile() || before.size > limit)
        throw new Error("SOL_IMPORT_FILE_LIMIT");
      this.#bytes += before.size;
      if (this.#bytes > this.#limits.bytes)
        throw new Error("SOL_IMPORT_BYTE_LIMIT");
      const bytes = Buffer.alloc(before.size + 1);
      const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
      const after = await handle.stat();
      if (
        bytesRead !== before.size ||
        before.size !== after.size ||
        before.mtimeMs !== after.mtimeMs
      ) {
        throw new Error("SOL_IMPORT_FILE_CHANGED");
      }
      const body = new TextDecoder("utf-8", { fatal: true }).decode(
        bytes.subarray(0, bytesRead),
      );
      const digest = sha256(body);
      this.#digests.push({
        path: relative,
        digest,
        modifiedAt: before.mtimeMs,
      });
      const value: unknown = text ? body : JSON.parse(body);
      return { value, modifiedAt: before.mtimeMs, digest };
    } finally {
      await handle.close();
    }
  }
}

function legacyReconciliationEvidence(source: SourceFile) {
  const artifact = LegacyArtifactReconciliationSchema.safeParse(source.value);
  const provenance = {
    sourceRevision: "19018523d8473a1ea3f124fbe94d833ff0f49a00",
    sourceSha256: source.digest,
  };
  return artifact.success
    ? {
        legacyArtifactReconciliationEvidence: {
          ...provenance,
          strategy: "legacy_artifact_reconciliation_evidence_v1",
          original: artifact.data,
        },
      }
    : {
        legacyContinuationEvidence: {
          ...provenance,
          strategy: "legacy_paid_continuation_evidence_v1",
          original: LegacyContinuationObservationSchema.parse(source.value),
        },
      };
}
