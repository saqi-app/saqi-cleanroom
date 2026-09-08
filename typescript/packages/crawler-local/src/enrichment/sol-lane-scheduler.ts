import { hash, randomUUID, timingSafeEqual } from "node:crypto";
import { constants } from "node:fs";
import { open } from "node:fs/promises";

import { json, z } from "zod";

import { inputHash } from "../persistence/work-key.js";
import {
  type ProviderCredentialEpochPort,
  type ProviderCredentialEpochSnapshot,
  ProviderCredentialEpochTracker,
} from "./provider-credential-epoch.js";
import type { ProviderCredentialSnapshot } from "./provider-credential-generation.js";

// Deliberately bounded: the supported LaunchAgent raises its descriptor limit
// to 4096, and each provider remains bounded independently from aggregate
// host admission in the operation config.
const MAXIMUM_CONCURRENCY = 256;
export const CURRENT_SCHEDULER_STATE_SCHEMA_VERSION = 11;
const DEFAULT_QUOTA_WAIT_MS = 24 * 60 * 60_000;
const DEFAULT_PROVIDER_WAIT_MS = 15 * 60_000;
const AUTH_PROVIDER_PROBE_INTERVAL_MS = 60_000;
const QUOTA_PROBE_INTERVAL_MS = 15 * 60_000;
const MAXIMUM_PROVIDER_WAIT_MS = 6 * 60 * 60_000;
const MAXIMUM_RESET_HORIZON_MS = 366 * 24 * 60 * 60_000;
const AMBIGUOUS_OUTCOME_WINDOW_MS = 30 * 60_000;
const AMBIGUOUS_OUTCOME_REDUCTION_THRESHOLD = 2;
// A recovery probe owns a complete enrichment claim: one generation and as
// many as two sequential reviews. Its durable lease must outlive every phase
// that can keep those invocations alive, otherwise a second
// scheduler could admit duplicate paid work while the first process is still
// terminating. These bounds mirror the runner's production limits: auth status
// may take five minutes, each of three invocations thirty minutes, and graceful
// process termination ten seconds. Ten additional minutes cover host scheduling,
// persistence, and shutdown jitter. Keep this invariant in the scheduler so a
// crash only becomes recoverable after the original paid operation is certain
// to have terminated.
const MAXIMUM_AUTH_STATUS_DURATION_MS = 5 * 60_000;
const MAXIMUM_PROVIDER_INVOCATION_DURATION_MS = 30 * 60_000;
const MAXIMUM_PROVIDER_INVOCATIONS_PER_CLAIM = 3;
const MAXIMUM_PROCESS_TERMINATION_DURATION_MS = 10_000;
const RECOVERY_PROBE_COORDINATION_MARGIN_MS = 10 * 60_000;
const RECOVERY_PROBE_LEASE_MS =
  MAXIMUM_AUTH_STATUS_DURATION_MS +
  MAXIMUM_PROVIDER_INVOCATION_DURATION_MS *
    MAXIMUM_PROVIDER_INVOCATIONS_PER_CLAIM +
  MAXIMUM_PROCESS_TERMINATION_DURATION_MS +
  RECOVERY_PROBE_COORDINATION_MARGIN_MS;
const AUTH_PROVIDER_ERROR_CODES: ReadonlySet<string> = new Set([
  "CODEX_CHATGPT_AUTH_REQUIRED",
  "CODEX_OAUTH_TOKEN_INVALIDATED",
  "CODEX_OAUTH_TOKEN_REVOKED",
  "ENRICHMENT_AUTH_REQUIRED",
  "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
]);
const NETWORK_PROVIDER_ERROR_CODE = "ENRICHMENT_NETWORK_UNAVAILABLE";

const SchedulerDigestSchema = z.string().regex(/^[a-f\d]{64}$/);
const LegacySchedulerDigestsSchema = z.array(SchedulerDigestSchema).max(10);
const MinimumLaunchIntervalSchema = z.number().int().min(0).max(60_000);
const PromotionSuccessesSchema = z.number().int().min(2).max(10_000);
const SchedulerStateKeySchema = z.string().trim().min(1).max(128);
const ProviderErrorCodeSchema = z.string().min(1).max(128);
const SchedulerIdentitySchema = json();
const ConcurrencySchema = z.number().int().min(1).max(MAXIMUM_CONCURRENCY);

const RecoveryCauseSchema = z.enum([
  "account_switch",
  "authentication",
  "network",
  "provider",
  "quota",
  "rate_limit",
]);
const RecoveryLeaseSchema = z
  .object({
    accountGeneration: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullable(),
    cause: RecoveryCauseSchema,
    dueAt: z.int().nonnegative(),
    leaseId: z.uuid().nullable(),
    leaseUntil: z.int().nonnegative(),
    ownerId: z.uuid().nullable(),
    pressureEpoch: z.int().nonnegative(),
  })
  .strict();

const StateSchema = z
  .object({
    ambiguousOutcomeCount: z.int().nonnegative().default(0),
    ambiguousWindowStartedAt: z.int().nonnegative().default(0),
    configDigest: z.string().regex(/^[a-f\d]{64}$/),
    consecutiveErrors: z.int().nonnegative(),
    consecutiveRateLimits: z.int().nonnegative(),
    errorDampenerUntil: z.int().nonnegative().default(0),
    ewmaLatencyMs: z.number().nonnegative().nullable(),
    providerErrorCode: z.string().min(1).max(128).nullable().default(null),
    providerCredentialGeneration: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullable()
      .default(null),
    providerUntil: z.int().nonnegative().default(0),
    observedCredentialMaterialEpoch: z.int().nonnegative().default(0),
    quotaProbeAt: z.int().nonnegative().default(0),
    consecutiveProviderFailures: z.int().nonnegative().default(0),
    quotaUntil: z.int().nonnegative(),
    rateLimitedUntil: z.int().nonnegative(),
    recoveryLease: RecoveryLeaseSchema.nullable().default(null),
    pressureEpoch: z.int().nonnegative().default(0),
    samples: z.int().nonnegative(),
    schemaVersion: z.literal([
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      CURRENT_SCHEDULER_STATE_SCHEMA_VERSION,
    ]),
    selectedConcurrency: z.int().min(1).max(MAXIMUM_CONCURRENCY),
    successStreak: z.int().nonnegative(),
    updatedAt: z.int().nonnegative(),
  })
  .strict();

type PersistedState = z.infer<typeof StateSchema>;

/** Read-only schema probe for bounded operator diagnostics. */
export function schedulerStateSchemaVersion(serialized: string): null | number {
  try {
    return StateSchema.parse(JSON.parse(serialized)).schemaVersion;
  } catch {
    return null;
  }
}

export interface SolSchedulerGates {
  readonly circuitOpen: boolean;
  readonly diskWritable: boolean;
  readonly paused: boolean;
  readonly quotaWaitUntil: null | number;
}

export type SolTaskOutcome =
  | {
      readonly errorCode: string;
      readonly kind: "provider_wait";
      readonly retryAt?: number;
    }
  | { readonly kind: "ambiguous_outcome" }
  | { readonly kind: "budget_exhausted" }
  | { readonly kind: "error"; readonly retryAt?: number }
  | { readonly kind: "idle" }
  | {
      readonly kind: "quota_wait";
      readonly resetText?: string;
      readonly retryAt?: number;
    }
  | { readonly kind: "rate_limited"; readonly retryAt?: number }
  | { readonly kind: "success" }
  | { readonly kind: "task_failure" };

export type SolSchedulerBlockReason =
  | "at_capacity"
  | "circuit_open"
  | "disk_pressure"
  | "error_dampener"
  | "launch_pacing"
  | "paused"
  | "provider_wait"
  | "quota_wait"
  | "rate_limited";

export interface SolSchedulerSnapshot {
  readonly active: number;
  readonly activeCurrentAccountEpoch: number;
  readonly activePreviousAccountEpoch: number;
  readonly activeUnattributed: number;
  readonly availableSlots: number;
  readonly blockReason: null | SolSchedulerBlockReason;
  readonly ceiling: number;
  readonly consecutiveErrors: number;
  readonly consecutiveProviderFailures: number;
  readonly consecutiveRateLimits: number;
  readonly credentialAccountEpoch: number;
  readonly credentialChange: "account_switch" | "material_refresh" | "none";
  readonly credentialChangedAt: null | number;
  readonly credentialLastVerifiedAt: null | number;
  readonly credentialMaterialEpoch: number;
  readonly credentialObservation:
    "absent" | "observed" | "transient_unavailable";
  readonly ewmaLatencyMs: null | number;
  readonly nextWakeAt: null | number;
  readonly providerErrorCode: null | string;
  readonly providerUntil: number;
  readonly quotaProbeAt: number;
  readonly quotaUntil: number;
  readonly recoveryCause: null | z.infer<typeof RecoveryCauseSchema>;
  readonly recoveryLeaseUntil: number;
  readonly samples: number;
  readonly selectedConcurrency: number;
  readonly successStreak: number;
}

export interface SolTaskPermit {
  complete(
    outcome: SolTaskOutcome,
    completedAt?: number,
  ): Promise<{ readonly accepted: boolean; readonly quotaCleared: boolean }>;
  readonly purpose: "normal" | "quota_probe";
  readonly recoveryLease: null | SolRecoveryLeaseOwnership;
}

export interface SolRecoveryLeaseOwnership {
  readonly leaseId: string;
  readonly pressureEpoch: number;
}

export interface SolLaneSchedulerOptions {
  readonly ceiling: number;
  readonly configDigest: string;
  readonly credentialGeneration?: () => null | Promise<null | string> | string;
  readonly credentialSnapshot?: () =>
    Promise<ProviderCredentialSnapshot> | ProviderCredentialSnapshot;
  readonly fixedConcurrency?: boolean;
  readonly initialConcurrency?: number;
  readonly legacyConfigDigests?: readonly string[];
  /** Read-only import candidate; never runtime authority. */
  readonly legacyCurrentStatePath?: string;
  readonly legacyStateKey?: string;
  readonly legacyStatePath?: string;
  readonly minimumLaunchIntervalMs?: number;
  readonly now?: () => number;
  readonly promotionSuccesses?: number;
  readonly requireCurrentStateStoreAuthority?: boolean;
  readonly stateKey: string;
  readonly stateStore: SolSchedulerStateStore;
}

export interface SolSchedulerStateStore {
  loadSchedulerState(
    stateKey: string,
  ):
    | { readonly digest: string; readonly serialized: string }
    | null
    | Promise<{ readonly digest: string; readonly serialized: string } | null>;
  saveSchedulerState(
    stateKey: string,
    serialized: string,
    digest: string,
    expectedDigest: null | string,
    now?: number,
  ): boolean | Promise<boolean>;
}

export interface SolLaneScheduler {
  acquire(gates: SolSchedulerGates): Promise<null | SolTaskPermit>;
  releaseOwnedIdleAccountSwitchLease(
    ownership: SolRecoveryLeaseOwnership,
  ): Promise<boolean>;
  snapshot(
    gates: SolSchedulerGates,
    now?: number,
  ): Promise<SolSchedulerSnapshot>;
}

export class QuotaAwareSolLaneScheduler implements SolLaneScheduler {
  readonly #ceiling: number;
  readonly #configDigest: string;
  readonly #credentialGeneration: () => null | Promise<null | string> | string;
  readonly #credentialEpochTracker: null | ProviderCredentialEpochPort;
  readonly #fixedConcurrency: boolean;
  readonly #initialConcurrency: number;
  readonly #legacyConfigDigests: ReadonlySet<string>;
  readonly #legacyStatePath: null | string;
  readonly #legacyStateKey: null | string;
  readonly #now: () => number;
  readonly #minimumLaunchIntervalMs: number;
  readonly #promotionSuccesses: number;
  readonly #requireCurrentStateStoreAuthority: boolean;
  readonly #stateKey: string;
  readonly #legacyCurrentStatePath: null | string;
  readonly #stateStore: SolSchedulerStateStore;
  #active = 0;
  readonly #activeAccountEpochs = new Map<number, number>();
  #congestionEpoch = 0;
  #credentialEpoch: ProviderCredentialEpochSnapshot = {
    accountEpoch: 0,
    change: "none",
    changedAt: null,
    lastVerifiedAt: null,
    materialEpoch: 0,
    observation: "transient_unavailable",
  };
  #mutationTail: Promise<void> = Promise.resolve();
  #persistedDigest: null | string = null;
  #nextAdmissionAt = 0;
  readonly #ownerId = randomUUID();
  readonly #ready: Promise<void>;
  #state: PersistedState;

  constructor(options: SolLaneSchedulerOptions) {
    this.#ceiling = concurrency(options.ceiling);
    this.#configDigest = SchedulerDigestSchema.parse(options.configDigest);
    this.#credentialGeneration = options.credentialGeneration ?? (() => null);
    this.#credentialEpochTracker = options.credentialSnapshot
      ? new ProviderCredentialEpochTracker({
          credentialSnapshot: options.credentialSnapshot,
          now: options.now ?? Date.now,
          stateKey: `${options.stateKey}:credential-epochs-v1`,
          stateStore: options.stateStore,
        })
      : null;
    this.#fixedConcurrency = options.fixedConcurrency ?? false;
    this.#initialConcurrency = Math.min(
      this.#ceiling,
      concurrency(options.initialConcurrency ?? 2),
    );
    this.#legacyConfigDigests = new Set(
      LegacySchedulerDigestsSchema.parse(options.legacyConfigDigests ?? []),
    );
    this.#legacyStatePath = options.legacyStatePath ?? null;
    this.#legacyStateKey = options.legacyStateKey ?? null;
    this.#now = options.now ?? Date.now;
    this.#minimumLaunchIntervalMs = MinimumLaunchIntervalSchema.parse(
      options.minimumLaunchIntervalMs ?? 0,
    );
    this.#promotionSuccesses = PromotionSuccessesSchema.parse(
      options.promotionSuccesses ?? 8,
    );
    this.#requireCurrentStateStoreAuthority =
      options.requireCurrentStateStoreAuthority ?? false;
    this.#stateKey = SchedulerStateKeySchema.parse(options.stateKey);
    this.#legacyCurrentStatePath = options.legacyCurrentStatePath ?? null;
    this.#stateStore = options.stateStore;
    this.#state = this.#fresh();
    this.#ready = this.#initialize();
  }

  async acquire(gates: SolSchedulerGates): Promise<null | SolTaskPermit> {
    return this.#serializeMutation(() => this.#acquire(gates));
  }

  async releaseOwnedIdleAccountSwitchLease(
    ownership: SolRecoveryLeaseOwnership,
  ): Promise<boolean> {
    return this.#serializeMutation(async () => {
      await this.#ready;
      await this.#refreshAuthoritativeState();
      const recovery = this.#state.recoveryLease;
      if (
        this.#active !== 0 ||
        recovery?.cause !== "account_switch" ||
        recovery.ownerId !== this.#ownerId ||
        recovery.leaseId !== ownership.leaseId ||
        recovery.pressureEpoch !== ownership.pressureEpoch
      )
        return false;
      this.#state = {
        ...this.#state,
        recoveryLease: null,
        updatedAt: this.#now(),
      };
      return this.#persist();
    });
  }

  async snapshot(
    gates: SolSchedulerGates,
    now = this.#now(),
  ): Promise<SolSchedulerSnapshot> {
    return this.#serializeMutation(async () => {
      await this.#ready;
      await this.#refreshAuthoritativeState();
      await this.#observeCredentialTransition();
      await this.#recoverAfterCredentialChange();
      return this.#snapshot(gates, now);
    });
  }

  async #acquire(gates: SolSchedulerGates): Promise<null | SolTaskPermit> {
    await this.#ready;
    await this.#refreshAuthoritativeState();
    const credentialEpoch = await this.#observeCredentialTransition();
    const credentialGeneration = await this.#recoverAfterCredentialChange();
    const startedAt = this.#now();
    const congestionEpoch = this.#congestionEpoch;
    const pressureEpoch = this.#state.pressureEpoch;
    const quotaProbe = this.#recoveryProbeEligible(startedAt);
    const blockReason = this.#snapshot(gates, startedAt).blockReason;
    if (
      blockReason !== null &&
      !(
        quotaProbe &&
        blockReason === "at_capacity" &&
        this.#active < this.#ceiling
      )
    )
      return null;
    if (quotaProbe) {
      const leaseId = randomUUID();
      this.#state = {
        ...this.#state,
        providerCredentialGeneration: credentialGeneration,
        recoveryLease: this.#state.recoveryLease
          ? {
              ...this.#state.recoveryLease,
              leaseId,
              leaseUntil: startedAt + RECOVERY_PROBE_LEASE_MS,
              ownerId: this.#ownerId,
            }
          : null,
        updatedAt: startedAt,
      };
      if (!(await this.#persist())) return null;
    }
    const recoveryLeaseId = quotaProbe
      ? (this.#state.recoveryLease?.leaseId ?? null)
      : null;
    const recoveryLease =
      quotaProbe && recoveryLeaseId !== null
        ? {
            leaseId: recoveryLeaseId,
            pressureEpoch,
          }
        : null;
    this.#active += 1;
    if (credentialEpoch.accountEpoch > 0)
      this.#activeAccountEpochs.set(
        credentialEpoch.accountEpoch,
        (this.#activeAccountEpochs.get(credentialEpoch.accountEpoch) ?? 0) + 1,
      );
    this.#nextAdmissionAt = startedAt + this.#minimumLaunchIntervalMs;
    let completed = false;
    return {
      purpose: quotaProbe ? "quota_probe" : "normal",
      recoveryLease,
      complete: (outcome, completedAt = this.#now()) =>
        this.#serializeMutation(async () => {
          if (completed)
            throw new Error("SOL_SCHEDULER_PERMIT_ALREADY_COMPLETED");
          if (!Number.isSafeInteger(completedAt) || completedAt < startedAt) {
            throw new Error("SOL_SCHEDULER_COMPLETION_TIME_INVALID");
          }
          completed = true;
          this.#active -= 1;
          if (credentialEpoch.accountEpoch > 0) {
            const remaining =
              (this.#activeAccountEpochs.get(credentialEpoch.accountEpoch) ??
                1) - 1;
            if (remaining === 0)
              this.#activeAccountEpochs.delete(credentialEpoch.accountEpoch);
            else
              this.#activeAccountEpochs.set(
                credentialEpoch.accountEpoch,
                remaining,
              );
          }
          await this.#refreshAuthoritativeState();
          await this.#observeCredentialTransition();
          await this.#recoverAfterCredentialChange();
          const staleCredentialEpoch =
            credentialEpoch.accountEpoch !==
              this.#credentialEpoch.accountEpoch ||
            credentialEpoch.materialEpoch !==
              this.#credentialEpoch.materialEpoch;
          const accepted = await this.#record(
            outcome,
            completedAt - startedAt,
            completedAt,
            congestionEpoch,
            credentialGeneration,
            quotaProbe,
            pressureEpoch,
            recoveryLeaseId,
            staleCredentialEpoch,
          );
          if (!staleCredentialEpoch && outcome.kind === "success")
            await this.#credentialEpochTracker?.markVerified(credentialEpoch);
          return {
            accepted,
            quotaCleared:
              accepted &&
              quotaProbe &&
              outcome.kind !== "budget_exhausted" &&
              this.#state.quotaUntil === 0,
          };
        }),
    };
  }

  #snapshot(gates: SolSchedulerGates, now: number): SolSchedulerSnapshot {
    const recoveryPending = this.#state.recoveryLease !== null;
    const selectedConcurrency = recoveryPending
      ? 1
      : this.#fixedConcurrency
        ? this.#ceiling
        : this.#state.selectedConcurrency;
    const externalQuotaUntil = gates.quotaWaitUntil ?? 0;
    const quotaUntil = Math.max(this.#state.quotaUntil, externalQuotaUntil);
    let blockReason: null | SolSchedulerBlockReason = null;
    let nextWakeAt: null | number = null;
    if (gates.paused) blockReason = "paused";
    else if (!gates.diskWritable) blockReason = "disk_pressure";
    else if (gates.circuitOpen) blockReason = "circuit_open";
    else if (recoveryPending && this.#active > 0) {
      blockReason = "at_capacity";
      nextWakeAt = this.#state.recoveryLease?.leaseUntil ?? null;
    } else if (recoveryPending && !this.#recoveryProbeEligible(now)) {
      const recovery = this.#state.recoveryLease;
      blockReason =
        recovery?.cause === "rate_limit"
          ? "rate_limited"
          : recovery?.cause === "network" ||
              recovery?.cause === "provider" ||
              recovery?.cause === "authentication"
            ? "provider_wait"
            : "quota_wait";
      nextWakeAt = recovery
        ? recovery.leaseId === null
          ? recovery.dueAt
          : recovery.leaseUntil
        : null;
    } else if (this.#state.providerUntil > now) {
      blockReason = "provider_wait";
      nextWakeAt = this.#state.providerUntil;
    } else if (quotaUntil > now && !this.#quotaProbeEligible(now)) {
      blockReason = "quota_wait";
      nextWakeAt =
        this.#state.quotaProbeAt > now
          ? Math.min(quotaUntil, this.#state.quotaProbeAt)
          : quotaUntil;
    } else if (this.#state.rateLimitedUntil > now) {
      blockReason = "rate_limited";
      nextWakeAt = this.#state.rateLimitedUntil;
    } else if (this.#state.errorDampenerUntil > now) {
      blockReason = "error_dampener";
      nextWakeAt = this.#state.errorDampenerUntil;
    } else if (this.#nextAdmissionAt > now) {
      blockReason = "launch_pacing";
      nextWakeAt = this.#nextAdmissionAt;
    } else if (this.#active >= selectedConcurrency) {
      blockReason = "at_capacity";
    }
    const credential = this.#credentialEpochSnapshot();
    const activeCurrentAccountEpoch =
      this.#activeAccountEpochs.get(credential.accountEpoch) ?? 0;
    let activePreviousAccountEpoch = 0;
    for (const [epoch, count] of this.#activeAccountEpochs)
      if (epoch !== credential.accountEpoch)
        activePreviousAccountEpoch += count;
    return {
      activeCurrentAccountEpoch,
      activePreviousAccountEpoch,
      activeUnattributed:
        this.#active - activeCurrentAccountEpoch - activePreviousAccountEpoch,
      active: this.#active,
      availableSlots:
        blockReason === null ? selectedConcurrency - this.#active : 0,
      blockReason,
      ceiling: this.#ceiling,
      consecutiveErrors: this.#state.consecutiveErrors,
      consecutiveProviderFailures: this.#state.consecutiveProviderFailures,
      consecutiveRateLimits: this.#state.consecutiveRateLimits,
      ewmaLatencyMs: this.#state.ewmaLatencyMs,
      nextWakeAt,
      providerErrorCode: this.#state.providerErrorCode,
      providerUntil: this.#state.providerUntil,
      quotaProbeAt: this.#state.quotaProbeAt,
      quotaUntil,
      recoveryCause: this.#state.recoveryLease?.cause ?? null,
      recoveryLeaseUntil: this.#state.recoveryLease?.leaseUntil ?? 0,
      samples: this.#state.samples,
      selectedConcurrency,
      successStreak: this.#state.successStreak,
      credentialAccountEpoch: credential.accountEpoch,
      credentialChange: credential.change,
      credentialChangedAt: credential.changedAt,
      credentialLastVerifiedAt: credential.lastVerifiedAt,
      credentialMaterialEpoch: credential.materialEpoch,
      credentialObservation: credential.observation,
    };
  }

  async #serializeMutation<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#mutationTail;
    const { promise, resolve: release } = Promise.withResolvers<undefined>();
    this.#mutationTail = promise;
    await previous;
    try {
      return await operation();
    } finally {
      release(undefined);
    }
  }

  #fresh(selectedConcurrency = this.#initialConcurrency): PersistedState {
    return {
      ambiguousOutcomeCount: 0,
      ambiguousWindowStartedAt: 0,
      configDigest: this.#configDigest,
      consecutiveErrors: 0,
      consecutiveRateLimits: 0,
      errorDampenerUntil: 0,
      ewmaLatencyMs: null,
      providerErrorCode: null,
      providerCredentialGeneration: null,
      providerUntil: 0,
      observedCredentialMaterialEpoch: 0,
      quotaProbeAt: 0,
      consecutiveProviderFailures: 0,
      quotaUntil: 0,
      rateLimitedUntil: 0,
      recoveryLease: null,
      pressureEpoch: 0,
      samples: 0,
      schemaVersion: CURRENT_SCHEDULER_STATE_SCHEMA_VERSION,
      selectedConcurrency,
      successStreak: 0,
      updatedAt: this.#now(),
    };
  }

  async #initialize(): Promise<void> {
    this.#state = await this.#load();
    await this.#persist();
  }

  async #load(): Promise<PersistedState> {
    let parsed: PersistedState;
    let loadedLegacyStore = false;
    let sourcePath: null | string = null;
    let serialized: Buffer | null = null;
    try {
      let stored = await this.#stateStore.loadSchedulerState(this.#stateKey);
      if (stored === null && this.#requireCurrentStateStoreAuthority) {
        throw new Error("SOL_SCHEDULER_CURRENT_AUTHORITY_MISSING");
      }
      if (stored === null && this.#legacyStateKey !== null) {
        stored = await this.#stateStore.loadSchedulerState(
          this.#legacyStateKey,
        );
        loadedLegacyStore = stored !== null;
      }
      if (stored) {
        serialized = Buffer.from(stored.serialized);
        if (!timingSafeDigestEqual(stored.digest, sha256(serialized))) {
          throw new Error("SOL_SCHEDULER_STATE_DIGEST_INVALID");
        }
      } else {
        serialized = await readLegacySchedulerState(
          this.#legacyCurrentStatePath,
        );
        sourcePath = this.#legacyCurrentStatePath;
        if (serialized === null) {
          serialized = await readLegacySchedulerState(this.#legacyStatePath);
          sourcePath = this.#legacyStatePath;
        }
        if (serialized === null) return this.#fresh();
      }
      const input: unknown = JSON.parse(serialized.toString("utf8"));
      parsed = StateSchema.parse(input);
    } catch (error) {
      throw new Error("SOL_SCHEDULER_STATE_INVALID", { cause: error });
    }
    this.#persistedDigest =
      loadedLegacyStore || sourcePath !== null ? null : sha256(serialized);
    if (
      (parsed.configDigest !== this.#configDigest &&
        !this.#legacyConfigDigests.has(parsed.configDigest)) ||
      parsed.schemaVersion < 3
    )
      return this.#fresh();
    const migrationGeneration = await this.#currentCredentialGeneration();
    const migrationNow = this.#now();
    const migrationProbe =
      parsed.schemaVersion < 6 &&
      parsed.providerErrorCode !== null &&
      AUTH_PROVIDER_ERROR_CODES.has(parsed.providerErrorCode) &&
      parsed.providerUntil > migrationNow &&
      migrationGeneration !== null;
    const recoverNeutralClassification =
      parsed.schemaVersion < 7 &&
      parsed.providerUntil <= migrationNow &&
      parsed.quotaUntil <= migrationNow &&
      parsed.rateLimitedUntil <= migrationNow;
    const migrationQuotaProbe =
      parsed.schemaVersion < 8 && parsed.quotaUntil > migrationNow;
    const recoverStaleAuthenticationWait =
      parsed.providerErrorCode !== null &&
      AUTH_PROVIDER_ERROR_CODES.has(parsed.providerErrorCode) &&
      parsed.providerUntil > migrationNow + AUTH_PROVIDER_PROBE_INTERVAL_MS;
    const recoverRepeatedProcessFailures =
      parsed.schemaVersion >= 8 &&
      parsed.consecutiveErrors >= Math.max(4, parsed.selectedConcurrency);
    const providerCredentialGeneration =
      migrationProbe || migrationQuotaProbe
        ? migrationGeneration
        : parsed.providerCredentialGeneration;
    let selectedConcurrency = Math.min(
      parsed.selectedConcurrency,
      this.#ceiling,
    );
    if (migrationProbe || migrationQuotaProbe || recoverRepeatedProcessFailures)
      selectedConcurrency = 1;
    else if (recoverNeutralClassification) {
      selectedConcurrency = Math.max(
        selectedConcurrency,
        Math.min(2, this.#ceiling),
      );
    }
    let errorDampenerUntil = parsed.errorDampenerUntil;
    if (recoverNeutralClassification) errorDampenerUntil = 0;
    else if (recoverRepeatedProcessFailures) {
      errorDampenerUntil = Math.max(errorDampenerUntil, migrationNow + 1_000);
    }
    const migratedPressureEpoch =
      parsed.recoveryLease === null &&
      (parsed.quotaUntil > migrationNow ||
        parsed.rateLimitedUntil > migrationNow ||
        parsed.providerUntil > migrationNow)
        ? parsed.pressureEpoch + 1
        : parsed.pressureEpoch;
    const recoveryLease =
      parsed.recoveryLease ??
      (parsed.quotaUntil > migrationNow
        ? this.#waitingRecovery(
            "quota",
            migrationQuotaProbe
              ? migrationNow
              : Math.min(
                  parsed.quotaUntil,
                  migrationNow + QUOTA_PROBE_INTERVAL_MS,
                ),
            providerCredentialGeneration,
            migratedPressureEpoch,
          )
        : parsed.rateLimitedUntil > migrationNow
          ? this.#waitingRecovery(
              "rate_limit",
              parsed.rateLimitedUntil,
              providerCredentialGeneration,
              migratedPressureEpoch,
            )
          : parsed.providerUntil > migrationNow
            ? this.#waitingRecovery(
                parsed.providerErrorCode === NETWORK_PROVIDER_ERROR_CODE
                  ? "network"
                  : parsed.providerErrorCode !== null &&
                      AUTH_PROVIDER_ERROR_CODES.has(parsed.providerErrorCode)
                    ? "authentication"
                    : "provider",
                recoverStaleAuthenticationWait
                  ? migrationNow
                  : parsed.providerUntil,
                providerCredentialGeneration,
                migratedPressureEpoch,
              )
            : null);
    return {
      ...parsed,
      configDigest: this.#configDigest,
      consecutiveErrors: recoverNeutralClassification
        ? 0
        : parsed.consecutiveErrors,
      consecutiveProviderFailures: migrationProbe
        ? 0
        : parsed.consecutiveProviderFailures,
      errorDampenerUntil,
      providerCredentialGeneration,
      providerUntil: migrationProbe
        ? 0
        : recoverStaleAuthenticationWait
          ? migrationNow
          : parsed.providerUntil,
      quotaProbeAt: migrationQuotaProbe ? migrationNow : parsed.quotaProbeAt,
      recoveryLease,
      pressureEpoch: migratedPressureEpoch,
      schemaVersion: CURRENT_SCHEDULER_STATE_SCHEMA_VERSION,
      selectedConcurrency,
    };
  }

  async #persist(): Promise<boolean> {
    const serialized = `${JSON.stringify(this.#state)}\n`;
    const digest = sha256(Buffer.from(serialized));
    const saved = await this.#stateStore.saveSchedulerState(
      this.#stateKey,
      serialized,
      digest,
      this.#persistedDigest,
      this.#now(),
    );
    if (saved) {
      this.#persistedDigest = digest;
      return true;
    }
    const winner = await this.#stateStore.loadSchedulerState(this.#stateKey);
    if (!winner) throw new Error("SOL_SCHEDULER_STATE_WRITE_CONFLICT");
    const winnerDigest = sha256(Buffer.from(winner.serialized));
    if (!timingSafeDigestEqual(winner.digest, winnerDigest)) {
      throw new Error("SOL_SCHEDULER_STATE_DIGEST_INVALID");
    }
    this.#state = StateSchema.parse(JSON.parse(winner.serialized));
    this.#persistedDigest = winner.digest;
    return false;
  }

  async #record(
    outcome: SolTaskOutcome,
    latencyMs: number,
    now: number,
    admissionEpoch: number,
    credentialGeneration: null | string,
    quotaProbe: boolean,
    pressureEpoch: number,
    recoveryLeaseId: null | string,
    staleCredentialEpoch: boolean,
  ): Promise<boolean> {
    if (outcome.kind === "budget_exhausted" || outcome.kind === "idle") {
      // An account switch is only a fence until the exact recovery owner has
      // reached the coordinator. Idle work and an internal budget fence make
      // no provider-capacity claim, but future claims still run the shared
      // pre-claim login verification. Clear this one cause atomically with
      // completion so a crash cannot strand the replacement for a full paid
      // operation lease. All other recovery causes remain authoritative.
      if (!quotaProbe || recoveryLeaseId === null) return true;
      await this.#refreshAuthoritativeState();
      const recovery = this.#state.recoveryLease;
      if (
        recovery?.cause !== "account_switch" ||
        recovery.ownerId !== this.#ownerId ||
        recovery.leaseId !== recoveryLeaseId ||
        recovery.pressureEpoch !== pressureEpoch
      )
        return true;
      this.#state = {
        ...this.#state,
        recoveryLease: null,
        updatedAt: now,
      };
      return this.#persist();
    }
    await this.#refreshAuthoritativeState();
    let requiresDurableWrite = true;
    const ewmaLatencyMs =
      this.#state.ewmaLatencyMs === null
        ? latencyMs
        : this.#state.ewmaLatencyMs * 0.8 + latencyMs * 0.2;
    const common = {
      ...this.#state,
      ewmaLatencyMs,
      samples: this.#state.samples + 1,
      updatedAt: now,
    };
    // A credential/account change can schedule a new recovery generation
    // while an old probe is still finishing. No outcome from that stale
    // probe may clear or replace the newer provider-pressure state.
    const authoritativeRecovery = this.#state.recoveryLease;
    if (
      quotaProbe &&
      (admissionEpoch !== this.#congestionEpoch ||
        authoritativeRecovery === null ||
        recoveryLeaseId === null ||
        authoritativeRecovery.leaseId !== recoveryLeaseId ||
        authoritativeRecovery.ownerId !== this.#ownerId ||
        authoritativeRecovery.pressureEpoch !== pressureEpoch)
    ) {
      this.#state = { ...common, successStreak: 0 };
      return false;
    }
    const staleAccountGeneration =
      credentialGeneration !== null &&
      this.#state.providerCredentialGeneration !== null &&
      !equalOpaqueGeneration(
        credentialGeneration,
        this.#state.providerCredentialGeneration,
      );
    const staleAccountPressure =
      !quotaProbe &&
      staleAccountGeneration &&
      (outcome.kind === "quota_wait" ||
        outcome.kind === "rate_limited" ||
        (outcome.kind === "provider_wait" &&
          AUTH_PROVIDER_ERROR_CODES.has(outcome.errorCode)));
    if (
      staleAccountPressure ||
      (staleCredentialEpoch &&
        outcome.kind === "provider_wait" &&
        AUTH_PROVIDER_ERROR_CODES.has(outcome.errorCode))
    ) {
      this.#state = { ...common, successStreak: 0 };
      return false;
    }
    const accountIndependentNetworkFailure =
      outcome.kind === "provider_wait" &&
      outcome.errorCode === NETWORK_PROVIDER_ERROR_CODE;
    if (
      !quotaProbe &&
      pressureEpoch !== this.#state.pressureEpoch &&
      !accountIndependentNetworkFailure
    ) {
      this.#state = { ...common, successStreak: 0 };
      return false;
    }

    switch (outcome.kind) {
      case "ambiguous_outcome": {
        const insideWindow =
          this.#state.ambiguousWindowStartedAt > 0 &&
          now - this.#state.ambiguousWindowStartedAt <=
            AMBIGUOUS_OUTCOME_WINDOW_MS;
        const count = insideWindow ? this.#state.ambiguousOutcomeCount + 1 : 1;
        const reduce = count >= AMBIGUOUS_OUTCOME_REDUCTION_THRESHOLD;
        if (reduce) this.#congestionEpoch += 1;
        this.#state = {
          ...common,
          ambiguousOutcomeCount: reduce ? 0 : count,
          ambiguousWindowStartedAt: reduce
            ? 0
            : insideWindow
              ? this.#state.ambiguousWindowStartedAt
              : now,
          errorDampenerUntil: reduce
            ? Math.max(this.#state.errorDampenerUntil, now + 30_000)
            : this.#state.errorDampenerUntil,
          selectedConcurrency: reduce
            ? Math.max(1, Math.floor(this.#state.selectedConcurrency / 2))
            : this.#state.selectedConcurrency,
          recoveryLease:
            quotaProbe && this.#state.recoveryLease
              ? this.#waitingRecovery(
                  this.#state.recoveryLease.cause,
                  now + 30_000,
                  this.#state.providerCredentialGeneration,
                  this.#state.pressureEpoch,
                )
              : this.#state.recoveryLease,
          successStreak: reduce ? 0 : this.#state.successStreak,
        };
        // Ambiguous outcomes are combined across process restarts, so even the
        // first signal is durable. Ordinary successes and task failures are
        // the high-volume telemetry paths that remain write-coalesced.
        requiresDurableWrite = true;
        break;
      }
      case "success": {
        // A completion admitted before provider pressure cannot prove the new,
        // reduced setting is healthy and must never undo congestion control.
        if (admissionEpoch !== this.#congestionEpoch) {
          this.#state = { ...common, successStreak: 0 };
          return false;
        }
        const successStreak = this.#state.successStreak + 1;
        const clearsProviderFailure = this.#state.providerErrorCode !== null;
        const warmingUp = this.#state.selectedConcurrency < this.#ceiling;
        const promote = successStreak >= this.#promotionSuccesses && warmingUp;
        this.#state = {
          ...common,
          consecutiveErrors: 0,
          consecutiveProviderFailures: 0,
          consecutiveRateLimits: 0,
          providerErrorCode: null,
          providerUntil: 0,
          providerCredentialGeneration: credentialGeneration,
          quotaProbeAt: 0,
          quotaUntil: 0,
          recoveryLease: quotaProbe ? null : this.#state.recoveryLease,
          selectedConcurrency: promote
            ? Math.min(this.#ceiling, this.#state.selectedConcurrency * 2)
            : this.#state.selectedConcurrency,
          successStreak: promote ? 0 : successStreak,
        };
        // Below the ceiling, accepted successes are durable promotion evidence:
        // restarts must not repeatedly reset the ramp-up. At the ceiling,
        // ordinary high-volume completion telemetry remains write-coalesced.
        // Clearing a durable quota gate is itself a safety-state transition.
        // Persist the single probe outcome even when it does not promote.
        // An expired provider wait can admit ordinary work without a quota
        // probe. Its successful recovery must not reload the old error after
        // restart merely because concurrency has not been promoted yet.
        requiresDurableWrite = warmingUp || quotaProbe || clearsProviderFailure;
        break;
      }
      case "quota_wait": {
        this.#congestionEpoch += 1;
        const parsedReset = outcome.resetText
          ? parseQuotaResetTimestamp(outcome.resetText, now)
          : null;
        const quotaUntil = Math.max(
          this.#state.quotaUntil,
          boundedFuture(
            outcome.retryAt ?? parsedReset ?? now + DEFAULT_QUOTA_WAIT_MS,
            now,
          ),
        );
        this.#state = {
          ...common,
          consecutiveErrors: 0,
          consecutiveRateLimits: 0,
          providerCredentialGeneration: credentialGeneration,
          quotaProbeAt: Math.min(quotaUntil, now + QUOTA_PROBE_INTERVAL_MS),
          quotaUntil,
          pressureEpoch: this.#state.pressureEpoch + 1,
          recoveryLease: this.#waitingRecovery(
            "quota",
            Math.min(quotaUntil, now + QUOTA_PROBE_INTERVAL_MS),
            credentialGeneration,
            this.#state.pressureEpoch + 1,
          ),
          selectedConcurrency: 1,
          successStreak: 0,
        };

        break;
      }
      case "rate_limited": {
        this.#congestionEpoch += 1;
        const streak = this.#state.consecutiveRateLimits + 1;
        const rateLimitedUntil = Math.max(
          this.#state.rateLimitedUntil,
          boundedFuture(
            outcome.retryAt ??
              now + Math.min(15 * 60_000, 30_000 * 2 ** streak),
            now,
          ),
        );
        this.#state = {
          ...common,
          consecutiveErrors: 0,
          consecutiveRateLimits: streak,
          providerCredentialGeneration: credentialGeneration,
          rateLimitedUntil,
          pressureEpoch: this.#state.pressureEpoch + 1,
          recoveryLease: this.#waitingRecovery(
            "rate_limit",
            rateLimitedUntil,
            credentialGeneration,
            this.#state.pressureEpoch + 1,
          ),
          quotaProbeAt: quotaProbe ? 0 : this.#state.quotaProbeAt,
          quotaUntil: quotaProbe ? 0 : this.#state.quotaUntil,
          selectedConcurrency: Math.max(
            1,
            Math.floor(this.#state.selectedConcurrency / 2),
          ),
          successStreak: 0,
        };

        break;
      }
      case "provider_wait": {
        const currentCredentialGeneration =
          await this.#currentCredentialGeneration();
        if (
          credentialGeneration !== null &&
          currentCredentialGeneration !== null &&
          !equalOpaqueGeneration(
            credentialGeneration,
            currentCredentialGeneration,
          ) &&
          AUTH_PROVIDER_ERROR_CODES.has(outcome.errorCode)
        ) {
          this.#state = { ...common, successStreak: 0 };
          await this.#persist();
          return false;
        }
        if (
          admissionEpoch !== this.#congestionEpoch &&
          this.#state.providerErrorCode !== null &&
          this.#state.providerUntil > now
        ) {
          const previousProviderErrorCode = this.#state.providerErrorCode;
          const providerErrorCode = moreSpecificProviderError(
            previousProviderErrorCode,
            outcome.errorCode,
          );
          this.#state = {
            ...common,
            providerErrorCode,
            successStreak: 0,
          };
          // The first pressure transition was already synchronously durable.
          // Persist a follower only when it adds a more specific diagnosis.
          if (providerErrorCode !== previousProviderErrorCode)
            return this.#persist();
          return true;
        }
        this.#congestionEpoch += 1;
        if (outcome.errorCode === NETWORK_PROVIDER_ERROR_CODE) {
          const providerUntil = Math.max(
            this.#state.providerUntil,
            boundedFuture(
              outcome.retryAt ?? now + DEFAULT_PROVIDER_WAIT_MS,
              now,
            ),
          );
          // Losing connectivity says nothing about provider capacity. Keep the
          // learned concurrency so recovery resumes at the proven level, but
          // globally close admission until the coordinator's jittered network
          // probe is due. Work attempts remain untouched by that coordinator.
          this.#state = {
            ...common,
            consecutiveErrors: 0,
            consecutiveRateLimits: 0,
            providerErrorCode: NETWORK_PROVIDER_ERROR_CODE,
            providerCredentialGeneration: staleAccountGeneration
              ? this.#state.providerCredentialGeneration
              : credentialGeneration,
            providerUntil,
            pressureEpoch: this.#state.pressureEpoch + 1,
            recoveryLease: this.#waitingRecovery(
              "network",
              providerUntil,
              staleAccountGeneration
                ? this.#state.providerCredentialGeneration
                : credentialGeneration,
              this.#state.pressureEpoch + 1,
            ),
            quotaProbeAt: quotaProbe ? 0 : this.#state.quotaProbeAt,
            quotaUntil: quotaProbe ? 0 : this.#state.quotaUntil,
            successStreak: 0,
          };
          break;
        }
        const failures = this.#state.consecutiveProviderFailures + 1;
        // Authentication recovery is checked by a free status command before
        // any work is claimed. Probe it at a stable cadence so a keychain or
        // browser-login change cannot remain hidden behind a multi-hour
        // provider backoff. Non-auth provider outages retain exponential
        // backoff to avoid hammering a broken executable/service.
        const authenticationFailure = AUTH_PROVIDER_ERROR_CODES.has(
          outcome.errorCode,
        );
        const exponentialRetryAt = authenticationFailure
          ? now + AUTH_PROVIDER_PROBE_INTERVAL_MS
          : now +
            Math.min(
              MAXIMUM_PROVIDER_WAIT_MS,
              DEFAULT_PROVIDER_WAIT_MS * 2 ** Math.min(failures - 1, 5),
            );
        const providerUntil = authenticationFailure
          ? exponentialRetryAt
          : Math.max(
              this.#state.providerUntil,
              boundedFuture(outcome.retryAt ?? exponentialRetryAt, now),
              exponentialRetryAt,
            );
        this.#state = {
          ...common,
          consecutiveErrors: 0,
          consecutiveProviderFailures: failures,
          consecutiveRateLimits: 0,
          providerErrorCode: ProviderErrorCodeSchema.parse(outcome.errorCode),
          providerCredentialGeneration: staleAccountGeneration
            ? this.#state.providerCredentialGeneration
            : credentialGeneration,
          providerUntil,
          pressureEpoch: this.#state.pressureEpoch + 1,
          recoveryLease: this.#waitingRecovery(
            authenticationFailure ? "authentication" : "provider",
            providerUntil,
            staleAccountGeneration
              ? this.#state.providerCredentialGeneration
              : credentialGeneration,
            this.#state.pressureEpoch + 1,
          ),
          quotaProbeAt: quotaProbe ? 0 : this.#state.quotaProbeAt,
          quotaUntil: quotaProbe ? 0 : this.#state.quotaUntil,
          selectedConcurrency: 1,
          successStreak: 0,
        };
        break;
      }
      case "task_failure": {
        // A content/schema/quality failure is scoped to one task. The
        // provider completed the invocation, so it neither proves capacity
        // health nor supplies evidence of capacity pressure. In particular,
        // preserve accepted-success evidence gathered around noisy inputs so
        // adaptive promotion cannot be starved by unrelated task quality.
        this.#state = {
          ...common,
          consecutiveErrors: 0,
          errorDampenerUntil: 0,
          providerCredentialGeneration: quotaProbe
            ? credentialGeneration
            : this.#state.providerCredentialGeneration,
          quotaProbeAt: quotaProbe ? 0 : this.#state.quotaProbeAt,
          quotaUntil: quotaProbe ? 0 : this.#state.quotaUntil,
          recoveryLease: quotaProbe ? null : this.#state.recoveryLease,
        };
        // A task-level failure still proves that the provider completed the
        // real-work quota probe, so the cleared quota fence must survive a
        // process restart. Ordinary task failures remain write-coalesced.
        requiresDurableWrite = quotaProbe;
        break;
      }
      case "error": {
        const errors = this.#state.consecutiveErrors + 1;
        const providerWideFailure =
          errors >= Math.max(4, this.#state.selectedConcurrency);
        if (providerWideFailure) this.#congestionEpoch += 1;
        this.#state = {
          ...common,
          consecutiveErrors: errors,
          consecutiveRateLimits: 0,
          // The durable ledger backs off the failed claim. Keep only a short
          // admission dampener here so unrelated work continues without a hot
          // retry loop or a provider-wide exponential stall.
          errorDampenerUntil: Math.max(
            this.#state.errorDampenerUntil,
            now + (providerWideFailure ? 30_000 : 1_000),
          ),
          // Isolated task/process failures are retried by the durable ledger;
          // they are not evidence that provider concurrency is too high.
          selectedConcurrency: providerWideFailure
            ? 1
            : this.#state.selectedConcurrency,
          recoveryLease:
            quotaProbe && this.#state.recoveryLease
              ? this.#waitingRecovery(
                  this.#state.recoveryLease.cause,
                  now + 30_000,
                  this.#state.providerCredentialGeneration,
                  this.#state.pressureEpoch,
                )
              : this.#state.recoveryLease,
          successStreak: 0,
        };
        requiresDurableWrite = providerWideFailure || quotaProbe;
      }
    }
    return requiresDurableWrite ? this.#persist() : true;
  }

  async #currentCredentialGeneration(): Promise<null | string> {
    try {
      const generation = await this.#credentialGeneration();
      return generation === null
        ? null
        : SchedulerDigestSchema.parse(generation);
    } catch {
      return null;
    }
  }

  #credentialEpochSnapshot(): ProviderCredentialEpochSnapshot {
    return this.#credentialEpoch;
  }

  async #observeCredentialTransition(): Promise<ProviderCredentialEpochSnapshot> {
    if (!this.#credentialEpochTracker) return this.#credentialEpoch;
    const observed = await this.#credentialEpochTracker.observe();
    this.#credentialEpoch = observed;
    if (
      observed.observation !== "observed" ||
      observed.materialEpoch <= this.#state.observedCredentialMaterialEpoch
    )
      return observed;
    // Consume each material epoch once, durably. A refreshed token can still
    // fail authentication; subsequent snapshots must retain that new wait.
    this.#state = {
      ...this.#state,
      observedCredentialMaterialEpoch: observed.materialEpoch,
    };
    if (
      observed.change === "material_refresh" &&
      this.#state.providerErrorCode !== null &&
      AUTH_PROVIDER_ERROR_CODES.has(this.#state.providerErrorCode)
    ) {
      this.#congestionEpoch += 1;
      this.#state = {
        ...this.#state,
        consecutiveProviderFailures: 0,
        providerErrorCode: null,
        providerUntil: 0,
        pressureEpoch: this.#state.pressureEpoch + 1,
        recoveryLease: null,
        updatedAt: this.#now(),
      };
    }
    await this.#persist();
    return observed;
  }

  #quotaProbeEligible(now: number): boolean {
    return this.#recoveryProbeEligible(now);
  }

  #recoveryProbeEligible(now: number): boolean {
    const recovery = this.#state.recoveryLease;
    return (
      recovery !== null &&
      this.#active === 0 &&
      recovery.dueAt <= now &&
      (recovery.leaseId === null || recovery.leaseUntil <= now)
    );
  }

  #waitingRecovery(
    cause: z.infer<typeof RecoveryCauseSchema>,
    dueAt: number,
    accountGeneration: null | string,
    pressureEpoch: number,
  ): PersistedState["recoveryLease"] {
    return {
      accountGeneration,
      cause,
      dueAt,
      leaseId: null,
      leaseUntil: 0,
      ownerId: null,
      pressureEpoch,
    };
  }

  async #refreshAuthoritativeState(): Promise<void> {
    const stored = await this.#stateStore.loadSchedulerState(this.#stateKey);
    if (stored === null)
      throw new Error("SOL_SCHEDULER_CURRENT_AUTHORITY_MISSING");
    if (stored.digest === this.#persistedDigest) return;
    const digest = sha256(Buffer.from(stored.serialized));
    if (!timingSafeDigestEqual(stored.digest, digest))
      throw new Error("SOL_SCHEDULER_STATE_DIGEST_INVALID");
    this.#state = StateSchema.parse(JSON.parse(stored.serialized));
    this.#persistedDigest = stored.digest;
  }

  async #recoverAfterCredentialChange(): Promise<null | string> {
    const generation = await this.#currentCredentialGeneration();
    const previousGeneration = this.#state.providerCredentialGeneration;
    const now = this.#now();
    if (
      generation !== null &&
      previousGeneration !== null &&
      !equalOpaqueGeneration(generation, previousGeneration)
    ) {
      const preserveGlobalProviderWait =
        this.#state.providerErrorCode !== null &&
        !AUTH_PROVIDER_ERROR_CODES.has(this.#state.providerErrorCode) &&
        this.#state.providerUntil > now;
      const pressureEpoch = this.#state.pressureEpoch + 1;
      this.#congestionEpoch += 1;
      this.#state = {
        ...this.#state,
        consecutiveErrors: 0,
        consecutiveProviderFailures: preserveGlobalProviderWait
          ? this.#state.consecutiveProviderFailures
          : 0,
        consecutiveRateLimits: 0,
        errorDampenerUntil: 0,
        providerCredentialGeneration: generation,
        providerErrorCode: preserveGlobalProviderWait
          ? this.#state.providerErrorCode
          : null,
        providerUntil: preserveGlobalProviderWait
          ? this.#state.providerUntil
          : 0,
        pressureEpoch,
        quotaProbeAt: 0,
        quotaUntil: 0,
        rateLimitedUntil: 0,
        recoveryLease: this.#waitingRecovery(
          preserveGlobalProviderWait
            ? this.#state.providerErrorCode === NETWORK_PROVIDER_ERROR_CODE
              ? "network"
              : "provider"
            : "account_switch",
          preserveGlobalProviderWait ? this.#state.providerUntil : now,
          generation,
          pressureEpoch,
        ),
        selectedConcurrency: 1,
        successStreak: 0,
        updatedAt: now,
      };
      await this.#persist();
    }
    return generation;
  }
}

async function readLegacySchedulerState(
  path: null | string,
): Promise<Buffer | null> {
  if (path === null) return null;
  let handle;
  try {
    handle = await open(
      path,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT")
      return null;
    throw error;
  }
  try {
    const maximumBytes = 64 * 1024;
    const metadata = await handle.stat();
    if (!metadata.isFile() || metadata.size > maximumBytes)
      throw new Error("SOL_SCHEDULER_LEGACY_STATE_UNSAFE");
    const bytes = Buffer.alloc(maximumBytes + 1);
    const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
    if (bytesRead > maximumBytes)
      throw new Error("SOL_SCHEDULER_LEGACY_STATE_UNSAFE");
    return bytes.subarray(0, bytesRead);
  } finally {
    await handle.close();
  }
}

function moreSpecificProviderError(current: string, incoming: string): string {
  const exact = new Set([
    "CODEX_OAUTH_TOKEN_INVALIDATED",
    "CODEX_OAUTH_TOKEN_REVOKED",
  ]);
  return exact.has(incoming) && !exact.has(current) ? incoming : current;
}

function equalOpaqueGeneration(left: string, right: string): boolean {
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}

function timingSafeDigestEqual(left: string, right: string): boolean {
  return /^[a-f\d]{64}$/.test(left) && equalOpaqueGeneration(left, right);
}

function sha256(value: Buffer): string {
  return hash("sha256", value, "hex");
}

export function schedulerStateDigest(identity: unknown): string {
  return inputHash({
    identity: SchedulerIdentitySchema.parse(identity),
    schemaId: "saqi.provider-scheduler-state",
    schemaVersion: 1,
  });
}

export function parseQuotaResetTimestamp(
  text: string,
  now = Date.now(),
): null | number {
  const iso =
    /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?(?:Z|[+-]\d{2}:\d{2})/.exec(
      text,
    )?.[0];
  if (iso) {
    const timestamp = Date.parse(iso);
    if (Number.isFinite(timestamp)) return boundedFuture(timestamp, now);
  }
  const epoch = /(?:reset|at)\D{0,20}(\d{10}|\d{13})(?!\d)/i.exec(text)?.[1];
  if (epoch) {
    const parsed = Number(epoch);
    return boundedFuture(epoch.length === 10 ? parsed * 1_000 : parsed, now);
  }
  return null;
}

function boundedFuture(timestamp: number, now: number): number {
  if (!Number.isSafeInteger(timestamp) || timestamp <= now) {
    return now + DEFAULT_QUOTA_WAIT_MS;
  }
  return Math.min(timestamp, now + MAXIMUM_RESET_HORIZON_MS);
}

function concurrency(value: number): number {
  return ConcurrencySchema.parse(value);
}
