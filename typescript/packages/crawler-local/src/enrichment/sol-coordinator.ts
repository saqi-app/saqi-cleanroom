import { randomUUID } from "node:crypto";
import { setInterval } from "node:timers";

import {
  ENRICHMENT_INPUT_SCHEMA_ID,
  ENRICHMENT_INPUT_SCHEMA_VERSION,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
  type PoemEnrichmentInputV2,
  PoemEnrichmentInputV2Schema,
  PoemEnrichmentOutputV2Schema,
  PoemEnrichmentReviewSchema,
  reviewAcceptsEnrichment,
  reviewsAcceptEnrichment,
} from "@saqi/precedent-iso";
import { z } from "zod";

import {
  type Artifact,
  type ArtifactStore,
  DiskPressureError,
} from "../persistence/artifact-store.js";
import { type Ledger, LostLeaseError } from "../persistence/ledger.js";
import type { WorkClaim } from "../persistence/schema.js";
import { canonicalJson, inputHash } from "../persistence/work-key.js";
import {
  NETWORK_UNAVAILABLE_ERROR_CODE,
  networkProbeDelayMs,
} from "../runtime/network-resilience.js";
import {
  type CodexSolRunner,
  ENRICHMENT_PROVIDER_SPECS,
  parseSolRepairContext,
  type SolGenerationResult,
  type SolReviewResult,
} from "./sol-runner.js";

export const SOL_ENRICHMENT_WORK_KIND = "poem-enrichment-sol";
export const POEM_ENRICHMENT_SCHEMA_VERSION = `${ENRICHMENT_INPUT_SCHEMA_ID}@${String(ENRICHMENT_INPUT_SCHEMA_VERSION)}`;
export const POEM_ENRICHMENT_V2_SCHEMA_VERSION = `${ENRICHMENT_INPUT_SCHEMA_ID}@2`;
export const SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION =
  "source-bound-publication-v1";
const SupportedPoemEnrichmentInputSchema = z.union([
  PoemEnrichmentInputV2Schema,
  PoemEnrichmentInputSchema,
]);
type SupportedPoemEnrichmentInput = PoemEnrichmentInput | PoemEnrichmentInputV2;
const LEASE_DURATION_MS = 2 * 60 * 60 * 1_000;
const REJECTION_RETRY_MS = 5 * 60_000;
const MAX_INVALID_ATTEMPTS = 5;
const MAX_SOURCE_BOUND_SOL_REVIEW_ATTEMPTS = 2;
export const SOL_REVIEW_REJECTED_MANUAL_ADJUDICATION_REQUIRED =
  "SOL_REVIEW_REJECTED_MANUAL_ADJUDICATION_REQUIRED";
const MAX_TRANSIENT_ATTEMPTS = 12;
const DISK_PRESSURE_RETRY_MS = 60_000;
class SolBudgetExhaustedError extends Error {}
const ArtifactPersistenceErrorCodeSchema = z.enum([
  "EBUSY",
  "EIO",
  "EMFILE",
  "ENFILE",
  "ENOSPC",
  "SQLITE_BUSY",
  "SQLITE_FULL",
  "SQLITE_LOCKED",
]);
const ArtifactPersistenceInterruptionSchema = z.object({
  code: ArtifactPersistenceErrorCodeSchema,
});
const PhaseSchema = z.strictObject({
  generationAttemptId: z.string().min(1),
  output: PoemEnrichmentOutputV2Schema,
  outputHash: z.string().regex(/^[a-f\d]{64}$/),
  rejected: z.boolean(),
  reviewAttemptIds: z.array(z.string().min(1)).max(2),
  reviews: z.array(PoemEnrichmentReviewSchema).max(2),
});
type SolPhase = z.infer<typeof PhaseSchema>;

export interface SolCoordinatorOptions {
  /** Explicit migration escape hatch. Production defaults to bound-only paid work. */
  readonly allowLegacyPaidClaims?: boolean;
  readonly artifacts: ArtifactStore;
  /** Require a durable three-operation reservation before every paid Sol claim. */
  readonly enforcePaidUsageBudget?: boolean;
  readonly leaseDurationMs?: number;
  readonly leaseHeartbeatMs?: number;
  readonly ledger: Ledger;
  readonly owner?: string;
  readonly random?: () => number;
  readonly recoverUnknownOperations?: boolean;
  readonly runner: CodexSolRunner;
  readonly startedWorkOnly?: boolean;
}
export interface SolRunOptions {
  readonly artifactReconciliationOnly?: boolean;
  readonly maximum?: number;
  readonly now?: () => number;
  readonly paused?: () => boolean | Promise<boolean>;
}
export interface SolRunSummary {
  readonly claimed: number;
  readonly deadLettered: number;
  readonly providerErrorCode?: string;
  readonly quotaWait: number;
  readonly retried: number;
  readonly retryAt: null | number;
  readonly schedulerOutcome:
    | "ambiguous_outcome"
    | "budget_exhausted"
    | "error"
    | "idle"
    | "network_wait"
    | "provider_wait"
    | "quota_wait"
    | "rate_limited"
    | "success"
    | "task_failure";
  readonly stopped: "aborted" | "disk_pressure" | "idle" | "maximum" | "paused";
  readonly succeeded: number;
}
export interface SolEnrichmentRunPort {
  run(signal?: AbortSignal, options?: SolRunOptions): Promise<SolRunSummary>;
}
export interface SolEnrichmentSeedPort {
  seed(
    rawInput: SupportedPoemEnrichmentInput,
    priority?: number,
  ): ReturnType<Ledger["seedMany"]>[number];
  seedMany(
    rawInputs: readonly SupportedPoemEnrichmentInput[],
    priority?: number,
  ): ReturnType<Ledger["seedMany"]>;
}
type MutableSolRunSummary = {
  -readonly [Key in keyof SolRunSummary]: SolRunSummary[Key];
};
interface RandomSource {
  sample(): number;
}

const SCHEDULER_OUTCOME_SEVERITY: Readonly<
  Record<SolRunSummary["schedulerOutcome"], number>
> = {
  ambiguous_outcome: 5,
  budget_exhausted: 8,
  error: 3,
  idle: 0,
  network_wait: 7,
  provider_wait: 6,
  quota_wait: 5,
  rate_limited: 4,
  success: 2,
  task_failure: 1,
};
const PAID_USAGE_BUDGET_RECHECK_MS = 60_000;
const PROVIDER_RETRY_MS = 15 * 60_000;
const UNKNOWN_OPERATION_RECONCILIATION_AGE_MS = 5 * 60_000;
const UNKNOWN_OPERATION_RECONCILIATION_BATCH = 8;
const UNKNOWN_OPERATION_MAXIMUM_ARTIFACT_CHECKS = 3;
const UNKNOWN_OPERATION_MAXIMUM_AGE_MS = 24 * 60 * 60_000;
const PROVIDER_WIDE_ERROR_CODES: ReadonlySet<string> = new Set([
  "CODEX_CHATGPT_AUTH_REQUIRED",
  "CODEX_OAUTH_TOKEN_INVALIDATED",
  "CODEX_OAUTH_TOKEN_REVOKED",
  "ENRICHMENT_AUTH_REQUIRED",
  "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
  "ENRICHMENT_PROVIDER_EXECUTABLE_MISSING",
  "ENRICHMENT_PROVIDER_EXECUTABLE_NOT_EXECUTABLE",
]);

export class SolEnrichmentCoordinator
  implements SolEnrichmentRunPort, SolEnrichmentSeedPort
{
  readonly #artifacts: ArtifactStore;
  readonly #allowLegacyPaidClaims: boolean;
  readonly #ledger: Ledger;
  readonly #enforcePaidUsageBudget: boolean;
  readonly #leaseDurationMs: number;
  readonly #leaseHeartbeatMs: number;
  readonly #owner: string;
  readonly #pipelineVersion: string;
  readonly #profile: CodexSolRunner["profile"];
  readonly #randomSource: RandomSource;
  readonly #recoverUnknownOperations: boolean;
  readonly #runner: CodexSolRunner;
  readonly #startedWorkOnly: boolean;
  readonly #workKind: string;
  #consecutiveNetworkFailures = 0;

  constructor(options: SolCoordinatorOptions) {
    this.#artifacts = options.artifacts;
    this.#ledger = options.ledger;
    this.#enforcePaidUsageBudget = options.enforcePaidUsageBudget ?? false;
    this.#leaseDurationMs = options.leaseDurationMs ?? LEASE_DURATION_MS;
    this.#leaseHeartbeatMs =
      options.leaseHeartbeatMs ?? Math.floor(this.#leaseDurationMs / 3);
    if (
      !Number.isSafeInteger(this.#leaseDurationMs) ||
      this.#leaseDurationMs <= 0 ||
      !Number.isSafeInteger(this.#leaseHeartbeatMs) ||
      this.#leaseHeartbeatMs <= 0 ||
      this.#leaseHeartbeatMs >= this.#leaseDurationMs
    ) {
      throw new Error("Invalid Sol lease heartbeat configuration");
    }
    this.#runner = options.runner;
    this.#startedWorkOnly = options.startedWorkOnly ?? false;
    this.#randomSource = { sample: options.random ?? Math.random };
    const runtimeProfile: unknown = Reflect.get(this.#runner, "profile");
    const profile: CodexSolRunner["profile"] = runtimeProfile
      ? this.#runner.profile
      : {
          ...ENRICHMENT_PROVIDER_SPECS.sol,
          provider: "sol" as const,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        };
    this.#profile = profile;
    this.#allowLegacyPaidClaims = options.allowLegacyPaidClaims ?? false;
    this.#recoverUnknownOperations = options.recoverUnknownOperations ?? true;
    this.#pipelineVersion = profile.pipelineVersion;
    this.#workKind = profile.workKind;
    this.#owner =
      options.owner ??
      `${profile.provider}-${String(process.pid)}-${randomUUID()}`;
  }

  seed(rawInput: SupportedPoemEnrichmentInput, priority = 0) {
    const [result] = this.seedMany([rawInput], priority);
    if (!result) throw new Error("SOL_SEED_RESULT_MISSING");
    return result;
  }

  seedMany(rawInputs: readonly SupportedPoemEnrichmentInput[], priority = 0) {
    return this.#ledger.seedMany(
      rawInputs.map((rawInput) => {
        const input = SupportedPoemEnrichmentInputSchema.parse(rawInput);
        return {
          implementationVersion: this.#pipelineVersion,
          input,
          inputHash: inputHash(input),
          kind: this.#workKind,
          priority,
          schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
        };
      }),
    );
  }

  async run(
    signal?: AbortSignal,
    options: SolRunOptions = {},
  ): Promise<SolRunSummary> {
    const maximum = options.maximum ?? Infinity;
    if (!(
      maximum === Infinity ||
      (Number.isSafeInteger(maximum) && maximum > 0)
    ))
      throw new Error("maximum must be a positive integer");
    const now = options.now ?? Date.now;
    const summary: MutableSolRunSummary = {
      claimed: 0,
      deadLettered: 0,
      quotaWait: 0,
      retried: 0,
      retryAt: null,
      schedulerOutcome: "idle",
      succeeded: 0,
      stopped: "idle",
    };
    try {
      await this.#artifacts.assertWritableCapacity();
    } catch (error) {
      if (error instanceof DiskPressureError) {
        summary.stopped = "disk_pressure";
        return summary;
      }
      throw error;
    }
    let recoveryClaims = 0;
    let providerVerified = false;
    while (
      summary.claimed < maximum &&
      !signal?.aborted &&
      // eslint-disable-next-line no-await-in-loop -- Pause is an admission gate that must be polled before every paid-work claim.
      !(await options.paused?.())
    ) {
      try {
        // eslint-disable-next-line no-await-in-loop -- Capacity must be revalidated before each paid-work claim.
        await this.#artifacts.assertWritableCapacity();
      } catch (error) {
        if (error instanceof DiskPressureError) {
          summary.stopped = "disk_pressure";
          break;
        }
        throw error;
      }
      let claimAt = now();
      const terminalizedReviewRejection =
        this.#ledger.classifyReadyErrorAtAttemptThreshold(
          this.#workKind,
          {
            implementationVersion: this.#pipelineVersion,
            schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
          },
          "SOL_REVIEW_REJECTED",
          MAX_SOURCE_BOUND_SOL_REVIEW_ATTEMPTS,
          SOL_REVIEW_REJECTED_MANUAL_ADJUDICATION_REQUIRED,
          claimAt,
        );
      if (terminalizedReviewRejection) {
        summary.claimed += 1;
        summary.deadLettered += 1;
        summary.schedulerOutcome = worseSchedulerOutcome(
          summary.schedulerOutcome,
          "task_failure",
        );
        continue;
      }
      // Exactly one Sol maintenance lane receives this capability. Artifact
      // inspection is local and free, so it remains available while provider
      // authentication or the network is unavailable.
      const recoveryClaim =
        this.#recoverUnknownOperations &&
        recoveryClaims < UNKNOWN_OPERATION_RECONCILIATION_BATCH
          ? (this.#claimUnknownOperationRecovery(
              claimAt,
              POEM_ENRICHMENT_V2_SCHEMA_VERSION,
            ) ??
            this.#claimUnknownOperationRecovery(
              claimAt,
              POEM_ENRICHMENT_SCHEMA_VERSION,
            ))
          : null;
      if (recoveryClaim) recoveryClaims += 1;
      if (!recoveryClaim && options.artifactReconciliationOnly) break;
      if (!recoveryClaim && !this.#hasProviderWork(claimAt)) break;
      // Budget denial is not an attempt. Keep fresh work untouched and avoid
      // authentication subprocesses; local recovery remains independently free.
      if (!recoveryClaim && this.#enforcePaidUsageBudget) {
        const budget = this.#ledger.solPaidUsageBudgetStatus();
        if (budget.state !== "active" || budget.remainingOperations < 3) {
          summary.retryAt = claimAt + PAID_USAGE_BUDGET_RECHECK_MS;
          summary.schedulerOutcome = "budget_exhausted";
          break;
        }
      }
      if (!recoveryClaim && !providerVerified) {
        try {
          // Authentication is an admission gate only for fresh paid work.
          // eslint-disable-next-line no-await-in-loop -- Verification occurs once, immediately before this run's first paid-work claim.
          await this.#runner.verifyChatGptLogin(signal);
          providerVerified = true;
        } catch (error) {
          const code = providerWideErrorCode(error);
          if (code === null) throw error;
          summary.providerErrorCode = code;
          if (code === NETWORK_UNAVAILABLE_ERROR_CODE) {
            this.#consecutiveNetworkFailures += 1;
            summary.retryAt =
              now() +
              networkProbeDelayMs(this.#consecutiveNetworkFailures, () =>
                this.#randomSource.sample(),
              );
            summary.schedulerOutcome = "network_wait";
          } else {
            summary.retryAt = now() + PROVIDER_RETRY_MS;
            summary.schedulerOutcome = "provider_wait";
          }
          return summary;
        }
      }
      if (!recoveryClaim) {
        // Authentication can outlive a pause/stop request or a short lease.
        // eslint-disable-next-line no-await-in-loop -- Recheck admission after asynchronous authentication and before any fresh claim or reservation.
        if (signal?.aborted || (await options.paused?.()) || signal?.aborted)
          break;
        this.#runner.assertOperationAdmission();
        claimAt = now();
      }
      const claim =
        recoveryClaim ??
        this.#claimProviderWork(claimAt, POEM_ENRICHMENT_V2_SCHEMA_VERSION) ??
        (this.#allowLegacyPaidClaims
          ? this.#claimProviderWork(claimAt, POEM_ENRICHMENT_SCHEMA_VERSION)
          : null);
      if (!claim) break;
      summary.claimed += 1;
      try {
        const previousOutcome = summary.schedulerOutcome;
        // Claims are intentionally sequential within one coordinator; process
        // parallelism is provided by independent coordinator lanes.
        // eslint-disable-next-line no-await-in-loop -- Parallelism belongs to independent coordinator lanes.
        await this.#runClaimWithHeartbeat(
          claim,
          summary,
          now,
          signal,
          recoveryClaim !== null,
        );
        summary.schedulerOutcome = worseSchedulerOutcome(
          previousOutcome,
          summary.schedulerOutcome,
        );
        if (
          summary.schedulerOutcome === "quota_wait" ||
          summary.schedulerOutcome === "network_wait" ||
          summary.schedulerOutcome === "provider_wait" ||
          summary.schedulerOutcome === "rate_limited"
        )
          break;
      } catch (failure) {
        const approvedCompletion =
          failure instanceof SolApprovedCompletionInterruption;
        const error = approvedCompletion ? failure.cause : failure;
        if (error instanceof SolBudgetExhaustedError) {
          const transitionAt = now();
          const retryAt = transitionAt + PAID_USAGE_BUDGET_RECHECK_MS;
          this.#ledger.operatorRelease(
            claim,
            "SOL_PAID_USAGE_BUDGET_EXHAUSTED",
            transitionAt,
            retryAt,
          );
          summary.retryAt = retryAt;
          summary.schedulerOutcome = "budget_exhausted";
          break;
        }
        if (error instanceof LostLeaseError) continue;
        if (error instanceof SolDiskPressureError) {
          try {
            const transitionAt = now();
            this.#ledger.checkpoint(
              claim,
              {
                artifactHash: null,
                kind: "sol_failure",
                payload: {
                  code: "ARTIFACT_STORE_DISK_PRESSURE",
                  diagnosticSchemaVersion: 1,
                  errorMessage: error.pressure.message.slice(0, 1_000),
                  errorName: error.pressure.name.slice(0, 100),
                  phase: error.phase,
                },
              },
              transitionAt,
            );
            this.#ledger.retry(
              claim,
              recoveryClaim || approvedCompletion
                ? "CODEX_OPERATION_OUTCOME_UNKNOWN"
                : "ARTIFACT_STORE_DISK_PRESSURE",
              transitionAt + DISK_PRESSURE_RETRY_MS,
              transitionAt,
            );
            summary.retried += 1;
            summary.schedulerOutcome = "error";
          } catch (transitionError) {
            if (!(transitionError instanceof LostLeaseError))
              throw transitionError;
          }
          summary.stopped = "disk_pressure";
          break;
        }
        try {
          const transitionAt = now();
          if (signal?.aborted) {
            if (recoveryClaim || approvedCompletion)
              this.#ledger.checkpoint(
                claim,
                {
                  artifactHash: null,
                  kind: "sol_failure",
                  payload: {
                    code: "SOL_ARTIFACT_RECOVERY_INTERRUPTED",
                    underlyingErrorCode: "SOL_OPERATOR_STOP",
                    diagnosticSchemaVersion: 1,
                  },
                },
                transitionAt,
              );
            this.#ledger.operatorRelease(
              claim,
              recoveryClaim || approvedCompletion
                ? "CODEX_OPERATION_OUTCOME_UNKNOWN"
                : "SOL_OPERATOR_STOP",
              transitionAt,
            );
            summary.schedulerOutcome = "idle";
          } else if (
            error instanceof Error &&
            error.message === "CODEX_OPERATION_STATE_INVALID"
          ) {
            this.#ledger.operatorRelease(
              claim,
              "CODEX_OPERATION_STATE_INVALID",
              transitionAt,
              transitionAt + 24 * 60 * 60_000,
            );
            summary.retried += 1;
            summary.schedulerOutcome = "error";
          } else if (
            !approvedCompletion &&
            claim.work.attemptCount >= MAX_TRANSIENT_ATTEMPTS
          ) {
            this.#ledger.deadLetter(
              claim,
              "SOL_RUNNER_EXCEPTION",
              transitionAt,
            );
            summary.deadLettered += 1;
            summary.schedulerOutcome = "error";
          } else {
            const persistenceFailure =
              ArtifactPersistenceInterruptionSchema.safeParse(error);
            const recoverableInterruption =
              (recoveryClaim !== null || approvedCompletion) &&
              persistenceFailure.success;
            if (recoverableInterruption)
              this.#ledger.checkpoint(
                claim,
                {
                  artifactHash: null,
                  kind: "sol_failure",
                  payload: {
                    code: "SOL_ARTIFACT_RECOVERY_INTERRUPTED",
                    underlyingErrorCode: persistenceFailure.data.code,
                    diagnosticSchemaVersion: 1,
                  },
                },
                transitionAt,
              );
            this.#ledger.retry(
              claim,
              recoverableInterruption
                ? "CODEX_OPERATION_OUTCOME_UNKNOWN"
                : "SOL_RUNNER_EXCEPTION",
              transitionAt + transientBackoffMs(claim.work.attemptCount),
              transitionAt,
            );
            summary.retried += 1;
            summary.schedulerOutcome = "error";
          }
        } catch (transitionError) {
          if (!(transitionError instanceof LostLeaseError))
            throw transitionError;
        }
        if (signal?.aborted) break;
      }
    }
    if (summary.stopped !== "disk_pressure") {
      summary.stopped = signal?.aborted
        ? "aborted"
        : (await options.paused?.())
          ? "paused"
          : summary.claimed >= maximum
            ? "maximum"
            : "idle";
    }
    return summary;
  }

  async #runClaimWithHeartbeat(
    claim: WorkClaim,
    summary: MutableSolRunSummary,
    now: () => number,
    signal?: AbortSignal,
    artifactOnly = false,
  ): Promise<void> {
    const leaseAbort = new AbortController();
    const operationSignal = signal
      ? AbortSignal.any([signal, leaseAbort.signal])
      : leaseAbort.signal;
    let leaseFailure: unknown = null;
    const heartbeat = setInterval(() => {
      try {
        this.#ledger.renew(claim, now(), this.#leaseDurationMs);
      } catch (error) {
        leaseFailure = error;
        leaseAbort.abort(error);
      }
    }, this.#leaseHeartbeatMs);
    heartbeat.unref();
    try {
      if (artifactOnly)
        await this.#reconcileClaimArtifacts(claim, summary, now);
      else await this.#runClaim(claim, summary, now, operationSignal);
      if (leaseFailure)
        throw leaseFailure instanceof Error
          ? leaseFailure
          : new Error("SOL_LEASE_HEARTBEAT_FAILED", { cause: leaseFailure });
    } finally {
      clearInterval(heartbeat);
    }
  }

  #claimProviderWork(claimAt: number, schemaVersion: string) {
    return this.#ledger.claim(
      this.#owner,
      claimAt,
      this.#leaseDurationMs,
      [this.#workKind],
      {
        implementationVersion: this.#pipelineVersion,
        schemaVersion,
        ...(this.#startedWorkOnly ? { minimumAttemptCount: 1 } : {}),
      },
      ["CODEX_OPERATION_OUTCOME_UNKNOWN"],
    );
  }

  #hasProviderWork(now: number): boolean {
    const versions = this.#allowLegacyPaidClaims
      ? [POEM_ENRICHMENT_V2_SCHEMA_VERSION, POEM_ENRICHMENT_SCHEMA_VERSION]
      : [POEM_ENRICHMENT_V2_SCHEMA_VERSION];
    return versions.some(
      (schemaVersion) =>
        this.#ledger.availability([this.#workKind], now, {
          implementationVersion: this.#pipelineVersion,
          schemaVersion,
          ...(this.#startedWorkOnly ? { minimumAttemptCount: 1 } : {}),
        }).ready > 0,
    );
  }

  #claimUnknownOperationRecovery(claimAt: number, schemaVersion: string) {
    return this.#ledger.claimUnknownOperationRecovery(
      this.#owner,
      claimAt,
      this.#leaseDurationMs,
      this.#workKind,
      claimAt - UNKNOWN_OPERATION_RECONCILIATION_AGE_MS,
      { implementationVersion: this.#pipelineVersion, schemaVersion },
    );
  }

  async #reconcileClaimArtifacts(
    claim: WorkClaim,
    summary: MutableSolRunSummary,
    now: () => number,
  ): Promise<void> {
    const input = SupportedPoemEnrichmentInputSchema.safeParse(
      claim.work.input,
    );
    const transitionAt = now();
    if (!input.success) {
      this.#ledger.deadLetter(
        claim,
        "ENRICHMENT_INPUT_SCHEMA_INVALID",
        transitionAt,
      );
      summary.deadLettered += 1;
      return;
    }
    let phase = await this.#loadPhase(claim.work.workKey);
    const repair = phase?.rejected
      ? parseSolRepairContext({
          generationAttemptId: phase.generationAttemptId,
          output: phase.output,
          outputHash: phase.outputHash,
          reviews: phase.reviews,
        })
      : undefined;
    if (!phase || phase.rejected) {
      const metadata = this.#runner.generationOperationMetadata(
        input.data,
        repair,
      );
      const recovered = this.#runner.recoverGenerationArtifact(
        input.data,
        repair,
      );
      if (recovered?.state !== "succeeded") {
        this.#retainUnknownOperation(claim, metadata, summary, transitionAt);
        return;
      }
      // Reconciliation is recorded before advancing the phase. If the process
      // dies at either boundary, the retained provider artifact is replayed and
      // the idempotent paid-operation row remains authoritative; no provider
      // invocation is needed to finish the checkpoint on restart.
      this.#recordArtifactReconciliation(
        claim,
        recovered.metadata,
        transitionAt,
      );
      phase = {
        generationAttemptId: recovered.metadata.attemptId,
        output: recovered.output,
        outputHash: recovered.outputHash,
        rejected: false,
        reviewAttemptIds: [],
        reviews: [],
      };
      await this.#savePhase(
        claim,
        phase,
        "artifact_reconciliation_checkpoint",
        transitionAt,
      );
    }

    const [firstReview] = phase.reviews;
    if (
      phase.reviews.length === 1 &&
      firstReview &&
      !reviewAcceptsEnrichment(firstReview)
    ) {
      await this.#rejectPhase(claim, phase, summary, now());
      return;
    }

    for (const reviewAttempt of [1, 2] as const) {
      if (phase.reviews.length >= reviewAttempt) continue;
      const metadata = this.#runner.reviewOperationMetadata(
        input.data,
        phase.output,
        reviewAttempt,
      );
      if (metadata === null) {
        // No retained operation exists for the next review. Keep the completed
        // free checkpoints; fresh paid work remains subject to its own budget gate.
        this.#releaseArtifactReconciliation(claim, now());
        return;
      }
      const recovered = this.#runner.recoverReviewArtifact(
        input.data,
        phase.output,
        reviewAttempt,
      );
      if (recovered?.state !== "succeeded") {
        this.#retainUnknownOperation(claim, metadata, summary, now());
        return;
      }
      this.#recordArtifactReconciliation(claim, recovered.metadata, now());
      phase = {
        ...phase,
        reviewAttemptIds: [
          ...phase.reviewAttemptIds,
          recovered.metadata.attemptId,
        ],
        reviews: [...phase.reviews, recovered.review],
      };
      // eslint-disable-next-line no-await-in-loop -- Each retained review must be durably checkpointed before validating the next review or completing publication.
      await this.#savePhase(
        claim,
        phase,
        "artifact_reconciliation_checkpoint",
        now(),
      );
      if (reviewAttempt === 1 && !reviewAcceptsEnrichment(recovered.review)) {
        // eslint-disable-next-line no-await-in-loop -- The first retained rejection must stop the ordered review gate without inspecting review two.
        await this.#rejectPhase(claim, phase, summary, now());
        return;
      }
    }
    await this.#completeApprovedPhase(claim, input.data, phase, summary, now);
  }

  #recordArtifactReconciliation(
    claim: WorkClaim,
    metadata: SolGenerationResult["metadata"],
    now: number,
  ): void {
    // Backfill legacy file-indexed ambiguity into the SQL diagnostic ledger
    // before marking the retained artifact authoritative.
    this.#ledger.recordPaidOperationUnknown(
      claim.work.workKey,
      metadata.operationKey,
      metadata.attemptId,
      now,
      now,
    );
    this.#ledger.recordPaidOperationReconciled(metadata.operationKey, now);
  }

  #releaseArtifactReconciliation(claim: WorkClaim, now: number): void {
    this.#ledger.operatorRelease(
      claim,
      "CODEX_OPERATION_ARTIFACT_RECONCILED",
      now,
    );
  }

  #retainUnknownOperation(
    claim: WorkClaim,
    metadata: null | SolGenerationResult["metadata"],
    summary: MutableSolRunSummary,
    now: number,
  ): void {
    const retryAt = now + UNKNOWN_OPERATION_RECONCILIATION_AGE_MS;
    const artifactChecks = Math.max(1, claim.work.attemptCount - 1);
    const shouldQuarantine =
      artifactChecks >= UNKNOWN_OPERATION_MAXIMUM_ARTIFACT_CHECKS ||
      now - claim.work.createdAt >= UNKNOWN_OPERATION_MAXIMUM_AGE_MS;
    if (metadata) {
      this.#ledger.recordPaidOperationUnknown(
        claim.work.workKey,
        metadata.operationKey,
        metadata.attemptId,
        retryAt,
        now,
      );
      if (shouldQuarantine)
        this.#ledger.recordPaidOperationQuarantined(
          metadata.operationKey,
          now,
          "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
        );
    }
    this.#ledger.deadLetter(
      claim,
      shouldQuarantine
        ? "CODEX_OPERATION_UNRESOLVED_QUARANTINED"
        : "CODEX_OPERATION_OUTCOME_UNKNOWN",
      now,
    );
    summary.deadLettered += 1;
    summary.schedulerOutcome = shouldQuarantine
      ? "task_failure"
      : "ambiguous_outcome";
  }

  async #runClaim(
    claim: WorkClaim,
    summary: MutableSolRunSummary,
    now: () => number,
    signal?: AbortSignal,
  ): Promise<void> {
    let reserved = false;
    const beforeNewOperation = () => {
      if (reserved || !this.#enforcePaidUsageBudget) return;
      if (!this.#ledger.reserveSolPaidClaim(claim, now())) {
        throw new SolBudgetExhaustedError();
      }
      reserved = true;
    };
    const parsedInput = SupportedPoemEnrichmentInputSchema.safeParse(
      claim.work.input,
    );
    if (!parsedInput.success) {
      this.#ledger.deadLetter(claim, "ENRICHMENT_INPUT_SCHEMA_INVALID", now());
      summary.deadLettered += 1;
      return;
    }
    let phase: null | SolPhase;
    try {
      phase = await this.#loadPhase(claim.work.workKey);
    } catch {
      this.#ledger.deadLetter(claim, "SOL_PHASE_CORRUPT", now());
      summary.deadLettered += 1;
      return;
    }
    if (!phase || phase.rejected) {
      const repair = phase?.rejected
        ? parseSolRepairContext({
            generationAttemptId: phase.generationAttemptId,
            output: phase.output,
            outputHash: phase.outputHash,
            reviews: phase.reviews,
          })
        : undefined;
      const generation = await this.#runner.generate(
        parsedInput.data,
        signal,
        repair,
        beforeNewOperation,
      );
      if (generation.state !== "succeeded") {
        const transitionAt = now();
        this.#checkpointNonSuccessAttempt(
          claim,
          generation.metadata.attemptId,
          generation.errorCode,
          generation.state,
          transitionAt,
        );
        if (signal?.aborted) {
          this.#ledger.operatorRelease(
            claim,
            "SOL_OPERATOR_STOP",
            transitionAt,
          );
          summary.schedulerOutcome = "idle";
          return;
        }
        this.#transitionNonSuccess(claim, generation, summary, transitionAt);
        return;
      }
      phase = {
        generationAttemptId: generation.metadata.attemptId,
        output: generation.output,
        outputHash: generation.outputHash,
        rejected: false,
        reviewAttemptIds: [],
        reviews: [],
      };
      await this.#savePhase(claim, phase, "generation_checkpoint", now());
    }

    // A crash or disk-pressure stop can occur after the first review is
    // checkpointed but before the rejection checkpoint is written. Preserve
    // the same short-circuit on resume instead of paying reviewer 2 for a
    // candidate that can no longer pass the two-review gate.
    const [firstReview] = phase.reviews;
    if (
      phase.reviews.length === 1 &&
      firstReview &&
      !reviewAcceptsEnrichment(firstReview)
    ) {
      await this.#rejectPhase(claim, phase, summary, now());
      return;
    }

    for (const reviewAttempt of [1, 2] as const) {
      if (phase.reviews.length >= reviewAttempt) continue;
      // Review 2 depends on review 1's durable checkpoint and acceptance.
      // eslint-disable-next-line no-await-in-loop -- Review 2 requires review 1 acceptance and checkpointing.
      const result = await this.#runner.review(
        parsedInput.data,
        phase.output,
        reviewAttempt,
        signal,
        beforeNewOperation,
      );
      if (result.state !== "succeeded") {
        const transitionAt = now();
        this.#checkpointNonSuccessAttempt(
          claim,
          result.metadata.attemptId,
          result.errorCode,
          result.state,
          transitionAt,
        );
        if (signal?.aborted) {
          this.#ledger.operatorRelease(
            claim,
            "SOL_OPERATOR_STOP",
            transitionAt,
          );
          summary.schedulerOutcome = "idle";
          return;
        }
        this.#transitionNonSuccess(claim, result, summary, transitionAt);
        return;
      }
      phase = {
        ...phase,
        reviewAttemptIds: [
          ...phase.reviewAttemptIds,
          result.metadata.attemptId,
        ],
        reviews: [...phase.reviews, result.review],
      };
      // eslint-disable-next-line no-await-in-loop -- Review checkpoints must be durable in validator order before the next review starts.
      await this.#savePhase(
        claim,
        phase,
        `review_${String(reviewAttempt)}_checkpoint`,
        now(),
      );
      if (reviewAttempt === 1 && !reviewAcceptsEnrichment(result.review)) {
        // eslint-disable-next-line no-await-in-loop -- Rejection depends on the just-completed ordered review and must transition before leaving the loop.
        await this.#rejectPhase(claim, phase, summary, now());
        return;
      }
    }

    await this.#completeApprovedPhase(
      claim,
      parsedInput.data,
      phase,
      summary,
      now,
    );
  }

  async #completeApprovedPhase(
    claim: WorkClaim,
    input: SupportedPoemEnrichmentInput,
    phase: SolPhase,
    summary: MutableSolRunSummary,
    now: () => number,
  ): Promise<void> {
    if (!reviewsAcceptEnrichment(phase.reviews)) {
      await this.#rejectPhase(claim, phase, summary, now());
      return;
    }
    try {
      const artifact = await this.#putArtifact(
        `${canonicalJson({
          input,
          generationAttemptId: phase.generationAttemptId,
          output: phase.output,
          outputHash: phase.outputHash,
          model: this.#profile.model,
          modelKey: this.#profile.modelKey,
          pipelineVersion: this.#pipelineVersion,
          provider: this.#profile.provider,
          reasoningEffort: this.#profile.reasoningEffort,
          reviews: phase.reviews,
          reviewAttemptIds: phase.reviewAttemptIds,
        })}\n`,
        "final_artifact",
      );
      const completedAt = now();
      if (input.schemaVersion === 2) {
        this.#ledger.completeApprovedAndSeedPublication(
          claim,
          artifact.hash,
          input.canonicalBinding,
          {
            implementationVersion:
              SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
            priority: claim.work.priority,
          },
          completedAt,
        );
      } else {
        this.#ledger.succeed(claim, artifact.hash, completedAt);
      }
      this.#consecutiveNetworkFailures = 0;
      summary.succeeded += 1;
      summary.schedulerOutcome = "success";
    } catch (error) {
      // Only the already validated, durably checkpointed approval boundary is
      // eligible. Partial/rejected phases and validation failures never enter
      // this free-finalization recovery classification.
      if (
        error instanceof SolDiskPressureError ||
        ArtifactPersistenceInterruptionSchema.safeParse(error).success
      )
        throw new SolApprovedCompletionInterruption(error);
      throw error;
    }
  }

  async #rejectPhase(
    claim: WorkClaim,
    phase: SolPhase,
    summary: {
      deadLettered: number;
      retried: number;
      schedulerOutcome: SolRunSummary["schedulerOutcome"];
    },
    transitionAt: number,
  ): Promise<void> {
    await this.#savePhase(
      claim,
      { ...phase, rejected: true },
      "rejection_checkpoint",
      transitionAt,
    );
    const sourceBoundSol =
      claim.work.schemaVersion === POEM_ENRICHMENT_V2_SCHEMA_VERSION;
    const maximumAttempts = sourceBoundSol
      ? MAX_SOURCE_BOUND_SOL_REVIEW_ATTEMPTS
      : MAX_INVALID_ATTEMPTS;
    if (claim.work.attemptCount < maximumAttempts) {
      this.#ledger.retry(
        claim,
        "SOL_REVIEW_REJECTED",
        transitionAt + REJECTION_RETRY_MS,
        transitionAt,
      );
      summary.retried += 1;
    } else {
      this.#ledger.deadLetter(
        claim,
        sourceBoundSol
          ? SOL_REVIEW_REJECTED_MANUAL_ADJUDICATION_REQUIRED
          : "SOL_REVIEW_REJECTED",
        transitionAt,
      );
      summary.deadLettered += 1;
    }
    // The provider responded, but a quality-gate rejection must neither
    // promote concurrency nor erase capacity evidence from accepted work.
    summary.schedulerOutcome = "task_failure";
  }

  async #loadPhase(workKey: string): Promise<null | SolPhase> {
    const checkpoint = this.#ledger.latestCheckpoint(workKey, "sol-phase");
    if (!checkpoint) return null;
    if (!checkpoint.artifactHash) throw new Error("SOL_PHASE_ARTIFACT_MISSING");
    const phaseContents = await this.#artifacts.read(checkpoint.artifactHash);
    return PhaseSchema.parse(JSON.parse(phaseContents.toString("utf8")));
  }

  async #savePhase(
    claim: WorkClaim,
    phase: SolPhase,
    storagePhase: string,
    now: number,
  ): Promise<void> {
    const parsed = PhaseSchema.parse(phase);
    const artifact = await this.#putArtifact(
      `${canonicalJson(parsed)}\n`,
      storagePhase,
    );
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: artifact.hash,
        kind: "sol-phase",
        payload: {
          attemptReferences: {
            generation: parsed.generationAttemptId,
            reviews: parsed.reviewAttemptIds,
          },
          rejected: parsed.rejected,
          reviewCount: parsed.reviews.length,
        },
      },
      now,
    );
  }

  async #putArtifact(value: string, phase: string): Promise<Artifact> {
    try {
      return await this.#artifacts.put(value);
    } catch (error) {
      if (error instanceof DiskPressureError) {
        throw new SolDiskPressureError(error, phase);
      }
      throw error;
    }
  }

  #checkpointNonSuccessAttempt(
    claim: WorkClaim,
    attemptId: string,
    errorCode: string,
    state: Exclude<SolGenerationResult["state"], "succeeded">,
    now: number,
  ): void {
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: null,
        kind: "sol-attempt-reference",
        payload: {
          attemptReferences: { nonSuccess: attemptId },
          errorCode,
          state,
        },
      },
      now,
    );
  }

  #transitionNonSuccess(
    claim: WorkClaim,
    result: SolGenerationResult | SolReviewResult,
    summary: {
      deadLettered: number;
      providerErrorCode?: string;
      quotaWait: number;
      retried: number;
      retryAt: null | number;
      schedulerOutcome: SolRunSummary["schedulerOutcome"];
    },
    now: number,
  ): void {
    if (result.state === "succeeded") return;
    if (result.state === "quota_wait") {
      this.#ledger.quotaWait(claim, result.errorCode, result.retryAt, now);
      summary.quotaWait += 1;
      summary.retryAt = result.retryAt;
      summary.schedulerOutcome = "quota_wait";
      return;
    }
    if (result.state === "network_wait") {
      this.#consecutiveNetworkFailures += 1;
      const retryAt =
        now +
        networkProbeDelayMs(this.#consecutiveNetworkFailures, () =>
          this.#randomSource.sample(),
        );
      // operatorRelease cancels the claim-attempt increment, so an arbitrarily
      // long outage can never exhaust the task or move it to dead-letter.
      this.#ledger.operatorRelease(claim, result.errorCode, now, retryAt);
      summary.retried += 1;
      summary.retryAt = retryAt;
      summary.providerErrorCode = result.errorCode;
      summary.schedulerOutcome = "network_wait";
      return;
    }
    if (result.state === "retry_wait") {
      if (result.errorCode === "SOL_SHARED_OPERATION_PENDING") {
        const retryAt = Math.max(result.retryAt ?? now, now + 60_000);
        // This claim did not launch a provider operation. Preserve its attempt
        // budget while the material owner finishes or reconciles the shared
        // durable operation.
        this.#ledger.operatorRelease(claim, result.errorCode, now, retryAt);
        summary.retried += 1;
        summary.retryAt = retryAt;
        summary.schedulerOutcome = "task_failure";
        return;
      }
      if (PROVIDER_WIDE_ERROR_CODES.has(result.errorCode)) {
        const retryAt = result.retryAt ?? now + PROVIDER_RETRY_MS;
        this.#ledger.operatorRelease(claim, result.errorCode, now, retryAt);
        summary.retried += 1;
        summary.retryAt = retryAt;
        summary.providerErrorCode = result.errorCode;
        summary.schedulerOutcome = "provider_wait";
        return;
      }
      if (result.errorCode === "CODEX_RATE_LIMITED") {
        summary.schedulerOutcome = "rate_limited";
        summary.retryAt = result.retryAt ?? null;
      }
      if (result.errorCode === "CODEX_OPERATION_OUTCOME_UNKNOWN") {
        // The provider may have accepted and billed this operation. Preserve
        // its durable attempt/index for operator reconciliation, but never
        // reclaim the poem automatically: even a runner-side fence should not
        // turn an unknowable paid call into a permanent retry loop.
        this.#ledger.recordPaidOperationUnknown(
          claim.work.workKey,
          result.metadata.operationKey,
          result.metadata.attemptId,
          now + UNKNOWN_OPERATION_RECONCILIATION_AGE_MS,
          now,
        );
        this.#ledger.deadLetter(claim, result.errorCode, now);
        summary.deadLettered += 1;
        summary.schedulerOutcome = "ambiguous_outcome";
        return;
      }
      if (result.errorCode === "CODEX_OPERATION_UNRESOLVED_QUARANTINED") {
        this.#ledger.deadLetter(claim, result.errorCode, now);
        summary.deadLettered += 1;
        // This is an already-accounted historic ambiguity. It must neither
        // reduce live capacity again nor enter the reconciliation queue.
        summary.schedulerOutcome = "task_failure";
        return;
      }
      if (claim.work.attemptCount >= MAX_TRANSIENT_ATTEMPTS) {
        this.#ledger.deadLetter(claim, result.errorCode, now);
        summary.deadLettered += 1;
      } else {
        this.#ledger.retry(
          claim,
          result.errorCode,
          Math.max(
            result.retryAt ?? now,
            now + transientBackoffMs(claim.work.attemptCount),
          ),
          now,
        );
        summary.retried += 1;
        summary.retryAt = result.retryAt ?? null;
        summary.schedulerOutcome =
          result.errorCode === "CODEX_RATE_LIMITED" ? "rate_limited" : "error";
      }
    } else if (claim.work.attemptCount < MAX_INVALID_ATTEMPTS) {
      this.#ledger.retry(
        claim,
        result.errorCode,
        now + REJECTION_RETRY_MS,
        now,
      );
      summary.retried += 1;
      summary.schedulerOutcome = "task_failure";
    } else {
      this.#ledger.deadLetter(claim, result.errorCode, now);
      summary.deadLettered += 1;
      summary.schedulerOutcome = "task_failure";
    }
  }
}

class SolApprovedCompletionInterruption extends Error {
  constructor(cause: unknown) {
    super("SOL_APPROVED_COMPLETION_INTERRUPTED", { cause });
  }
}

class SolDiskPressureError extends Error {
  readonly phase: string;
  readonly pressure: DiskPressureError;

  constructor(pressure: DiskPressureError, phase: string) {
    super(pressure.message, { cause: pressure });
    this.name = "SolDiskPressureError";
    this.phase = phase;
    this.pressure = pressure;
  }
}

function transientBackoffMs(attemptCount: number): number {
  return Math.min(
    30 * 60_000,
    30_000 * 2 ** Math.min(Math.max(attemptCount - 1, 0), 6),
  );
}

function providerWideErrorCode(error: unknown): null | string {
  if (
    !(error instanceof Error) ||
    (!PROVIDER_WIDE_ERROR_CODES.has(error.message) &&
      error.message !== NETWORK_UNAVAILABLE_ERROR_CODE)
  )
    return null;
  return error.message;
}

function worseSchedulerOutcome(
  left: SolRunSummary["schedulerOutcome"],
  right: SolRunSummary["schedulerOutcome"],
): SolRunSummary["schedulerOutcome"] {
  return SCHEDULER_OUTCOME_SEVERITY[left] >= SCHEDULER_OUTCOME_SEVERITY[right]
    ? left
    : right;
}
