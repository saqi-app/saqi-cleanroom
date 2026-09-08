import { hostname } from "node:os";
import { resolve } from "node:path";
import { setInterval } from "node:timers";
import { setTimeout as delay } from "node:timers/promises";

import {
  type AuthorPoemManifestProjection,
  canonicalAuthorUrl,
  canonicalPoemUrl,
  currentSource,
  parseAuthorPoemManifest,
  parsePoemDetail,
  type PoemDetailProjection,
  PROJECTION_SCHEMA_VERSION,
  sha256Canonical,
  sourceAuthorUrl,
  SourceProjectionError,
} from "@saqi/source-adapter";
import { z } from "zod";

import {
  type Artifact,
  type ArtifactVerification,
  DiskPressureError,
  type StorageCapacity,
} from "../persistence/artifact-store.js";
import {
  type Ledger,
  LostLeaseError,
  type OriginLease,
} from "../persistence/ledger.js";
import type { WorkClaim, WorkDefinition } from "../persistence/schema.js";
import { canonicalJson, inputHash } from "../persistence/work-key.js";
import {
  COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE,
  isNetworkFailureText,
  networkProbeDelayMs,
} from "../runtime/network-resilience.js";
import {
  CollectionLaneScheduler,
  collectionWorkKinds,
} from "./collection-scheduler.js";
import { SourceBrowserError } from "./collection-source-browser.js";
import type { CatalogInventory } from "./inventory-reconciliation.js";

export { collectionWorkKinds } from "./collection-scheduler.js";

export function collectorImplementationVersion(): string {
  return `${currentSource().name}-chrome-v4`;
}

export function collectorSchemaVersion(): string {
  return `${currentSource().name}-projection-v${String(PROJECTION_SCHEMA_VERSION)}`;
}

export function collectionRuntimeProfile(): {
  readonly implementationVersion: string;
  readonly schemaVersion: string;
} {
  return {
    implementationVersion: collectorImplementationVersion(),
    schemaVersion: collectorSchemaVersion(),
  };
}

const RefreshGenerationSchema = z.string().regex(/^[\w.-]{1,64}$/);
const AuthorInputSchema = z
  .object({
    authorHref: z.url(),
    authorNameArabic: z.string().trim().min(1).max(512).optional(),
    refreshGeneration: RefreshGenerationSchema.optional(),
  })
  .strict();
const PoemInputSchema = z
  .object({
    authorHref: z.url(),
    poemHref: z.url(),
  })
  // Older ledgers contain discovery metadata in this input. Strip it while
  // draining those rows; all newly seeded detail work has identity-only input.
  .strip();
const LegacyPoemMetadataSchema = z.looseObject({
  authorNameArabic: z.string().trim().min(1).max(512).optional(),
  refreshGeneration: RefreshGenerationSchema.optional(),
});
const RETRYABLE_RENDER_PROJECTION_CODES: ReadonlySet<string> = new Set([
  "SOURCE_CLASSICAL_LINE_COUNT_MISMATCH",
  "SOURCE_POEM_CONTENT_EMPTY",
  "SOURCE_POEM_TITLE_EMPTY",
]);
const MAXIMUM_RENDER_PROJECTION_ATTEMPTS = 3;
const MAXIMUM_FEED_CONFIGURATION_ATTEMPTS = 3;
const HUMAN_CHALLENGE_BASE_RETRY_MS = 15 * 60_000;
const HUMAN_CHALLENGE_MAXIMUM_RETRY_MS = 6 * 60 * 60_000;
const BROWSER_RESTART_REQUIRED_CODES: ReadonlySet<string> = new Set([
  "SOURCE_BROWSER_RESTART_REQUIRED",
  "SOURCE_PROFILE_LOCKED",
  "SOURCE_PROFILE_LOCK_INVALID",
]);
const BOUNDED_OPERATION_TIMEOUT_CODES: ReadonlySet<string> = new Set([
  "SOURCE_AUTHOR_OPERATION_TIMEOUT",
  "SOURCE_INVENTORY_OPERATION_TIMEOUT",
  "SOURCE_POEM_OPERATION_TIMEOUT",
]);

export interface CollectorOptions {
  readonly artifacts: CollectorArtifactStore;
  readonly browser: CollectorBrowser;
  readonly detailBurst?: number;
  readonly leaseDurationMs?: number;
  readonly leaseHeartbeatMs?: number;
  readonly ledger: Ledger;
  readonly minimumOriginGapMs?: number;
  /** Best-effort notification emitted only after source success is durable. */
  readonly onDurableSuccess?: (completion: CollectorCompletion) => void;
  readonly owner?: string;
  readonly random?: () => number;
  readonly retryDelayMs?: number;
}

export interface CollectorCompletion {
  readonly artifactHash: string;
  readonly kind: string;
  readonly workKey: string;
}

/** The durable artifact capabilities owned by the collection workflow. */
export interface CollectorArtifactStore {
  assertWritableCapacity(requiredBytes?: number): Promise<StorageCapacity>;
  put(value: string | Uint8Array): Promise<Artifact>;
  verify(hash: string): Promise<ArtifactVerification>;
}

export interface CollectorBrowser {
  close(): Promise<void>;
  collectAuthorManifest(
    authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection>;
  collectPoemDetail(
    poemValue: string,
    expectedAuthorValue: string,
    signal: AbortSignal,
  ): Promise<PoemDetailProjection>;
}

export interface CollectorRunResult {
  readonly duplicateRejected: boolean;
  readonly failed: number;
  readonly originRetryAt?: number;
  /** Source-origin health, kept separate from the work-item lifecycle. */
  readonly originState:
    | "blocked"
    | "challenge_wait"
    | "disk_wait"
    | "healthy"
    | "network_wait"
    | "rate_wait";
  /** Durable source-gate reason, when the ledger has stopped the origin. */
  readonly originStopReason?: string;
  readonly processed: number;
  readonly stopped:
    | "aborted"
    | "blocked"
    | "disk_pressure"
    | "human_required"
    | "idle"
    | "maximum"
    | "network_wait"
    | "paused"
    | "restart_required";
  readonly succeeded: number;
}

type CollectorWorkDecision =
  | { readonly claim: WorkClaim; readonly state: "claimed" }
  | { readonly reason: string; readonly state: "stopped" }
  | {
      readonly reason?: string;
      readonly retryAt: number;
      readonly state: "waiting";
    }
  | { readonly state: "idle" };

export type CollectorOriginHealthState =
  | "blocked"
  | "challenge_wait"
  | "disabled"
  | "disk_wait"
  | "healthy"
  | "network_wait"
  | "rate_wait";

export interface CollectorOriginHealthInput {
  readonly configured: boolean;
  readonly consecutiveFailures: number;
  readonly cooldownUntil: number;
  readonly nextAllowedAt: number;
  readonly now: number;
  readonly runtimeState?: CollectorRunResult["originState"];
  readonly stopReason: null | string;
}

/** Derives truthful source health from durable gate state after every restart. */
export function deriveCollectorOriginHealthState(
  input: CollectorOriginHealthInput,
): CollectorOriginHealthState {
  if (!input.configured) return "disabled";
  if (input.stopReason) return originStateForReason(input.stopReason);
  if (
    input.consecutiveFailures > 0 &&
    Math.max(input.cooldownUntil, input.nextAllowedAt) > input.now
  )
    return "blocked";
  return input.runtimeState ?? "healthy";
}

function originStateForReason(
  reason: string,
): Exclude<CollectorOriginHealthState, "disabled" | "healthy"> {
  if (reason.includes("HUMAN_REQUIRED") || reason.includes("CHALLENGE"))
    return "challenge_wait";
  if (reason.includes("NETWORK")) return "network_wait";
  if (reason.includes("RATE_LIMIT")) return "rate_wait";
  if (
    reason.includes("DISK") ||
    reason.includes("STORAGE") ||
    reason.includes("CAPACITY")
  )
    return "disk_wait";
  return "blocked";
}

/** Lifecycle boundary consumed by the rig and command entrypoints. */
export interface CollectorCoordinatorPort {
  close(): Promise<void>;
  run(
    signal: AbortSignal,
    options?: {
      /** When present, claims are restricted to this exact durable cohort. */
      readonly includedWorkKeys?: readonly string[];
      readonly maximum?: number;
      readonly paused?: () => boolean | Promise<boolean>;
    },
  ): Promise<CollectorRunResult>;
  scheduleSnapshot(): ReturnType<CollectionLaneScheduler["snapshot"]>;
  seedAuthor(
    authorValue: string,
    priority?: number,
  ): { inserted: boolean; workKey: string };
}

export class CollectorCoordinator implements CollectorCoordinatorPort {
  readonly #artifacts: CollectorArtifactStore;
  readonly #browser: CollectorBrowser;
  readonly #collectionScheduler: CollectionLaneScheduler;
  readonly #leaseDurationMs: number;
  readonly #leaseHeartbeatMs: number;
  readonly #ledger: Ledger;
  readonly #minimumOriginGapMs: number;
  readonly #onDurableSuccess: CollectorOptions["onDurableSuccess"];
  readonly #owner: string;
  readonly #random: () => number;
  readonly #retryDelayMs: number;
  #originRetryAt: null | number = null;
  #consecutiveNetworkFailures = 0;

  constructor(options: CollectorOptions) {
    this.#artifacts = options.artifacts;
    this.#browser = options.browser;
    this.#collectionScheduler = new CollectionLaneScheduler(
      options.detailBurst,
    );
    this.#ledger = options.ledger;
    this.#leaseDurationMs = options.leaseDurationMs ?? 5 * 60_000;
    this.#leaseHeartbeatMs =
      options.leaseHeartbeatMs ??
      Math.max(1_000, Math.floor(this.#leaseDurationMs / 3));
    if (
      this.#leaseHeartbeatMs <= 0 ||
      this.#leaseHeartbeatMs >= this.#leaseDurationMs
    ) {
      throw new Error(
        "Lease heartbeat must be positive and shorter than lease",
      );
    }
    this.#minimumOriginGapMs = options.minimumOriginGapMs ?? 13_000;
    this.#onDurableSuccess = options.onDurableSuccess;
    this.#owner =
      options.owner ?? `${hostname()}:${String(process.pid)}:collector`;
    this.#random = options.random ?? Math.random;
    this.#retryDelayMs = options.retryDelayMs ?? 60_000;
  }

  seedAuthor(
    authorValue: string,
    priority = 0,
  ): { inserted: boolean; workKey: string } {
    return seedAuthorManifest(this.#ledger, authorValue, priority);
  }

  async run(
    signal: AbortSignal,
    options: {
      readonly includedWorkKeys?: readonly string[];
      readonly maximum?: number;
      readonly paused?: () => boolean | Promise<boolean>;
    } = {},
  ): Promise<CollectorRunResult> {
    let originStopReason: null | string = null;
    let processed = 0;
    let succeeded = 0;
    let duplicateRejected = false;
    while (!signal.aborted && processed < (options.maximum ?? Infinity)) {
      // eslint-disable-next-line no-await-in-loop -- Pause state is sampled before each serial durable claim.
      if (await options.paused?.())
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState: "healthy",
          processed,
          stopped: "paused",
          succeeded,
        };
      try {
        // eslint-disable-next-line no-await-in-loop -- Capacity must be revalidated immediately before each serial durable claim.
        await this.#artifacts.assertWritableCapacity();
      } catch (error) {
        if (!(error instanceof DiskPressureError)) throw error;
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState: "disk_wait",
          processed,
          stopped: "disk_pressure",
          succeeded,
        };
      }
      this.#ledger.recoverExpired();
      const kinds = collectionWorkKinds();
      this.#ledger.retireIncompatible(
        [kinds.authorManifest, kinds.poemDetail],
        collectionRuntimeProfile(),
      );
      const next = this.#claimNextWork(Date.now(), options.includedWorkKeys);
      if (next.state === "stopped") {
        const originState = originStateForReason(next.reason);
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState,
          originStopReason: next.reason,
          processed,
          stopped:
            originState === "challenge_wait"
              ? "human_required"
              : originState === "network_wait"
                ? "network_wait"
                : originState === "disk_wait"
                  ? "disk_pressure"
                  : "blocked",
          succeeded,
        };
      }
      if (next.state === "idle") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState: "healthy",
          processed,
          stopped: "idle",
          succeeded,
        };
      }
      if (next.state === "waiting") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originRetryAt: next.retryAt,
          ...(next.reason ? { originStopReason: next.reason } : {}),
          originState: next.reason
            ? originStateForReason(next.reason)
            : "healthy",
          processed,
          stopped: "idle",
          succeeded,
        };
      }
      const claim = next.claim;
      processed += 1;
      // eslint-disable-next-line no-await-in-loop -- One coordinator processes claims serially; parallelism uses separate lanes.
      const result = await this.#execute(claim, signal);
      if (result !== "succeeded")
        originStopReason = this.#ledger.originStopReason(
          currentSource().origin,
        );
      if (result === "succeeded") succeeded += 1;
      if (result === "duplicate_rejected") duplicateRejected = true;
      if (result === "disk_pressure") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState: "disk_wait",
          processed,
          stopped: "disk_pressure",
          succeeded,
        };
      }
      if (result === "human_required") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          ...(this.#originRetryAt === null
            ? {}
            : { originRetryAt: this.#originRetryAt }),
          originState: "challenge_wait",
          ...(originStopReason === null ? {} : { originStopReason }),
          processed,
          stopped: "human_required",
          succeeded,
        };
      }
      if (result === "network_wait") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          ...(this.#originRetryAt === null
            ? {}
            : { originRetryAt: this.#originRetryAt }),
          originState: "network_wait",
          processed,
          stopped: "network_wait",
          succeeded,
        };
      }
      if (result === "restart_required") {
        return {
          duplicateRejected,
          failed: processed - succeeded,
          originState: "healthy",
          processed,
          stopped: "restart_required",
          succeeded,
        };
      }
    }
    return {
      duplicateRejected,
      failed: processed - succeeded,
      ...(this.#originRetryAt !== null && this.#originRetryAt > Date.now()
        ? { originRetryAt: this.#originRetryAt }
        : {}),
      originState:
        originStopReason === null
          ? "healthy"
          : originStateForReason(originStopReason),
      ...(originStopReason === null ? {} : { originStopReason }),
      processed,
      stopped: signal.aborted ? "aborted" : "maximum",
      succeeded,
    };
  }

  async close(): Promise<void> {
    await this.#browser.close();
  }

  scheduleSnapshot(): ReturnType<CollectionLaneScheduler["snapshot"]> {
    return this.#collectionScheduler.snapshot();
  }

  #claimNextWork(
    now: number,
    includedWorkKeys?: readonly string[],
  ): CollectorWorkDecision {
    for (const kind of this.#collectionScheduler.preferredKinds()) {
      const result = this.#ledger.claimUnlessOriginStopped(
        currentSource().origin,
        this.#owner,
        now,
        this.#leaseDurationMs,
        [kind],
        collectionRuntimeProfile(),
        includedWorkKeys,
      );
      if (result.state === "stopped") return result;
      if (result.state === "waiting") return result;
      if (result.state === "claimed") {
        this.#collectionScheduler.recordClaim(result.claim.work.kind);
        return result;
      }
    }
    return { state: "idle" };
  }

  async #execute(
    claim: WorkClaim,
    signal: AbortSignal,
  ): Promise<
    | "disk_pressure"
    | "duplicate_rejected"
    | "failed"
    | "human_required"
    | "network_wait"
    | "restart_required"
    | "succeeded"
  > {
    let originLease: null | OriginLease = null;
    let originFailure: {
      readonly retryAt: number;
      readonly stopReason?: string;
    } | null = null;
    let networkUnavailable = false;
    let operatorCancelled = false;
    let restartRequired = false;
    let leaseFailure: unknown = null;
    const leaseAbort = new AbortController();
    const operationSignal = AbortSignal.any([signal, leaseAbort.signal]);
    let phase = "claim_origin";
    const heartbeat: NodeJS.Timeout = setInterval(() => {
      try {
        const now = Date.now();
        this.#ledger.renew(claim, now, this.#leaseDurationMs);
        if (originLease) {
          this.#ledger.renewOrigin(originLease, now, this.#leaseDurationMs);
        }
      } catch (error) {
        leaseFailure = error;
        leaseAbort.abort(error);
      }
    }, this.#leaseHeartbeatMs);
    heartbeat.unref();
    try {
      originLease = await this.#claimOrigin(operationSignal);
      throwIfLeaseFailed(leaseFailure);
      this.#ledger.checkpoint(claim, {
        artifactHash: null,
        kind: "source_request_started",
        payload: { origin: currentSource().origin },
      });
      phase = "collect_source";
      const payload = await this.#collect(claim, operationSignal);
      throwIfLeaseFailed(leaseFailure);
      phase = "store_projection";
      const artifact = await this.#artifacts.put(canonicalJson(payload));
      phase = "checkpoint_projection";
      this.#ledger.checkpoint(claim, {
        artifactHash: artifact.hash,
        kind: "source_projection_committed",
        payload: { bytes: artifact.bytes, kind: claim.work.kind },
      });
      phase = "commit_success";
      this.#ledger.succeed(claim, artifact.hash);
      try {
        this.#onDurableSuccess?.({
          artifactHash: artifact.hash,
          kind: claim.work.kind,
          workKey: claim.work.workKey,
        });
      } catch {
        // This hint only accelerates a durable cursor scan. Its failure must
        // never roll a successfully committed source back into error handling.
      }
      this.#consecutiveNetworkFailures = 0;
      return "succeeded";
    } catch (error) {
      if (error instanceof LostLeaseError || leaseFailure) return "failed";
      const code = classifyError(error);
      const now = Date.now();
      try {
        this.#checkpointFailureDiagnostic(claim, code, error, phase, now);
        if (signal.aborted) {
          operatorCancelled = true;
          this.#ledger.operatorRelease(claim, "COLLECTOR_OPERATOR_STOP", now);
        } else if (error instanceof DiskPressureError) {
          this.#ledger.retry(claim, code, now + this.#retryDelayMs, now);
        } else if (code === COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE) {
          networkUnavailable = true;
          this.#consecutiveNetworkFailures += 1;
          const retryAt =
            now +
            networkProbeDelayMs(this.#consecutiveNetworkFailures, this.#random);
          this.#originRetryAt = retryAt;
          // Network availability is not a source failure. Release the work
          // without consuming an attempt and do not trip the origin circuit.
          this.#ledger.operatorRelease(claim, code, now, retryAt);
        } else if (BOUNDED_OPERATION_TIMEOUT_CODES.has(code)) {
          // A bounded local browser timeout is not a source defect. Yield this
          // item without consuming an attempt so other collection work can
          // proceed with the freshly recycled browser.
          this.#ledger.operatorRelease(
            claim,
            code,
            now,
            now + this.#retryDelayMs,
          );
        } else if (BROWSER_RESTART_REQUIRED_CODES.has(code)) {
          restartRequired = true;
          this.#ledger.operatorRelease(
            claim,
            code,
            now,
            now + this.#retryDelayMs,
          );
        } else if (code === "SOURCE_HUMAN_REQUIRED") {
          const retryAt =
            now +
            computeHumanChallengeRetryDelayMs(
              this.#ledger.originConsecutiveFailures(currentSource().origin) +
                1,
              this.#random,
            );
          originFailure = { retryAt, stopReason: code };
          // A source challenge is not a defect in this poem. Defer it without
          // consuming a work attempt; the fenced origin gate admits one probe
          // after the persisted retry boundary.
          this.#ledger.operatorRelease(claim, code, now, retryAt);
        } else if (
          error instanceof SourceBrowserError &&
          error.code === "SOURCE_RATE_LIMITED"
        ) {
          const retryAt = Math.max(
            now +
              computeRetryDelayMs(
                this.#retryDelayMs,
                claim.work.attemptCount,
                this.#random,
              ),
            now + boundedRetryAfterMs(error.retryAfterMs ?? 5 * 60_000),
          );
          originFailure = { retryAt, stopReason: error.code };
          this.#ledger.retry(claim, code, retryAt, now);
        } else if (
          error instanceof SourceProjectionError &&
          code === "SOURCE_POEM_CONTENT_EMPTY"
        ) {
          const retryAt =
            now +
            computeRetryDelayMs(
              this.#retryDelayMs,
              claim.work.attemptCount,
              this.#random,
            );
          originFailure = { retryAt };
          if (claim.work.attemptCount < MAXIMUM_RENDER_PROJECTION_ATTEMPTS) {
            this.#ledger.retry(claim, code, retryAt, now);
          } else {
            this.#ledger.deadLetter(claim, code);
          }
        } else if (
          error instanceof SourceProjectionError &&
          RETRYABLE_RENDER_PROJECTION_CODES.has(code) &&
          claim.work.attemptCount < MAXIMUM_RENDER_PROJECTION_ATTEMPTS
        ) {
          this.#ledger.retry(
            claim,
            code,
            now +
              computeRetryDelayMs(
                this.#retryDelayMs,
                claim.work.attemptCount,
                this.#random,
              ),
            now,
          );
        } else if (
          error instanceof SourceBrowserError &&
          error.code === "SOURCE_FEED_CONFIG_MISSING"
        ) {
          // This is scoped to one author page, not evidence that the entire
          // origin is unhealthy. Retrying it forever would repeatedly trip
          // the shared origin circuit and starve healthy poem-detail work.
          if (claim.work.attemptCount < MAXIMUM_FEED_CONFIGURATION_ATTEMPTS) {
            this.#ledger.retry(
              claim,
              code,
              now +
                computeRetryDelayMs(
                  this.#retryDelayMs,
                  claim.work.attemptCount,
                  this.#random,
                ),
              now,
            );
          } else {
            this.#ledger.deadLetter(claim, code);
          }
        } else if (error instanceof SourceBrowserError && !error.retryable) {
          originFailure = { retryAt: now + this.#minimumOriginGapMs };
          this.#ledger.deadLetter(claim, code);
        } else if (error instanceof SourceProjectionError && !error.retryable) {
          this.#ledger.deadLetter(claim, code);
        } else {
          const retryAt =
            now +
            computeRetryDelayMs(
              this.#retryDelayMs,
              claim.work.attemptCount,
              this.#random,
            );
          if (error instanceof SourceBrowserError && error.retryable) {
            originFailure = { retryAt };
          }
          this.#ledger.retry(claim, code, retryAt, now);
        }
      } catch (transitionError) {
        if (!(transitionError instanceof LostLeaseError)) throw transitionError;
      }
      if (error instanceof DiskPressureError) return "disk_pressure";
      if (code === "SOURCE_POEM_DUPLICATE") return "duplicate_rejected";
      if (code === COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE)
        return "network_wait";
      if (restartRequired) return "restart_required";
      return code === "SOURCE_HUMAN_REQUIRED" ? "human_required" : "failed";
    } finally {
      clearInterval(heartbeat);
      if (originLease) {
        try {
          const now = Date.now();
          if (networkUnavailable) {
            if (this.#originRetryAt === null)
              throw new Error("COLLECTOR_NETWORK_RETRY_MISSING");
            this.#ledger.deferOrigin(
              originLease,
              now,
              this.#originRetryAt,
              COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE,
            );
          } else if (operatorCancelled || restartRequired) {
            this.#ledger.releaseOrigin(originLease, now);
            this.#originRetryAt = null;
          } else if (originFailure) {
            const failure = this.#ledger.failOrigin(
              originLease,
              now,
              this.#minimumOriginGapMs,
              {
                circuitBreakerAfter: 3,
                circuitBreakerCooldownMs: 15 * 60_000,
                retryAt: Math.max(now, originFailure.retryAt),
                ...(originFailure.stopReason
                  ? { stopReason: originFailure.stopReason }
                  : {}),
              },
            );
            this.#originRetryAt = failure.nextAllowedAt;
          } else {
            this.#ledger.completeOrigin(
              originLease,
              now,
              this.#minimumOriginGapMs,
            );
            this.#originRetryAt = null;
          }
        } catch (error) {
          if (!(error instanceof LostLeaseError)) throw error;
        }
      }
    }
  }

  #checkpointFailureDiagnostic(
    claim: WorkClaim,
    code: string,
    error: unknown,
    phase: string,
    now: number,
  ): void {
    const detail = diagnosticError(error);
    const sourceAccess =
      error instanceof SourceBrowserError ? error.sourceAccess : null;
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: null,
        kind: "collector_failure",
        payload: {
          code,
          diagnosticSchemaVersion: 2,
          errorMessage: detail.message,
          errorName: detail.name,
          phase,
          ...(sourceAccess === null ? {} : { sourceAccess }),
        },
      },
      now,
    );
  }

  async #collect(claim: WorkClaim, signal: AbortSignal): Promise<unknown> {
    const kinds = collectionWorkKinds();
    if (claim.work.kind === kinds.authorManifest) {
      const input = AuthorInputSchema.parse(claim.work.input);
      const projection = await this.#browser.collectAuthorManifest(
        input.authorHref,
        signal,
      );
      const manifest = parseAuthorPoemManifest(projection);
      if (
        manifest.author.canonicalId !==
        canonicalAuthorUrl(input.authorHref).canonicalId
      ) {
        throw new SourceBrowserError(
          "SOURCE_MANIFEST_ID_MISMATCH",
          "Projected manifest differs from requested author",
          false,
        );
      }
      if (
        input.authorNameArabic !== undefined &&
        input.refreshGeneration !== undefined
      ) {
        this.#ledger.recordSourceAuthorMetadata(
          manifest.author.href,
          input.authorNameArabic,
          input.refreshGeneration,
          claim.work.createdAt,
        );
      }
      const candidates = manifest.poems.map((poem) => {
        const poemInput = {
          authorHref: manifest.author.href,
          poemHref: poem.href,
        };
        return { inputHash: inputHash(poemInput), poemInput };
      });
      const priorArtifacts = this.#ledger.successfulArtifactsForInputs(
        kinds.poemDetail,
        collectorSchemaVersion(),
        candidates.map(({ inputHash: hash }) => hash),
      );
      const candidateDefinitions = await Promise.all(
        candidates.map(async ({ inputHash: hash, poemInput }) => {
          const priorArtifact = priorArtifacts.get(hash);
          const verification = priorArtifact
            ? await this.#artifacts.verify(priorArtifact)
            : null;
          return verification?.ok
            ? []
            : [definition(kinds.poemDetail, poemInput, claim.work.priority)];
        }),
      );
      const definitions = candidateDefinitions.flat();
      const conflict = this.#ledger.seedPoems(definitions).conflicts[0] ?? null;
      if (conflict) {
        throw new SourceProjectionError(
          "SOURCE_POEM_DUPLICATE",
          conflict.message,
          false,
          { cause: conflict },
        );
      }
      return envelope(claim, manifest);
    }
    if (claim.work.kind === kinds.poemDetail) {
      const input = PoemInputSchema.parse(claim.work.input);
      const projection = await this.#browser.collectPoemDetail(
        input.poemHref,
        input.authorHref,
        signal,
      );
      const detail = parsePoemDetail(projection);
      if (detail.canonicalId !== canonicalPoemUrl(input.poemHref).canonicalId) {
        throw new SourceBrowserError(
          "SOURCE_POEM_ID_MISMATCH",
          "Projected poem differs from requested poem",
          false,
        );
      }
      const legacyMetadata = LegacyPoemMetadataSchema.parse(claim.work.input);
      const authorMetadata =
        this.#ledger.sourceAuthorMetadata(input.authorHref) ??
        (legacyMetadata.authorNameArabic && legacyMetadata.refreshGeneration
          ? {
              authorNameArabic: legacyMetadata.authorNameArabic,
              refreshGeneration: legacyMetadata.refreshGeneration,
            }
          : null);
      return envelope(
        claim,
        detail,
        authorMetadata
          ? {
              authorNameArabic: authorMetadata.authorNameArabic,
              refreshGeneration: authorMetadata.refreshGeneration,
            }
          : undefined,
      );
    }
    throw new Error("COLLECTOR_WORK_KIND_UNSUPPORTED");
  }

  async #claimOrigin(signal: AbortSignal): Promise<OriginLease> {
    for (;;) {
      if (signal.aborted) throw signal.reason ?? new Error("Aborted");
      const result = this.#ledger.claimOrigin(
        currentSource().origin,
        Date.now(),
        this.#leaseDurationMs,
      );
      if (result.state === "claimed") return result.lease;
      if (result.state === "stopped") {
        throw new SourceBrowserError(
          "SOURCE_HUMAN_REQUIRED",
          `Origin is stopped pending operator action: ${result.reason}`,
        );
      }
      // eslint-disable-next-line no-await-in-loop -- Retry admission must wait for the origin lease window.
      await delay(Math.max(10, result.retryAt - Date.now()), undefined, {
        signal,
      });
    }
  }
}

function throwIfLeaseFailed(error: unknown): void {
  if (error)
    throw error instanceof Error
      ? error
      : new Error("COLLECTOR_LEASE_FAILED", { cause: error });
}

export function seedAuthorManifest(
  ledger: Ledger,
  authorValue: string,
  priority = 0,
  refreshGeneration?: string,
  authorNameArabic?: string,
): { inserted: boolean; workKey: string } {
  const author = canonicalAuthorUrl(authorValue);
  return ledger.seed(
    authorManifestDefinition(
      author.href,
      priority,
      refreshGeneration,
      authorNameArabic,
    ),
  );
}

export function seedAuthorManifests(
  ledger: Ledger,
  inventory: CatalogInventory,
  priority = 0,
  refreshGeneration?: string,
): readonly { inserted: boolean; workKey: string }[] {
  return ledger.seedMany(
    inventory.authors.map((author) =>
      authorManifestDefinition(
        author.href,
        priority,
        refreshGeneration,
        author.name,
      ),
    ),
  );
}

function authorManifestDefinition(
  authorHref: string,
  priority: number,
  refreshGeneration?: string,
  authorNameArabic?: string,
): WorkDefinition {
  if (refreshGeneration !== undefined)
    RefreshGenerationSchema.parse(refreshGeneration);
  return definition(
    collectionWorkKinds().authorManifest,
    {
      authorHref,
      ...(authorNameArabic === undefined ? {} : { authorNameArabic }),
      ...(refreshGeneration === undefined ? {} : { refreshGeneration }),
    },
    priority,
  );
}

export function authorUrlFromCatalogSlug(slug: string): string {
  return sourceAuthorUrl(slug).href;
}

function definition(
  kind: string,
  input: Readonly<Record<string, unknown>>,
  priority: number,
): WorkDefinition {
  return {
    implementationVersion: collectorImplementationVersion(),
    input,
    inputHash: inputHash(input),
    kind,
    priority,
    schemaVersion: collectorSchemaVersion(),
  };
}

async function envelope(
  claim: WorkClaim,
  source: unknown,
  sourceContext?: Readonly<{
    authorNameArabic: string;
    refreshGeneration: string;
  }>,
): Promise<unknown> {
  return {
    artifactSchemaVersion: 1,
    collectedBy: collectorImplementationVersion(),
    source,
    sourceHash: await sha256Canonical(source),
    ...(sourceContext === undefined ? {} : { sourceContext }),
    workKey: claim.work.workKey,
  };
}

function classifyError(error: unknown): string {
  if (error instanceof DiskPressureError) {
    return "ARTIFACT_STORE_DISK_PRESSURE";
  }
  if (
    error instanceof Error &&
    isNetworkFailureText(`${error.name}\n${error.message}`)
  )
    return COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE;
  if (error instanceof SourceBrowserError) return error.code;
  if (error instanceof SourceProjectionError) return error.code;
  if (error instanceof z.ZodError) return "COLLECTOR_SCHEMA_INVALID";
  if (error instanceof Error && /^[A-Z][A-Z0-9_]{0,99}$/.test(error.message)) {
    return error.message;
  }
  return "COLLECTOR_UNEXPECTED";
}

function diagnosticError(error: unknown): { message: string; name: string } {
  const name = error instanceof Error ? error.name : typeof error;
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "Non-Error value thrown";
  return {
    message: message.slice(0, 1_000),
    name: name.slice(0, 100),
  };
}

export function defaultChromeProfile(stateRoot: string): string {
  return resolve(stateRoot, "chrome-profile");
}

const RETRY_AFTER_MINIMUM_MS = 3_000;
const RETRY_AFTER_MAXIMUM_MS = 24 * 60 * 60_000;
const RETRY_BACKOFF_MAXIMUM_MS = 6 * 60 * 60_000;

export function computeRetryDelayMs(
  baseDelayMs: number,
  attemptCount: number,
  random: () => number,
): number {
  if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs <= 0) {
    throw new Error("Retry delay must be a positive integer");
  }
  if (!Number.isSafeInteger(attemptCount) || attemptCount < 0) {
    throw new Error("Attempt count must be a non-negative integer");
  }
  const sample = random();
  if (!Number.isFinite(sample) || sample < 0 || sample >= 1) {
    throw new Error("Retry random sample must be in [0, 1)");
  }
  const exponent = Math.min(Math.max(0, attemptCount - 1), 10);
  const ceiling = Math.min(
    RETRY_BACKOFF_MAXIMUM_MS,
    baseDelayMs * 2 ** exponent,
  );
  return Math.max(1, Math.floor(ceiling * (0.5 + sample * 0.5)));
}

/** Ordinary-canary cadence after a human challenge. The first retry is never
 * earlier than 15 minutes; repeated challenges back off to at most six hours. */
export function computeHumanChallengeRetryDelayMs(
  consecutiveChallenges: number,
  random: () => number = Math.random,
): number {
  if (!Number.isSafeInteger(consecutiveChallenges) || consecutiveChallenges < 1)
    throw new Error("COLLECTOR_CHALLENGE_COUNT_INVALID");
  const sample = random();
  if (!Number.isFinite(sample) || sample < 0 || sample >= 1)
    throw new Error("COLLECTOR_CHALLENGE_RANDOM_INVALID");
  const exponential =
    HUMAN_CHALLENGE_BASE_RETRY_MS *
    2 ** Math.min(consecutiveChallenges - 1, 20);
  return Math.min(
    HUMAN_CHALLENGE_MAXIMUM_RETRY_MS,
    Math.floor(exponential * (1 + sample * 0.25)),
  );
}

function boundedRetryAfterMs(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    return RETRY_AFTER_MINIMUM_MS;
  }
  return Math.min(
    RETRY_AFTER_MAXIMUM_MS,
    Math.max(RETRY_AFTER_MINIMUM_MS, value),
  );
}
