import { randomUUID, timingSafeEqual } from "node:crypto";

import {
  type CanonicalPoemBindingV1,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
  PoemEnrichmentInputV2Schema,
  SourceAdmissionV2ResponseSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { z } from "zod";

import { collectionWorkKinds } from "../collection/collection-scheduler.js";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector.js";
import { assertCollectedArtifactBinding } from "../enrichment/local-enrichment-fanout.js";
import type { SolEnrichmentSeedPort } from "../enrichment/sol-coordinator.js";
import {
  SOL_ENRICHMENT_WORK_KIND,
  SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
} from "../enrichment/sol-coordinator.js";
import { ENRICHMENT_PROVIDER_SPECS } from "../enrichment/sol-runner.js";
import type { ArtifactStore } from "../persistence/artifact-store.js";
import type { Ledger } from "../persistence/ledger.js";
import { LostLeaseError } from "../persistence/ledger.js";
import {
  BASELINE_NO_TRANSLATION_PRIORITY,
  poemLengthPriorityBonus,
} from "../persistence/production-baseline-planner.js";
import type { WorkClaim, WorkItem } from "../persistence/schema.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";
import {
  bindCollectedPoem,
  type CatalogPoemMapping,
  prepareCollectedPoem,
  prepareSourceAdmission,
  prepareUnboundCollectedMapping,
} from "./corpus-import-actions.js";
import type { PublicationLane, PublicationSource } from "./publication-lane.js";
import {
  COLLECTION_PUBLICATION_WORK_KIND,
  prepareBoundEnrichmentPublication,
  prepareEnrichmentPublication,
  PUBLICATION_IMPLEMENTATION_VERSION,
  PUBLICATION_SCHEMA_VERSION,
} from "./publication-lane.js";

export const FANOUT_CONTROL_KIND = "fanout-reconciliation-control";
export const FANOUT_DETAIL_KIND = "fanout-collected-detail";
export const FANOUT_ENRICHMENT_KIND = "fanout-succeeded-enrichment";
export const FANOUT_SOL_KIND = "fanout-succeeded-sol";
export const FANOUT_IMPLEMENTATION_VERSION = "fanout-reconciler-v1";
export const FANOUT_SCHEMA_VERSION = "fanout@1";
const FANOUT_SOURCE_KINDS = [
  FANOUT_DETAIL_KIND,
  FANOUT_ENRICHMENT_KIND,
  FANOUT_SOL_KIND,
] as const;
const FANOUT_CLAIM_ORDER = [
  FANOUT_SOL_KIND,
  FANOUT_ENRICHMENT_KIND,
  FANOUT_DETAIL_KIND,
] as const;
const MAX_ATTEMPTS = 100;
// Resolution-cache work is local and bounded, but a detail admission may cross
// the network. Keep local batches small enough to remain well inside the lease
// while claiming network-bound details one at a time.
const CLAIM_BATCH_SIZE = 5;
// This pass only reads immutable local artifacts and writes compact local hash
// indexes. It must not inherit the much smaller network fanout batch: doing so
// made a 6k-artifact compatibility scan take hundreds of supervisor cycles.
const MATERIAL_BACKFILL_BATCH_SIZE = 500;
// Legacy completion discovery is an indexed, local-only scan. Keep it
// independent from the much smaller network fanout claim budget so replay can
// advance quickly without increasing source-admission or D1 concurrency.
const LEGACY_SOL_BACKFILL_BATCH_SIZE = 500;
const RESOLUTION_WAITER_BACKFILL_BATCH_SIZE = 100;
const MAX_REQUESTED_WORK_KEYS = 500;
const RequestedWorkKeysSchema = z
  .array(z.string().regex(/^[a-f\d]{64}$/))
  .max(MAX_REQUESTED_WORK_KEYS);
const FanoutRetryAtSchema = z.number().int().nonnegative();
const FanoutSourceLaneSchema = z.enum(["detail", "enrichment", "sol"]);
const MAX_TARGETED_CLAIMS_PER_CYCLE = 100;
// Detail fanout may perform a source-admission HTTP request. Bound it
// independently so a large translated-detail priority backlog cannot consume
// the entire cycle and starve local enrichment/publication replay.
const MAX_DETAIL_CLAIMS_PER_CYCLE = 5;
const LEASE_DURATION_MS = 60_000;
const LEASE_HEARTBEAT_MS = 20_000;
const TRANSLATED_SOURCE_ADMISSION_PRIORITY = 1_000;
const MAX_PUBLICATION_SUCCESSOR_DEPTH = 32;
const LEGACY_SOL_PIPELINE_VERSION = "sol-enrichment-v1";
// v1 could reach `complete` before later baseline imports transitioned their
// immutable legacy rows to succeeded/imported. A new checkpoint identity
// safely replays the completion stream once; idempotent fanout keys suppress
// already-seeded rows, while resolution still validates exact poem/revision
// lineage before any publication is created.
const LEGACY_SOL_BACKFILL_CHECKPOINT_KIND = "fanout-legacy-sol-backfill-v2";

const SourceSchema = z.strictObject({
  artifactHash: z.string().regex(/^[a-f\d]{64}$/),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
const PublicationSuccessorSchema = z.strictObject({
  successorWorkKey: z.string().regex(/^[a-f\d]{64}$/),
});
const ControlSchema = z.strictObject({ singleton: z.literal("fanout-v1") });
const SourceJobSchema = z.strictObject({
  lane: FanoutSourceLaneSchema,
  modelKey: z.string().min(1).max(100).optional(),
  source: SourceSchema,
});
const CursorSchema = z.strictObject({
  detail: z.number().int().nonnegative(),
  enrichment: z
    .record(z.string().min(1).max(100), z.number().int().nonnegative())
    .default({}),
  sol: z.number().int().nonnegative(),
  materialBackfill: z
    .strictObject({
      complete: z.boolean(),
      detailCursor: z
        .string()
        .regex(/^[a-f\d]{64}$/)
        .nullable(),
      solCursor: z
        .string()
        .regex(/^[a-f\d]{64}$/)
        .nullable(),
      metadataRevision: z.number().int().nonnegative().default(0),
    })
    .default({
      complete: false,
      detailCursor: null,
      metadataRevision: 0,
      solCursor: null,
    }),
  resolutionWaiterBackfill: z
    .strictObject({
      complete: z.boolean(),
      cutoffAt: z.number().int().nonnegative().nullable().default(null),
      cursor: z
        .strictObject({
          availableAt: z.number().int().nonnegative(),
          createdAt: z.number().int().nonnegative(),
          priority: z.number().int(),
          workKey: z.string().regex(/^[a-f\d]{64}$/),
        })
        .nullable(),
    })
    .default({ complete: false, cursor: null, cutoffAt: null }),
});
const LegacySolBackfillSchema = z.strictObject({
  complete: z.boolean(),
  eventCursor: z.number().int().nonnegative(),
});
const DetailPhaseSchema = z.strictObject({
  collectionBootstrap: z
    .strictObject({
      mapping: z.strictObject({
        authorId: z.string().min(1),
        authorNameArabic: z.string().min(1),
        canonicalPoemId: z.string().nullable(),
        poemId: z.string().min(1),
        sourceAuthorSlug: z.string().min(1),
        sourcePoemId: z.string().min(1),
      }),
      observedAt: z.iso.datetime(),
      writerEpoch: z.number().int().positive(),
    })
    .optional(),
  collectionPublicationWorkKey: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .optional(),
  solWorkKey: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .optional(),
  enrichmentWorkKeys: z
    .record(z.string().min(1).max(100), z.string().regex(/^[a-f\d]{64}$/))
    .default({}),
});
const SolPhaseSchema = z.strictObject({
  enrichmentPublicationWorkKey: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .optional(),
});
const EnrichmentArtifactInputSchema = z.looseObject({
  input: PoemEnrichmentInputSchema,
});
const DetailMaterialSchema = z.looseObject({
  source: z.looseObject({
    author: z.looseObject({ href: z.url() }),
    lines: z.array(z.string()).min(1).max(2_000),
    title: z.string().trim().min(1),
  }),
  sourceContext: z
    .strictObject({
      authorNameArabic: z.string().trim().min(1).max(512),
      refreshGeneration: z.string().regex(/^[\w.-]{1,64}$/),
    })
    .optional(),
});

export interface CollectedResolution {
  readonly binding?: CanonicalPoemBindingV1;
  readonly mapping: CatalogPoemMapping;
  readonly observedAt: string;
  readonly writerEpoch: number;
}

export interface EnrichmentPublicationResolution {
  readonly expectedPointerVersion: null | number;
  readonly poemId?: string;
  readonly sourceRevisionId?: string;
  readonly writerEpoch: number;
}

export type EnrichmentPublicationResolutionResult =
  | { readonly errorCode: string; readonly status: "conflict" }
  | { readonly status: "waiting" }
  | EnrichmentPublicationResolution;

export interface FanoutEnrichmentProfile {
  readonly implementationVersion: string;
  readonly modelKey: string;
  /** Omitted by publication-only runtimes, which may reuse but never seed paid work. */
  readonly seed?: SolEnrichmentSeedPort["seed"];
  readonly workKind: string;
}

export interface FanoutResolvers {
  readonly approvedSol?: (
    source: WorkItem,
    input: PoemEnrichmentInput,
    profile: Pick<
      FanoutEnrichmentProfile,
      "implementationVersion" | "modelKey" | "workKind"
    >,
    fanoutWorkKey: string,
  ) =>
    | { readonly binding: CanonicalPoemBindingV1; readonly status: "resolved" }
    | { readonly errorCode: string; readonly status: "conflict" }
    | { readonly status: "waiting" }
    | Promise<
        | {
            readonly binding: CanonicalPoemBindingV1;
            readonly status: "resolved";
          }
        | { readonly errorCode: string; readonly status: "conflict" }
        | { readonly status: "waiting" }
      >;
  readonly collected: (
    source: WorkItem,
    artifact: unknown,
  ) => CollectedResolution | null | Promise<CollectedResolution | null>;
  readonly enrichment: (
    source: WorkItem,
    input: PoemEnrichmentInput,
    profile: Pick<
      FanoutEnrichmentProfile,
      "implementationVersion" | "modelKey" | "workKind"
    >,
    fanoutWorkKey: string,
  ) =>
    | EnrichmentPublicationResolutionResult
    | null
    | Promise<EnrichmentPublicationResolutionResult | null>;
  readonly retireApprovedSol?: (
    input: PoemEnrichmentInput,
    modelKey: string,
    fanoutWorkKey: string,
  ) => void;
}

type FanoutBoundary =
  | "collection-publication"
  | "enrichment-publication"
  | "enrichment"
  | "legacy-sol-backfill"
  | "scan"
  | "sol";

interface FanoutSourceClaimResult {
  processed: number;
  processedDetails: number;
  processedSol: number;
  settledWorkKeys: readonly string[];
}

export interface FanoutReconcilerOptions {
  /** Test-only crash injection immediately after an idempotent downstream seed. */
  readonly afterBoundary?: ((boundary: FanoutBoundary) => void) | undefined;
  readonly artifacts: ArtifactStore;
  readonly batchSize?: number;
  readonly enrichment?: readonly FanoutEnrichmentProfile[];
  readonly ledger: Ledger;
  readonly owner?: string;
  readonly publication: Pick<
    PublicationLane,
    "admitSource" | "seedCollection" | "seedEnrichment"
  >;
  /** Disable only when the runtime owns periodic global lease recovery. */
  readonly recoverExpiredLeases?: boolean;
  readonly resolvers: FanoutResolvers;
}

export interface FanoutCycleSummary {
  readonly collectionPublicationsSeeded: number;
  readonly deadLettered: number;
  readonly detailCursor: number;
  readonly earliestWakeAt: null | number;
  readonly enrichmentCursors: Readonly<Record<string, number>>;
  readonly enrichmentPublicationsSeeded: number;
  readonly enrichmentSeeded: number;
  readonly pendingResolution: number;
  readonly ready: number;
  readonly retried: number;
  readonly scannedDetails: number;
  readonly scannedEnrichments: number;
  readonly scannedSol: number;
  readonly solCursor: number;
  readonly solSeeded: number;
  readonly translatedDetailsPrioritized: number;
}

export interface FanoutReconciliationPort {
  close(): Promise<void>;
  cycle(options?: {
    maximum?: number;
    now?: () => number;
    signal?: AbortSignal;
  }): Promise<FanoutCycleSummary>;
  requestSourceScan(now?: number): void;
  requestWork(workKeys: readonly string[]): void;
  status(now?: number): ReturnType<Ledger["availability"]>;
}

type FanoutCycleCounterKey = Exclude<
  keyof FanoutCycleSummary,
  | "detailCursor"
  | "earliestWakeAt"
  | "enrichmentCursors"
  | "ready"
  | "solCursor"
>;
type FanoutCycleCounters = {
  -readonly [Key in FanoutCycleCounterKey]: FanoutCycleSummary[Key];
};

export class FanoutReconciler implements FanoutReconciliationPort {
  readonly #afterBoundary: ((boundary: FanoutBoundary) => void) | undefined;
  readonly #artifacts: ArtifactStore;
  readonly #batchSize: number;
  readonly #controlWorkKey: string;
  readonly #enrichment: readonly FanoutEnrichmentProfile[];
  readonly #ledger: Ledger;
  readonly #recoverExpiredLeases: boolean;
  readonly #owner: string;
  readonly #publication: Pick<
    PublicationLane,
    "admitSource" | "seedCollection" | "seedEnrichment"
  >;
  readonly #resolvers: FanoutResolvers;
  #nextSourceKind = 0;
  #preferTargetedWork = true;
  readonly #registeredResolutionWorkKeys = new Set<string>();
  #sourceScanRequestGeneration = 0;
  #activeCycle: null | Promise<FanoutCycleSummary> = null;
  #closing = false;
  #ownedControl: null | WorkClaim = null;

  constructor(options: FanoutReconcilerOptions) {
    this.#afterBoundary = options.afterBoundary;
    this.#artifacts = options.artifacts;
    this.#batchSize = options.batchSize ?? 250;
    if (
      !Number.isSafeInteger(this.#batchSize) ||
      this.#batchSize < 1 ||
      this.#batchSize > 1_000
    )
      throw new Error("FANOUT_BATCH_SIZE_INVALID");
    this.#ledger = options.ledger;
    this.#recoverExpiredLeases = options.recoverExpiredLeases ?? true;
    this.#publication = options.publication;
    this.#resolvers = options.resolvers;
    this.#enrichment = options.enrichment ?? [];
    const [profile] = this.#enrichment;
    if (
      this.#enrichment.length !== 1 ||
      profile?.implementationVersion !==
        ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion ||
      profile.modelKey !== ENRICHMENT_PROVIDER_SPECS.sol.modelKey ||
      profile.workKind !== SOL_ENRICHMENT_WORK_KIND
    ) {
      throw new Error("FANOUT_CODEX_PROFILE_REQUIRED");
    }
    this.#owner =
      options.owner ?? `fanout-${String(process.pid)}-${randomUUID()}`;
    const input = ControlSchema.parse({ singleton: "fanout-v1" });
    this.#controlWorkKey = this.#ledger.seed({
      implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
      input,
      inputHash: inputHash(input),
      kind: FANOUT_CONTROL_KIND,
      priority: 1_000,
      schemaVersion: FANOUT_SCHEMA_VERSION,
    }).workKey;
  }

  status(now = Date.now()) {
    return this.#ledger.availability(
      [FANOUT_CONTROL_KIND, ...FANOUT_SOURCE_KINDS],
      now,
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
    );
  }

  requestSourceScan(now = Date.now()): void {
    this.#sourceScanRequestGeneration += 1;
    this.#ledger.expediteReadyWork(
      [this.#controlWorkKey],
      [FANOUT_CONTROL_KIND],
      1_000,
      "FANOUT_SOURCE_SUCCEEDED",
      now,
    );
  }

  requestWork(workKeys: readonly string[]): void {
    const keys = RequestedWorkKeysSchema.parse(workKeys);
    this.#ledger.prioritizeFanoutWork(keys, FANOUT_SOURCE_KINDS);
  }

  async cycle(
    options: {
      maximum?: number;
      now?: () => number;
      signal?: AbortSignal;
    } = {},
  ): Promise<FanoutCycleSummary> {
    if (this.#closing) throw new Error("FANOUT_CLOSED");
    if (this.#activeCycle !== null) throw new Error("FANOUT_CYCLE_ACTIVE");
    this.#releaseOwnedControl();
    const active = this.#runCycle(options);
    this.#activeCycle = active;
    try {
      return await active;
    } finally {
      this.#activeCycle = null;
    }
  }

  async close(): Promise<void> {
    this.#closing = true;
    const failures: unknown[] = [];
    try {
      await this.#activeCycle;
    } catch (error) {
      failures.push(error);
    }
    try {
      // Never release ownership while the scan can still mutate its cursor.
      // A failed release rejects close so the supervisor retains RUN.lock.
      this.#releaseOwnedControl();
    } catch (error) {
      failures.push(error);
    }
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1)
      throw new AggregateError(failures, "Fanout drain and settlement failed");
  }

  #releaseOwnedControl(): void {
    const claim = this.#ownedControl;
    if (claim === null) return;
    const current = this.#ledger.get(claim.work.workKey);
    if (
      current?.state === "running" &&
      current.leaseOwner === this.#owner &&
      timingSafeEqual(
        Buffer.from(sha256(current.leaseToken ?? ""), "hex"),
        Buffer.from(sha256(claim.leaseToken), "hex"),
      ) &&
      current.leaseEpoch === claim.leaseEpoch
    ) {
      const now = Date.now();
      this.#renewAfterSuspend(claim, now);
      this.#ledger.operatorRelease(claim, "FANOUT_CYCLE_SETTLEMENT", now);
    }
    this.#ownedControl = null;
  }

  async #runCycle(options: {
    maximum?: number;
    now?: () => number;
    signal?: AbortSignal;
  }): Promise<FanoutCycleSummary> {
    this.#registeredResolutionWorkKeys.clear();
    const now = options.now ?? Date.now;
    const maximum = options.maximum ?? this.#batchSize;
    if (!Number.isSafeInteger(maximum) || maximum < 1)
      throw new Error("FANOUT_MAXIMUM_INVALID");
    const summary = {
      collectionPublicationsSeeded: 0,
      deadLettered: 0,
      enrichmentPublicationsSeeded: 0,
      enrichmentSeeded: 0,
      pendingResolution: 0,
      retried: 0,
      scannedDetails: 0,
      scannedEnrichments: 0,
      scannedSol: 0,
      solSeeded: 0,
      translatedDetailsPrioritized: 0,
    };
    if (this.#recoverExpiredLeases) this.#ledger.recoverExpired(now());
    let backfilled = 0;
    for (const kind of FANOUT_CLAIM_ORDER) {
      if (backfilled >= MAX_TARGETED_CLAIMS_PER_CYCLE) break;
      backfilled += this.#ledger.backfillFanoutPriorityWork(
        [kind],
        MAX_TARGETED_CLAIMS_PER_CYCLE - backfilled,
        now(),
      );
    }
    // Drain work that is already ready before the producer scan. Scans can be
    // slow on a large historical ledger, and must not delay a queued Sol item
    // from registering its event-driven resolution demand.
    const targetedLimit =
      maximum === 1
        ? this.#preferTargetedWork
          ? 1
          : 0
        : Math.min(maximum - 1, MAX_TARGETED_CLAIMS_PER_CYCLE);
    const requestedWorkKeys: string[] = [];
    for (const kind of FANOUT_CLAIM_ORDER) {
      if (requestedWorkKeys.length >= targetedLimit) break;
      requestedWorkKeys.push(
        ...this.#ledger.listFanoutPriorityWorkKeys(
          [kind],
          targetedLimit - requestedWorkKeys.length,
          now(),
        ),
      );
    }
    if (maximum === 1 && requestedWorkKeys.length > 0)
      this.#preferTargetedWork = false;
    else if (maximum === 1 && !this.#preferTargetedWork)
      this.#preferTargetedWork = true;
    const targetedMaximum = requestedWorkKeys.length;
    const targeted = await this.#claimSources(
      targetedMaximum,
      0,
      0,
      0,
      summary,
      now,
      requestedWorkKeys,
      options.signal,
    );
    this.#ledger.acknowledgeFanoutPriorityWork(targeted.settledWorkKeys);
    const ordinary = await this.#claimSources(
      maximum,
      targeted.processed,
      targeted.processedSol,
      targeted.processedDetails,
      summary,
      now,
      undefined,
      options.signal,
    );
    this.#ledger.acknowledgeFanoutPriorityWork(ordinary.settledWorkKeys);
    const { processed, processedDetails, processedSol } = ordinary;
    const control = options.signal?.aborted
      ? null
      : this.#ledger.claim(
          this.#owner,
          now(),
          LEASE_DURATION_MS,
          [FANOUT_CONTROL_KIND],
          {
            implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
            schemaVersion: FANOUT_SCHEMA_VERSION,
          },
        );
    if (control) {
      this.#ownedControl = control;
      try {
        await this.#withLeaseHeartbeat(control, now, () =>
          this.#scan(control, summary, now),
        );
      } catch (error) {
        if (!(error instanceof LostLeaseError)) {
          this.#ledger.retry(
            control,
            "FANOUT_SCAN_EXCEPTION",
            now() + retryDelay(control.work.attemptCount),
            now(),
          );
          summary.retried += 1;
        }
      }
    }
    // Preserve same-cycle processing for source jobs discovered by this scan,
    // while sharing the cycle-wide budget and detail-admission cap with the
    // pre-scan drain.
    const discovered = await this.#claimSources(
      maximum,
      processed,
      processedSol,
      processedDetails,
      summary,
      now,
      undefined,
      options.signal,
    );
    this.#ledger.acknowledgeFanoutPriorityWork(discovered.settledWorkKeys);
    const cursor = await this.#cursor();
    const availability = this.status(now());
    await Promise.resolve();
    return {
      ...summary,
      detailCursor: cursor.detail,
      earliestWakeAt: availability.earliestAvailableAt,
      enrichmentCursors: cursor.enrichment,
      ready: availability.ready,
      solCursor: cursor.sol,
    };
  }

  async #claimSources(
    maximum: number,
    alreadyProcessed: number,
    alreadyProcessedSol: number,
    alreadyProcessedDetails: number,
    summary: FanoutCycleCounters,
    now: () => number,
    includedWorkKeys?: readonly string[],
    signal?: AbortSignal,
  ): Promise<FanoutSourceClaimResult> {
    if (maximum === 0)
      return {
        processed: alreadyProcessed,
        processedDetails: alreadyProcessedDetails,
        processedSol: alreadyProcessedSol,
        settledWorkKeys: [],
      };
    let emptyKinds = 0;
    let processed = alreadyProcessed;
    let processedDetails = alreadyProcessedDetails;
    let processedSol = alreadyProcessedSol;
    const settledWorkKeys: string[] = [];
    while (
      processed < maximum &&
      emptyKinds < FANOUT_CLAIM_ORDER.length &&
      !signal?.aborted
    ) {
      const kind = FANOUT_CLAIM_ORDER[this.#nextSourceKind];
      if (kind === undefined) throw new Error("FANOUT_CLAIM_CURSOR_INVALID");
      this.#nextSourceKind =
        (this.#nextSourceKind + 1) % FANOUT_CLAIM_ORDER.length;
      if (
        kind === FANOUT_DETAIL_KIND &&
        processedDetails >= MAX_DETAIL_CLAIMS_PER_CYCLE
      ) {
        emptyKinds += 1;
        continue;
      }
      const claimedAt = now();
      const claims = this.#ledger.claimMany(
        this.#owner,
        claimedAt,
        LEASE_DURATION_MS,
        [kind],
        Math.min(
          kind === FANOUT_DETAIL_KIND ? 1 : CLAIM_BATCH_SIZE,
          maximum - processed,
        ),
        {
          implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
          schemaVersion: FANOUT_SCHEMA_VERSION,
        },
        [],
        includedWorkKeys,
      );
      if (claims.length === 0) {
        emptyKinds += 1;
        continue;
      }
      emptyKinds = 0;
      processed += claims.length;
      if (kind === FANOUT_DETAIL_KIND) processedDetails += claims.length;
      if (kind === FANOUT_SOL_KIND) processedSol += claims.length;
      let batchLeaseExpiresAt = claimedAt + LEASE_DURATION_MS;
      const staleWorkKeys = new Set<string>();
      const outstanding = new Map(
        claims.map((claim) => [claim.work.workKey, claim]),
      );
      let batchLeaseFailure: unknown = null;
      const heartbeat = setInterval(() => {
        try {
          const heartbeatAt = now();
          const renewal = this.#ledger.renewMany(
            outstandingClaims(outstanding),
            heartbeatAt,
            LEASE_DURATION_MS,
            true,
          );
          for (const workKey of renewal.stale) {
            staleWorkKeys.add(workKey);
            outstanding.delete(workKey);
          }
          if (renewal.renewed.length > 0)
            batchLeaseExpiresAt = heartbeatAt + LEASE_DURATION_MS;
        } catch (error) {
          batchLeaseFailure = error;
        }
      }, LEASE_HEARTBEAT_MS);
      heartbeat.unref();
      try {
        for (const claim of claims) {
          if (signal?.aborted) {
            const releaseAt = now();
            for (const outstandingClaim of outstanding.values()) {
              this.#ledger.operatorRelease(
                outstandingClaim,
                "FANOUT_SHUTDOWN",
                releaseAt,
                releaseAt,
              );
            }
            outstanding.clear();
            break;
          }
          if (staleWorkKeys.has(claim.work.workKey)) continue;
          let settled = false;
          try {
            // The batch heartbeat protects both this operation and every claim
            // still queued behind it, including during a long async boundary.
            // eslint-disable-next-line no-await-in-loop -- Each claimed source must finish its durable reconciliation before the next claim starts.
            const result = await this.#reconcile(claim, summary, now, signal);
            if (batchLeaseFailure !== null)
              throw batchLeaseFailure instanceof Error
                ? batchLeaseFailure
                : new Error("FANOUT_BATCH_LEASE_HEARTBEAT_FAILED", {
                    cause: batchLeaseFailure,
                  });
            if (result !== "complete") {
              if (result === "event-driven-wait")
                this.#registeredResolutionWorkKeys.add(claim.work.workKey);
              const releaseAt = now();
              this.#renewAfterSuspend(claim, releaseAt);
              this.#ledger.operatorRelease(
                claim,
                "FANOUT_RESOLUTION_PENDING",
                releaseAt,
                releaseAt +
                  (result === "event-driven-wait"
                    ? 6 * 60 * 60_000
                    : 5 * 60_000),
              );
            } else {
              settled = true;
            }
          } catch (error) {
            if (!(error instanceof LostLeaseError)) {
              const code = fanoutErrorCode(error);
              const failedAt = now();
              this.#renewAfterSuspend(claim, failedAt);
              if (
                error instanceof TerminalFanoutError ||
                (!(error instanceof RetryableFanoutError) &&
                  claim.work.attemptCount >= MAX_ATTEMPTS)
              ) {
                this.#ledger.deadLetter(claim, code, failedAt);
                summary.deadLettered += 1;
              } else {
                this.#ledger.retry(
                  claim,
                  code,
                  error instanceof RetryableFanoutError
                    ? Math.max(error.retryAt, failedAt + 1_000)
                    : failedAt + retryDelay(claim.work.attemptCount),
                  failedAt,
                );
                summary.retried += 1;
              }
            }
          } finally {
            outstanding.delete(claim.work.workKey);
            if (settled) settledWorkKeys.push(claim.work.workKey);
          }
          // Resolution-cache and artifact operations are synchronous. Yield per
          // item while retaining the cheaper shared durable claim boundary.
          // eslint-disable-next-line no-await-in-loop -- Cooperative scheduling is part of the bounded fanout admission contract.
          await new Promise<void>((resolvePromise) => {
            setImmediate(resolvePromise);
          });
          const renewalAt = now();
          if (renewalAt + LEASE_HEARTBEAT_MS >= batchLeaseExpiresAt) {
            const renewal = this.#ledger.renewMany(
              outstandingClaims(outstanding),
              renewalAt,
              LEASE_DURATION_MS,
              true,
            );
            for (const workKey of renewal.stale) {
              staleWorkKeys.add(workKey);
              outstanding.delete(workKey);
            }
            if (renewal.renewed.length > 0)
              batchLeaseExpiresAt = renewalAt + LEASE_DURATION_MS;
          }
        }
        if (batchLeaseFailure !== null)
          throw batchLeaseFailure instanceof Error
            ? batchLeaseFailure
            : new Error("FANOUT_BATCH_LEASE_HEARTBEAT_FAILED", {
                cause: batchLeaseFailure,
              });
      } finally {
        clearInterval(heartbeat);
      }
    }
    return { processed, processedDetails, processedSol, settledWorkKeys };
  }

  async #withLeaseHeartbeat<T>(
    claim: WorkClaim,
    now: () => number,
    operation: () => Promise<T>,
  ): Promise<T> {
    let leaseFailure: unknown = null;
    const heartbeat = setInterval(() => {
      try {
        this.#ledger.renew(claim, now(), LEASE_DURATION_MS, true);
      } catch (error) {
        leaseFailure = error;
      }
    }, LEASE_HEARTBEAT_MS);
    heartbeat.unref();
    try {
      const result = await operation();
      this.#renewAfterSuspend(claim, now());
      leaseFailure = null;
      if (leaseFailure !== null) {
        throw leaseFailure instanceof Error
          ? leaseFailure
          : new Error("FANOUT_LEASE_HEARTBEAT_FAILED", {
              cause: leaseFailure,
            });
      }
      return result;
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #scan(
    claim: WorkClaim,
    summary: {
      scannedDetails: number;
      scannedEnrichments: number;
      scannedSol: number;
      translatedDetailsPrioritized: number;
    },
    now: () => number,
  ): Promise<void> {
    const requestGeneration = this.#sourceScanRequestGeneration;
    const cursor = await this.#cursor();
    const details = this.#ledger.listSucceededAfter(
      cursor.detail,
      [collectionWorkKinds().poemDetail],
      this.#batchSize,
      {
        implementationVersion: collectorImplementationVersion(),
        schemaVersion: collectorSchemaVersion(),
      },
    );
    summary.translatedDetailsPrioritized += await this.#seedSources(
      "detail",
      details.items.map(({ work }) => work),
    );
    const enrichmentCursors = { ...cursor.enrichment };
    let solCursor = cursor.sol;
    for (const profile of this.#enrichment) {
      const isSol = profile.modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey;
      const succeeded = this.#ledger.listSucceededAfter(
        isSol
          ? Math.max(cursor.sol, cursor.enrichment[profile.modelKey] ?? 0)
          : (cursor.enrichment[profile.modelKey] ?? 0),
        [profile.workKind],
        this.#batchSize,
        { implementationVersion: profile.implementationVersion },
      );
      // eslint-disable-next-line no-await-in-loop -- Each provider completion page is durably indexed before its cursor advances.
      summary.translatedDetailsPrioritized += await this.#seedSources(
        isSol ? "sol" : "enrichment",
        succeeded.items.map(({ work }) => work),
        profile.modelKey,
      );
      if (isSol) {
        solCursor = succeeded.cursor;
        summary.scannedSol += succeeded.items.length;
      } else {
        summary.scannedEnrichments += succeeded.items.length;
      }
      enrichmentCursors[profile.modelKey] = succeeded.cursor;
    }
    await this.#backfillLegacySol(claim, summary, now);
    await this.#backfillLegacySol(claim, summary, now, "sol-word-gloss-v2");
    const metadataRevision = this.#ledger.sourceAuthorMetadataRevision();
    const backfillState =
      cursor.materialBackfill.metadataRevision === metadataRevision
        ? cursor.materialBackfill
        : {
            complete: false,
            detailCursor: null,
            metadataRevision,
            solCursor: cursor.materialBackfill.solCursor,
          };
    const materialBackfill = backfillState.complete
      ? backfillState
      : await this.#backfillDetailMaterialIndex(backfillState);
    const resolutionWaiterBackfill = cursor.resolutionWaiterBackfill.complete
      ? cursor.resolutionWaiterBackfill
      : await this.#backfillResolutionWaiters(
          cursor.resolutionWaiterBackfill,
          now,
        );
    this.#afterBoundary?.("scan");
    summary.scannedDetails += details.items.length;
    const next = CursorSchema.parse({
      detail: details.cursor,
      enrichment: enrichmentCursors,
      materialBackfill,
      resolutionWaiterBackfill,
      sol: solCursor,
    });
    // An idle poll must not rewrite and re-verify an identical cursor artifact
    // or append a redundant checkpoint/event pair. Producer-driven fanout will
    // eventually remove this poll; until then, make empty scans read-only.
    if (canonicalJson(next) !== canonicalJson(cursor)) {
      const artifact = await this.#artifacts.put(`${canonicalJson(next)}\n`);
      this.#renewAfterSuspend(claim, now());
      this.#ledger.checkpoint(
        claim,
        { artifactHash: artifact.hash, kind: "fanout-cursor", payload: next },
        now(),
      );
    }
    const completedAt = now();
    this.#renewAfterSuspend(claim, completedAt);
    if (requestGeneration === this.#sourceScanRequestGeneration) {
      this.#ledger.retry(
        claim,
        "FANOUT_POLL",
        completedAt + 30_000,
        completedAt,
      );
    } else {
      // A completion may arrive after this scan captured its high watermark.
      // Preserve the request while the control item is leased by releasing it
      // immediately for one cursor-driven replay.
      this.#ledger.operatorRelease(
        claim,
        "FANOUT_SOURCE_SUCCEEDED",
        completedAt,
        completedAt,
      );
    }
  }

  async #seedSources(
    lane: "detail" | "enrichment" | "sol",
    works: readonly WorkItem[],
    modelKey?: string,
  ): Promise<number> {
    const definitions = works.map((work) => {
      if (!work.outputArtifactHash)
        throw new Error("FANOUT_SOURCE_ARTIFACT_MISSING");
      const input = SourceJobSchema.parse({
        lane,
        ...(modelKey ? { modelKey } : {}),
        source: {
          artifactHash: work.outputArtifactHash,
          workKey: work.workKey,
        },
      });
      return {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input,
        inputHash: inputHash(input),
        kind:
          lane === "detail"
            ? FANOUT_DETAIL_KIND
            : lane === "sol"
              ? FANOUT_SOL_KIND
              : FANOUT_ENRICHMENT_KIND,
        priority:
          lane === "detail"
            ? Math.max(BASELINE_NO_TRANSLATION_PRIORITY, work.priority)
            : work.priority,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      };
    });
    const seeded = this.#ledger.seedMany(definitions);
    let prioritized = 0;
    for (const [index, result] of seeded.entries()) {
      const source = works[index];
      if (!source) throw new Error("FANOUT_SOURCE_INDEX_MISMATCH");
      if (lane === "detail") {
        // eslint-disable-next-line no-await-in-loop -- Bounded completion pages are indexed before advancing their durable cursor.
        await this.#indexDetailMaterial(result.workKey, source);
      } else if (lane === "sol") {
        const input = PoemEnrichmentInputSchema.safeParse(source.input);
        if (input.success) {
          this.#indexReusableEnrichment(
            result.workKey,
            source,
            modelKey ?? ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            input.data,
          );
          prioritized += this.#expediteMatchingDetail(
            input.data,
            source.priority,
          );
        }
      }
    }
    return prioritized;
  }

  async #reconcile(
    claim: WorkClaim,
    summary: {
      collectionPublicationsSeeded: number;
      deadLettered: number;
      enrichmentPublicationsSeeded: number;
      pendingResolution: number;
      retried: number;
      enrichmentSeeded: number;
      solSeeded: number;
      translatedDetailsPrioritized: number;
    },
    now: () => number,
    signal?: AbortSignal,
  ): Promise<"complete" | "event-driven-wait" | "polled-wait"> {
    const job = SourceJobSchema.parse(claim.work.input);
    if (job.lane === "detail") {
      const source = await this.#source(job.source);
      const sourceContents = await this.#artifacts.read(
        job.source.artifactHash,
      );
      const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
      let resolution = await this.#resolvers.collected(source, artifact);
      let phase = await this.#detailPhase(claim.work.workKey);
      if (!resolution?.binding && phase.collectionPublicationWorkKey) {
        const publication = this.#resolveCollectionPublication(
          phase.collectionPublicationWorkKey,
        );
        if (publication.state === "terminal") {
          throw new TerminalFanoutError(publication.errorCode);
        }
        if (publication.workKey !== phase.collectionPublicationWorkKey) {
          phase = {
            ...phase,
            collectionPublicationWorkKey: publication.workKey,
          };
          await this.#saveDetailPhase(claim, phase, now);
        }
        return this.#waitResolution(summary, false);
      }
      if (resolution && !resolution.binding) {
        phase = await this.#seedCollectionBootstrap(
          claim,
          job.source,
          artifact,
          resolution,
          phase,
          summary,
          now,
        );
        return this.#waitResolution(summary, false);
      }
      if (!resolution?.binding) {
        const admissionItem = prepareSourceAdmission(artifact);
        const admission = await this.#publication.admitSource(
          admissionItem,
          signal,
        );
        if (admission.state !== "confirmed") {
          if (
            admission.state === "auth_wait" ||
            admission.state === "network_wait" ||
            admission.state === "retry_wait" ||
            admission.state === "service_wait"
          ) {
            throw new RetryableFanoutError(
              admission.errorCode,
              admission.retryAt,
            );
          }
          if (admission.state === "conflict") {
            throw new RetryableFanoutError(
              admission.errorCode,
              now() + retryDelay(claim.work.attemptCount),
            );
          }
          throw new TerminalFanoutError(admission.errorCode);
        }
        const response = SourceAdmissionV2ResponseSchema.parse(
          admission.result,
        );
        const outcome = response.results[0];
        if (!outcome || response.results.length !== 1) {
          throw new Error("SOURCE_ADMISSION_RESPONSE_MISMATCH");
        }
        if (outcome.status === "rejected") {
          const bootstrapMapping =
            outcome.code === "AUTHOR_NOT_FOUND"
              ? prepareUnboundCollectedMapping(
                  artifact,
                  this.#ledger.sourceAuthorMetadata(
                    admissionItem.sourceAuthorUrl,
                  )?.authorNameArabic,
                )
              : null;
          if (bootstrapMapping) {
            phase = await this.#seedCollectionBootstrap(
              claim,
              job.source,
              artifact,
              {
                mapping: bootstrapMapping,
                observedAt: response.observedAt,
                writerEpoch: response.writerEpoch,
              },
              phase,
              summary,
              now,
            );
            return this.#waitResolution(summary, false);
          }
          if (outcome.retryable) {
            throw new RetryableFanoutError(
              `SOURCE_ADMISSION_${outcome.code}`,
              now() + retryDelay(claim.work.attemptCount),
            );
          }
          throw new TerminalFanoutError(`SOURCE_ADMISSION_${outcome.code}`);
        }
        resolution = {
          binding: outcome.binding,
          mapping: {
            authorId: outcome.binding.authorId,
            authorNameArabic: outcome.binding.authorNameArabic,
            canonicalPoemId: outcome.binding.poemId,
            poemId: outcome.binding.poemId,
            sourceAuthorSlug: admissionItem.sourceAuthorId,
            sourcePoemId: admissionItem.externalPoemId,
          },
          observedAt: response.observedAt,
          writerEpoch: response.writerEpoch,
        };
      }
      const prepared = prepareCollectedPoem(
        artifact,
        resolution.mapping,
        resolution.observedAt,
        resolution.writerEpoch,
      );
      if (!resolution.binding) {
        throw new Error("SOURCE_ADMISSION_BINDING_MISSING");
      }
      const boundInput = bindCollectedPoem(
        prepared.enrichmentInput,
        resolution.binding,
      );
      for (const profile of this.#enrichment) {
        const isSol =
          profile.modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey;
        const existing = isSol
          ? (phase.solWorkKey ?? phase.enrichmentWorkKeys[profile.modelKey])
          : phase.enrichmentWorkKeys[profile.modelKey];
        if (existing) continue;
        if (isSol) {
          const reusable = this.#ledger.reusableEnrichmentForMaterial(
            profile.modelKey,
            sha256(sourcePromptMaterialHashBody(boundInput)),
          );
          if (reusable && reusable.fanoutState !== "dead_letter") {
            // Validate the exact approved artifact against the new canonical
            // binding before atomically attaching provenance and seeding its
            // direct publication. This reuses model work; it never copies or
            // approximately joins translation payloads.
            // eslint-disable-next-line no-await-in-loop -- Each provider boundary is validated and durably attached before continuing.
            const reusableContents = await this.#artifacts.read(
              reusable.outputArtifactHash,
            );
            const reusableArtifact: unknown = JSON.parse(
              reusableContents.toString("utf8"),
            );
            prepareBoundEnrichmentPublication(
              reusableArtifact,
              resolution.binding,
              reusable.translationWorkKey,
            );
            this.#ledger.attachApprovedBindingAndSeedPublication(
              reusable.translationWorkKey,
              reusable.outputArtifactHash,
              resolution.binding,
              {
                implementationVersion:
                  SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
                priority: claim.work.priority,
              },
              now(),
            );
            this.#afterBoundary?.("enrichment-publication");
            summary.enrichmentPublicationsSeeded += 1;
            continue;
          }
        }
        if (!profile.seed) continue;
        const seeded = profile.seed(
          boundInput,
          BASELINE_NO_TRANSLATION_PRIORITY +
            poemLengthPriorityBonus(boundInput.linesArabic.length),
        );
        this.#afterBoundary?.(isSol ? "sol" : "enrichment");
        phase = {
          ...phase,
          ...(isSol ? { solWorkKey: seeded.workKey } : {}),
          enrichmentWorkKeys: {
            ...phase.enrichmentWorkKeys,
            [profile.modelKey]: seeded.workKey,
          },
        };
        // eslint-disable-next-line no-await-in-loop -- Each provider seed boundary is checkpointed before the next provider can be admitted.
        await this.#saveDetailPhase(claim, phase, now);
        if (seeded.inserted) {
          summary.enrichmentSeeded += 1;
          if (isSol) summary.solSeeded += 1;
        }
      }
    } else {
      // The immutable source work already contains the exact enrichment input.
      // Resolve from it before touching the much larger output artifact so an
      // unresolved backlog performs no artifact I/O while it registers once in
      // the durable waiter index.
      const source = await this.#source(job.source, false);
      if (PoemEnrichmentInputV2Schema.safeParse(source.input).success) {
        // Bound completions atomically seed their direct-publication work item
        // in the ledger. Retire this legacy scan record without creating a
        // second, pointer-resolution-dependent publication path.
        const receipt = await this.#artifacts.put(`${canonicalJson(job)}\n`);
        this.#renewAfterSuspend(claim, now());
        this.#ledger.succeed(claim, receipt.hash, now());
        return "complete";
      }
      const input = PoemEnrichmentInputSchema.parse(source.input);
      if (job.lane === "sol") {
        summary.translatedDetailsPrioritized += this.#expediteMatchingDetail(
          input,
          source.priority,
        );
      }
      const modelKey =
        job.modelKey ??
        (job.lane === "sol" ? ENRICHMENT_PROVIDER_SPECS.sol.modelKey : null);
      const profile = this.#enrichment.find(
        (candidate) => candidate.modelKey === modelKey,
      );
      if (profile?.workKind !== source.kind)
        throw new Error("FANOUT_ENRICHMENT_PROFILE_MISMATCH");
      // Historical fanout rows used both `sol` and `enrichment` as the lane
      // label for the same Codex profile. Model identity is the authority;
      // routing on the old label can recreate the retired generic publisher.
      const isSolProfile =
        profile.modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey;
      if (isSolProfile) {
        const publishedAttachment =
          this.#ledger.approvedPublicationAttachmentForTranslation(
            job.source.workKey,
            job.source.artifactHash,
          );
        if (publishedAttachment) {
          await this.#closePublishedSolSibling(
            claim,
            job,
            source,
            input,
            profile,
            publishedAttachment,
            now,
          );
          return "complete";
        }
      }
      if (isSolProfile && this.#resolvers.approvedSol) {
        const approved = await this.#resolvers.approvedSol(
          source,
          input,
          {
            implementationVersion: profile.implementationVersion,
            modelKey: profile.modelKey,
            workKind: profile.workKind,
          },
          claim.work.workKey,
        );
        if (approved.status === "waiting") {
          return this.#waitResolution(summary, true);
        }
        if (approved.status === "conflict") {
          this.#renewAfterSuspend(claim, now());
          this.#ledger.deadLetter(claim, approved.errorCode, now());
          summary.deadLettered += 1;
          return "complete";
        }
        const sourceContents = await this.#artifacts.read(
          job.source.artifactHash,
        );
        const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
        const parsed = EnrichmentArtifactInputSchema.parse(artifact);
        if (canonicalJson(parsed.input) !== canonicalJson(input))
          throw new Error("FANOUT_ENRICHMENT_INPUT_MISMATCH");
        try {
          prepareBoundEnrichmentPublication(
            artifact,
            approved.binding,
            job.source.workKey,
          );
        } catch {
          // This branch only handles a historical, already-completed source.
          // If it cannot satisfy the direct contract, retain its immutable
          // artifact and terminate the recovery item instead of recreating
          // the retired generic publication pipeline.
          this.#renewAfterSuspend(claim, now());
          this.#ledger.deadLetter(
            claim,
            "LEGACY_GENERIC_PUBLICATION_RETIRED",
            now(),
          );
          summary.deadLettered += 1;
          return "complete";
        }
        this.#ledger.attachApprovedBindingAndSeedPublication(
          job.source.workKey,
          job.source.artifactHash,
          approved.binding,
          {
            implementationVersion:
              SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
            priority: claim.work.priority,
          },
          now(),
        );
        this.#indexOrWakeReusableEnrichment(
          claim.work.workKey,
          source,
          profile.modelKey,
          input,
          now(),
        );
        this.#afterBoundary?.("enrichment-publication");
        summary.enrichmentPublicationsSeeded += 1;
        const receipt = await this.#artifacts.put(`${canonicalJson(job)}\n`);
        this.#renewAfterSuspend(claim, now());
        this.#ledger.succeed(claim, receipt.hash, now());
        return "complete";
      }
      const resolution = await this.#resolvers.enrichment(
        source,
        input,
        {
          implementationVersion: profile.implementationVersion,
          modelKey: profile.modelKey,
          workKind: profile.workKind,
        },
        claim.work.workKey,
      );
      if (
        !resolution ||
        ("status" in resolution && resolution.status === "waiting")
      ) {
        return this.#waitResolution(summary, true);
      }
      if ("status" in resolution) {
        this.#renewAfterSuspend(claim, now());
        this.#ledger.deadLetter(claim, resolution.errorCode, now());
        summary.deadLettered += 1;
        return "complete";
      }
      const sourceContents = await this.#artifacts.read(
        job.source.artifactHash,
      );
      const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
      const parsed = EnrichmentArtifactInputSchema.parse(artifact);
      if (canonicalJson(parsed.input) !== canonicalJson(input))
        throw new Error("FANOUT_ENRICHMENT_INPUT_MISMATCH");
      let phase = await this.#solPhase(claim.work.workKey);
      if (phase.enrichmentPublicationWorkKey) {
        const priorPublication = this.#ledger.get(
          phase.enrichmentPublicationWorkKey,
        );
        if (
          priorPublication?.state === "dead_letter" &&
          priorPublication.lastErrorCode === "CORPUS_IMPORT_REJECTED"
        ) {
          // The historical generic action was built against a local poem id.
          // Keep its immutable checkpoint and dead letter as audit evidence,
          // but supersede that phase in memory so the newly confirmed
          // canonical resolution derives a different deterministic action.
          // The following checkpoint becomes the durable latest phase; a crash
          // before it is written merely re-seeds the same replacement key.
          phase = {};
        }
      }
      if (!phase.enrichmentPublicationWorkKey) {
        const action = prepareEnrichmentPublication(artifact, {
          ...resolution,
          source: job.source,
          taskKey: job.source.workKey,
        });
        const seeded = await this.#publication.seedEnrichment(
          action,
          job.source,
          claim.work.priority,
        );
        this.#afterBoundary?.("enrichment-publication");
        phase = { enrichmentPublicationWorkKey: seeded.workKey };
        await this.#saveSolPhase(claim, phase, now);
        if (seeded.inserted) summary.enrichmentPublicationsSeeded += 1;
      }
    }
    const receipt = await this.#artifacts.put(`${canonicalJson(job)}\n`);
    this.#renewAfterSuspend(claim, now());
    this.#ledger.succeed(claim, receipt.hash, now());
    return "complete";
  }

  async #seedCollectionBootstrap(
    claim: WorkClaim,
    source: PublicationSource,
    artifact: unknown,
    resolution: CollectedResolution,
    phase: z.infer<typeof DetailPhaseSchema>,
    summary: { collectionPublicationsSeeded: number },
    now: () => number,
  ): Promise<z.infer<typeof DetailPhaseSchema>> {
    if (resolution.binding || phase.collectionPublicationWorkKey) return phase;
    let nextPhase = phase;
    if (!nextPhase.collectionBootstrap) {
      nextPhase = {
        ...phase,
        collectionBootstrap: {
          mapping: {
            ...resolution.mapping,
            canonicalPoemId: resolution.mapping.canonicalPoemId ?? null,
          },
          observedAt: resolution.observedAt,
          writerEpoch: resolution.writerEpoch,
        },
      };
      // The immutable bootstrap inputs must precede the downstream seed. A
      // crash after seeding can then replay the identical publication key even
      // if a refreshed production snapshot has a new timestamp or epoch.
      await this.#saveDetailPhase(claim, nextPhase, now);
    }
    const bootstrap = nextPhase.collectionBootstrap;
    if (!bootstrap) throw new Error("COLLECTION_BOOTSTRAP_PHASE_MISSING");
    const prepared = prepareCollectedPoem(
      artifact,
      bootstrap.mapping,
      bootstrap.observedAt,
      bootstrap.writerEpoch,
    );
    const [seeded] = await this.#publication.seedCollection(
      [{ action: prepared.stageAction, sources: [source] }],
      claim.work.priority,
    );
    if (!seeded) throw new Error("COLLECTION_PUBLICATION_SEED_MISSING");
    this.#afterBoundary?.("collection-publication");
    const next = {
      ...nextPhase,
      collectionPublicationWorkKey: seeded.workKey,
    };
    await this.#saveDetailPhase(claim, next, now);
    if (seeded.inserted) summary.collectionPublicationsSeeded += 1;
    return next;
  }

  #resolveCollectionPublication(
    initialWorkKey: string,
  ):
    | { readonly errorCode: string; readonly state: "terminal" }
    | { readonly state: "waiting"; readonly workKey: string } {
    const visited = new Set<string>();
    let workKey = initialWorkKey;
    for (let depth = 0; depth < MAX_PUBLICATION_SUCCESSOR_DEPTH; depth += 1) {
      if (visited.has(workKey)) {
        return {
          errorCode: "COLLECTION_PUBLICATION_SUCCESSOR_CYCLE",
          state: "terminal",
        };
      }
      visited.add(workKey);
      const work = this.#ledger.get(workKey);
      if (
        work?.kind !== COLLECTION_PUBLICATION_WORK_KIND ||
        work.implementationVersion !== PUBLICATION_IMPLEMENTATION_VERSION ||
        work.schemaVersion !== PUBLICATION_SCHEMA_VERSION
      ) {
        return {
          errorCode: "COLLECTION_PUBLICATION_STATE_CORRUPT",
          state: "terminal",
        };
      }
      if (work.state !== "dead_letter") return { state: "waiting", workKey };
      if (
        work.lastErrorCode !== "PUBLICATION_BATCHED" &&
        work.lastErrorCode !== "PUBLICATION_SUPERSEDED"
      ) {
        return {
          errorCode: "COLLECTION_PUBLICATION_TERMINAL",
          state: "terminal",
        };
      }
      const successor = PublicationSuccessorSchema.safeParse(
        this.#ledger.latestCheckpoint(workKey, "publication-successor")
          ?.payload,
      );
      if (!successor.success) {
        return {
          errorCode: "COLLECTION_PUBLICATION_SUCCESSOR_CORRUPT",
          state: "terminal",
        };
      }
      workKey = successor.data.successorWorkKey;
    }
    return {
      errorCode: "COLLECTION_PUBLICATION_SUCCESSOR_DEPTH_EXCEEDED",
      state: "terminal",
    };
  }

  async #source(ref: PublicationSource, verify = true): Promise<WorkItem> {
    const source = this.#ledger.get(ref.workKey);
    if (
      source?.outputArtifactHash !== ref.artifactHash ||
      !["succeeded", "imported"].includes(source.state)
    )
      throw new Error("FANOUT_SOURCE_REVISION_INVALID");
    if (verify) {
      const verification = await this.#artifacts.verify(ref.artifactHash);
      if (!verification.ok) throw new Error("FANOUT_SOURCE_REVISION_INVALID");
    }
    return source;
  }

  async #indexDetailMaterial(
    detailFanoutWorkKey: string,
    source: WorkItem,
  ): Promise<null | string> {
    const existingMaterialHash =
      this.#ledger.fanoutDetailMaterialHash(detailFanoutWorkKey);
    if (existingMaterialHash) return existingMaterialHash;
    if (!source.outputArtifactHash) return null;
    const contents = await this.#artifacts.read(source.outputArtifactHash);
    const parsedArtifact: unknown = JSON.parse(contents.toString("utf8"));
    assertCollectedArtifactBinding(source, parsedArtifact);
    const artifact = DetailMaterialSchema.parse(parsedArtifact);
    const authorNameArabic =
      artifact.sourceContext?.authorNameArabic ??
      this.#ledger.sourceAuthorMetadata(artifact.source.author.href)
        ?.authorNameArabic;
    if (!authorNameArabic) return null;
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: authorNameArabic,
        linesArabic: artifact.source.lines,
        titleArabic: artifact.source.title,
      }),
    );
    this.#ledger.indexFanoutDetailMaterial({
      detailFanoutWorkKey,
      lineNfcHash: sha256(sourceLineNfcHashBody(artifact.source.lines)),
      promptMaterialHash,
      sourceWorkKey: source.workKey,
    });
    return promptMaterialHash;
  }

  #expediteMatchingDetail(
    input: PoemEnrichmentInput,
    priority: number,
  ): number {
    const promptMaterialHash = sha256(sourcePromptMaterialHashBody(input));
    return this.#ledger.expediteReadyWork(
      this.#ledger.fanoutDetailWorkKeysForMaterial(promptMaterialHash),
      [FANOUT_DETAIL_KIND],
      Math.max(priority, TRANSLATED_SOURCE_ADMISSION_PRIORITY),
      "TRANSLATED_SOURCE_ADMISSION",
    );
  }

  #indexReusableEnrichment(
    fanoutWorkKey: string,
    source: WorkItem,
    modelKey: string,
    input: PoemEnrichmentInput,
  ): boolean {
    if (
      !["succeeded", "imported"].includes(source.state) ||
      !source.outputArtifactHash
    )
      return false;
    return this.#ledger.indexReusableEnrichment({
      fanoutWorkKey,
      modelKey,
      outputArtifactHash: source.outputArtifactHash,
      promptMaterialHash: sha256(sourcePromptMaterialHashBody(input)),
      translationWorkKey: source.workKey,
    });
  }

  #indexOrWakeReusableEnrichment(
    fanoutWorkKey: string,
    source: WorkItem,
    modelKey: string,
    input: PoemEnrichmentInput,
    now: number,
  ): void {
    const promptMaterialHash = sha256(sourcePromptMaterialHashBody(input));
    const existing = this.#ledger.reusableEnrichmentForMaterial(
      modelKey,
      promptMaterialHash,
    );
    if (
      existing?.translationWorkKey === source.workKey &&
      existing.outputArtifactHash === source.outputArtifactHash
    ) {
      if (existing.fanoutWorkKey !== fanoutWorkKey) {
        this.#ledger.expediteReadyWork(
          [existing.fanoutWorkKey],
          [FANOUT_SOL_KIND],
          TRANSLATED_SOURCE_ADMISSION_PRIORITY,
          "APPROVED_PUBLICATION_ATTACHMENT_REPLAY",
          now,
        );
        this.#ledger.prioritizeFanoutWork(
          [existing.fanoutWorkKey],
          [FANOUT_SOL_KIND],
          now,
        );
      }
      return;
    }
    this.#indexReusableEnrichment(fanoutWorkKey, source, modelKey, input);
  }

  async #closePublishedSolSibling(
    claim: WorkClaim,
    job: z.infer<typeof SourceJobSchema>,
    source: WorkItem,
    input: PoemEnrichmentInput,
    profile: FanoutEnrichmentProfile,
    attachment: {
      readonly binding: CanonicalPoemBindingV1;
      readonly publicationPriority: number;
    },
    now: () => number,
  ): Promise<void> {
    const sourceContents = await this.#artifacts.read(job.source.artifactHash);
    const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
    const parsed = EnrichmentArtifactInputSchema.parse(artifact);
    if (canonicalJson(parsed.input) !== canonicalJson(input))
      throw new Error("FANOUT_ENRICHMENT_INPUT_MISMATCH");
    prepareBoundEnrichmentPublication(
      artifact,
      attachment.binding,
      job.source.workKey,
    );
    this.#ledger.attachApprovedBindingAndSeedPublication(
      job.source.workKey,
      job.source.artifactHash,
      attachment.binding,
      {
        implementationVersion: SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
        priority: attachment.publicationPriority,
      },
      now(),
    );
    this.#indexOrWakeReusableEnrichment(
      claim.work.workKey,
      source,
      profile.modelKey,
      input,
      now(),
    );
    this.#resolvers.retireApprovedSol?.(
      input,
      profile.modelKey,
      claim.work.workKey,
    );
    const receipt = await this.#artifacts.put(`${canonicalJson(job)}\n`);
    this.#renewAfterSuspend(claim, now());
    this.#ledger.succeed(claim, receipt.hash, now());
  }

  async #backfillDetailMaterialIndex(
    state: z.infer<typeof CursorSchema>["materialBackfill"],
  ): Promise<z.infer<typeof CursorSchema>["materialBackfill"]> {
    const detailPage = this.#ledger.listWorkDefinitionsAfter(
      state.detailCursor,
      FANOUT_DETAIL_KIND,
      MATERIAL_BACKFILL_BATCH_SIZE,
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
    );
    for (const fanout of detailPage.items) {
      const job = SourceJobSchema.safeParse(fanout.input);
      if (!job.success || job.data.lane !== "detail") continue;
      const source = this.#ledger.get(job.data.source.workKey);
      if (
        !source ||
        !["succeeded", "imported"].includes(source.state) ||
        source.outputArtifactHash !== job.data.source.artifactHash
      )
        continue;
      // eslint-disable-next-line no-await-in-loop -- The durable keyset cursor bounds and resumes legacy index construction.
      const promptMaterialHash = await this.#indexDetailMaterial(
        fanout.workKey,
        source,
      );
      if (
        promptMaterialHash &&
        this.#enrichment.some((profile) =>
          this.#ledger.reusableEnrichmentForMaterial(
            profile.modelKey,
            promptMaterialHash,
          ),
        )
      ) {
        this.#ledger.expediteReadyWork(
          [fanout.workKey],
          [FANOUT_DETAIL_KIND],
          TRANSLATED_SOURCE_ADMISSION_PRIORITY,
          "TRANSLATED_SOURCE_ADMISSION",
        );
      }
    }
    const solPage = this.#ledger.listWorkDefinitionsAfter(
      state.solCursor,
      FANOUT_SOL_KIND,
      MATERIAL_BACKFILL_BATCH_SIZE,
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
    );
    for (const fanout of solPage.items) {
      const job = SourceJobSchema.safeParse(fanout.input);
      if (!job.success || job.data.lane !== "sol") continue;
      const source = this.#ledger.get(job.data.source.workKey);
      if (
        !source ||
        !["succeeded", "imported"].includes(source.state) ||
        source.outputArtifactHash !== job.data.source.artifactHash
      )
        continue;
      const input = PoemEnrichmentInputSchema.safeParse(source.input);
      if (!input.success) continue;
      if (
        this.#ledger.approvedPublicationAttachmentForTranslation(
          job.data.source.workKey,
          job.data.source.artifactHash,
        )
      ) {
        this.#ledger.expediteReadyWork(
          [fanout.workKey],
          [FANOUT_SOL_KIND],
          TRANSLATED_SOURCE_ADMISSION_PRIORITY,
          "APPROVED_PUBLICATION_ATTACHMENT_REPLAY",
        );
        this.#ledger.prioritizeFanoutWork([fanout.workKey], [FANOUT_SOL_KIND]);
        continue;
      }
      this.#indexReusableEnrichment(
        fanout.workKey,
        source,
        job.data.modelKey ?? ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        input.data,
      );
    }
    return {
      complete: detailPage.done && solPage.done,
      detailCursor: detailPage.cursor,
      metadataRevision: state.metadataRevision,
      solCursor: solPage.cursor,
    };
  }
  #waitResolution(
    summary: { pendingResolution: number; retried: number },
    eventDriven: boolean,
  ): "event-driven-wait" | "polled-wait" {
    // Identity/pointer resolution is an external prerequisite, not a failed
    // processing attempt. Keep it resumable indefinitely instead of exhausting
    // MAX_ATTEMPTS after enough ordinary polling cycles.
    summary.pendingResolution += 1;
    summary.retried += 1;
    // Resolution completion wakes an event-driven job through the durable
    // demand-cache outbox. Its six-hour availability is only a repair fallback;
    // detail admission retains the shorter bounded poll.
    return eventDriven ? "event-driven-wait" : "polled-wait";
  }
  async #cursor(): Promise<z.infer<typeof CursorSchema>> {
    const checkpoint = this.#ledger.latestCheckpoint(
      this.#controlWorkKey,
      "fanout-cursor",
    );
    if (!checkpoint)
      return {
        detail: 0,
        enrichment: {},
        materialBackfill: {
          complete: false,
          detailCursor: null,
          metadataRevision: this.#ledger.sourceAuthorMetadataRevision(),
          solCursor: null,
        },
        resolutionWaiterBackfill: {
          complete: false,
          cursor: null,
          cutoffAt: null,
        },
        sol: 0,
      };
    if (!checkpoint.artifactHash)
      throw new Error("FANOUT_CURSOR_ARTIFACT_MISSING");
    const cursorContents = await this.#artifacts.read(checkpoint.artifactHash);
    return CursorSchema.parse(JSON.parse(cursorContents.toString("utf8")));
  }

  async #backfillResolutionWaiters(
    state: z.infer<typeof CursorSchema>["resolutionWaiterBackfill"],
    now: () => number,
  ): Promise<z.infer<typeof CursorSchema>["resolutionWaiterBackfill"]> {
    const cutoffAt = state.cutoffAt ?? now();
    const page = this.#ledger.listResolutionPendingAfter(
      state.cursor,
      [FANOUT_ENRICHMENT_KIND, FANOUT_SOL_KIND],
      RESOLUTION_WAITER_BACKFILL_BATCH_SIZE,
      cutoffAt,
    );
    for (const fanout of page.items) {
      if (this.#registeredResolutionWorkKeys.has(fanout.workKey)) continue;
      const job = SourceJobSchema.safeParse(fanout.input);
      if (!job.success || job.data.lane === "detail") {
        this.#ledger.wakeResolutionPending(
          [fanout.workKey],
          [fanout.kind],
          now(),
        );
        continue;
      }
      const source = this.#ledger.get(job.data.source.workKey);
      const input = source
        ? PoemEnrichmentInputSchema.safeParse(source.input)
        : null;
      const modelKey =
        job.data.modelKey ??
        (job.data.lane === "sol"
          ? ENRICHMENT_PROVIDER_SPECS.sol.modelKey
          : null);
      const profile = this.#enrichment.find(
        (candidate) => candidate.modelKey === modelKey,
      );
      if (
        !source ||
        !input?.success ||
        PoemEnrichmentInputV2Schema.safeParse(source.input).success ||
        profile?.workKind !== source.kind
      ) {
        this.#ledger.wakeResolutionPending(
          [fanout.workKey],
          [fanout.kind],
          now(),
        );
        continue;
      }
      try {
        const resolution =
          job.data.lane === "sol" && this.#resolvers.approvedSol
            ? // eslint-disable-next-line no-await-in-loop -- Each bounded compatibility row must durably register its exact waiter before the cursor advances.
              await this.#resolvers.approvedSol(
                source,
                input.data,
                profile,
                fanout.workKey,
              )
            : // eslint-disable-next-line no-await-in-loop -- Each bounded compatibility row must durably register its exact waiter before the cursor advances.
              await this.#resolvers.enrichment(
                source,
                input.data,
                profile,
                fanout.workKey,
              );
        if (
          resolution &&
          (!("status" in resolution) || resolution.status !== "waiting")
        ) {
          this.#ledger.wakeResolutionPending(
            [fanout.workKey],
            [fanout.kind],
            now(),
          );
        }
      } catch {
        // Let the ordinary leased reconciliation path retain its existing
        // retry/dead-letter classification for malformed or unavailable state.
        this.#ledger.wakeResolutionPending(
          [fanout.workKey],
          [fanout.kind],
          now(),
        );
      }
    }
    return { complete: page.complete, cursor: page.cursor, cutoffAt };
  }
  async #backfillLegacySol(
    claim: WorkClaim,
    summary: { scannedEnrichments: number },
    now: () => number,
    fallbackVersion = LEGACY_SOL_PIPELINE_VERSION,
  ): Promise<void> {
    const profile = this.#enrichment.find(
      ({ modelKey }) => modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
    );
    if (!profile) return;
    const checkpointKind =
      fallbackVersion === LEGACY_SOL_PIPELINE_VERSION
        ? LEGACY_SOL_BACKFILL_CHECKPOINT_KIND
        : `fanout-completed-${fallbackVersion}-backfill-v1`;
    const checkpoint = this.#ledger.latestCheckpoint(
      this.#controlWorkKey,
      checkpointKind,
    );
    let state = { complete: false, eventCursor: 0 };
    if (checkpoint) {
      if (!checkpoint.artifactHash)
        throw new Error("LEGACY_SOL_BACKFILL_ARTIFACT_MISSING");
      const contents = await this.#artifacts.read(checkpoint.artifactHash);
      state = LegacySolBackfillSchema.parse(
        JSON.parse(contents.toString("utf8")),
      );
    }
    if (state.complete && fallbackVersion === LEGACY_SOL_PIPELINE_VERSION)
      return;
    const page = this.#ledger.listSucceededSolFallbackAfter(
      state.eventCursor,
      profile.workKind,
      LEGACY_SOL_BACKFILL_BATCH_SIZE,
      fallbackVersion,
      profile.implementationVersion,
    );
    if (state.complete && page.cursor === state.eventCursor && page.done)
      return;
    await this.#seedSources(
      "enrichment",
      page.items.map(({ work }) => work),
      profile.modelKey,
    );
    summary.scannedEnrichments += page.items.length;
    this.#afterBoundary?.("legacy-sol-backfill");
    state = LegacySolBackfillSchema.parse({
      complete: page.done,
      eventCursor: page.cursor,
    });
    const artifact = await this.#artifacts.put(`${canonicalJson(state)}\n`);
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: artifact.hash,
        kind: checkpointKind,
        payload: state,
      },
      now(),
    );
  }
  async #detailPhase(key: string) {
    return this.#phase(key, "fanout-detail-phase", DetailPhaseSchema, {
      enrichmentWorkKeys: {},
    });
  }
  async #solPhase(key: string) {
    return this.#phase(key, "fanout-sol-phase", SolPhaseSchema, {});
  }
  async #phase<T>(
    key: string,
    kind: string,
    schema: z.ZodType<T>,
    fallback: T,
  ): Promise<T> {
    const checkpoint = this.#ledger.latestCheckpoint(key, kind);
    if (!checkpoint) return fallback;
    if (!checkpoint.artifactHash)
      throw new Error("FANOUT_PHASE_ARTIFACT_MISSING");
    const phaseContents = await this.#artifacts.read(checkpoint.artifactHash);
    return schema.parse(JSON.parse(phaseContents.toString("utf8")));
  }
  async #saveDetailPhase(
    claim: WorkClaim,
    phase: z.infer<typeof DetailPhaseSchema>,
    now: () => number,
  ): Promise<void> {
    await this.#savePhase(
      claim,
      "fanout-detail-phase",
      DetailPhaseSchema.parse(phase),
      now,
    );
  }
  async #saveSolPhase(
    claim: WorkClaim,
    phase: z.infer<typeof SolPhaseSchema>,
    now: () => number,
  ): Promise<void> {
    await this.#savePhase(
      claim,
      "fanout-sol-phase",
      SolPhaseSchema.parse(phase),
      now,
    );
  }
  async #savePhase(
    claim: WorkClaim,
    kind: string,
    phase: object,
    now: () => number,
  ): Promise<void> {
    const artifact = await this.#artifacts.put(`${canonicalJson(phase)}\n`);
    const completedAt = now();
    this.#renewAfterSuspend(claim, completedAt);
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: artifact.hash,
        kind,
        payload: { boundaries: Object.keys(phase).length },
      },
      completedAt,
    );
  }

  #renewAfterSuspend(claim: WorkClaim, now: number): void {
    // Lease expiry makes work reclaimable; it does not create a second owner.
    // This token-and-epoch CAS wins only if no replacement claimed the row
    // while JavaScript timers were suspended by host sleep.
    this.#ledger.renew(claim, now, LEASE_DURATION_MS, true);
  }
}

function retryDelay(attempt: number) {
  return Math.min(
    30 * 60_000,
    30_000 * 2 ** Math.min(Math.max(attempt - 1, 0), 6),
  );
}

class RetryableFanoutError extends Error {
  readonly retryAt: number;

  constructor(code: string, retryAt: number) {
    super(code);
    this.name = "RetryableFanoutError";
    this.retryAt = FanoutRetryAtSchema.parse(retryAt);
  }
}

class TerminalFanoutError extends Error {
  constructor(code: string) {
    super(code);
    this.name = "TerminalFanoutError";
  }
}

function fanoutErrorCode(error: unknown): string {
  if (error instanceof Error && /^[A-Z][A-Z\d_]{2,99}$/.test(error.message)) {
    return error.message;
  }
  return "FANOUT_EXCEPTION";
}

function outstandingClaims(
  claims: ReadonlyMap<string, WorkClaim>,
): readonly WorkClaim[] {
  const result: WorkClaim[] = [];
  // eslint-disable-next-line unicorn/prefer-spread -- Iterator helpers are outside the configured TypeScript runtime library.
  for (const claim of claims.values()) result.push(claim);
  return result;
}
