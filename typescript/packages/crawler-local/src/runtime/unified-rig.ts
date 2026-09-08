import { randomUUID } from "node:crypto";
import { access, readFile } from "node:fs/promises";

import { APPROVED_ENRICHMENT_PROFILES } from "@saqi/precedent-iso";
import { z } from "zod";

import {
  AuthorInventoryCollector,
  type AuthorInventoryLaneStatus,
} from "../collection/author-inventory-lane.js";
import { SourceChromeCollector } from "../collection/collection-source-browser.js";
import {
  collectionWorkKinds,
  CollectorCoordinator,
  collectorImplementationVersion,
  type CollectorRunResult,
  collectorSchemaVersion,
  defaultChromeProfile,
  deriveCollectorOriginHealthState,
} from "../collection/collector.js";
import {
  CollectorRecoveryController,
  type CollectorRecoveryState,
} from "../collection/collector-recovery.js";
import {
  type EnrichmentStartupReconciliationReport,
  reconcileExistingEnrichmentInputs,
} from "../enrichment/enrichment-startup-reconciliation.js";
import { LegacySourceBindingReconciler } from "../enrichment/legacy-source-binding-reconciler.js";
import {
  LocalEnrichmentFanout,
  type LocalEnrichmentFanoutSummary,
  type LocalEnrichmentResolver,
} from "../enrichment/local-enrichment-fanout.js";
import {
  providerCredentialGeneration,
  providerCredentialSnapshot,
} from "../enrichment/provider-credential-generation.js";
import {
  SolAttemptRetention,
  type SolRetentionReport,
} from "../enrichment/sol-attempt-retention.js";
import {
  POEM_ENRICHMENT_V2_SCHEMA_VERSION,
  SOL_ENRICHMENT_WORK_KIND,
  SolEnrichmentCoordinator,
  type SolEnrichmentRunPort,
  type SolEnrichmentSeedPort,
} from "../enrichment/sol-coordinator.js";
import {
  QuotaAwareSolLaneScheduler,
  schedulerStateDigest,
  type SolLaneScheduler,
  type SolSchedulerSnapshot,
  type SolTaskPermit,
} from "../enrichment/sol-lane-scheduler.js";
import {
  CodexSolRunner,
  ENRICHMENT_PROVIDER_SPECS,
  type EnrichmentProvider,
  SOL_MODEL,
  SOL_PIPELINE_VERSION,
} from "../enrichment/sol-runner.js";
import { ArtifactStore } from "../persistence/artifact-store.js";
import { Ledger } from "../persistence/ledger.js";
import { planProductionBaselineFiles } from "../persistence/production-baseline-planner.js";
import { ProductionResolutionDemandCache } from "../persistence/production-resolution-demand-cache.js";
import {
  openProductionResolutionStore,
  type ReadableProductionResolutionStore,
} from "../persistence/scoped-production-resolution.js";
import { canonicalJson, sha256 } from "../persistence/work-key.js";
import {
  type D1CapacityPreflightReport,
  planD1CapacityPreflight,
} from "../publication/d1-capacity-preflight.js";
import { DemandDrivenProductionResolutionRefresher } from "../publication/demand-driven-production-resolution-refresher.js";
import {
  FANOUT_DETAIL_KIND,
  FANOUT_ENRICHMENT_KIND,
  FANOUT_SOL_KIND,
  type FanoutCycleSummary,
  FanoutReconciler,
} from "../publication/fanout-reconciler.js";
import {
  type AuthenticatedPublicationTransport,
  createAuthenticatedPublicationTransport,
  type PublicationAuthPreflight,
} from "../publication/publication-auth-client.js";
import {
  PublicationClient,
  type PublicationTransport,
} from "../publication/publication-client.js";
import {
  PublicationLane,
  type PublicationRunSummary,
} from "../publication/publication-lane.js";
import { COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE } from "./network-resilience.js";
import type { ScraperOperationConfig } from "./operations-contract.js";
import {
  PipelineDiagnostics,
  type PipelineHealthInput,
} from "./pipeline-diagnostics.js";
import {
  ProviderExecutionDiagnostics,
  type ProviderExecutionHealth,
  type ProviderExecutionHealthEntryInput,
} from "./provider-execution-health.js";
import { ProviderHostAdmission } from "./resource-pressure.js";
import { RUNTIME_ENVIRONMENT } from "./runtime-environment.js";
import {
  CoalescingLaneWake,
  type SupervisorLane,
  type SupervisorPreflight,
} from "./supervisor.js";

type ApprovedSolResolutionCache = Pick<
  ProductionResolutionDemandCache,
  | "resolveCanonicalEnrichment"
  | "resolveOrRegisterFingerprintEnrichment"
  | "resolveOrRegisterPublication"
  | "retireCanonicalDemand"
  | "retireFingerprintDemand"
  | "retireFingerprintWaiter"
  | "retirePublicationWaiter"
  | "terminalCanonicalFailure"
  | "terminalFingerprintFailure"
>;

type ApprovedSolResolution =
  | { readonly errorCode: string; readonly status: "conflict" }
  | ReturnType<
      ProductionResolutionDemandCache["resolveOrRegisterFingerprintEnrichment"]
    >;

const PUBLICATION_BLOCKING_RESOLUTION_PRIORITY = 1_000;
const CanonicalPoemIdSchema = z.uuid();
const SourceLineageMaintenanceStateSchema = z.enum([
  "active",
  "blocked",
  "complete",
  "failed",
  "idle",
]);
const SourceLineageMaintenanceResponseSchema = z.strictObject({
  ok: z.literal(true),
  result: z.looseObject({
    remaining: z.number().int().nonnegative(),
    state: SourceLineageMaintenanceStateSchema,
  }),
});

export function resolveApprovedSolBinding(
  cache: ApprovedSolResolutionCache,
  input: Parameters<
    ProductionResolutionDemandCache["resolveCanonicalEnrichment"]
  >[0],
  modelKey: string,
  priority: number,
  fanoutWorkKey: string,
): ApprovedSolResolution {
  const resolutionPriority = Math.max(
    priority,
    PUBLICATION_BLOCKING_RESOLUTION_PRIORITY,
  );
  const hasCanonicalCoordinates =
    CanonicalPoemIdSchema.safeParse(input.poemId).success &&
    /^[a-f\d]{64}$/.test(input.sourceRevisionId);
  if (hasCanonicalCoordinates) {
    const canonical = cache.resolveCanonicalEnrichment(input, modelKey);
    if (canonical.status === "resolved") {
      cache.retireFingerprintDemand(input, modelKey);
      cache.retireFingerprintWaiter(fanoutWorkKey);
      cache.retirePublicationWaiter(fanoutWorkKey);
      return canonical;
    }
    if (canonical.status === "conflict") {
      return { errorCode: canonical.code, status: "conflict" };
    }
    // UUID-shaped legacy enrichment coordinates are not proof that the poem is
    // present under the same identity in the current production corpus. Keep
    // the direct lookup as the cheapest path, but also admit the exact source
    // fingerprint so a migrated poem can be rebound without guessing an ID.
    const fingerprint = cache.resolveOrRegisterFingerprintEnrichment({
      input,
      modelKey,
      priority: resolutionPriority,
      workKey: fanoutWorkKey,
    });
    if (fingerprint.status === "resolved") {
      cache.retireCanonicalDemand(
        input.poemId,
        modelKey,
        input.sourceRevisionId,
      );
      cache.retirePublicationWaiter(fanoutWorkKey);
      return fingerprint;
    }
    const canonicalTerminal = cache.terminalCanonicalFailure(
      input.poemId,
      modelKey,
      input.sourceRevisionId,
    );
    const fingerprintTerminal = cache.terminalFingerprintFailure(
      input,
      modelKey,
    );
    if (canonicalTerminal) {
      cache.retireCanonicalDemand(
        input.poemId,
        modelKey,
        input.sourceRevisionId,
      );
      cache.retirePublicationWaiter(fanoutWorkKey);
    }
    if (fingerprintTerminal) {
      cache.retireFingerprintDemand(input, modelKey);
      cache.retireFingerprintWaiter(fanoutWorkKey);
    }
    if (canonicalTerminal && fingerprintTerminal) {
      return { errorCode: fingerprintTerminal, status: "conflict" };
    }
    if (!canonicalTerminal) {
      cache.resolveOrRegisterPublication({
        modelKey,
        poemId: input.poemId,
        priority: resolutionPriority,
        sourceNfcSha256: sha256(
          canonicalJson(input.linesArabic.map((line) => line.normalize("NFC"))),
        ),
        sourceRevisionId: input.sourceRevisionId,
        workKey: fanoutWorkKey,
      });
    }
    return fingerprint;
  }
  const terminal = cache.terminalFingerprintFailure(input, modelKey);
  if (terminal) {
    cache.retireFingerprintDemand(input, modelKey);
    cache.retireFingerprintWaiter(fanoutWorkKey);
    return { errorCode: terminal, status: "conflict" };
  }
  return cache.resolveOrRegisterFingerprintEnrichment({
    input,
    modelKey,
    priority: resolutionPriority,
    workKey: fanoutWorkKey,
  });
}

export function retireApprovedSolResolution(
  cache: ApprovedSolResolutionCache,
  input: Parameters<
    ProductionResolutionDemandCache["resolveCanonicalEnrichment"]
  >[0],
  modelKey: string,
  fanoutWorkKey: string,
): void {
  cache.retireCanonicalDemand(input.poemId, modelKey, input.sourceRevisionId);
  cache.retireFingerprintDemand(input, modelKey);
  cache.retireFingerprintWaiter(fanoutWorkKey);
  cache.retirePublicationWaiter(fanoutWorkKey);
}

const PublicationPreflightStateValueSchema = z.enum([
  "paused",
  "ready",
  "retry_wait",
]);
const PublicationPreflightStateSchema = z.object({
  state: PublicationPreflightStateValueSchema,
});
const MAXIMUM_STATUS_BYTES = 2 * 1024 * 1024;
const GROWTH_SUCCESS_SLO_MS = 60 * 60_000;
const FANOUT_MINIMUM_CADENCE_MS = 5_000;
// Retention is background maintenance, not part of the supervisor's hot path.
// A short global idle poll must not turn a multi-window attempt scan into
// continuous filesystem and compression work while publication is draining.
const RETENTION_SCAN_CONTINUATION_INTERVAL_MS = 30_000;
const RETENTION_SCAN_INTERVAL_MS = 60 * 60_000;
const RETENTION_SCAN_BATCH_SIZE = 250;
const SOL_MILESTONE_BACKFILL_BATCH_SIZE = 250;
const SOL_MILESTONE_BACKFILL_CONTINUATION_INTERVAL_MS = 5_000;
const SOL_MILESTONE_BACKFILL_COMPLETE_INTERVAL_MS = 60 * 60_000;

function providerPoemThroughput(
  throughput: ReturnType<Ledger["poemThroughput"]>,
): NonNullable<ProviderExecutionHealthEntryInput["throughput"]> {
  return {
    ...throughput,
    coverage: {
      ...throughput.coverage,
      state: throughput.coverage.backfillComplete ? "complete" : "backfilling",
    },
  };
}

export function providerWaitIsActive(
  providerUntil: null | number | undefined,
  now: number,
): boolean {
  return (providerUntil ?? 0) > now;
}
const RetentionScanStateSchema = z.strictObject({
  cursor: z
    .string()
    .regex(
      /^[\da-f]{8}-[\da-f]{4}-[1-8][\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/iu,
    )
    .nullable(),
  schemaVersion: z.literal(1),
});
const SEMANTIC_FAILURE_CODES: ReadonlySet<string> = new Set([
  "CODEX_OUTPUT_INVALID",
  "CODEX_OUTPUT_SCHEMA_INVALID",
  "CODEX_REVIEW_SCHEMA_INVALID",
  "SOL_REVIEW_REJECTED",
]);
const RECOVERABLE_UNKNOWN_OPERATION_CODES: ReadonlySet<string> = new Set([
  "CODEX_OPERATION_OUTCOME_UNKNOWN",
]);
const NONRECOVERABLE_UNKNOWN_OPERATION_CODES: ReadonlySet<string> = new Set([
  "CODEX_OPERATION_STATE_INVALID",
]);
const QUARANTINED_OPERATION_CODES: ReadonlySet<string> = new Set([
  "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
]);
type EnrichmentLaneCoordinator = SolEnrichmentRunPort &
  Pick<SolEnrichmentSeedPort, "seed">;
export interface UnifiedRigPaths {
  readonly artifacts: string;
  readonly database: string;
  readonly paidWorkPaused?: string;
  readonly paused: string;
  readonly root: string;
  readonly schedulerState: string;
  readonly solAttempts: string;
}

export interface UnifiedRigOptions {
  readonly config: ScraperOperationConfig;
  readonly configDigest: string;
  readonly desiredConfigDigest?: () => null | Promise<null | string> | string;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly localEnrichmentResolver?: LocalEnrichmentResolver;
  readonly now?: () => number;
  readonly paths: UnifiedRigPaths;
  /** Shared publication authentication seam used by publication and resolution. */
  readonly publicationAuth?: AuthenticatedPublicationTransport;
  readonly requireCurrentSchedulerAuthority?: boolean;
  readonly resourcePressure?: () => Promise<{
    readonly nextProbeAt: number;
    readonly reasons: readonly (
      | "DISK_PRESSURE"
      | "FILE_DESCRIPTOR_PRESSURE"
      | "MEMORY_PRESSURE"
      | "PROCESS_MEMORY_PRESSURE"
      | "RESOURCE_PROBE_FAILED"
    )[];
    readonly state: "ready" | "resource_wait";
  }>;
  readonly solCoordinatorFactory?:
    | ((options: {
        readonly artifacts: ArtifactStore;
        readonly index: number;
        readonly ledger: Ledger;
        readonly owner: string;
      }) => EnrichmentLaneCoordinator)
    | undefined;
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This composition root constructs and owns the concrete runtime graph; component ports define substitution boundaries.
export class UnifiedRigRuntime {
  readonly #config: ScraperOperationConfig;
  readonly #configDigest: string;
  readonly #environment: Readonly<Record<string, string | undefined>>;
  readonly #desiredConfigDigest: () => null | Promise<null | string> | string;
  readonly #now: () => number;
  readonly #localEnrichmentResolver: LocalEnrichmentResolver | undefined;
  readonly #paths: UnifiedRigPaths;
  readonly #pipelineDiagnostics: PipelineDiagnostics;
  readonly #providerExecutionDiagnostics: ProviderExecutionDiagnostics;
  readonly #providerExecutionRunId = randomUUID();
  readonly #providerHostAdmission: ProviderHostAdmission;
  readonly #resourcePressure: UnifiedRigOptions["resourcePressure"];
  readonly #requireCurrentSchedulerAuthority: boolean;
  readonly #solCoordinatorFactory:
    | ((options: {
        readonly artifacts: ArtifactStore;
        readonly index: number;
        readonly ledger: Ledger;
        readonly owner: string;
      }) => EnrichmentLaneCoordinator)
    | undefined;
  #artifacts: ArtifactStore | undefined;
  #authorInventoryStatus: AuthorInventoryLaneStatus | null = null;
  #baselineStatus: unknown = null;
  #capacityStatus: D1CapacityPreflightReport | null = null;
  #collectorCoordinator: CollectorCoordinator | undefined;
  #collectorRecovery: CollectorRecoveryController | undefined;
  #collectorOriginState: "disabled" | CollectorRunResult["originState"] =
    "disabled";
  #fanoutStatus: FanoutCycleSummary | null = null;
  #fanoutResolutionError: {
    readonly code: string;
    readonly retryAt: number;
  } | null = null;
  #enrichmentReconciliationStatus: EnrichmentStartupReconciliationReport | null =
    null;
  #ledger: Ledger | undefined;
  #localFanoutStatus: Partial<
    Record<EnrichmentProvider, LocalEnrichmentFanoutSummary>
  > = {};
  readonly #localFanoutResolutionErrors = new Map<
    EnrichmentProvider,
    { readonly code: string; readonly retryAt: number }
  >();
  #lastSolDisposition: NonNullable<
    PipelineHealthInput["sol"]
  >["lastDisposition"] = "idle";
  #lastSolRetryAt: null | number = null;
  #providerExecutionHealth: null | ProviderExecutionHealth = null;
  #publicationStatus: null | PublicationRunSummary = null;
  #publicationAuth: AuthenticatedPublicationTransport | undefined;
  #publicationAuthPreflight: unknown = null;
  #publicationAllowed = false;
  #resolutionDemandCache: ProductionResolutionDemandCache | undefined;
  #resolutionRefreshError: {
    readonly code: string;
    readonly retryAt: number;
  } | null = null;
  #retentionStatus: Partial<Record<EnrichmentProvider, SolRetentionReport>> =
    {};
  #scheduler: SolLaneScheduler | undefined;
  readonly #schedulers: Partial<Record<EnrichmentProvider, SolLaneScheduler>> =
    {};
  readonly #resolutionStores = new Map<
    string,
    ReadableProductionResolutionStore
  >();

  constructor(options: UnifiedRigOptions) {
    this.#config = options.config;
    this.#configDigest = options.configDigest;
    this.#desiredConfigDigest =
      options.desiredConfigDigest ?? (() => this.#configDigest);
    this.#environment = options.environment ?? RUNTIME_ENVIRONMENT;
    this.#localEnrichmentResolver = options.localEnrichmentResolver;
    this.#now = options.now ?? Date.now;
    this.#paths = options.paths;
    this.#pipelineDiagnostics = new PipelineDiagnostics({
      configDigest: options.configDigest,
      model: SOL_MODEL,
      now: this.#now,
      stateDirectory: options.paths.root,
    });
    this.#providerExecutionDiagnostics = new ProviderExecutionDiagnostics(
      options.paths.root,
    );
    this.#providerHostAdmission = new ProviderHostAdmission(
      options.config.resources,
    );
    this.#resourcePressure = options.resourcePressure;
    this.#requireCurrentSchedulerAuthority =
      options.requireCurrentSchedulerAuthority ?? false;
    this.#publicationAuth = options.publicationAuth;
    this.#solCoordinatorFactory = options.solCoordinatorFactory;
  }

  async preflight(): Promise<SupervisorPreflight> {
    if (
      this.#config.continuousFreePublication &&
      !this.#pauseState().paidWorkPaused
    ) {
      throw new Error("CONTINUOUS_FREE_PUBLICATION_REQUIRES_PAID_FENCE");
    }
    const capacity = planD1CapacityPreflight(this.#config.publication.d1);
    this.#capacityStatus = capacity;
    this.#publicationAuthPreflight = null;
    let publicationAllowed = false;
    if (this.#config.publication.enabled) {
      try {
        this.#publicationAuth ??= createAuthenticatedPublicationTransport({
          config: this.#config.publication.auth,
          environment: this.#environment,
          now: this.#now,
        });
        if (capacity.go) {
          this.#publicationAuthPreflight =
            await this.#publicationAuth.preflight();
          publicationAllowed =
            PublicationPreflightStateSchema.parse(
              this.#publicationAuthPreflight,
            ).state === "ready";
        }
      } catch (error) {
        this.#publicationAuthPreflight = {
          errorCode:
            error instanceof Error
              ? error.message
              : "PUBLICATION_AUTH_PREFLIGHT_FAILED",
          state: "paused",
        };
      }
    }
    this.#publicationAllowed = publicationAllowed;
    return {
      publicationAllowed,
      status: {
        auth: this.#publicationAuthPreflight,
        d1: capacity,
        publicationConfigured: this.#config.publication.enabled,
      },
    };
  }

  async createLanes(
    preflight: null | SupervisorPreflight,
  ): Promise<readonly SupervisorLane[]> {
    if (!preflight) throw new Error("UNIFIED_RIG_PREFLIGHT_REQUIRED");
    this.#publicationAllowed = preflight.publicationAllowed;
    const ledger = this.#ledger ?? Ledger.initialize(this.#paths.database);
    this.#ledger = ledger;
    if (this.#config.sol.enabled && !this.#solCoordinatorFactory)
      ledger.solOperations.assertImported();
    // Recover already-expired claims immediately; claims abandoned just before
    // restart can expire later and need the single maintenance lane below.
    ledger.recoverExpired(this.#now());
    const artifacts = new ArtifactStore(this.#paths.artifacts, {
      minimumFreeBytes: this.#config.retention.minimumFreeBytes,
    });
    this.#artifacts = artifacts;
    await artifacts.capacity();
    const lanes: SupervisorLane[] = [
      {
        name: "lease-recovery",
        close: () => undefined,
        honorNextWakeAt: true,
        resourcePressureExempt: true,
        runOnce: () => {
          const now = this.#now();
          const recovered = ledger.recoverExpired(now);
          return Promise.resolve({
            nextWakeAt: now + 30_000,
            result: recovered > 0 ? "recovered" : "idle",
          });
        },
      },
    ];
    this.#createSourceLineageMaintenanceLane(lanes);
    const detailFanoutWake = new CoalescingLaneWake();
    let requestDetailFanoutScan: ((now: number) => void) | undefined;
    let requestFanoutWork: ((workKeys: readonly string[]) => void) | undefined;

    if (this.#config.baseline.enabled) {
      if (
        !this.#config.baseline.authorsPath ||
        !this.#config.baseline.poemsPath
      )
        throw new Error("BASELINE_PATHS_REQUIRED");
      const baseline = await planProductionBaselineFiles({
        authorsPath: this.#config.baseline.authorsPath,
        batchSize: this.#config.baseline.batchSize,
        enrichmentProfiles: this.#config.sol.enabled
          ? [
              {
                implementationVersion:
                  ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion,
                kind: SOL_ENRICHMENT_WORK_KIND,
                modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
              },
            ]
          : [],
        ledger,
        poemsPath: this.#config.baseline.poemsPath,
      });
      this.#baselineStatus = baseline.report;
    }
    this.#enrichmentReconciliationStatus = this.#config.startupReconciliation
      .enabled
      ? reconcileExistingEnrichmentInputs({
          ledger,
          providers: this.#config.sol.enabled ? ["sol"] : [],
        })
      : null;
    this.#baselineStatus = {
      ...(this.#baselineStatus && typeof this.#baselineStatus === "object"
        ? this.#baselineStatus
        : {}),
      enrichmentReconciliation: this.#enrichmentReconciliationStatus,
    };
    if (this.#config.collector.enabled) {
      ledger.retireIncompatible(
        [
          collectionWorkKinds().authorManifest,
          collectionWorkKinds().poemDetail,
        ],
        {
          implementationVersion: collectorImplementationVersion(),
          schemaVersion: collectorSchemaVersion(),
        },
      );
      const browser = await SourceChromeCollector.create({
        headless: this.#config.collector.headless,
        minimumSourceGapMs: this.#config.collector.minimumSourceGapMs,
        profileDirectory: defaultChromeProfile(this.#paths.root),
      });
      if (this.#config.inventory.enabled) {
        const productionAuthorsPath =
          this.#config.inventory.productionAuthorsPath;
        const refreshGeneration = this.#config.inventory.refreshGeneration;
        if (!productionAuthorsPath || !refreshGeneration)
          throw new Error("INVENTORY_CONFIGURATION_REQUIRED");
        const inventory = new AuthorInventoryCollector({
          artifacts,
          browser,
          ledger,
          productionSourceAuthors: await this.#readBoundedJson(
            productionAuthorsPath,
          ),
          refreshGeneration,
        });
        inventory.seed();
        lanes.push({
          name: "author-inventory",
          close: () => undefined,
          runOnce: async (signal) => {
            const result = await inventory.run(signal);
            if (result.stopped === "succeeded")
              this.#authorInventoryStatus = result.status;
            return {
              nextWakeAt: this.#now() + this.#config.restart.idlePollMs,
              result: result.stopped,
            };
          },
        });
      }
      const coordinator = new CollectorCoordinator({
        artifacts,
        browser,
        detailBurst: this.#config.collector.detailBurst,
        ledger,
        minimumOriginGapMs: this.#config.collector.minimumSourceGapMs,
        onDurableSuccess: ({ kind }) => {
          if (kind !== collectionWorkKinds().poemDetail) return;
          requestDetailFanoutScan?.(this.#now());
        },
      });
      this.#collectorCoordinator = coordinator;
      const recovery = this.#config.collector.recovery.enabled
        ? new CollectorRecoveryController({ ledger })
        : undefined;
      this.#collectorRecovery = recovery;
      lanes.push({
        name: "collector",
        close: () => coordinator.close(),
        ...(recovery ? { honorNextWakeAt: true, maximumSleepMs: 30_000 } : {}),
        runOnce: async (signal) => {
          const recoveryBefore = recovery?.cycle(this.#now());
          if (recoveryBefore && recoveryBefore.state.phase !== "observing") {
            return {
              nextWakeAt: recoveryBefore.state.nextActionAt,
              result: `recovery_${recoveryBefore.state.phase}`,
            };
          }
          const includedWorkKeys = recoveryBefore?.state.reservation?.workKeys;
          const before = ledger.availability(
            [
              collectionWorkKinds().authorManifest,
              collectionWorkKinds().poemDetail,
            ],
            this.#now(),
            {
              implementationVersion: collectorImplementationVersion(),
              schemaVersion: collectorSchemaVersion(),
            },
          );
          if (!recovery && before.ready === 0)
            return { nextWakeAt: before.earliestAvailableAt, result: "idle" };
          const result = await coordinator.run(signal, {
            ...(includedWorkKeys === undefined ? {} : { includedWorkKeys }),
            maximum: this.#config.collector.maximumPerCycle,
            paused: () => this.paused(),
          });
          this.#collectorOriginState = result.originState;
          if (
            recovery &&
            (result.stopped === "human_required" ||
              (result.originStopReason?.includes("RATE_LIMIT") ?? false))
          ) {
            recovery.stop(
              result.originStopReason ?? "SOURCE_HUMAN_REQUIRED",
              this.#now(),
            );
          }
          const recoveryAfter = recovery?.cycle(this.#now());
          const after = ledger.availability(
            [
              collectionWorkKinds().authorManifest,
              collectionWorkKinds().poemDetail,
            ],
            this.#now(),
            {
              implementationVersion: collectorImplementationVersion(),
              schemaVersion: collectorSchemaVersion(),
            },
          );
          return {
            nextWakeAt: recoveryAfter
              ? collectorRecoveryWakeAt(
                  ledger,
                  recoveryAfter.state,
                  this.#now(),
                  this.#config.restart.idlePollMs,
                )
              : result.stopped === "disk_pressure"
                ? this.#now() + this.#config.restart.idlePollMs
                : (result.originRetryAt ??
                  (after.ready > 0 ? this.#now() : after.earliestAvailableAt)),
            result: result.stopped,
            ...(result.stopped === "restart_required"
              ? {
                  restartRequiredCode: "SOURCE_BROWSER_RESTART_REQUIRED",
                }
              : {}),
            urgentStatus: result.duplicateRejected,
          };
        },
      });
    }

    const solCoordinators = await this.#createSolLanes(
      lanes,
      artifacts,
      ledger,
    );
    if (this.#config.localFanout.enabled) {
      const resolutionPath = this.#config.localFanout.resolutionPath;
      if (!this.#localEnrichmentResolver && !resolutionPath)
        throw new Error("LOCAL_ENRICHMENT_RESOLUTION_PATH_REQUIRED");
      const activeLocalResolver =
        async (): Promise<LocalEnrichmentResolver> => {
          if (this.#localEnrichmentResolver)
            return this.#localEnrichmentResolver;
          if (!resolutionPath)
            throw new Error("LOCAL_ENRICHMENT_RESOLUTION_PATH_REQUIRED");
          return this.#resolutionStore(resolutionPath);
        };
      for (const [provider, coordinator] of [
        ["sol", solCoordinators[0]],
      ] as const) {
        if (!coordinator) continue;
        const spec = ENRICHMENT_PROVIDER_SPECS[provider];
        if (
          !this.#localEnrichmentResolver &&
          !this.#config.fanout.refresh.enabled
        ) {
          try {
            // eslint-disable-next-line no-await-in-loop -- Shared external resolution must be initialized before constructing each provider's fanout lane.
            await activeLocalResolver();
          } catch (error) {
            this.#localFanoutResolutionErrors.set(provider, {
              code: resolutionErrorCode(error),
              retryAt: this.#now() + this.#config.restart.idlePollMs,
            });
          }
        }
        // Resolve through a retrying boundary so unavailable production identity
        // state cannot prevent collectors and paid-provider lanes from starting.
        const resolver: LocalEnrichmentResolver = {
          resolve: async (source, artifact) => {
            try {
              const cache = this.#resolutionDemandCache;
              if (cache) {
                const cached = cache.resolveCollected(source, artifact);
                if (cached) return cached;
                cache.registerCollectedDemand(source, artifact, [
                  spec.modelKey,
                ]);
                // Demand-driven resolution owns freshness. Probing the large
                // snapshot after a cache miss repeats expensive synchronous
                // SQLite work and cannot make this request fresher.
                return null;
              }
              const activeResolver = await activeLocalResolver();
              const result = await activeResolver.resolve(source, artifact);
              this.#localFanoutResolutionErrors.delete(provider);
              return result;
            } catch (error) {
              this.#localFanoutResolutionErrors.set(provider, {
                code: resolutionErrorCode(error),
                retryAt: this.#now() + this.#config.restart.idlePollMs,
              });
              return null;
            }
          },
        };
        const localFanout = new LocalEnrichmentFanout({
          recoverExpiredLeases: false,
          artifacts,
          batchSize: this.#config.localFanout.batchSize,
          ledger,
          profile: {
            modelKey: spec.modelKey,
            pipelineVersion: spec.pipelineVersion,
            seed: coordinator.seed.bind(coordinator),
          },
          resolver,
        });
        lanes.push({
          name: `local-enrichment-fanout-${provider}`,
          close: () => undefined,
          runOnce: async () => {
            const result = await localFanout.cycle({
              maximum: this.#config.localFanout.batchSize,
              now: this.#now,
            });
            this.#localFanoutStatus[provider] = result;
            const resolutionError =
              this.#localFanoutResolutionErrors.get(provider);
            if (resolutionError)
              this.#localFanoutResolutionErrors.set(provider, {
                ...resolutionError,
                retryAt:
                  result.earliestWakeAt ??
                  this.#now() + this.#config.restart.idlePollMs,
              });
            return {
              nextWakeAt:
                result.ready > 0 ? this.#now() : result.earliestWakeAt,
              result:
                result.scanned > 0 || result.seeded > 0
                  ? "progress"
                  : result.pendingResolution > 0
                    ? "identity_pending"
                    : "idle",
            };
          },
        });
      }
    }
    const publication = this.#createPublicationLane(artifacts, ledger);
    this.#createResolutionRefreshLane(lanes, (workKeys) => {
      requestFanoutWork?.(workKeys);
      detailFanoutWake.notify();
    });
    if (this.#config.fanout.enabled) {
      const resolutionPath = this.#config.fanout.resolutionPath;
      const staticResolutionStore = () => {
        if (!resolutionPath) throw new Error("FANOUT_RESOLUTION_PATH_REQUIRED");
        return this.#resolutionStore(resolutionPath);
      };
      const enrichment = ([["sol", solCoordinators[0]]] as const).flatMap(
        ([provider, coordinator]) => {
          if (!coordinator && !this.#config.continuousFreePublication)
            return [];
          const spec = ENRICHMENT_PROVIDER_SPECS[provider];
          return [
            {
              implementationVersion: spec.pipelineVersion,
              modelKey: spec.modelKey,
              ...(coordinator
                ? { seed: coordinator.seed.bind(coordinator) }
                : {}),
              workKind: SOL_ENRICHMENT_WORK_KIND,
            },
          ];
        },
      );
      if (!publication || enrichment.length === 0)
        throw new Error("FANOUT_DEPENDENCIES_DISABLED");
      if (!this.#resolutionDemandCache) {
        try {
          await staticResolutionStore();
        } catch (error) {
          this.#fanoutResolutionError = {
            code: resolutionErrorCode(error),
            retryAt: this.#now() + this.#config.restart.idlePollMs,
          };
        }
      }
      const resolveOrPending = async <T>(
        operation: () => Promise<T>,
      ): Promise<null | T> => {
        try {
          const result = await operation();
          this.#fanoutResolutionError = null;
          return result;
        } catch (error) {
          this.#fanoutResolutionError = {
            code: resolutionErrorCode(error),
            retryAt: this.#now() + this.#config.restart.idlePollMs,
          };
          return null;
        }
      };
      const fanout = new FanoutReconciler({
        recoverExpiredLeases: false,
        artifacts,
        batchSize: this.#config.fanout.batchSize,
        ledger,
        publication,
        resolvers: {
          approvedSol: (source, input, profile, fanoutWorkKey) => {
            const cache = this.#resolutionDemandCache;
            if (!cache) return { status: "waiting" };
            return resolveApprovedSolBinding(
              cache,
              input,
              profile.modelKey,
              source.priority,
              fanoutWorkKey,
            );
          },
          retireApprovedSol: (input, modelKey, fanoutWorkKey) => {
            const cache = this.#resolutionDemandCache;
            if (!cache) return;
            retireApprovedSolResolution(cache, input, modelKey, fanoutWorkKey);
          },
          collected: async (source, artifact) => {
            const cache = this.#resolutionDemandCache;
            if (cache) {
              const cached = cache.resolveCollected(source, artifact);
              if (cached) return cached;
              cache.registerCollectedDemand(
                source,
                artifact,
                enrichment.map(({ modelKey }) => modelKey),
              );
              return null;
            }
            const resolved = await resolveOrPending(() =>
              staticResolutionStore().then((store) =>
                store.resolve(source, artifact),
              ),
            );
            return resolved;
          },
          enrichment: async (source, input, profile, fanoutWorkKey) => {
            const cache = this.#resolutionDemandCache;
            if (cache) {
              if (profile.modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey) {
                const approved = resolveApprovedSolBinding(
                  cache,
                  input,
                  profile.modelKey,
                  source.priority,
                  fanoutWorkKey,
                );
                if (approved.status !== "resolved") return approved;
                const resolution = cache.resolvePublication(
                  approved.binding.poemId,
                  profile.modelKey,
                  approved.binding.sourceRevisionId,
                  approved.binding.lineNfcHash,
                );
                if (!resolution) return { status: "waiting" as const };
                return { ...resolution, poemId: approved.binding.poemId };
              }
              const sourceNfcSha256 =
                cache.getPublicationFingerprint(fanoutWorkKey) ??
                sha256(
                  canonicalJson(
                    input.linesArabic.map((line) => line.normalize("NFC")),
                  ),
                );
              const result = cache.resolveOrRegisterPublication({
                modelKey: profile.modelKey,
                poemId: input.poemId,
                priority: source.priority,
                sourceNfcSha256,
                sourceRevisionId: input.sourceRevisionId,
                workKey: fanoutWorkKey,
              });
              if (result.status === "resolved") return result.resolution;
              const terminal = cache.terminalCanonicalFailure(
                input.poemId,
                profile.modelKey,
                input.sourceRevisionId,
              );
              if (!terminal) return { status: "waiting" as const };
              cache.retireCanonicalDemand(
                input.poemId,
                profile.modelKey,
                input.sourceRevisionId,
              );
              cache.retirePublicationWaiter(fanoutWorkKey);
              return {
                errorCode: terminal,
                status: "conflict" as const,
              };
            }
            const resolved = await resolveOrPending(() =>
              staticResolutionStore().then((store) =>
                store.resolvePublication(
                  input.poemId,
                  profile.modelKey,
                  profile.modelKey === ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
                  input.sourceRevisionId,
                ),
              ),
            );
            return resolved;
          },
        },
        enrichment,
      });
      requestFanoutWork = (workKeys) => fanout.requestWork(workKeys);
      requestDetailFanoutScan = (now) => {
        fanout.requestSourceScan(now);
        detailFanoutWake.notify();
      };
      lanes.push({
        name: "fanout",
        close: () => fanout.close(),
        // The fanout control item supplies a 30-second repair cadence, while
        // the resolution refresher wakes exact jobs as soon as they resolve.
        // Avoid reopening the ledger every global five-second idle poll.
        honorNextWakeAt: true,
        maximumSleepMs: 30_000,
        waitForNextRun: detailFanoutWake.waitForNextRun,
        runOnce: async (signal) => {
          this.#fanoutStatus = await fanout.cycle({
            maximum: this.#config.fanout.batchSize,
            now: this.#now,
            signal,
          });
          const completedAt = this.#now();
          if (this.#fanoutResolutionError)
            this.#fanoutResolutionError = {
              ...this.#fanoutResolutionError,
              retryAt:
                this.#fanoutStatus.earliestWakeAt ??
                completedAt + this.#config.restart.idlePollMs,
            };
          const requestedWakeAt =
            this.#fanoutStatus.ready > 0
              ? completedAt
              : (this.#fanoutStatus.earliestWakeAt ??
                completedAt + this.#config.restart.idlePollMs);
          return {
            nextWakeAt: Math.max(
              completedAt + FANOUT_MINIMUM_CADENCE_MS,
              requestedWakeAt,
            ),
            result:
              this.#fanoutStatus.ready > 0 ||
              this.#fanoutStatus.enrichmentSeeded > 0 ||
              this.#fanoutStatus.solSeeded > 0 ||
              this.#fanoutStatus.collectionPublicationsSeeded > 0 ||
              this.#fanoutStatus.enrichmentPublicationsSeeded > 0
                ? "progress"
                : "idle",
          };
        },
      });
    }
    if (publication) {
      lanes.push({
        name: "publication",
        close: () => undefined,
        // Publication service/auth/recovery transitions are operator-visible
        // health changes. Refresh once per transition instead of waiting for
        // the five-minute aggregate cadence or writing on every retry.
        refreshStatusOnResultChange: true,
        runOnce: async (signal) => {
          if (
            !this.#publicationAllowed ||
            this.#publicationAuth?.status().paused
          ) {
            const refreshed = await this.preflight();
            if (!refreshed.publicationAllowed)
              return {
                nextWakeAt: Math.max(
                  this.#now() + this.#config.restart.errorBackoffMs,
                  this.#publicationAuth?.status().retryAt ?? 0,
                ),
                result: "preflight_gated",
              };
          }
          if (!this.#publicationAuth) {
            return {
              nextWakeAt: this.#now() + this.#config.restart.errorBackoffMs,
              result: "auth_unavailable",
            };
          }
          this.#publicationStatus = await publication.run(signal, {
            maximum: Math.min(
              this.#config.publication.maximumPerCycle,
              this.#capacityStatus?.recommendedChunk ?? 1,
            ),
            now: this.#now,
            paused: () => this.paused(),
          });
          return {
            nextWakeAt:
              this.#publicationStatus.retryAt ??
              (this.#publicationStatus.ready > 0
                ? this.#now()
                : this.#publicationStatus.earliestWakeAt),
            result: this.#publicationStatus.stopped,
          };
        },
      });
    }
    this.#createRetentionLane(lanes, ledger);
    this.#createSolPoemMilestoneBackfillLane(lanes, ledger);
    return lanes;
  }

  close(): void {
    this.#resolutionDemandCache?.close();
    this.#resolutionDemandCache = undefined;
    for (const store of this.#resolutionStores.values()) store.close();
    this.#resolutionStores.clear();
    this.#ledger?.close();
    this.#ledger = undefined;
  }

  paused(): Promise<boolean> {
    return Promise.resolve(this.#pauseState().paused);
  }

  paidWorkPaused(): Promise<boolean> {
    const state = this.#pauseState();
    return Promise.resolve(state.paused || state.paidWorkPaused);
  }

  async status(): Promise<unknown> {
    const gate = {
      circuitOpen: false,
      diskWritable: this.#artifacts?.capacitySnapshot.writable ?? false,
      paused: await this.paidWorkPaused(),
      quotaWaitUntil: null,
    };
    const providerSchedulers: Partial<
      Record<EnrichmentProvider, SolSchedulerSnapshot>
    > = {};
    for (const provider of ["sol"] as const) {
      const scheduler = this.#schedulers[provider];
      if (scheduler) {
        // eslint-disable-next-line no-await-in-loop -- Provider snapshots are small, ordered state reads for one coherent health document.
        providerSchedulers[provider] = await scheduler.snapshot(gate);
      }
    }
    const gates = this.#scheduler
      ? await this.#scheduler.snapshot({
          circuitOpen: false,
          diskWritable: this.#artifacts?.capacitySnapshot.writable ?? false,
          paused: await this.paidWorkPaused(),
          quotaWaitUntil: null,
        })
      : null;
    const ledgerStatus = this.#ledger?.status() ?? null;
    if (ledgerStatus)
      await this.#writePipelineHealth(ledgerStatus, providerSchedulers);
    return {
      authorInventory:
        this.#authorInventoryStatus ??
        (await this.#readOptionalStatus(this.#config.inventory.statusPath)),
      baseline: this.#baselineStatus,
      capacity: this.#capacityStatus,
      collectorSchedule: this.#collectorCoordinator?.scheduleSnapshot() ?? null,
      collectorRecovery: this.#collectorRecovery?.status() ?? null,
      enrichmentReconciliation: this.#enrichmentReconciliationStatus,
      fanout: this.#fanoutStatus,
      ledger: ledgerStatus,
      localFanout: this.#localFanoutStatus,
      publication: this.#publicationStatus,
      publicationAuth: this.#publicationAuth?.status() ?? null,
      providerExecution: this.#providerExecutionHealth,
      providerHostAdmission: this.#providerHostAdmission.snapshot(),
      retention: this.#retentionStatus,
      providerSchedulers,
      solScheduler: gates,
    };
  }

  /** Lightweight, exact-profile diagnostics for the supervisor heartbeat. */
  async providerExecutionStatus(): Promise<unknown> {
    const ledger = this.#ledger;
    if (!ledger) throw new Error("UNIFIED_RIG_LEDGER_NOT_READY");
    const now = this.#now();
    const scheduler = this.#schedulers.sol;
    const gate = {
      circuitOpen: false,
      diskWritable: this.#artifacts?.capacitySnapshot.writable ?? false,
      paused: await this.paidWorkPaused(),
      quotaWaitUntil: null,
    } as const;
    const snapshot = scheduler ? await scheduler.snapshot(gate) : undefined;
    const spec = ENRICHMENT_PROVIDER_SPECS.sol;
    const requirements = {
      implementationVersion: spec.pipelineVersion,
      schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
    } as const;
    const diagnostics = ledger.providerProfileDiagnostics(
      SOL_ENRICHMENT_WORK_KIND,
      requirements,
      now,
      {
        quarantined: [
          ...NONRECOVERABLE_UNKNOWN_OPERATION_CODES,
          ...QUARANTINED_OPERATION_CODES,
        ],
        recoverableUnknown: [...RECOVERABLE_UNKNOWN_OPERATION_CODES],
        semantic: [...SEMANTIC_FAILURE_CODES],
      },
    );
    const { paused: globalPaused, paidWorkPaused } = this.#pauseState();
    const monitoredResources = await this.#resourcePressure?.();
    const resourceReasons = new Set(monitoredResources?.reasons);
    if (!(this.#artifacts?.capacitySnapshot.writable ?? false))
      resourceReasons.add("DISK_PRESSURE");
    const schedulerBlock = snapshot?.blockReason ?? null;
    const budget = diagnostics.budget;
    const actualQuotaWait =
      budget.state === "active" &&
      snapshot?.recoveryCause === "quota" &&
      snapshot.quotaUntil > now;
    const providerWaitActive = providerWaitIsActive(
      snapshot?.providerUntil,
      now,
    );
    const authenticationWait =
      providerWaitActive && snapshot?.recoveryCause === "authentication";
    const providerState =
      providerWaitActive && snapshot?.recoveryCause === "network"
        ? ("network_wait" as const)
        : schedulerBlock === "rate_limited"
          ? ("rate_limited" as const)
          : providerWaitActive && snapshot?.recoveryCause === "provider"
            ? ("backoff" as const)
            : ("ready" as const);
    const schedulerState =
      schedulerBlock === "at_capacity" ||
      schedulerBlock === "launch_pacing" ||
      schedulerBlock === "error_dampener" ||
      schedulerBlock === "circuit_open"
        ? schedulerBlock
        : snapshot && snapshot.selectedConcurrency < snapshot.ceiling
          ? ("adaptive" as const)
          : ("ready" as const);
    const health = await this.#providerExecutionDiagnostics.write({
      configDigest: this.#configDigest,
      observedAt: now,
      providers: [
        {
          credentials: providerCredentialHealth(
            snapshot,
            authenticationWait,
            now,
            this.#config.restart.errorBackoffMs,
          ),
          debt: {
            quarantinedOperations: diagnostics.quarantinedOperations,
            recoverableUnknownOperations:
              diagnostics.recoverableUnknownOperations,
            semanticFailures: diagnostics.semanticFailures,
          },
          enabled: this.#config.sol.enabled,
          gates: {
            authentication: authenticationWait
              ? {
                  errorCode:
                    snapshot.providerErrorCode ?? "CODEX_CHATGPT_AUTH_REQUIRED",
                  retryAt: snapshot.providerUntil,
                  state: "waiting" as const,
                }
              : { errorCode: null, retryAt: null, state: "ready" as const },
            budget,
            operator: { globalPaused, paidWorkPaused },
            provider:
              providerState === "ready"
                ? { errorCode: null, retryAt: null, state: "ready" as const }
                : {
                    errorCode:
                      snapshot?.providerErrorCode ??
                      (providerState === "rate_limited"
                        ? "CODEX_RATE_LIMITED"
                        : "ENRICHMENT_PROVIDER_UNAVAILABLE"),
                    retryAt:
                      snapshot?.nextWakeAt ??
                      snapshot?.providerUntil ??
                      now + this.#config.restart.errorBackoffMs,
                    state: providerState,
                  },
            quota: actualQuotaWait
              ? {
                  errorCode: "CODEX_QUOTA_EXHAUSTED",
                  nextProbeAt:
                    snapshot.quotaProbeAt > now ? snapshot.quotaProbeAt : null,
                  retryAt: snapshot.quotaUntil,
                  state: "waiting" as const,
                }
              : {
                  errorCode: null,
                  nextProbeAt: null,
                  retryAt: null,
                  state: "clear" as const,
                },
            resources: {
              nextProbeAt:
                resourceReasons.size > 0
                  ? (monitoredResources?.nextProbeAt ??
                    now + this.#config.resources.probeIntervalMs)
                  : null,
              reasons: [...resourceReasons].toSorted(),
              state: resourceReasons.size > 0 ? "waiting" : "ready",
            },
            scheduler: {
              activeInvocations: snapshot?.active ?? 0,
              configuredConcurrency: this.#config.sol.concurrency,
              nextWakeAt: snapshot?.nextWakeAt ?? null,
              selectedConcurrency:
                snapshot?.selectedConcurrency ??
                this.#config.sol.initialConcurrency,
              state: schedulerState,
            },
          },
          model: spec.model,
          modelKey: spec.modelKey,
          progress: {
            accepted: diagnostics.progress.completed,
            activeInvocations: snapshot?.active ?? 0,
            delayedWork: Math.max(
              0,
              diagnostics.progress.byState.pending +
                diagnostics.progress.byState.retry_wait +
                diagnostics.progress.byState.quota_wait -
                diagnostics.availability.ready,
            ),
            lastAcceptedAt: diagnostics.progress.lastSuccessAt,
            readyWork: diagnostics.availability.ready,
            state:
              (snapshot?.active ?? 0) > 0
                ? "active"
                : isRecent(diagnostics.progress.lastSuccessAt, now)
                  ? "recent"
                  : diagnostics.availability.ready > 0
                    ? "stalled"
                    : "idle",
            terminalWork: diagnostics.progress.terminal,
          },
          provider: "sol",
          sessions: {
            activeCurrentAccountEpoch: snapshot?.activeCurrentAccountEpoch ?? 0,
            activePreviousAccountEpoch:
              snapshot?.activePreviousAccountEpoch ?? 0,
            activeUnattributed: snapshot?.activeUnattributed ?? 0,
          },
          throughput: providerPoemThroughput(diagnostics.poemThroughput),
        },
      ],
      runId: this.#providerExecutionRunId,
    });
    this.#providerExecutionHealth = health;
    return { providerExecution: health };
  }

  #pauseState() {
    this.#ledger ??= Ledger.initialize(this.#paths.database);
    return this.#ledger.pauseControls.read();
  }

  async #createSolLanes(
    lanes: SupervisorLane[],
    artifacts: ArtifactStore,
    ledger: Ledger,
  ): Promise<EnrichmentLaneCoordinator[]> {
    if (!this.#config.sol.enabled) return [];
    const scheduler = new QuotaAwareSolLaneScheduler({
      ceiling: this.#config.sol.concurrency,
      configDigest: providerSchedulerDigest("sol", this.#config.sol),
      credentialGeneration: () =>
        providerCredentialGeneration("sol", this.#environment),
      credentialSnapshot: () =>
        providerCredentialSnapshot("sol", this.#environment),
      fixedConcurrency:
        this.#config.sol.initialConcurrency === this.#config.sol.concurrency,
      initialConcurrency: this.#config.sol.initialConcurrency,
      legacyConfigDigests: [
        this.#configDigest,
        legacyProviderSchedulerDigest("sol", this.#config.sol),
      ],
      legacyStatePath: (await pathExists(`${this.#paths.schedulerState}.v8`))
        ? `${this.#paths.schedulerState}.v8`
        : this.#paths.schedulerState,
      legacyStateKey: "provider:sol",
      minimumLaunchIntervalMs: this.#config.sol.minimumLaunchIntervalMs,
      now: this.#now,
      promotionSuccesses: this.#config.sol.promotionSuccesses,
      requireCurrentStateStoreAuthority: this.#requireCurrentSchedulerAuthority,
      stateKey: "provider-v10:sol",
      legacyCurrentStatePath: `${this.#paths.schedulerState}.v10`,
      stateStore: ledger,
    });
    await scheduler.snapshot({
      circuitOpen: false,
      diskWritable: true,
      paused: false,
      quotaWaitUntil: null,
    });
    this.#scheduler = scheduler;
    this.#schedulers.sol = scheduler;
    const sharedRunner = this.#solCoordinatorFactory
      ? null
      : new CodexSolRunner({
          operations: ledger.solOperations,
          attemptRoot: this.#paths.solAttempts,
          credentialSnapshot: () =>
            providerCredentialSnapshot("sol", this.#environment),
          credentialGeneration: () =>
            providerCredentialGeneration("sol", this.#environment),
          cwd: process.cwd(),
        });
    const legacyRunner = this.#solCoordinatorFactory
      ? null
      : new CodexSolRunner({
          operations: ledger.solOperations,
          attemptRoot: this.#paths.solAttempts,
          credentialSnapshot: () =>
            providerCredentialSnapshot("sol", this.#environment),
          credentialGeneration: () =>
            providerCredentialGeneration("sol", this.#environment),
          cwd: process.cwd(),
          pipelineVersion: "sol-word-gloss-v2",
        });
    const legacyCoordinators = legacyRunner
      ? Array.from(
          { length: this.#config.sol.concurrency },
          (_, index) =>
            new SolEnrichmentCoordinator({
              artifacts,
              enforcePaidUsageBudget: true,
              ledger,
              owner: `sol-v2-drain-${String(process.pid)}-${String(index)}`,
              recoverUnknownOperations: false,
              runner: legacyRunner,
              startedWorkOnly: true,
            }),
        )
      : [];
    const coordinators = Array.from(
      { length: this.#config.sol.concurrency },
      (_, index) => {
        const owner = `sol-${String(process.pid)}-${String(index)}`;
        if (this.#solCoordinatorFactory)
          return this.#solCoordinatorFactory({
            artifacts,
            index,
            ledger,
            owner,
          });
        if (!sharedRunner) throw new Error("SOL_RUNNER_MISSING");
        return new SolEnrichmentCoordinator({
          artifacts,
          enforcePaidUsageBudget: true,
          ledger,
          owner,
          recoverUnknownOperations: false,
          runner: sharedRunner,
        });
      },
    );
    const reconciliationOwner = `sol-reconciliation-${String(process.pid)}`;
    const reconciliationCoordinator = this.#solCoordinatorFactory
      ? this.#solCoordinatorFactory({
          artifacts,
          index: this.#config.sol.concurrency,
          ledger,
          owner: reconciliationOwner,
        })
      : new SolEnrichmentCoordinator({
          artifacts,
          ledger,
          owner: reconciliationOwner,
          recoverUnknownOperations: true,
          runner:
            sharedRunner ??
            (() => {
              throw new Error("SOL_RUNNER_MISSING");
            })(),
        });
    const legacyReconciliationCoordinator = legacyRunner
      ? new SolEnrichmentCoordinator({
          artifacts,
          ledger,
          owner: `${reconciliationOwner}-v2`,
          recoverUnknownOperations: true,
          runner: legacyRunner,
          startedWorkOnly: true,
        })
      : null;
    let unattemptedMigrationComplete = false;
    lanes.push({
      name: "sol-artifact-reconciliation",
      close: () => undefined,
      runOnce: async (signal) => {
        if (!unattemptedMigrationComplete) {
          const migration = ledger.migrateUnattemptedSolV2Work(50, this.#now());
          unattemptedMigrationComplete = migration.migrated < 50;
        }
        if (legacyReconciliationCoordinator)
          await legacyReconciliationCoordinator.run(signal, {
            artifactReconciliationOnly: true,
            maximum: 8,
            now: this.#now,
            paused: () => this.paidWorkPaused(),
          });
        const result = await reconciliationCoordinator.run(signal, {
          artifactReconciliationOnly: true,
          maximum: 8,
          now: this.#now,
          paused: () => this.paidWorkPaused(),
        });
        return {
          nextWakeAt:
            (!unattemptedMigrationComplete ? this.#now() + 30_000 : null) ??
            result.retryAt ??
            this.#now() +
              (result.stopped === "idle"
                ? 5 * 60_000
                : this.#config.restart.idlePollMs),
          result: result.stopped,
        };
      },
    });
    for (const [index, coordinator] of coordinators.entries()) {
      let gracefulShutdownLease: SolTaskPermit["recoveryLease"] = null;
      lanes.push({
        name: `sol-${String(index + 1)}`,
        // Supervisor closes lanes only after every run loop has returned. The
        // exact scheduler instance may then release only its own idle account-
        // switch lease; a crash or replacement has no matching ownership token.
        close: async () => {
          if (gracefulShutdownLease !== null)
            await scheduler.releaseOwnedIdleAccountSwitchLease(
              gracefulShutdownLease,
            );
        },
        runOnce: async (signal) => {
          const capacity = await artifacts.capacity();
          const diskWritable = capacity.writable;
          const snapshot = await scheduler.snapshot({
            circuitOpen: false,
            diskWritable,
            paused: await this.paidWorkPaused(),
            quotaWaitUntil: null,
          });
          // These lanes create fresh paid claims only. Free artifact recovery
          // has its own lane and must continue while the budget is closed.
          const budget = ledger.solPaidUsageBudgetStatus();
          if (
            !(await this.paidWorkPaused()) &&
            (budget.state !== "active" || budget.remainingOperations < 3)
          ) {
            return {
              nextWakeAt: this.#now() + this.#config.restart.idlePollMs,
              result: "budget_exhausted",
            };
          }
          const permit = await scheduler.acquire({
            circuitOpen: false,
            diskWritable,
            paused: await this.paidWorkPaused(),
            quotaWaitUntil: null,
          });
          if (!permit)
            return {
              nextWakeAt:
                snapshot.nextWakeAt ??
                this.#now() + this.#config.restart.idlePollMs,
              result: snapshot.blockReason ?? "gated",
            };
          if (permit.recoveryLease !== null)
            gracefulShutdownLease = permit.recoveryLease;
          const hostPermit = this.#providerHostAdmission.acquire();
          if (!hostPermit) {
            await permit.complete({ kind: "idle" }, this.#now());
            return {
              nextWakeAt: this.#now() + this.#config.resources.probeIntervalMs,
              result: "host_capacity",
            };
          }
          let result: Awaited<ReturnType<SolEnrichmentRunPort["run"]>>;
          try {
            const legacyCoordinator = legacyCoordinators[index];
            const selectedCoordinator =
              legacyCoordinator && ledger.hasReadyStartedSolV2Work(this.#now())
                ? legacyCoordinator
                : coordinator;
            result = await selectedCoordinator.run(signal, {
              maximum:
                permit.purpose === "quota_probe"
                  ? 1
                  : this.#config.sol.maximumPerCyclePerLane,
              now: this.#now,
              paused: () => this.paidWorkPaused(),
            });
          } catch (error) {
            try {
              await permit.complete({ kind: "error" }, this.#now());
            } finally {
              hostPermit.release();
            }
            throw error;
          }
          let completion: Awaited<ReturnType<typeof permit.complete>>;
          try {
            completion = await permit.complete(
              result.schedulerOutcome === "ambiguous_outcome"
                ? { kind: "ambiguous_outcome" }
                : result.schedulerOutcome === "quota_wait"
                  ? result.retryAt === null
                    ? { kind: "quota_wait" }
                    : { kind: "quota_wait", retryAt: result.retryAt }
                  : result.schedulerOutcome === "provider_wait"
                    ? {
                        errorCode:
                          result.providerErrorCode ??
                          "ENRICHMENT_PROVIDER_UNAVAILABLE",
                        kind: "provider_wait",
                        ...(result.retryAt === null
                          ? {}
                          : { retryAt: result.retryAt }),
                      }
                    : result.schedulerOutcome === "network_wait"
                      ? {
                          errorCode:
                            result.providerErrorCode ??
                            "ENRICHMENT_NETWORK_UNAVAILABLE",
                          kind: "provider_wait" as const,
                          ...(result.retryAt === null
                            ? {}
                            : { retryAt: result.retryAt }),
                        }
                      : result.schedulerOutcome === "rate_limited"
                        ? result.retryAt === null
                          ? { kind: "rate_limited" }
                          : { kind: "rate_limited", retryAt: result.retryAt }
                        : { kind: result.schedulerOutcome },
              this.#now(),
            );
          } finally {
            hostPermit.release();
          }
          if (completion.accepted) {
            if (
              result.schedulerOutcome === "quota_wait" &&
              result.retryAt !== null
            ) {
              await this.#pipelineDiagnostics.recordQuota(
                "CODEX_QUOTA_EXHAUSTED",
                result.retryAt,
              );
            } else if (
              permit.purpose === "quota_probe" &&
              completion.quotaCleared
            ) {
              await this.#pipelineDiagnostics.clearQuotaAfterProbe(SOL_MODEL);
            } else if (result.schedulerOutcome === "success") {
              await this.#pipelineDiagnostics.clearQuotaAfterSuccess(SOL_MODEL);
            }
            this.#lastSolDisposition =
              result.schedulerOutcome === "success"
                ? "healthy"
                : result.schedulerOutcome === "task_failure"
                  ? "neutral"
                  : result.schedulerOutcome === "ambiguous_outcome"
                    ? "transient_error"
                    : result.schedulerOutcome === "network_wait"
                      ? "network_wait"
                      : result.schedulerOutcome === "error" ||
                          result.schedulerOutcome === "provider_wait"
                        ? "transient_error"
                        : result.schedulerOutcome;
            this.#lastSolRetryAt = result.retryAt;
          }
          const after =
            result.stopped === "idle"
              ? ledger.availability([SOL_ENRICHMENT_WORK_KIND], this.#now(), {
                  implementationVersion: SOL_PIPELINE_VERSION,
                  schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
                })
              : null;
          return {
            nextWakeAt:
              after === null
                ? (result.retryAt ?? this.#now())
                : after.ready > 0
                  ? this.#now()
                  : after.earliestAvailableAt,
            result: result.stopped,
          };
        },
      });
    }
    return coordinators;
  }

  async #writePipelineHealth(
    ledgerStatus: ReturnType<Ledger["status"]>,
    schedulers: Partial<Record<EnrichmentProvider, SolSchedulerSnapshot>>,
  ): Promise<void> {
    const ledger = this.#ledger;
    if (!ledger) throw new Error("UNIFIED_RIG_LEDGER_NOT_READY");
    const capacity = this.#artifacts?.capacitySnapshot;
    const now = this.#now();
    const lastProgressAt = ledgerStatus.lastSuccessAt ?? 0;
    const desiredConfigDigest = await this.#desiredConfigDigest();
    const duplicatePoems =
      ledgerStatus.affectedByErrorCode.find(
        ({ code }) => code === "SOURCE_POEM_DUPLICATE",
      )?.count ?? 0;
    const providerProgress = new Map<
      EnrichmentProvider,
      ReturnType<Ledger["profileProgress"]>
    >();
    const providerAvailability = new Map<
      EnrichmentProvider,
      ReturnType<Ledger["availability"]>
    >();
    const providers = (["sol"] as const).flatMap((provider) => {
      const scheduler = schedulers[provider];
      if (!this.#config[provider].enabled || !scheduler) return [];
      const spec = ENRICHMENT_PROVIDER_SPECS[provider];
      const workKind = SOL_ENRICHMENT_WORK_KIND;
      const requirements = {
        implementationVersion: spec.pipelineVersion,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      } as const;
      const progress = ledger.profileProgress(workKind, requirements);
      providerProgress.set(provider, progress);
      providerAvailability.set(
        provider,
        ledger.availability([workKind], now, requirements),
      );
      const potentiallyRecoverableOperations = ledger.countErrors(
        workKind,
        [...RECOVERABLE_UNKNOWN_OPERATION_CODES],
        requirements,
      );
      const nonrecoverableOperations = ledger.countErrors(
        workKind,
        [...NONRECOVERABLE_UNKNOWN_OPERATION_CODES],
        requirements,
      );
      const explicitlyQuarantinedOperations = ledger.countErrors(
        workKind,
        [...QUARANTINED_OPERATION_CODES],
        requirements,
      );
      const recoverableUnknownOperations = potentiallyRecoverableOperations;
      const quarantinedOperations =
        explicitlyQuarantinedOperations + nonrecoverableOperations;
      return [
        {
          accepted: progress.completed,
          activeInvocations: scheduler.active,
          blockReason: scheduler.blockReason,
          invocationConcurrency: scheduler.ceiling,
          lastDisposition:
            scheduler.blockReason === "provider_wait"
              ? "transient_error"
              : this.#lastSolDisposition,
          model: spec.model,
          modelKey: spec.modelKey,
          nextQuotaProbeAt:
            scheduler.quotaProbeAt > now ? scheduler.quotaProbeAt : null,
          provider,
          providerErrorCode: scheduler.providerErrorCode,
          quarantinedOperations,
          recoveryCause: scheduler.recoveryCause,
          recoveryLeaseUntil:
            scheduler.recoveryLeaseUntil > now
              ? scheduler.recoveryLeaseUntil
              : null,
          recoverableUnknownOperations,
          retryAt:
            scheduler.blockReason === "provider_wait"
              ? scheduler.providerUntil
              : this.#lastSolRetryAt,
          selectedConcurrency: scheduler.selectedConcurrency,
          semanticFailures: ledger.countErrors(
            workKind,
            [...SEMANTIC_FAILURE_CODES],
            requirements,
          ),
          // Retain the aggregate for backward-compatible monitors while the
          // explicit fields distinguish repairable from safely fenced work.
          unknownOperations:
            recoverableUnknownOperations + quarantinedOperations,
        },
      ];
    });
    const solHealth = providers[0];
    const solSpec = ENRICHMENT_PROVIDER_SPECS.sol;
    const solRequirements = {
      implementationVersion: solSpec.pipelineVersion,
      schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
    } as const;
    const solProgress =
      providerProgress.get("sol") ??
      ledger.profileProgress(SOL_ENRICHMENT_WORK_KIND, solRequirements);
    const solAvailability =
      providerAvailability.get("sol") ??
      ledger.availability([SOL_ENRICHMENT_WORK_KIND], now, solRequirements);
    const solScheduler = schedulers.sol;
    const { paused: globalPaused, paidWorkPaused } = this.#pauseState();
    const monitoredResources = await this.#resourcePressure?.();
    const resourceReasons = new Set(monitoredResources?.reasons);
    if (!(capacity?.writable ?? false)) resourceReasons.add("DISK_PRESSURE");
    const resourceWaiting = resourceReasons.size > 0;
    const schedulerBlock = solScheduler?.blockReason ?? null;
    const solBudget = ledger.solPaidUsageBudgetStatus();
    const actualQuotaWait =
      solBudget.state === "active" &&
      solScheduler?.recoveryCause === "quota" &&
      solScheduler.quotaUntil > now;
    const providerWaitActive = providerWaitIsActive(
      solScheduler?.providerUntil,
      now,
    );
    const authenticationWait =
      providerWaitActive && solScheduler?.recoveryCause === "authentication";
    const providerState =
      providerWaitActive && solScheduler?.recoveryCause === "network"
        ? ("network_wait" as const)
        : schedulerBlock === "rate_limited"
          ? ("rate_limited" as const)
          : providerWaitActive && solScheduler?.recoveryCause === "provider"
            ? ("backoff" as const)
            : ("ready" as const);
    const schedulerState =
      schedulerBlock === "at_capacity" ||
      schedulerBlock === "launch_pacing" ||
      schedulerBlock === "error_dampener" ||
      schedulerBlock === "circuit_open"
        ? schedulerBlock
        : solScheduler &&
            solScheduler.selectedConcurrency < solScheduler.ceiling
          ? ("adaptive" as const)
          : ("ready" as const);
    const solEntry: ProviderExecutionHealthEntryInput = {
      credentials: providerCredentialHealth(
        solScheduler,
        authenticationWait,
        now,
        this.#config.restart.errorBackoffMs,
      ),
      debt: {
        quarantinedOperations:
          solHealth?.quarantinedOperations ??
          ledger.countErrors(
            SOL_ENRICHMENT_WORK_KIND,
            [
              ...NONRECOVERABLE_UNKNOWN_OPERATION_CODES,
              ...QUARANTINED_OPERATION_CODES,
            ],
            solRequirements,
          ),
        recoverableUnknownOperations:
          solHealth?.recoverableUnknownOperations ??
          ledger.countErrors(
            SOL_ENRICHMENT_WORK_KIND,
            [...RECOVERABLE_UNKNOWN_OPERATION_CODES],
            solRequirements,
          ),
        semanticFailures:
          solHealth?.semanticFailures ??
          ledger.countErrors(
            SOL_ENRICHMENT_WORK_KIND,
            [...SEMANTIC_FAILURE_CODES],
            solRequirements,
          ),
      },
      enabled: this.#config.sol.enabled,
      gates: {
        authentication: authenticationWait
          ? {
              errorCode:
                solScheduler.providerErrorCode ?? "CODEX_CHATGPT_AUTH_REQUIRED",
              retryAt: solScheduler.providerUntil,
              state: "waiting",
            }
          : { errorCode: null, retryAt: null, state: "ready" },
        budget: solBudget,
        operator: { globalPaused, paidWorkPaused },
        provider:
          providerState === "ready"
            ? { errorCode: null, retryAt: null, state: "ready" }
            : {
                errorCode:
                  solScheduler?.providerErrorCode ??
                  (providerState === "rate_limited"
                    ? "CODEX_RATE_LIMITED"
                    : "ENRICHMENT_PROVIDER_UNAVAILABLE"),
                retryAt:
                  solScheduler?.nextWakeAt ??
                  solScheduler?.providerUntil ??
                  now + this.#config.restart.errorBackoffMs,
                state: providerState,
              },
        quota: actualQuotaWait
          ? {
              errorCode: "CODEX_QUOTA_EXHAUSTED",
              nextProbeAt:
                solScheduler.quotaProbeAt > now
                  ? solScheduler.quotaProbeAt
                  : null,
              retryAt: solScheduler.quotaUntil,
              state: "waiting",
            }
          : {
              errorCode: null,
              nextProbeAt: null,
              retryAt: null,
              state: "clear",
            },
        resources: {
          nextProbeAt: resourceWaiting
            ? (monitoredResources?.nextProbeAt ??
              now + this.#config.resources.probeIntervalMs)
            : null,
          reasons: [...resourceReasons].toSorted(),
          state: resourceWaiting ? "waiting" : "ready",
        },
        scheduler: {
          activeInvocations: solScheduler?.active ?? 0,
          configuredConcurrency: this.#config.sol.concurrency,
          nextWakeAt: solScheduler?.nextWakeAt ?? null,
          selectedConcurrency:
            solScheduler?.selectedConcurrency ??
            this.#config.sol.initialConcurrency,
          state: schedulerState,
        },
      },
      model: solSpec.model,
      modelKey: solSpec.modelKey,
      progress: {
        accepted: solProgress.completed,
        activeInvocations: solScheduler?.active ?? 0,
        delayedWork: Math.max(
          0,
          solProgress.byState.pending +
            solProgress.byState.retry_wait +
            solProgress.byState.quota_wait -
            solAvailability.ready,
        ),
        lastAcceptedAt: solProgress.lastSuccessAt,
        readyWork: solAvailability.ready,
        state:
          (solScheduler?.active ?? 0) > 0
            ? "active"
            : isRecent(solProgress.lastSuccessAt, now)
              ? "recent"
              : solAvailability.ready > 0
                ? "stalled"
                : "idle",
        terminalWork: solProgress.terminal,
      },
      provider: "sol",
      sessions: {
        activeCurrentAccountEpoch: solScheduler?.activeCurrentAccountEpoch ?? 0,
        activePreviousAccountEpoch:
          solScheduler?.activePreviousAccountEpoch ?? 0,
        activeUnattributed: solScheduler?.activeUnattributed ?? 0,
      },
      throughput: providerPoemThroughput(
        ledger.poemThroughput(solRequirements, now),
      ),
    };
    this.#providerExecutionHealth =
      await this.#providerExecutionDiagnostics.write({
        configDigest: this.#configDigest,
        observedAt: now,
        providers: [solEntry],
        runId: this.#providerExecutionRunId,
      });
    const providerHostAdmission = this.#providerHostAdmission.snapshot();
    const collectorProgress = ledgerStatus.kindProgress.filter(({ kind }) =>
      [
        collectionWorkKinds().authorManifest,
        collectionWorkKinds().poemDetail,
      ].includes(kind),
    );
    const collectorNetworkWait = ledgerStatus.affectedByKindAndErrorCode.some(
      ({ code, count, kind }) =>
        count > 0 &&
        code === COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE &&
        [
          collectionWorkKinds().authorManifest,
          collectionWorkKinds().poemDetail,
        ].includes(kind),
    );
    const lanes = [
      ...(this.#config.collector.enabled
        ? [
            growthLaneHealth(
              "collector",
              collectorProgress,
              capacity?.writable ?? false,
              null,
              now,
            ),
          ]
        : []),
      ...(["sol"] as const).flatMap((provider) => {
        const scheduler = schedulers[provider];
        if (!this.#config[provider].enabled || !scheduler) return [];
        const progress = providerProgress.get(provider);
        return [
          growthLaneHealth(
            provider,
            progress ? [progress] : [],
            capacity?.writable ?? false,
            scheduler,
            now,
          ),
        ];
      }),
    ];
    const viableLanes = lanes.filter(
      ({ state }) => state !== "blocked" && state !== "fenced",
    ).length;
    const localLastSuccessAt = maximumTimestamp(
      lanes.map(({ lastProgressAt: timestamp }) => timestamp),
    );
    const localGrowthState =
      lanes.length === 0
        ? "disabled"
        : lanes.some(({ state }) => state === "running") ||
            isRecent(localLastSuccessAt, now)
          ? "active"
          : viableLanes > 0
            ? "retrying"
            : "stalled";
    const publicationProgress = ledgerStatus.kindProgress.filter(({ kind }) =>
      kind.includes("publication"),
    );
    const publicationLastSuccessAt = maximumTimestamp(
      publicationProgress.map(({ lastSuccessAt }) => lastSuccessAt),
    );
    const publicationOutstanding = publicationProgress.reduce(
      (total, progress) =>
        total +
        progress.byState.pending +
        progress.byState.quota_wait +
        progress.byState.retry_wait +
        progress.byState.running,
      0,
    );
    const publicationRunning = publicationProgress.reduce(
      (total, progress) => total + progress.byState.running,
      0,
    );
    const publicationAvailability =
      publicationProgress.length === 0
        ? null
        : ledger.availability(
            publicationProgress.map(({ kind }) => kind),
            now,
          );
    const publicationOldestReadyAt =
      publicationAvailability?.ready === 0
        ? null
        : (publicationAvailability?.earliestAvailableAt ?? null);
    const publicationStalled =
      publicationOutstanding > 0 &&
      (publicationAvailability?.ready !== 0 || publicationRunning > 0) &&
      (publicationLastSuccessAt === null
        ? publicationOldestReadyAt !== null &&
          now - publicationOldestReadyAt > GROWTH_SUCCESS_SLO_MS
        : !isRecent(publicationLastSuccessAt, now));
    await this.#pipelineDiagnostics.writeHealth({
      checks: [
        ...(desiredConfigDigest === null
          ? [
              {
                code: "DESIRED_CONFIG_UNAVAILABLE",
                detail: "The desired runtime configuration cannot be loaded",
                retryAt: null,
                state: "blocked" as const,
              },
            ]
          : []),
        ...(capacity && !capacity.writable
          ? [
              {
                code: "ARTIFACT_STORE_DISK_PRESSURE",
                detail: `${String(capacity.availableBytes)} bytes available; ${String(capacity.minimumFreeBytes)} reserved`,
                retryAt: null,
                state: "blocked" as const,
              },
            ]
          : []),
        ...providers.flatMap(({ provider, recoverableUnknownOperations }) =>
          recoverableUnknownOperations === 0
            ? []
            : [
                {
                  code: "CODEX_OPERATION_OUTCOME_UNKNOWN",
                  detail: `${String(recoverableUnknownOperations)} ${provider} paid operation outcome(s) await reconciliation`,
                  retryAt: null,
                  state: "warning" as const,
                },
              ],
        ),
        ...(["sol"] as const).flatMap((provider) => {
          const error = this.#localFanoutResolutionErrors.get(provider);
          return error
            ? [
                {
                  code: `${provider.toUpperCase()}_LOCAL_FANOUT_RESOLUTION_UNAVAILABLE`,
                  detail: `${provider} local fanout is waiting for production resolution (${error.code})`,
                  retryAt: error.retryAt,
                  state: "warning" as const,
                },
              ]
            : [];
        }),
        ...(this.#fanoutResolutionError
          ? [
              {
                code: "FANOUT_RESOLUTION_UNAVAILABLE",
                detail: `production fanout is waiting for production resolution (${this.#fanoutResolutionError.code})`,
                retryAt: this.#fanoutResolutionError.retryAt,
                state: "warning" as const,
              },
            ]
          : []),
        ...(this.#resolutionRefreshError
          ? [
              {
                code: "PRODUCTION_RESOLUTION_REFRESH_UNAVAILABLE",
                detail: `production resolution refresh is unavailable (${this.#resolutionRefreshError.code})`,
                retryAt: this.#resolutionRefreshError.retryAt,
                state: "warning" as const,
              },
            ]
          : []),
        ...(duplicatePoems > 0
          ? [
              {
                code: "SOURCE_POEM_DUPLICATE",
                detail: `${String(duplicatePoems)} canonical poem ownership conflict(s) were rejected before collection`,
                retryAt: null,
                state: "blocked" as const,
              },
            ]
          : []),
      ],
      configDigest: this.#configDigest,
      desiredConfigDigest,
      growth: {
        local: {
          enabledLanes: lanes.length,
          lastSuccessAt: localLastSuccessAt,
          state: localGrowthState,
          viableLanes,
        },
        production: {
          configured: this.#config.publication.enabled,
          lastSuccessAt: publicationLastSuccessAt,
          state: !this.#config.publication.enabled
            ? "disabled"
            : !this.#publicationAllowed
              ? "gated"
              : isRecent(publicationLastSuccessAt, now)
                ? "active"
                : publicationStalled
                  ? "stalled"
                  : "ready",
        },
      },
      lanes,
      lastProgressAt: lastProgressAt === 0 ? null : lastProgressAt,
      localFanout: (["sol"] as const).flatMap((provider) => {
        const status = this.#localFanoutStatus[provider];
        return status
          ? [
              {
                cursor: status.cursor,
                pendingResolution: status.pendingResolution,
                provider,
                seeded: status.seeded,
              },
            ]
          : [];
      }),
      origins: ledgerStatus.origins.map((origin) => ({
        ...origin,
        state: deriveCollectorOriginHealthState({
          configured: this.#config.collector.enabled,
          consecutiveFailures: origin.consecutiveFailures,
          cooldownUntil: origin.cooldownUntil,
          nextAllowedAt: origin.nextAllowedAt,
          now,
          ...(this.#collectorOriginState === "disabled"
            ? {}
            : {
                runtimeState:
                  this.#collectorOriginState === "healthy" &&
                  collectorNetworkWait
                    ? "network_wait"
                    : this.#collectorOriginState,
              }),
          stopReason: origin.stopReason,
        }),
      })),
      providerHostAdmission: {
        activeProcesses: providerHostAdmission.activeProcesses,
        maximumProcesses: providerHostAdmission.maximumProcesses,
        remainingProcesses: providerHostAdmission.remainingProcesses,
      },
      queues: [
        ...ledgerStatus.kindProgress.filter(
          ({ kind }) =>
            kind !== SOL_ENRICHMENT_WORK_KIND &&
            !kind.startsWith("poem-enrichment-"),
        ),
        ...providerProgress.values(),
      ].map(({ byState, kind, lastSuccessAt, total }) => {
        const exactAvailability = (["sol"] as const)
          .map((provider) => ({
            availability: providerAvailability.get(provider),
            progress: providerProgress.get(provider),
          }))
          .find(({ progress }) => progress?.kind === kind)?.availability;
        return {
          active: byState.running,
          deadLetter: byState.dead_letter,
          kind,
          lastSuccessAt,
          oldestReadyAt:
            exactAvailability === undefined
              ? ledgerStatus.earliestWorkAvailableAt
              : exactAvailability.earliestAvailableAt,
          pending: byState.pending,
          quotaWait: byState.quota_wait,
          retryWait: byState.retry_wait,
          succeeded: byState.succeeded + byState.imported,
          total,
        };
      }),
      retention: (["sol"] as const).flatMap((provider) => {
        const status = this.#retentionStatus[provider];
        return status
          ? [
              {
                applied: status.applied,
                candidates: status.candidates.length,
                projectedArchiveBytes: status.projectedArchiveBytes,
                provider,
              },
            ]
          : [];
      }),
      runId: `process:${String(process.pid)}`,
      providers,
      sol:
        this.#config.sol.enabled && schedulers.sol && solHealth
          ? {
              accepted: solHealth.accepted,
              activeInvocations: solHealth.activeInvocations,
              invocationConcurrency: solHealth.invocationConcurrency,
              lastDisposition: solHealth.lastDisposition,
              model: solHealth.model,
              retryAt: solHealth.retryAt,
              selectedConcurrency: solHealth.selectedConcurrency,
              semanticFailures: solHealth.semanticFailures,
            }
          : null,
      storage: capacity
        ? {
            availableBytes: capacity.availableBytes,
            headroomBytes: capacity.availableBytes - capacity.minimumFreeBytes,
            minimumFreeBytes: capacity.minimumFreeBytes,
            writable: capacity.writable,
          }
        : null,
    });
  }

  #createPublicationLane(
    artifacts: ArtifactStore,
    ledger: Ledger,
  ): null | PublicationLane {
    if (!this.#config.publication.enabled) return null;
    const endpoint = this.#config.publication.endpoint;
    if (!endpoint) throw new Error("PUBLICATION_ENDPOINT_REQUIRED");
    const transport: PublicationTransport = (request) => {
      const current = this.#publicationAuth?.transport;
      return current
        ? current(request)
        : Promise.reject(new Error("PUBLICATION_PREFLIGHT_GATED"));
    };
    return new PublicationLane({
      recoverExpiredLeases: false,
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: this.#config.publication.allowInsecureLocalhost,
        endpoint,
        transport,
      }),
      ledger,
      onCollectionPromoted: (input) => {
        this.#resolutionDemandCache?.wakeSourceResolution(input);
      },
      refreshEnrichment: async (action) => {
        const cache = this.#resolutionDemandCache;
        if (cache)
          return cache.refreshPublicationConflict({
            eventId: sha256(
              canonicalJson({ action, reason: "publication-conflict" }),
            ),
            expectedPointerVersion:
              action.input.publication.expectedPointerVersion,
            expectedWriterEpoch: action.input.publication.writerEpoch,
            modelKey: action.input.artifact.modelKey,
            poemId: action.input.publication.poemId,
            priority: 0,
            sourceRevisionId: action.input.artifact.sourceRevisionId,
          });
        const resolutionPath = this.#config.fanout.resolutionPath;
        if (!resolutionPath) return null;
        const resolutionStore = await this.#resolutionStore(resolutionPath);
        return resolutionStore.resolvePublication(
          action.input.publication.poemId,
          action.input.artifact.modelKey,
          true,
          action.input.artifact.sourceRevisionId,
        );
      },
      refreshWriterEpoch: async (action) => {
        const cache = this.#resolutionDemandCache;
        if (cache)
          return cache.refreshCollectionConflict({
            eventId: sha256(
              canonicalJson({ action, reason: "publication-conflict" }),
            ),
            expectedWriterEpoch: action.input.bundle.writerEpoch,
            targets: action.input.records.map((record) => ({
              sourceAuthorSlug: record.sourceAuthorId,
              sourcePoemId: record.sourcePoemId,
            })),
          });
        const resolutionPath = this.#config.fanout.resolutionPath;
        if (!resolutionPath) return null;
        const resolutionStore = await this.#resolutionStore(resolutionPath);
        const report = await resolutionStore.report();
        return report.writerEpoch;
      },
    });
  }

  #createRetentionLane(lanes: SupervisorLane[], ledger: Ledger): void {
    if (!this.#config.retention.enabled) return;
    const providers = (["sol"] as const).flatMap((provider) => {
      if (!this.#config[provider].enabled) return [];
      const spec = ENRICHMENT_PROVIDER_SPECS[provider];
      return [
        {
          provider,
          retention: new SolAttemptRetention(this.#paths.solAttempts),
          spec,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ];
    });
    if (providers.length === 0) return;
    const nextEligibleByProvider = new Map<EnrichmentProvider, null | number>();
    const nextScanByProvider = new Map<EnrichmentProvider, number>();
    const retentionCursorByProvider = new Map<
      EnrichmentProvider,
      null | string
    >();
    const retentionDigestByProvider = new Map<EnrichmentProvider, string>();
    for (const { provider } of providers) {
      const stored = ledger.loadSchedulerState(retentionStateKey(provider));
      if (!stored) {
        retentionCursorByProvider.set(provider, null);
        continue;
      }
      if (sha256(Buffer.from(stored.serialized, "utf8")) !== stored.digest)
        throw new Error("RETENTION_SCAN_STATE_DIGEST_INVALID");
      const state = RetentionScanStateSchema.parse(
        JSON.parse(stored.serialized),
      );
      retentionCursorByProvider.set(provider, state.cursor);
      retentionDigestByProvider.set(provider, stored.digest);
    }
    let providerIndex = 0;
    lanes.push({
      name: "maintenance-retention",
      close: () => undefined,
      honorNextWakeAt: true,
      maximumSleepMs: RETENTION_SCAN_CONTINUATION_INTERVAL_MS,
      resourcePressureExempt: true,
      runOnce: async () => {
        const target = providers[providerIndex];
        if (!target) throw new Error("RETENTION_PROVIDER_MISSING");
        const now = this.#now();
        const nextScanAt = nextScanByProvider.get(target.provider) ?? 0;
        if (now < nextScanAt) {
          providerIndex = (providerIndex + 1) % providers.length;
          return {
            nextWakeAt: providerIndex === 0 ? nextScanAt : now,
            result: "waiting",
          };
        }
        const scanCursor =
          retentionCursorByProvider.get(target.provider) ?? null;
        const attemptScan = await target.retention.scanAttemptIds(
          scanCursor,
          RETENTION_SCAN_BATCH_SIZE,
        );
        const eligibility = ledger.attemptRetentionEligibility(
          target.workKind,
          target.spec.pipelineVersion,
          now,
          this.#config.retention.safetyAgeMs,
          attemptScan.attemptIds,
        );
        const policy = {
          attemptIds: attemptScan.attemptIds,
          maximumAttempts: this.#config.retention.maximumAttemptsPerCycle,
          maximumInputBytes: this.#config.retention.maximumInputBytesPerCycle,
          maximumScannedAttempts: RETENTION_SCAN_BATCH_SIZE,
          minimumFreeBytes: this.#config.retention.minimumFreeBytes,
          purgeArchivedDiagnostics:
            this.#config.retention.purgeArchivedDiagnostics,
          scanCursor,
          scanWindowComplete: attemptScan.done,
        };
        const report = this.#config.retention.apply
          ? await target.retention.apply(eligibility, policy)
          : await target.retention.plan(eligibility, policy);
        this.#retentionStatus[target.provider] = report;
        const priorDigest = retentionDigestByProvider.get(target.provider);
        if (report.scanCursor !== null || priorDigest !== undefined) {
          const serialized = `${JSON.stringify({
            cursor: report.scanCursor,
            schemaVersion: 1,
          })}\n`;
          const digest = sha256(Buffer.from(serialized, "utf8"));
          if (
            !ledger.saveSchedulerState(
              retentionStateKey(target.provider),
              serialized,
              digest,
              priorDigest ?? null,
              now,
            )
          ) {
            throw new Error("RETENTION_SCAN_STATE_WRITE_CONFLICT");
          }
          retentionDigestByProvider.set(target.provider, digest);
        }
        retentionCursorByProvider.set(target.provider, report.scanCursor);
        nextEligibleByProvider.set(target.provider, eligibility.nextEligibleAt);
        nextScanByProvider.set(
          target.provider,
          now +
            (report.scanComplete
              ? RETENTION_SCAN_INTERVAL_MS
              : RETENTION_SCAN_CONTINUATION_INTERVAL_MS),
        );
        providerIndex = (providerIndex + 1) % providers.length;
        const completedRound = providerIndex === 0;
        let nextCachedWake = now + RETENTION_SCAN_INTERVAL_MS;
        for (const value of nextScanByProvider.values())
          nextCachedWake = Math.min(nextCachedWake, value);
        for (const value of nextEligibleByProvider.values()) {
          if (value !== null) nextCachedWake = Math.min(nextCachedWake, value);
        }
        return {
          nextWakeAt: completedRound ? nextCachedWake : now,
          result: report.applied ? "archived" : "dry_run",
        };
      },
    });
  }

  #createSolPoemMilestoneBackfillLane(
    lanes: SupervisorLane[],
    ledger: Ledger,
  ): void {
    const eventTypes = ["succeeded", "imported"] as const;
    const complete = new Set<(typeof eventTypes)[number]>();
    let eventIndex = 0;
    lanes.push({
      name: "maintenance-sol-poem-milestones",
      close: () => undefined,
      honorNextWakeAt: true,
      maximumSleepMs: SOL_MILESTONE_BACKFILL_COMPLETE_INTERVAL_MS,
      runOnce: () => {
        const now = this.#now();
        const eventType = eventTypes[eventIndex];
        if (!eventType) throw new Error("SOL_MILESTONE_EVENT_TYPE_MISSING");
        const result = ledger.backfillSolPoemMilestones(
          eventType,
          SOL_MILESTONE_BACKFILL_BATCH_SIZE,
          now,
        );
        if (result.complete) complete.add(eventType);
        else complete.delete(eventType);
        eventIndex = (eventIndex + 1) % eventTypes.length;
        const backfillComplete = complete.size === eventTypes.length;
        return Promise.resolve({
          nextWakeAt:
            now +
            (backfillComplete
              ? SOL_MILESTONE_BACKFILL_COMPLETE_INTERVAL_MS
              : result.complete
                ? 0
                : SOL_MILESTONE_BACKFILL_CONTINUATION_INTERVAL_MS),
          result: backfillComplete ? "complete" : "backfilling",
        });
      },
    });
  }

  #createResolutionRefreshLane(
    lanes: SupervisorLane[],
    notifyFanoutWork: (workKeys: readonly string[]) => void,
  ): void {
    const refresh = this.#config.fanout.refresh;
    const publicationEndpoint = this.#config.publication.endpoint;
    if (!refresh.enabled) return;
    if (!publicationEndpoint)
      throw new Error("PRODUCTION_RESOLUTION_REFRESH_CONFIG_REQUIRED");
    const endpoint =
      refresh.endpoint ??
      new URL("/api/corpus-resolution", publicationEndpoint).toString();
    if (!refresh.cachePath)
      throw new Error("PRODUCTION_RESOLUTION_CACHE_PATH_REQUIRED");
    this.#resolutionDemandCache = new ProductionResolutionDemandCache({
      now: this.#now,
      path: refresh.cachePath,
    });
    this.#resolutionDemandCache.retireInactiveModelState(
      APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey),
    );
    if (this.#config.continuousFreePublication) {
      this.#resolutionDemandCache.retireSourceDemands();
      this.#resolutionDemandCache.retireUnboundCanonicalDemands();
    }
    const sharedAuth =
      this.#publicationAuth ??
      createAuthenticatedPublicationTransport({
        config: this.#config.publication.auth,
        environment: this.#environment,
        now: this.#now,
      });
    this.#publicationAuth = sharedAuth;
    const refresher = new DemandDrivenProductionResolutionRefresher({
      auth: sharedAuth,
      authorize: (signal) => this.#authorizePublication(sharedAuth, signal),
      // Publish-only recovery is driven by durable canonical/fingerprint
      // waiters. Replaying the source bootstrap would reintroduce collection
      // refresh work after it was deliberately retired above.
      bootstrapRequestPath: this.#config.continuousFreePublication
        ? null
        : refresh.requestPath,
      cache: this.#resolutionDemandCache,
      endpoint,
      now: this.#now,
      retryBackoffMs: this.#config.restart.errorBackoffMs,
      wakePublications: (workKeys) => {
        const ledger = this.#ledger;
        if (!ledger) throw new Error("FANOUT_LEDGER_UNAVAILABLE");
        const result = ledger.wakeResolutionPending(
          workKeys,
          [FANOUT_DETAIL_KIND, FANOUT_ENRICHMENT_KIND, FANOUT_SOL_KIND],
          this.#now(),
        );
        if (result.acknowledge.length > 0) notifyFanoutWork(result.acknowledge);
        return result;
      },
    });
    lanes.push({
      name: "production-resolution-demand-refresh",
      close: () => {
        this.#resolutionDemandCache?.close();
        this.#resolutionDemandCache = undefined;
      },
      runOnce: async (signal) => {
        try {
          const result = await refresher.runOnce(signal);
          this.#resolutionRefreshError = null;
          return { nextWakeAt: result.nextWakeAt, result: result.state };
        } catch (error) {
          this.#resolutionRefreshError = {
            code: resolutionErrorCode(error),
            retryAt: this.#now() + this.#config.restart.errorBackoffMs,
          };
          throw error;
        }
      },
    });
    // Publish-only runs must not migrate the untranslated Sol backlog. That
    // migration registers canonical resolution demand for every queued paid
    // job and can starve the completed-artifact fanout this run exists to
    // drain. Resume the migration when the Sol lane is explicitly enabled.
    if (!this.#config.sol.enabled) return;
    const ledger = this.#ledger;
    if (!ledger) throw new Error("LEGACY_SOURCE_BINDING_LEDGER_UNAVAILABLE");
    const legacyBindings = new LegacySourceBindingReconciler({
      cache: this.#resolutionDemandCache,
      ledger,
    });
    lanes.push({
      name: "legacy-source-binding-reconciliation",
      close: () => undefined,
      runOnce: () => {
        const result = legacyBindings.cycle(this.#now());
        return Promise.resolve({
          nextWakeAt:
            result.seeded > 0 || result.conflicts > 0
              ? this.#now()
              : this.#now() + this.#config.restart.idlePollMs,
          result:
            result.seeded > 0
              ? "progress"
              : result.pendingResolution > 0
                ? "identity_pending"
                : result.conflicts > 0
                  ? "classified"
                  : "idle",
        });
      },
    });
  }

  #createSourceLineageMaintenanceLane(lanes: SupervisorLane[]): void {
    const publicationEndpoint = this.#config.publication.endpoint;
    if (!this.#config.publication.enabled || !publicationEndpoint) return;
    const endpoint = new URL(
      "/api/source-lineage-maintenance",
      publicationEndpoint,
    ).toString();
    lanes.push({
      name: "maintenance-source-lineage",
      close: () => undefined,
      honorNextWakeAt: true,
      maximumSleepMs: 30_000,
      resourcePressureExempt: true,
      runOnce: async (signal) => {
        const now = this.#now();
        const auth = this.#publicationAuth;
        if (!this.#publicationAllowed || !auth) {
          return { nextWakeAt: now + 30_000, result: "waiting" };
        }
        const response = await auth.transport({
          body: canonicalJson({ maxPages: 2 }),
          signal,
          url: endpoint,
        });
        if (response.status !== 200) {
          return {
            nextWakeAt: now + this.#config.restart.errorBackoffMs,
            result: response.authFailure ? "auth-wait" : "retry-wait",
          };
        }
        const parsed = SourceLineageMaintenanceResponseSchema.parse(
          JSON.parse(response.body),
        );
        if (
          parsed.result.state === "complete" ||
          parsed.result.state === "blocked"
        ) {
          return {
            nextWakeAt: now + 5 * 60_000,
            result: parsed.result.state,
          };
        }
        return {
          nextWakeAt: now + 1_000 + (now % 1_001),
          result: parsed.result.remaining === 0 ? "complete" : "progress",
        };
      },
    });
  }

  async #authorizePublication(
    auth: AuthenticatedPublicationTransport,
    signal?: AbortSignal,
  ): Promise<PublicationAuthPreflight> {
    const status = auth.status();
    const previous = PublicationPreflightStateSchema.safeParse(
      this.#publicationAuthPreflight,
    );
    if (previous.success && previous.data.state === "ready" && !status.paused)
      return { state: "ready", status };
    const result = await auth.preflight(signal);
    this.#publicationAuthPreflight = result;
    this.#publicationAllowed =
      result.state === "ready" && (this.#capacityStatus?.go ?? false);
    return result;
  }

  async #resolutionStore(
    path: string,
  ): Promise<ReadableProductionResolutionStore> {
    const existing = this.#resolutionStores.get(path);
    if (existing) return existing;
    const store = await openProductionResolutionStore(path);
    this.#resolutionStores.set(path, store);
    return store;
  }

  async #readOptionalStatus(path: null | string): Promise<unknown> {
    if (!path || !(await pathExists(path))) return null;
    try {
      return await this.#readBoundedJson(path);
    } catch (error) {
      return {
        errorCode:
          error instanceof Error ? error.message : "STATUS_INPUT_INVALID",
        state: "invalid",
      };
    }
  }

  async #readBoundedJson(path: string): Promise<unknown> {
    const data = await readFile(path);
    if (data.byteLength > MAXIMUM_STATUS_BYTES)
      throw new Error("UNIFIED_RIG_INPUT_TOO_LARGE");
    const parsed: unknown = JSON.parse(data.toString("utf8"));
    return parsed;
  }
}

function resolutionErrorCode(error: unknown): string {
  const candidate =
    error instanceof Error && /^[A-Z][A-Z0-9_]{0,99}$/.test(error.message)
      ? error.message
      : typeof error === "object" &&
          error !== null &&
          "code" in error &&
          typeof error.code === "string" &&
          /^[A-Z][A-Z0-9_]{0,99}$/.test(error.code)
        ? error.code
        : "PRODUCTION_RESOLUTION_UNAVAILABLE";
  return candidate;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function providerSchedulerDigest(
  provider: EnrichmentProvider,
  _configuration: ScraperOperationConfig[EnrichmentProvider],
): string {
  return schedulerStateDigest({
    profile: ENRICHMENT_PROVIDER_SPECS[provider],
    provider,
    schemaVersion: 2,
  });
}

/** Pre-v10 scheduler identity included mutable throughput tuning. Accept its
 * exact current value once so deployment preserves existing safety pressure;
 * all later concurrency changes use the stable provider/profile identity. */
function legacyProviderSchedulerDigest(
  provider: EnrichmentProvider,
  configuration: ScraperOperationConfig[EnrichmentProvider],
): string {
  return schedulerStateDigest({
    configuration,
    profile: ENRICHMENT_PROVIDER_SPECS[provider],
    provider,
  });
}

function providerCredentialHealth(
  scheduler: SolSchedulerSnapshot | undefined,
  authenticationWait: boolean,
  now: number,
  errorBackoffMs: number,
): ProviderExecutionHealthEntryInput["credentials"] {
  if (!scheduler)
    return {
      accountEpoch: 0,
      change: "none",
      changedAt: null,
      errorCode: null,
      lastVerifiedAt: null,
      materialEpoch: 0,
      retryAt: null,
      state: "ready",
    };
  const common = {
    accountEpoch: scheduler.credentialAccountEpoch,
    change: scheduler.credentialChange,
    changedAt: scheduler.credentialChangedAt,
    lastVerifiedAt: scheduler.credentialLastVerifiedAt,
    materialEpoch: scheduler.credentialMaterialEpoch,
  } as const;
  if (scheduler.credentialObservation === "absent")
    return {
      ...common,
      change: "none",
      errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
      retryAt: null,
      state: "absent",
    };
  if (scheduler.credentialObservation === "transient_unavailable")
    return {
      ...common,
      change: "none",
      errorCode: "ENRICHMENT_PROVIDER_AUTH_TIMEOUT",
      retryAt: now + errorBackoffMs,
      state: "observation_wait",
    };
  if (scheduler.credentialChange === "material_refresh")
    return {
      ...common,
      errorCode: "ENRICHMENT_AUTH_REQUIRED",
      retryAt: now + errorBackoffMs,
      state: "material_verification",
    };
  if (scheduler.credentialChange === "account_switch")
    return {
      ...common,
      errorCode: "ENRICHMENT_AUTH_REQUIRED",
      retryAt: now + errorBackoffMs,
      state:
        scheduler.activePreviousAccountEpoch > 0
          ? "account_switch_wait"
          : "account_verification",
    };
  if (authenticationWait)
    return {
      ...common,
      errorCode: scheduler.providerErrorCode ?? "CODEX_CHATGPT_AUTH_REQUIRED",
      retryAt: scheduler.providerUntil,
      state: "observation_wait",
    };
  return {
    ...common,
    errorCode: null,
    retryAt: null,
    state: "ready",
  };
}

function growthLaneHealth(
  name: string,
  progress: readonly ReturnType<Ledger["status"]>["kindProgress"][number][],
  diskWritable: boolean,
  scheduler: null | SolSchedulerSnapshot,
  now: number,
): PipelineHealthInput["lanes"][number] {
  const active = progress.reduce(
    (total, item) => total + item.byState.running,
    0,
  );
  const completed = progress.reduce((total, item) => total + item.completed, 0);
  const failed = progress.reduce(
    (total, item) => total + item.byState.dead_letter,
    0,
  );
  const ready = progress.reduce(
    (total, item) =>
      total +
      item.byState.pending +
      item.byState.quota_wait +
      item.byState.retry_wait,
    0,
  );
  const lastSuccessAt = maximumTimestamp(
    progress.map((item) => item.lastSuccessAt),
  );
  const knownWait = [
    "error_dampener",
    "provider_wait",
    "quota_wait",
    "rate_limited",
  ].includes(scheduler?.blockReason ?? "");
  const stalled =
    ready > 0 &&
    !knownWait &&
    ((lastSuccessAt === null &&
      (failed > 0 || (scheduler?.consecutiveErrors ?? 0) > 0)) ||
      (lastSuccessAt !== null && !isRecent(lastSuccessAt, now)));
  const state = !diskWritable
    ? "fenced"
    : scheduler?.blockReason === "paused"
      ? "blocked"
      : active > 0
        ? "running"
        : knownWait
          ? "waiting"
          : stalled
            ? "blocked"
            : "idle";
  return {
    active,
    completed,
    failed,
    lastDurationMs: null,
    lastErrorCode: !diskWritable
      ? "ARTIFACT_STORE_DISK_PRESSURE"
      : stalled
        ? "GROWTH_SUCCESS_STALLED"
        : (scheduler?.providerErrorCode ?? null),
    lastProgressAt: lastSuccessAt,
    name,
    state,
  };
}

function isRecent(timestamp: null | number, now: number): boolean {
  return timestamp !== null && now - timestamp <= GROWTH_SUCCESS_SLO_MS;
}

function collectorRecoveryWakeAt(
  ledger: Ledger,
  state: CollectorRecoveryState,
  now: number,
  idlePollMs: number,
): null | number {
  if (state.phase === "ready" || state.phase === "resting")
    return state.nextActionAt;
  if (state.phase !== "observing" || !state.reservation) return null;
  const wakeTimes = state.reservation.workKeys.flatMap((workKey) => {
    const work = ledger.get(workKey);
    if (!work || work.state === "dead_letter") return [now];
    if (
      work.state === "pending" ||
      work.state === "quota_wait" ||
      work.state === "retry_wait"
    )
      return [Math.max(now, work.availableAt)];
    if (work.state === "running" && work.leaseExpiresAt !== null)
      return [Math.max(now, work.leaseExpiresAt)];
    return [];
  });
  return wakeTimes.length === 0 ? now + idlePollMs : Math.min(...wakeTimes);
}

function retentionStateKey(provider: EnrichmentProvider): string {
  return `retention-scan:${provider}`;
}

function maximumTimestamp(
  timestamps: readonly (null | number)[],
): null | number {
  const values = timestamps.filter(
    (timestamp): timestamp is number => timestamp !== null,
  );
  return values.length === 0 ? null : Math.max(...values);
}
