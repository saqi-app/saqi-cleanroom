import { setMaxListeners } from "node:events";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { type JsonlLoggerHealth, RotatingJsonlLogger } from "./jsonl-logger.js";
import type { ScraperOperationConfig } from "./operations-contract.js";
import {
  ResourcePressureMonitor,
  type ResourcePressureSnapshot,
} from "./resource-pressure.js";
import type { RunLockRecord } from "./run-lock.js";
import { acquireRunLock } from "./run-lock.js";
import type { RuntimeReleaseIdentity } from "./runtime-installer.js";
import {
  MAXIMUM_STATUS_AGE_MS,
  STATUS_HEARTBEAT_MS,
} from "./status-freshness.js";

const STATUS_SNAPSHOT_INTERVAL_MS = 5 * 60_000;
const PROVIDER_STATUS_HEARTBEAT_MS = 60_000;
const LISTENER_HEADROOM = 32;

export function supervisorMaximumListeners(
  config: Pick<ScraperOperationConfig, "sol">,
): number {
  return (config.sol.enabled ? config.sol.concurrency : 0) + LISTENER_HEADROOM;
}

export interface SupervisorLaneResult {
  readonly nextWakeAt: null | number;
  readonly restartRequiredCode?: string;
  readonly result: string;
  readonly urgentStatus?: boolean;
}

export interface SupervisorLane {
  close(): Promise<void> | void;
  /**
   * Honor a future `nextWakeAt` instead of applying the supervisor's ordinary
   * idle-poll ceiling. Use only for lanes whose producers have a separate
   * bounded wake path or whose own requested wake already bounds latency.
   */
  readonly honorNextWakeAt?: boolean;
  /** Maximum sleep for a lane that honors its requested wake time. */
  readonly maximumSleepMs?: number;
  readonly name: string;
  /** Publish aggregate diagnostics when this lane enters a different result state. */
  readonly refreshStatusOnResultChange?: boolean;
  /** Maintenance may run under pressure to reclaim bounded diagnostics. */
  readonly resourcePressureExempt?: boolean;
  runOnce(signal: AbortSignal): Promise<SupervisorLaneResult>;
  /** Optional lane-local, interruptible replacement for the post-cycle wait. */
  readonly waitForNextRun?: (
    milliseconds: number,
    signal: AbortSignal,
  ) => Promise<void>;
}

/** Coalesces producer hints without losing a hint that arrives before sleep. */
// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete wake primitive owns coalescing state; it is not an independently selected service implementation.
export class CoalescingLaneWake {
  #pending = false;
  #waiter: (() => void) | null = null;

  notify(): void {
    this.#pending = true;
    this.#waiter?.();
  }

  readonly waitForNextRun = (
    milliseconds: number,
    signal: AbortSignal,
  ): Promise<void> => {
    if (this.#pending) {
      this.#pending = false;
      return Promise.resolve();
    }
    if (signal.aborted) return Promise.resolve();
    if (this.#waiter) throw new Error("Lane wake already has an active waiter");
    return new Promise((resolvePromise) => {
      const timer = setTimeout(() => finish(false), milliseconds);
      const onAbort = () => finish(false);
      const finish = (consumeWake: boolean) => {
        clearTimeout(timer);
        signal.removeEventListener("abort", onAbort);
        this.#waiter = null;
        if (consumeWake) this.#pending = false;
        resolvePromise();
      };
      this.#waiter = () => finish(true);
      signal.addEventListener("abort", onAbort, { once: true });
    });
  };
}

export interface SupervisorOptions {
  readonly config: ScraperOperationConfig;
  readonly configDigest: string;
  readonly createLanes: (
    preflight: null | SupervisorPreflight,
  ) => Promise<readonly SupervisorLane[]> | readonly SupervisorLane[];
  readonly desiredConfigDigest?: () => null | Promise<null | string> | string;
  readonly now?: () => number;
  readonly paused: () => boolean | Promise<boolean>;
  readonly preflight?: () => Promise<SupervisorPreflight> | SupervisorPreflight;
  /** Bounded, exact-profile provider diagnostics; must not scan full history. */
  readonly providerStatus?: () => unknown;
  readonly resourcePressure?: Pick<ResourcePressureMonitor, "snapshot">;
  readonly runtimeRelease?: null | RuntimeReleaseIdentity;
  readonly sleep?: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly status?: () => unknown;
  readonly statusSleep?: (
    milliseconds: number,
    signal: AbortSignal,
  ) => Promise<void>;
}

export interface SupervisorPreflight {
  readonly publicationAllowed: boolean;
  readonly status: unknown;
}

export type SupervisorDrainReason = "external" | "maximum_runtime";

export const MAXIMUM_RUNTIME_DRAIN_REASON = "maximum_runtime";

export interface SupervisorRunResult {
  readonly cycles: number;
  readonly drainReason?: SupervisorDrainReason;
  readonly runId: string;
  readonly stopped:
    "aborted" | "config_changed" | "drained" | "maximum" | "restart_requested";
}

type SupervisorStatusState =
  "fenced" | "paused" | "running" | "starting" | "stopped";

interface SupervisorStartupStatus {
  readonly errorCode?: string;
  readonly lanesReady: boolean;
  readonly phase: "creating_lanes" | "failed" | "preflight" | "ready";
}

function abortableSleep(
  milliseconds: number,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolvePromise) => {
    const timer = setTimeout(finish, milliseconds);
    function finish() {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolvePromise();
    }
    signal.addEventListener("abort", finish, { once: true });
  });
}

function supervisorRunResult(
  cycles: number,
  runId: string,
  signal: AbortSignal,
  drainSignal: AbortSignal,
  configDrainSignal: AbortSignal,
  restartRequested: boolean,
): SupervisorRunResult {
  const stopped = signal.aborted
    ? "aborted"
    : drainSignal.aborted
      ? "drained"
      : configDrainSignal.aborted
        ? restartRequested
          ? "restart_requested"
          : "config_changed"
        : "maximum";
  return {
    cycles,
    ...(stopped === "drained"
      ? {
          drainReason:
            drainSignal.reason === MAXIMUM_RUNTIME_DRAIN_REASON
              ? MAXIMUM_RUNTIME_DRAIN_REASON
              : ("external" as const),
        }
      : {}),
    runId,
    stopped,
  };
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete lifecycle owner composes SupervisorLane contracts; a mirror supervisor interface adds no substitution boundary.
export class UnifiedSupervisor {
  readonly #config: ScraperOperationConfig;
  readonly #configDigest: string;
  readonly #createLanes: SupervisorOptions["createLanes"];
  readonly #desiredConfigDigest: () => null | Promise<null | string> | string;
  readonly #now: () => number;
  readonly #paused: () => boolean | Promise<boolean>;
  readonly #preflight: SupervisorOptions["preflight"];
  readonly #providerStatus: (() => unknown) | undefined;
  readonly #sleep: (milliseconds: number, signal: AbortSignal) => Promise<void>;
  readonly #status: (() => unknown) | undefined;
  readonly #statusSleep: (
    milliseconds: number,
    signal: AbortSignal,
  ) => Promise<void>;
  readonly #runtimeRelease: null | RuntimeReleaseIdentity;
  readonly #resourcePressure: Pick<ResourcePressureMonitor, "snapshot">;

  constructor(options: SupervisorOptions) {
    this.#config = options.config;
    this.#configDigest = options.configDigest;
    this.#createLanes = options.createLanes;
    this.#desiredConfigDigest =
      options.desiredConfigDigest ?? (() => this.#configDigest);
    this.#now = options.now ?? Date.now;
    this.#paused = options.paused;
    this.#preflight = options.preflight;
    this.#providerStatus = options.providerStatus;
    this.#sleep = options.sleep ?? abortableSleep;
    this.#status = options.status;
    this.#statusSleep = options.statusSleep ?? abortableSleep;
    this.#runtimeRelease = options.runtimeRelease ?? null;
    this.#resourcePressure =
      options.resourcePressure ??
      new ResourcePressureMonitor(
        this.#config.stateDirectory,
        this.#config.resources,
        { now: this.#now },
      );
  }

  async run(
    signal: AbortSignal,
    options: {
      readonly drainSignal?: AbortSignal;
      readonly maximumCycles?: number;
      /** Processing budget measured from lane readiness, excluding startup. */
      readonly maximumRuntimeMs?: number;
    } = {},
  ): Promise<SupervisorRunResult> {
    const maximumCycles = options.maximumCycles ?? Infinity;
    const maximumRuntimeMs = options.maximumRuntimeMs;
    if (
      maximumRuntimeMs !== undefined &&
      (!Number.isSafeInteger(maximumRuntimeMs) || maximumRuntimeMs < 1_000)
    )
      throw new Error("Maximum runtime must be an integer of at least 1000");
    const externalDrainSignal =
      options.drainSignal ?? new AbortController().signal;
    const maximumRuntimeDrainController = new AbortController();
    const maximumRuntimeAbortController = new AbortController();
    const operationSignal = AbortSignal.any([
      signal,
      maximumRuntimeAbortController.signal,
    ]);
    const drainSignal = AbortSignal.any([
      externalDrainSignal,
      maximumRuntimeDrainController.signal,
    ]);
    const configDrainController = new AbortController();
    const schedulingSignal = AbortSignal.any([
      operationSignal,
      drainSignal,
      configDrainController.signal,
    ]);
    setMaxListeners(supervisorMaximumListeners(this.#config), schedulingSignal);
    const root = this.#config.stateDirectory;
    await mkdir(root, { mode: 0o700, recursive: true });
    const logger = new RotatingJsonlLogger(
      resolve(root, "logs"),
      this.#config.logging.maximumBytes,
      this.#config.logging.retainedFiles,
    );
    const lock = await acquireRunLock(
      resolve(root, "RUN.lock"),
      this.#configDigest,
    );
    let lanes: readonly SupervisorLane[] = [];
    let preflight: null | SupervisorPreflight = null;
    let cycles = 0;
    let nextFullStatusWriteAt = -Infinity;
    let nextProviderStatusWriteAt = -Infinity;
    let providerStatusWritable = true;
    let statusWritable = true;
    let statusWriteTail: Promise<void> = Promise.resolve();
    // Retain one aggregate only within this run; heartbeat timestamps must not
    // make its slower ledger diagnostics appear newly sampled.
    let aggregateWorkload: { observedAt: number; value: object } | undefined;
    let schedulingState: "paused" | "running" | null = null;
    let resourceState: null | ResourcePressureSnapshot["state"] = null;
    let configDrainReason: "CONFIG_CHANGED" | "CONFIG_INVALID" | null = null;
    const restartRequest: { reason: null | string } = { reason: null };
    let terminalResult: null | SupervisorRunResult = null;
    let maximumRuntimeAbortTimer: ReturnType<typeof setTimeout> | undefined;
    let maximumRuntimeDrainTimer: ReturnType<typeof setTimeout> | undefined;
    const providerHeartbeatController = new AbortController();
    let providerHeartbeatTask: Promise<void> = Promise.resolve();
    let startup: SupervisorStartupStatus = {
      lanesReady: false,
      phase: "preflight",
    };
    const log = (
      lane: string,
      event: string,
      fields: Partial<{
        durationMs: number;
        errorCode: string;
        result: string;
      }> = {},
    ) =>
      logger.write({
        ...fields,
        event,
        lane,
        runId: lock.record.runId,
        timestamp: new Date(this.#now()).toISOString(),
      });
    const writeStatus = async (
      state: SupervisorStatusState,
      force = false,
      scope: "full" | "identity" | "provider" = "full",
    ) => {
      const pending = statusWriteTail.then(async () => {
        const now = this.#now();
        const nextWriteAt =
          scope === "full"
            ? nextFullStatusWriteAt
            : scope === "provider"
              ? nextProviderStatusWriteAt
              : -Infinity;
        if (!force && now < nextWriteAt) return;
        const writable = await this.#writeStatus(
          root,
          lock.record,
          logger.path,
          logger.health(),
          state,
          preflight,
          startup,
          scope === "identity"
            ? undefined
            : async () => {
                const sample = await (scope === "full"
                  ? this.#status?.()
                  : this.#providerStatus?.());
                const observedAt = this.#now();
                if (scope === "full") {
                  aggregateWorkload = isWorkloadObject(sample)
                    ? { observedAt, value: sample }
                    : undefined;
                } else if (
                  aggregateWorkload &&
                  observedAt >= aggregateWorkload.observedAt &&
                  observedAt - aggregateWorkload.observedAt <=
                    MAXIMUM_STATUS_AGE_MS &&
                  isWorkloadObject(sample)
                ) {
                  return {
                    observedAt: aggregateWorkload.observedAt,
                    value: { ...aggregateWorkload.value, ...sample },
                  };
                }
                return { observedAt, value: sample };
              },
        );
        statusWritable = writable;
        if (scope !== "identity") providerStatusWritable = writable;
        // Pace from completion, not snapshot start. Large ledgers can make a
        // workload snapshot expensive; starting the deadline beforehand can
        // cause queued callers to begin another scan immediately and starve
        // every provider lane. Serialized callers coalesce behind this value.
        const completedAt = this.#now();
        if (scope === "full") {
          nextFullStatusWriteAt = completedAt + STATUS_SNAPSHOT_INTERVAL_MS;
          nextProviderStatusWriteAt =
            completedAt + PROVIDER_STATUS_HEARTBEAT_MS;
        } else if (scope === "provider") {
          nextProviderStatusWriteAt =
            completedAt + PROVIDER_STATUS_HEARTBEAT_MS;
        }
      });
      statusWriteTail = pending.catch((error: unknown) => {
        statusWritable = false;
        if (scope !== "identity") {
          providerStatusWritable = false;
          nextProviderStatusWriteAt =
            this.#now() + PROVIDER_STATUS_HEARTBEAT_MS;
        }
        console.error(
          "SAQI_STATUS_WRITE_FAILED",
          operationalErrorCode(error, "STATUS_WRITE_FAILED"),
        );
      });
      await pending;
    };
    const startProviderHeartbeat = (): void => {
      if (this.#providerStatus === undefined) return;
      providerHeartbeatTask = (async () => {
        const delay = PROVIDER_STATUS_HEARTBEAT_MS;
        for (;;) {
          // eslint-disable-next-line no-await-in-loop -- Heartbeats are intentionally serial and completion-paced.
          await this.#statusSleep(delay, providerHeartbeatController.signal);
          if (providerHeartbeatController.signal.aborted) break;
          // eslint-disable-next-line no-await-in-loop -- Long provider calls still need bounded configuration-drift detection.
          if (!(await observeConfig())) break;
          const scope =
            this.#now() >= nextFullStatusWriteAt ? "full" : "provider";
          try {
            // eslint-disable-next-line no-await-in-loop -- A later heartbeat must not overlap the current atomic status write.
            await writeStatus(schedulingState ?? "running", false, scope);
          } catch (error) {
            // Keep the heartbeat alive so a transient snapshot or filesystem
            // failure fences admission only until the next successful write.
            console.error(
              "SAQI_PROVIDER_STATUS_HEARTBEAT_FAILED",
              operationalErrorCode(error, "STATUS_WRITE_FAILED"),
            );
          }
        }
      })().catch((error: unknown) => {
        statusWritable = false;
        console.error(
          "SAQI_PROVIDER_STATUS_HEARTBEAT_FAILED",
          operationalErrorCode(error, "STATUS_WRITE_FAILED"),
        );
      });
    };
    const observeSchedulingState = async (
      paused: boolean,
      force = false,
      sample = true,
    ) => {
      const state = paused ? "paused" : "running";
      const changed = schedulingState !== state;
      if (changed) {
        const previousState = schedulingState;
        schedulingState = state;
        if (previousState !== null)
          await log("supervisor", paused ? "paused" : "resumed");
      }
      if (sample || force || changed)
        await writeStatus(state, force || changed);
    };
    const observeResourceState = async (
      snapshot: ResourcePressureSnapshot,
    ): Promise<void> => {
      const changed = resourceState !== snapshot.state;
      if (changed && resourceState !== null) {
        resourceState = snapshot.state;
        const reason = snapshot.reasons[0];
        await log(
          "supervisor",
          snapshot.state === "resource_wait"
            ? "resource_wait"
            : "resource_resumed",
          reason ? { errorCode: reason } : {},
        );
      }
      resourceState = snapshot.state;
      if (changed) await writeStatus("running", true);
    };
    const observeConfig = async (): Promise<boolean> => {
      const desired = await this.#desiredConfigDigest();
      if (desired === this.#configDigest) return true;
      if (configDrainReason === null) {
        configDrainReason =
          desired === null ? "CONFIG_INVALID" : "CONFIG_CHANGED";
        // Configuration drift is a graceful drain, not a hard cancellation.
        // Lanes already inside runOnce retain the hard-stop signal and may
        // finish/fence paid work; sleeping and not-yet-started lanes stop
        // admission immediately. launchd can then restart with one coherent
        // configuration generation.
        await log("supervisor", "config_drain", {
          errorCode: configDrainReason,
        });
        await writeStatus("running", true);
        configDrainController.abort(new Error(configDrainReason));
      }
      return false;
    };
    try {
      await log("supervisor", "started");
      // Acknowledge the exact process, run, configuration, and immutable
      // release as soon as ownership is fenced. Startup is deliberately not a
      // ready state: no lane exists yet, and no network or paid work can be
      // admitted until createLanes has completed successfully.
      await writeStatus("starting", true, "identity");
      try {
        preflight = this.#preflight ? await this.#preflight() : null;
      } catch (error) {
        const errorCode = operationalErrorCode(error, "PREFLIGHT_FAILED");
        startup = { errorCode, lanesReady: false, phase: "failed" };
        await log("supervisor", "startup_fault", { errorCode });
        await writeStatus("stopped", true, "identity");
        throw error;
      }
      await log("supervisor", "preflight", {
        result: preflight?.publicationAllowed ? "go" : "gated",
      });
      startup = { lanesReady: false, phase: "creating_lanes" };
      await writeStatus("starting", true, "identity");
      try {
        lanes = await this.#createLanes(preflight);
        for (const lane of lanes) {
          if (
            lane.maximumSleepMs !== undefined &&
            (!Number.isSafeInteger(lane.maximumSleepMs) ||
              lane.maximumSleepMs < 1)
          )
            throw new Error("Lane maximum sleep must be a positive integer");
        }
      } catch (error) {
        const errorCode = operationalErrorCode(
          error,
          "LANE_CONSTRUCTION_FAILED",
        );
        startup = { errorCode, lanesReady: false, phase: "failed" };
        await log("supervisor", "startup_fault", { errorCode });
        await writeStatus("stopped", true, "identity");
        throw error;
      }
      startup = { lanesReady: true, phase: "ready" };
      // Publish the new process, run, configuration, and immutable release
      // identity before any lane can start network or paid work. Service
      // control can then distinguish a real restart from a fresh RUN.lock
      // paired with the previous process's status document.
      const initiallyPaused = await this.#paused();
      const initialResources = await this.#resourcePressure.snapshot(true);
      resourceState = initialResources.state;
      schedulingState = initiallyPaused ? "paused" : "running";
      if (initiallyPaused) await log("supervisor", "paused");
      // Publish ready identity immediately. The first aggregate workload scan
      // is deferred so provider lanes can begin admission without waiting on
      // a production-scale ledger traversal.
      await writeStatus(schedulingState, true, "identity");
      nextFullStatusWriteAt = this.#now() + STATUS_SNAPSHOT_INTERVAL_MS;
      if (this.#providerStatus !== undefined) {
        try {
          // The provider snapshot is bounded/indexed and must become durable
          // before synchronous first-cycle SQLite work can monopolize the
          // event loop. A failure keeps admission fenced while the heartbeat
          // retries on its normal cadence.
          await writeStatus(schedulingState, true, "provider");
        } catch {
          // writeStatus already logged the failure and closed both status
          // admission fences. Startup continues only so the bounded heartbeat
          // can repair the durable diagnostic and reopen admission.
          providerStatusWritable = false;
        }
      }
      startProviderHeartbeat();
      await observeConfig();
      // The bounded runtime is a processing budget, not a startup deadline.
      // Arm it only after lanes and the ready identity are durable so an
      // expensive preflight or lane construction cannot consume the entire
      // window and drain before the first admission. Startup health remains
      // observable through the explicit `starting` status.
      if (
        maximumRuntimeMs !== undefined &&
        !operationSignal.aborted &&
        !drainSignal.aborted &&
        !configDrainController.signal.aborted
      ) {
        maximumRuntimeDrainTimer = setTimeout(
          () =>
            maximumRuntimeDrainController.abort(MAXIMUM_RUNTIME_DRAIN_REASON),
          maximumRuntimeMs,
        );
        maximumRuntimeAbortTimer = setTimeout(
          () =>
            maximumRuntimeAbortController.abort(
              new Error("Maximum runtime drain grace exceeded"),
            ),
          maximumRuntimeMs + this.#config.restart.shutdownGraceMs,
        );
      }
      const runLane = async (lane: SupervisorLane): Promise<number> => {
        let laneCycles = 0;
        let lastReportedAt = 0;
        let lastReportedResult: null | string = null;
        while (
          !operationSignal.aborted &&
          !drainSignal.aborted &&
          !configDrainController.signal.aborted &&
          laneCycles < maximumCycles
        ) {
          // eslint-disable-next-line no-await-in-loop -- Every lane admission revalidates the active configuration generation.
          if (!(await observeConfig())) break;
          // eslint-disable-next-line no-await-in-loop -- Pause state is re-read before every lane admission.
          if (await this.#paused()) {
            // eslint-disable-next-line no-await-in-loop -- Pause transitions must be durable before the lane sleeps.
            await observeSchedulingState(true);
            // eslint-disable-next-line no-await-in-loop -- Each lane must remain serial while independently scheduled.
            await this.#sleep(
              this.#config.restart.idlePollMs,
              schedulingSignal,
            );
            continue;
          }
          // eslint-disable-next-line no-await-in-loop -- Resume state must be durable before admitting lane work.
          await observeSchedulingState(false, false, false);
          if (
            !lane.resourcePressureExempt &&
            (!statusWritable || !providerStatusWritable)
          ) {
            // A runtime that cannot publish its identity and health must not
            // admit new network or paid work. Retention remains eligible to
            // reclaim space; all other lanes retry the atomic status write on
            // the bounded snapshot cadence without consuming attempts.
            // eslint-disable-next-line no-await-in-loop -- Admission remains closed until diagnostics are durable again.
            await this.#sleep(
              Math.max(1, nextProviderStatusWriteAt - this.#now()),
              schedulingSignal,
            );
            continue;
          }
          if (!lane.resourcePressureExempt) {
            // eslint-disable-next-line no-await-in-loop -- Resource pressure is re-read before every non-exempt lane admission.
            const resources = await this.#resourcePressure.snapshot();
            // eslint-disable-next-line no-await-in-loop -- Resource transitions gate each independent admission cycle.
            await observeResourceState(resources);
            if (resources.state === "resource_wait") {
              // eslint-disable-next-line no-await-in-loop -- Admission remains closed without consuming attempts until resources recover.
              await this.#sleep(
                Math.max(
                  1,
                  Math.min(
                    this.#config.restart.idlePollMs,
                    resources.nextProbeAt - this.#now(),
                  ),
                ),
                schedulingSignal,
              );
              continue;
            }
          }
          laneCycles += 1;
          const started = this.#now();
          let outcome: SupervisorLaneResult;
          try {
            // eslint-disable-next-line no-await-in-loop -- A lane cannot overlap its previous invocation.
            outcome = await lane.runOnce(operationSignal);
          } catch (error) {
            // eslint-disable-next-line no-await-in-loop -- Fault logging must complete before the lane enters backoff.
            await log(lane.name, "fault", {
              durationMs: this.#now() - started,
              errorCode: operationalErrorCode(error, "UNKNOWN_LANE_ERROR"),
            });
            outcome = {
              nextWakeAt: this.#now() + this.#config.restart.errorBackoffMs,
              result: "fault",
            };
          }
          const completedAt = this.#now();
          const resultChanged = outcome.result !== lastReportedResult;
          if (
            outcome.restartRequiredCode !== undefined &&
            restartRequest.reason === null
          ) {
            restartRequest.reason = operationalErrorCode(
              { code: outcome.restartRequiredCode },
              "LANE_RESTART_REQUIRED",
            );
            // eslint-disable-next-line no-await-in-loop -- Restart intent must be logged before the global admission drain begins.
            await log(lane.name, "restart_requested", {
              errorCode: restartRequest.reason,
            });
            // eslint-disable-next-line no-await-in-loop -- Restart fencing must be durable before admission drains.
            await writeStatus("running", true);
            // This is a graceful admission drain: in-flight provider work gets
            // its original hard-stop signal and may finish/fence before the
            // launchd replacement starts with a fresh browser process.
            configDrainController.abort(new Error(restartRequest.reason));
          }
          // eslint-disable-next-line no-await-in-loop -- A completed lane may have crossed a configuration reload boundary.
          await observeConfig();
          const report =
            resultChanged ||
            completedAt - lastReportedAt >= STATUS_HEARTBEAT_MS;
          if (report) {
            if (outcome.result !== "fault")
              // eslint-disable-next-line no-await-in-loop -- Per-lane cycle records preserve completion order.
              await log(lane.name, "cycle", {
                durationMs: completedAt - started,
                result: outcome.result,
              });
            lastReportedAt = completedAt;
            lastReportedResult = outcome.result;
          }
          // The global deadline makes this cheap for every lane while keeping
          // diagnostics fresh even when all lane outcomes remain unchanged.
          // eslint-disable-next-line no-await-in-loop -- Pause state is sampled after each completed lane cycle.
          const pausedAfterCycle = await this.#paused();
          // The first pause transition already forces a complete aggregate
          // snapshot. Do not let many simultaneously finishing urgent lanes
          // bypass the global snapshot throttle after that transition.
          // eslint-disable-next-line no-await-in-loop -- Each cycle publishes pause and urgent-status transitions in order.
          await observeSchedulingState(
            pausedAfterCycle,
            (outcome.urgentStatus === true ||
              (lane.refreshStatusOnResultChange === true && resultChanged)) &&
              !pausedAfterCycle,
          );
          if (laneCycles >= maximumCycles) break;
          const now = this.#now();
          const idlePollAt = now + this.#config.restart.idlePollMs;
          const requestedWakeAt = outcome.nextWakeAt ?? idlePollAt;
          const wakeAt =
            outcome.nextWakeAt === null
              ? idlePollAt
              : lane.honorNextWakeAt
                ? Math.min(
                    requestedWakeAt,
                    now + (lane.maximumSleepMs ?? requestedWakeAt - now),
                  )
                : Math.min(idlePollAt, requestedWakeAt);
          // eslint-disable-next-line no-await-in-loop -- Wake times and backoff are lane-local.
          await (lane.waitForNextRun ?? this.#sleep)(
            Math.max(1, wakeAt - now),
            schedulingSignal,
          );
        }
        return laneCycles;
      };
      if (lanes.length === 0) {
        while (
          !operationSignal.aborted &&
          !drainSignal.aborted &&
          !configDrainController.signal.aborted &&
          cycles < maximumCycles
        ) {
          // eslint-disable-next-line no-await-in-loop -- Lane-free supervisors still revalidate configuration on every poll.
          if (!(await observeConfig())) break;
          // eslint-disable-next-line no-await-in-loop -- Lane-free supervisors still sample pause state on each ordered poll.
          const paused = await this.#paused();
          // eslint-disable-next-line no-await-in-loop -- Lane-free pause transitions remain durable and ordered.
          await observeSchedulingState(paused);
          if (!paused) cycles += 1;
          if (cycles >= maximumCycles) break;
          // eslint-disable-next-line no-await-in-loop -- A lane-free supervisor still polls for pause and shutdown.
          await this.#sleep(this.#config.restart.idlePollMs, schedulingSignal);
        }
      } else {
        const completedCycles = await Promise.all(
          lanes.map((lane) => runLane(lane)),
        );
        cycles = Math.min(...completedCycles);
      }
      terminalResult = supervisorRunResult(
        cycles,
        lock.record.runId,
        operationSignal,
        drainSignal,
        configDrainController.signal,
        restartRequest.reason !== null,
      );
      return terminalResult;
    } finally {
      providerHeartbeatController.abort();
      await providerHeartbeatTask;
      if (maximumRuntimeDrainTimer !== undefined)
        clearTimeout(maximumRuntimeDrainTimer);
      if (maximumRuntimeAbortTimer !== undefined)
        clearTimeout(maximumRuntimeAbortTimer);
      const closing = Promise.allSettled(
        lanes.map((lane) => Promise.resolve(lane.close())),
      );
      let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
      const settled = await Promise.race([
        closing,
        new Promise<null>(
          (resolvePromise) =>
            (shutdownTimer = setTimeout(
              resolvePromise,
              this.#config.restart.shutdownGraceMs,
              null,
            )),
        ),
      ]);
      if (shutdownTimer !== undefined) clearTimeout(shutdownTimer);
      if (settled === null) {
        await log("supervisor", "shutdown_timeout", {
          errorCode: "SHUTDOWN_GRACE_EXCEEDED",
        });
        await writeStatus("fenced", true);
        // Retaining the lock fences a replacement process from overlapping
        // lanes whose shutdown could not be proven complete.
        throw new Error("Lane shutdown grace exceeded; RUN.lock retained");
      }
      for (const [index, outcome] of settled.entries()) {
        if (outcome.status === "rejected")
          // eslint-disable-next-line no-await-in-loop -- Shutdown faults are recorded in deterministic lane order.
          await log(lanes[index]?.name ?? "unknown", "close_fault", {
            errorCode: operationalErrorCode(
              outcome.reason,
              "UNKNOWN_CLOSE_ERROR",
            ),
          });
      }
      if (settled.some((outcome) => outcome.status === "rejected")) {
        // A rejected close leaves ownership uncertain. Retain the lock so a
        // replacement process cannot overlap a possibly live lane.
        await writeStatus("fenced", true);
        throw new Error("Lane shutdown failed; RUN.lock retained");
      }
      try {
        terminalResult ??= supervisorRunResult(
          cycles,
          lock.record.runId,
          operationSignal,
          drainSignal,
          configDrainController.signal,
          restartRequest.reason !== null,
        );
        await log("supervisor", "stopped", {
          result: startup.phase === "failed" ? "fault" : terminalResult.stopped,
        });
        await statusWriteTail;
        await writeStatus(
          "stopped",
          true,
          startup.phase === "failed" ? "identity" : "full",
        );
      } finally {
        await lock.release();
      }
    }
  }

  async #writeStatus(
    root: string,
    lock: RunLockRecord,
    activeLog: string,
    logging: JsonlLoggerHealth,
    state: SupervisorStatusState,
    preflight: null | SupervisorPreflight,
    startup: SupervisorStartupStatus,
    workload:
      (() => Promise<{ observedAt: number; value: unknown }>) | undefined,
  ): Promise<boolean> {
    const path = resolve(root, "status.json");
    const temporary = `${path}.${lock.runId}.tmp`;
    try {
      const sample = await workload?.();
      await writeFile(
        temporary,
        `${JSON.stringify({
          activeLog,
          configDigest: lock.configDigest,
          desiredConfigDigest: await this.#desiredConfigDigest(),
          loadedConfigDigest: lock.configDigest,
          logging,
          ownerPid: lock.pid,
          runId: lock.runId,
          runtimeRelease: this.#runtimeRelease,
          schemaId: this.#config.schemaId,
          schemaVersion: this.#config.schemaVersion,
          startedAt: lock.startedAt,
          state,
          startup,
          ...(preflight ? { preflight: preflight.status } : {}),
          resourcePressure: await this.#resourcePressure.snapshot(),
          ...(sample
            ? {
                workload: sample.value,
                workloadObservedAt: new Date(sample.observedAt).toISOString(),
              }
            : {}),
          // Timestamp completion, not snapshot start. Service control and the
          // widget must measure the age of the document actually published.
          observedAt: new Date(this.#now()).toISOString(),
        })}\n`,
        { mode: 0o600 },
      );
      await rename(temporary, path);
      return true;
    } catch (error) {
      try {
        await rm(temporary, { force: true });
      } catch {
        // Preserve the original status-write failure.
      }
      if (!isRecoverableFilesystemPressure(error)) throw error;
      // The last atomic status remains valid. Admission stays fail-closed and
      // the next throttled snapshot retries after pressure clears.
      return false;
    }
  }
}

function isWorkloadObject(value: unknown): value is object {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isRecoverableFilesystemPressure(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("code" in error))
    return false;
  return (
    typeof error.code === "string" &&
    ["EDQUOT", "EIO", "EMFILE", "ENFILE", "ENOSPC", "EROFS"].includes(
      error.code,
    )
  );
}

function operationalErrorCode(error: unknown, fallback: string): string {
  if (typeof error === "object" && error !== null && "code" in error) {
    const code = error.code;
    if (typeof code === "string" && /^[A-Z][A-Z0-9_]{1,127}$/.test(code))
      return code;
  }
  if (error instanceof Error) {
    const code = /^([A-Z][A-Z0-9_]{1,127})(?::|$)/.exec(error.message)?.[1];
    if (code) return code;
    if (/^[A-Z][A-Za-z0-9]{1,127}Error$/.test(error.name))
      return error.name.replaceAll(/([a-z\d])([A-Z])/g, "$1_$2").toUpperCase();
  }
  return fallback;
}
