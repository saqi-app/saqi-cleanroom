import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  open,
  readFile,
  rename,
  rm,
  stat,
  unlink,
} from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { promisify } from "node:util";

import { z } from "zod";

import {
  LaunchdServiceLabelSchema,
  minimumLaunchdExitTimeoutSeconds,
} from "./launchd-contract.js";
import { serviceEnabledPath } from "./launchd-service.js";
import {
  type LoadedOperationConfig,
  loadScraperOperationConfig,
} from "./operations-contract.js";
import {
  actOnRunOwner,
  inspectRunOwner,
  releaseDeadRunOwner,
} from "./run-lock.js";
import {
  readServiceEnabled,
  writeServiceEnabled,
} from "./service-enabled-control.js";
import { MAXIMUM_STATUS_AGE_MS } from "./status-freshness.js";

const executeFile = promisify(execFile);
const ActionSchema = z.enum(["restart", "start", "status", "stop"]);
const RuntimeStateSchema = z.enum([
  "fenced",
  "paused",
  "running",
  "starting",
  "stopped",
]);
const StartupPhaseSchema = z.enum([
  "creating_lanes",
  "failed",
  "preflight",
  "ready",
]);
const AcknowledgedConfigDigestSchema = z.string().regex(/^[a-f\d]{64}$/);
const POLL_MS = 500;
const MAXIMUM_STATUS_BYTES = 1024 * 1024;
const CONTROL_LOCK_INITIALIZATION_GRACE_MS = 30_000;
const DesiredRuntimeManifestSchema = z.looseObject({
  commit: z.string().regex(/^[a-f\d]{40}$/),
});
const RuntimeStatusIdentitySchema = z.looseObject({
  configDigest: z.string().regex(/^[a-f\d]{64}$/),
  loadedConfigDigest: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .optional(),
  observedAt: z.iso.datetime(),
  ownerPid: z.int().positive(),
  runId: z.uuid(),
  runtimeRelease: z
    .looseObject({
      commit: z.string().regex(/^[a-f\d]{40}$/),
      manifestPath: z.string().min(1),
    })
    .nullish(),
  // Older installed runtimes did not emit state. Keep their exact
  // PID/run/config acknowledgement readable during a controlled upgrade;
  // explicit terminal states from current runtimes are still rejected.
  state: RuntimeStateSchema.optional(),
  startup: z
    .looseObject({
      lanesReady: z.boolean(),
      phase: StartupPhaseSchema,
    })
    .optional(),
});
const EmergencyStopConfigSchema = z.looseObject({
  restart: z
    .looseObject({
      shutdownGraceMs: z
        .int()
        .min(1_000)
        .max(15 * 60_000),
    })
    .optional(),
  stateDirectory: z.string().min(1).max(4_096),
});
export interface SourceIndependentStopPlan {
  readonly configDigest: string;
  readonly root: string;
  readonly shutdownGraceMs: number;
}
type RuntimeStatusIssue = Exclude<
  ReturnType<typeof evaluateRuntimeStatusIdentity>,
  null
>;

export type LaunchdControlAction = z.infer<typeof ActionSchema>;

export interface LaunchdControlSnapshot {
  readonly action: LaunchdControlAction;
  readonly actualState:
    "fenced" | "running_outdated" | "running" | "starting" | "stopped";
  readonly allowedActions: readonly LaunchdControlAction[];
  readonly configDigest: string;
  readonly desiredConfigDigest: string;
  readonly desiredRuntimeCommit: null | string;
  readonly detail: null | string;
  readonly loadedConfigDigest: null | string;
  readonly observedAt: string;
  readonly ownerPid: null | number;
  readonly runId: null | string;
  readonly runtimeCommit: null | string;
  readonly schemaId: "saqi.launchd-control";
  readonly schemaVersion: 1;
  readonly serviceEnabled: boolean;
  readonly sourceIdentityStatus?: "configured" | "retained_unverified";
}

interface LaunchdControlOptions {
  readonly action: unknown;
  readonly configPath: string;
  readonly label?: string;
  readonly statusInspection?: {
    readonly loaded: LoadedOperationConfig;
    readonly serviceEnabled: boolean;
    readonly sourceIdentityStatus: "configured" | "retained_unverified";
  };
}

export async function controlLaunchdService(
  raw: LaunchdControlOptions,
): Promise<LaunchdControlSnapshot> {
  return controlLaunchdServiceInternal(raw, false);
}

async function controlLaunchdServiceInternal(
  raw: LaunchdControlOptions,
  lockHeld: boolean,
): Promise<LaunchdControlSnapshot> {
  const action = ActionSchema.parse(raw.action);
  if (raw.statusInspection !== undefined && action !== "status")
    throw new Error("STATUS_INSPECTION_CANNOT_CONTROL_SERVICE");
  const label = LaunchdServiceLabelSchema.parse(raw.label);
  if (action === "stop") return stopWithoutSourceIdentity(raw, label, lockHeld);
  const loaded =
    raw.statusInspection?.loaded ??
    (await loadScraperOperationConfig(raw.configPath));
  const root = loaded.config.stateDirectory;
  const target = `gui/${String(requireUid())}/${label}`;
  const minimumExitTimeoutSeconds = minimumLaunchdExitTimeoutSeconds(
    loaded.config.restart.shutdownGraceMs,
  );
  if (action === "status") {
    const result = await snapshot(
      action,
      target,
      root,
      loaded.configDigest,
      minimumExitTimeoutSeconds,
      raw.statusInspection?.serviceEnabled,
    );
    return raw.statusInspection?.sourceIdentityStatus === "retained_unverified"
      ? retainedSourceStatus(result)
      : { ...result, sourceIdentityStatus: "configured" };
  }

  const release = lockHeld
    ? () => Promise.resolve()
    : await acquireControlLock(root, action);
  try {
    if (action === "start")
      return await start(
        target,
        root,
        loaded.configDigest,
        minimumExitTimeoutSeconds,
        loaded.config.restart.startupTimeoutMs,
        action,
      );
    return await restart(
      target,
      root,
      loaded.configDigest,
      minimumExitTimeoutSeconds,
      loaded.config.restart.shutdownGraceMs + 60_000,
      loaded.config.restart.startupTimeoutMs,
      action,
    );
  } finally {
    await release();
  }
}

async function stopWithoutSourceIdentity(
  raw: LaunchdControlOptions,
  label: string,
  lockHeld: boolean,
): Promise<LaunchdControlSnapshot> {
  const configPath = resolve(raw.configPath);
  const configInput: unknown = JSON.parse(await readFile(configPath, "utf8"));
  const configuredRoot = emergencyStopRoot(configPath, configInput);
  const plan = sourceIndependentStopPlan(configPath, configInput, undefined);
  const minimumExitTimeoutSeconds = minimumLaunchdExitTimeoutSeconds(
    plan.shutdownGraceMs,
  );
  const target = `gui/${String(requireUid())}/${label}`;
  const release = lockHeld
    ? () => Promise.resolve()
    : await acquireControlLock(plan.root, "stop");
  try {
    await setLaunchdServiceEnabled(configuredRoot, false);
    const owner = await inspectRunOwner(resolve(configuredRoot, "RUN.lock"));
    const verifiedPlan = sourceIndependentStopPlan(
      configPath,
      configInput,
      owner.lock?.configDigest,
    );
    return await stop(
      target,
      plan.root,
      verifiedPlan.configDigest,
      minimumExitTimeoutSeconds,
      plan.shutdownGraceMs + 60_000,
      "stop",
    );
  } finally {
    await release();
  }
}

function emergencyStopRoot(configPath: string, input: unknown): string {
  const parsed = EmergencyStopConfigSchema.parse(input);
  return resolve(dirname(configPath), parsed.stateDirectory);
}

/** Plans an emergency stop from public config shape and acknowledged ownership only. */
export function sourceIndependentStopPlan(
  configPathInput: string,
  input: unknown,
  acknowledgedConfigDigest: string | undefined,
): SourceIndependentStopPlan {
  const configPath = resolve(configPathInput);
  const parsed = EmergencyStopConfigSchema.parse(input);
  return {
    configDigest: AcknowledgedConfigDigestSchema.parse(
      acknowledgedConfigDigest ?? "0".repeat(64),
    ),
    root: resolve(dirname(configPath), parsed.stateDirectory),
    shutdownGraceMs: parsed.restart?.shutdownGraceMs ?? 120_000,
  };
}

/** Serializes a config mutation with its service acknowledgement. */
export async function withLaunchdControlLock<T>(
  configPath: string,
  action: LaunchdControlAction,
  operation: (control: typeof controlLaunchdService) => Promise<T>,
): Promise<T> {
  const { config } = await loadScraperOperationConfig(configPath);
  const release = await acquireControlLock(config.stateDirectory, action);
  try {
    return await operation((raw) => controlLaunchdServiceInternal(raw, true));
  } finally {
    await release();
  }
}

async function start(
  target: string,
  root: string,
  configDigest: string,
  minimumExitTimeoutSeconds: number,
  startupTimeoutMs: number,
  action: LaunchdControlAction,
  previousRunId: null | string = null,
): Promise<LaunchdControlSnapshot> {
  const before = await snapshot(
    action,
    target,
    root,
    configDigest,
    minimumExitTimeoutSeconds,
  );
  if (before.actualState === "fenced")
    throw new Error(`LAUNCHD_FENCED: ${before.detail ?? "unknown"}`);
  if (before.actualState === "running_outdated" && previousRunId === null)
    throw new Error("LAUNCHD_ACTION_NOT_ALLOWED: restart required");
  if (
    before.actualState === "running" &&
    (previousRunId === null || before.runId !== previousRunId)
  ) {
    await setLaunchdServiceEnabled(root, true);
    return snapshot(
      action,
      target,
      root,
      configDigest,
      minimumExitTimeoutSeconds,
    );
  }
  await setLaunchdServiceEnabled(root, true);
  // A matching launchd process and RUN.lock already own startup. Preserve that
  // ownership and wait for readiness instead of creating a competing process.
  if (before.actualState !== "starting") await launchctl(["kickstart", target]);
  return poll(
    startupTimeoutMs,
    async () => {
      const current = await snapshot(
        action,
        target,
        root,
        configDigest,
        minimumExitTimeoutSeconds,
      );
      return current.actualState === "running" &&
        current.runId !== null &&
        current.runId !== previousRunId
        ? current
        : null;
    },
    "LAUNCHD_START_NOT_ACKNOWLEDGED",
  );
}

/** Keeps desired-running durable even when replacement fails or is interrupted. */
export async function withLaunchdServiceEnabled<T>(
  stateDirectory: string,
  replace: () => Promise<T>,
): Promise<T> {
  await setLaunchdServiceEnabled(stateDirectory, true);
  return replace();
}

export function replacementAcknowledged(
  current: LaunchdControlSnapshot,
  previousRunId: null | string,
): boolean {
  return (
    current.actualState === "running" &&
    current.serviceEnabled &&
    current.runId !== null &&
    current.runId !== previousRunId &&
    current.loadedConfigDigest === current.desiredConfigDigest &&
    (current.desiredRuntimeCommit === null ||
      current.runtimeCommit === current.desiredRuntimeCommit)
  );
}

async function restart(
  target: string,
  root: string,
  configDigest: string,
  minimumExitTimeoutSeconds: number,
  shutdownTimeoutMs: number,
  startupTimeoutMs: number,
  action: LaunchdControlAction,
): Promise<LaunchdControlSnapshot> {
  const before = await snapshot(
    action,
    target,
    root,
    configDigest,
    minimumExitTimeoutSeconds,
  );
  if (before.actualState === "fenced")
    throw new Error(`LAUNCHD_FENCED: ${before.detail ?? "unknown"}`);
  if (before.actualState === "stopped")
    return start(
      target,
      root,
      configDigest,
      minimumExitTimeoutSeconds,
      startupTimeoutMs,
      action,
    );

  return withLaunchdServiceEnabled(root, async () => {
    const ownerPid = before.ownerPid;
    const previousRunId = before.runId;
    if (ownerPid === null || previousRunId === null)
      throw new Error("LAUNCHD_PROCESS_IDENTITY_MISSING");
    await signalVerifiedRunOwner(root, ownerPid, previousRunId);
    let escalationError: Error | null = null;
    const hardStopTimer = setTimeout(
      () =>
        void signalVerifiedRunOwner(root, ownerPid, previousRunId).catch(
          (error: unknown) => {
            escalationError =
              error instanceof Error ? error : new Error(String(error));
          },
        ),
      Math.max(POLL_MS, shutdownTimeoutMs - 60_000),
    );
    hardStopTimer.unref();
    const forceStopTimer = setTimeout(
      () =>
        void signalVerifiedRunOwner(
          root,
          ownerPid,
          previousRunId,
          "SIGKILL",
        ).catch((error: unknown) => {
          escalationError =
            error instanceof Error ? error : new Error(String(error));
        }),
      Math.max(POLL_MS, shutdownTimeoutMs - 45_000),
    );
    forceStopTimer.unref();
    let kickstarted = false;
    try {
      return await poll(
        shutdownTimeoutMs + startupTimeoutMs,
        async () => {
          if (escalationError !== null)
            throw new Error("LAUNCHD_RESTART_ESCALATION_FAILED", {
              cause: escalationError,
            });
          let current = await snapshot(
            action,
            target,
            root,
            configDigest,
            minimumExitTimeoutSeconds,
          );
          if (
            current.actualState === "fenced" &&
            current.detail === "RUN_LOCK_RETAINED" &&
            (await removeDeadOwnerRunLock(root, ownerPid, previousRunId))
          ) {
            current = await snapshot(
              action,
              target,
              root,
              configDigest,
              minimumExitTimeoutSeconds,
            );
          }
          if (replacementAcknowledged(current, previousRunId)) return current;
          if (current.actualState === "stopped" && !kickstarted) {
            await launchctl(["kickstart", target]);
            kickstarted = true;
          }
          return null;
        },
        "LAUNCHD_RESTART_NOT_ACKNOWLEDGED",
      );
    } finally {
      clearTimeout(hardStopTimer);
      clearTimeout(forceStopTimer);
    }
  });
}

async function stop(
  target: string,
  root: string,
  configDigest: string,
  minimumExitTimeoutSeconds: number,
  timeoutMs: number,
  action: LaunchdControlAction,
): Promise<LaunchdControlSnapshot> {
  await setLaunchdServiceEnabled(root, false);
  const before = await snapshot(
    action,
    target,
    root,
    configDigest,
    minimumExitTimeoutSeconds,
  );
  // Startup may not have acknowledged RUN.lock yet. Persist disabled intent
  // before refusing to signal an unverified PID, so recovery cannot relaunch.
  if (before.actualState === "stopped") {
    return snapshot(
      action,
      target,
      root,
      configDigest,
      minimumExitTimeoutSeconds,
    );
  }
  if (before.actualState === "fenced")
    throw new Error(
      `LAUNCHD_STOP_DISABLED_BUT_UNACKNOWLEDGED: ${before.detail ?? "unknown"}`,
    );
  const ownerPid = before.ownerPid;
  const runId = before.runId;
  if (ownerPid === null || runId === null)
    throw new Error(
      "LAUNCHD_STOP_DISABLED_BUT_UNACKNOWLEDGED: OWNER_IDENTITY_MISSING",
    );
  await signalVerifiedRunOwner(root, ownerPid, runId);
  let escalationError: unknown;
  // The run command interprets its first termination signal as a graceful
  // drain request and its second as the bounded hard stop. Mirror that
  // contract here instead of waiting past the grace window with no escalation.
  const hardStopTimer = setTimeout(
    () => {
      void signalVerifiedRunOwner(root, ownerPid, runId).catch(
        (error: unknown) => {
          escalationError = error;
        },
      );
    },
    Math.max(POLL_MS, timeoutMs - 60_000),
  );
  hardStopTimer.unref();
  // A synchronous native/SQLite section can prevent Node from servicing both
  // termination handlers. After an additional bounded cleanup window, force
  // process exit; SQLite transactions and durable work leases recover safely.
  const forceStopTimer = setTimeout(
    () => {
      void signalVerifiedRunOwner(root, ownerPid, runId, "SIGKILL").catch(
        (error: unknown) => {
          escalationError = error;
        },
      );
    },
    Math.max(POLL_MS, timeoutMs - 45_000),
  );
  forceStopTimer.unref();
  try {
    return await poll(
      timeoutMs,
      async () => {
        if (escalationError !== undefined)
          throw new Error("LAUNCHD_STOP_ESCALATION_FAILED", {
            cause: escalationError,
          });
        let current = await snapshot(
          action,
          target,
          root,
          configDigest,
          minimumExitTimeoutSeconds,
        );
        if (
          current.actualState === "fenced" &&
          current.detail === "RUN_LOCK_RETAINED" &&
          before.runId !== null &&
          (await removeDeadOwnerRunLock(root, ownerPid, before.runId))
        ) {
          current = await snapshot(
            action,
            target,
            root,
            configDigest,
            minimumExitTimeoutSeconds,
          );
        }
        if (current.actualState === "stopped") return current;
        return null;
      },
      "LAUNCHD_STOP_NOT_ACKNOWLEDGED",
    );
  } finally {
    clearTimeout(hardStopTimer);
    clearTimeout(forceStopTimer);
  }
}

async function removeDeadOwnerRunLock(
  root: string,
  ownerPid: null | number,
  runId: string,
): Promise<boolean> {
  if (ownerPid === null || processAlive(ownerPid)) return false;
  const path = resolve(root, "RUN.lock");
  return releaseDeadRunOwner(path, ownerPid, runId);
}

export function runOwnershipMatches(
  first: { readonly pid: number; readonly runId: string } | null,
  second: { readonly pid: number; readonly runId: string } | null,
  ownerPid: number,
  runId: string,
): boolean {
  return (
    first?.pid === ownerPid &&
    first.runId === runId &&
    second?.pid === ownerPid &&
    second.runId === runId
  );
}

async function signalVerifiedRunOwner(
  root: string,
  ownerPid: number,
  runId: string,
  signal: NodeJS.Signals = "SIGTERM",
): Promise<void> {
  const path = resolve(root, "RUN.lock");
  await actOnRunOwner(path, ownerPid, runId, () => {
    signalVerifiedProcess(ownerPid, signal);
    return undefined;
  });
}

async function snapshot(
  action: LaunchdControlAction,
  target: string,
  root: string,
  configDigest: string,
  minimumExitTimeoutSeconds: number,
  inspectedServiceEnabled?: boolean,
): Promise<LaunchdControlSnapshot> {
  const service = await launchdStatus(target);
  const observation = await inspectRunOwner(resolve(root, "RUN.lock"));
  const lock = observation.lock;
  const runtimeStatus = await readRuntimeStatusIdentity(root);
  const desiredRuntimeCommit = await readDesiredRuntimeCommit(runtimeStatus);
  const contractIssue = loadedServiceContractIssue(
    service.loaded,
    service.exitTimeoutSeconds,
    minimumExitTimeoutSeconds,
  );
  const identityIssue = evaluateLaunchdIdentity(
    service.running,
    service.pid,
    lock?.pid ?? null,
  );
  const baseEvaluation: Pick<LaunchdControlSnapshot, "actualState" | "detail"> =
    observation.issue !== null
      ? { actualState: "fenced", detail: observation.issue }
      : contractIssue !== null
        ? { actualState: "fenced", detail: contractIssue }
        : identityIssue === null
          ? evaluateLaunchdState(service.running, lock, configDigest)
          : { actualState: "fenced", detail: identityIssue };
  const statusIssue = evaluateRuntimeStatusIdentity(
    service.running,
    service.pid,
    lock,
    runtimeStatus,
    configDigest,
    desiredRuntimeCommit,
    Date.now(),
  );
  const evaluation = evaluateRuntimeAcknowledgement(
    baseEvaluation,
    statusIssue,
  );
  return {
    action,
    actualState: evaluation.actualState,
    allowedActions: allowedActions(evaluation.actualState),
    configDigest,
    desiredConfigDigest: configDigest,
    desiredRuntimeCommit,
    detail: evaluation.detail,
    observedAt: new Date().toISOString(),
    loadedConfigDigest:
      lock?.configDigest ??
      runtimeStatus?.loadedConfigDigest ??
      runtimeStatus?.configDigest ??
      null,
    ownerPid: lock?.pid ?? null,
    runId: lock?.runId ?? null,
    runtimeCommit: runtimeStatus?.runtimeRelease?.commit ?? null,
    serviceEnabled: inspectedServiceEnabled ?? readServiceEnabled(root),
    schemaId: "saqi.launchd-control",
    schemaVersion: 1,
  };
}

export function retainedSourceStatus(
  observed: LaunchdControlSnapshot,
): LaunchdControlSnapshot {
  return {
    ...observed,
    actualState:
      observed.actualState === "running"
        ? "running_outdated"
        : observed.actualState,
    allowedActions: observed.allowedActions.filter(
      (action) => action === "status" || action === "stop",
    ),
    detail: observed.detail ?? "SOURCE_IDENTITY_UNVERIFIED",
    sourceIdentityStatus: "retained_unverified",
  };
}

export function evaluateRuntimeAcknowledgement(
  base: Pick<LaunchdControlSnapshot, "actualState" | "detail">,
  statusIssue: null | RuntimeStatusIssue,
): Pick<LaunchdControlSnapshot, "actualState" | "detail"> {
  if (base.actualState !== "running" || statusIssue === null) return base;
  return statusIssue === "SERVICE_STATUS_MISSING" ||
    statusIssue === "SERVICE_STATUS_NOT_READY"
    ? { actualState: "starting", detail: statusIssue }
    : { actualState: "running_outdated", detail: statusIssue };
}

async function readRuntimeStatusIdentity(
  stateDirectory: string,
): Promise<null | z.infer<typeof RuntimeStatusIdentitySchema>> {
  const path = resolve(stateDirectory, "status.json");
  try {
    const value = await readFile(path);
    if (value.byteLength > MAXIMUM_STATUS_BYTES)
      throw new Error("SERVICE_STATUS_TOO_LARGE");
    return RuntimeStatusIdentitySchema.parse(
      JSON.parse(value.toString("utf8")),
    );
  } catch (error) {
    if (errorCode(error) === "ENOENT") return null;
    throw new Error("SERVICE_STATUS_INVALID", { cause: error });
  }
}

export function setLaunchdServiceEnabled(
  stateDirectory: string,
  enabled: boolean,
): Promise<void> {
  return setLaunchdServiceEnabledInternal(stateDirectory, enabled);
}

async function setLaunchdServiceEnabledInternal(
  stateDirectory: string,
  enabled: boolean,
): Promise<void> {
  writeServiceEnabled(stateDirectory, enabled);
  try {
    await writeLegacyServiceMirror(stateDirectory, enabled);
  } catch {
    process.emitWarning(
      "SQLite service intent saved; legacy rollback mirror unavailable",
      {
        code: "SAQI_SERVICE_MIRROR_UNAVAILABLE",
      },
    );
  }
}

async function writeLegacyServiceMirror(
  stateDirectory: string,
  enabled: boolean,
): Promise<void> {
  const path = serviceEnabledPath(stateDirectory);
  await mkdir(dirname(path), { mode: 0o700, recursive: true });
  if (!enabled) {
    try {
      await unlink(path);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    await fsyncDirectory(dirname(path));
    return;
  }
  const temporary = `${path}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(
      `${JSON.stringify({ enabledAt: new Date().toISOString(), schemaVersion: 1 })}\n`,
    );
    await handle.sync();
  } catch (error) {
    try {
      await unlink(temporary);
    } catch {
      // Preserve the original write error.
    }
    throw error;
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
  await fsyncDirectory(dirname(path));
}

async function fsyncDirectory(path: string): Promise<void> {
  const handle = await open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export function loadedServiceContractIssue(
  loaded: boolean,
  exitTimeoutSeconds: null | number,
  minimumExitTimeoutSeconds: number,
): "SERVICE_EXIT_TIMEOUT_TOO_SHORT" | null {
  return loaded &&
    (exitTimeoutSeconds === null ||
      exitTimeoutSeconds < minimumExitTimeoutSeconds)
    ? "SERVICE_EXIT_TIMEOUT_TOO_SHORT"
    : null;
}

export function evaluateLaunchdIdentity(
  running: boolean,
  launchdPid: null | number,
  lockPid: null | number,
): "SERVICE_PID_MISMATCH" | null {
  if (!running) return null;
  return launchdPid !== null && launchdPid === lockPid
    ? null
    : "SERVICE_PID_MISMATCH";
}

export function evaluateLaunchdState(
  running: boolean,
  lock: { readonly configDigest: string } | null,
  configDigest: string,
): Pick<LaunchdControlSnapshot, "actualState" | "detail"> {
  if (running && lock?.configDigest === configDigest)
    return { actualState: "running", detail: null };
  if (running && lock === null)
    return { actualState: "fenced", detail: "RUN_LOCK_MISSING" };
  if (running && lock?.configDigest !== configDigest)
    return {
      actualState: "running_outdated",
      detail: "CONFIG_DIGEST_MISMATCH",
    };
  if (lock && lock.configDigest !== configDigest)
    return { actualState: "fenced", detail: "CONFIG_DIGEST_MISMATCH" };
  if (lock) return { actualState: "fenced", detail: "RUN_LOCK_RETAINED" };
  return { actualState: "stopped", detail: null };
}

export function evaluateRuntimeStatusIdentity(
  running: boolean,
  launchdPid: null | number,
  lock: {
    readonly configDigest: string;
    readonly pid: number;
    readonly runId: string;
  } | null,
  status: {
    readonly configDigest: string;
    readonly loadedConfigDigest?: string | undefined;
    readonly observedAt: string;
    readonly ownerPid: number;
    readonly runId: string;
    readonly runtimeRelease?:
      | { readonly commit: string; readonly manifestPath: string }
      | null
      | undefined;
    readonly state?:
      "fenced" | "paused" | "running" | "starting" | "stopped" | undefined;
    readonly startup?:
      | {
          readonly lanesReady: boolean;
          readonly phase: "creating_lanes" | "failed" | "preflight" | "ready";
        }
      | undefined;
  } | null,
  desiredConfigDigest: string,
  desiredRuntimeCommit: null | string,
  now: number,
):
  | "SERVICE_RUNTIME_COMMIT_MISMATCH"
  | "SERVICE_STATUS_IDENTITY_MISMATCH"
  | "SERVICE_STATUS_MISSING"
  | "SERVICE_STATUS_NOT_READY"
  | "SERVICE_STATUS_STALE"
  | null {
  if (!running) return null;
  if (!lock || !status) return "SERVICE_STATUS_MISSING";
  if (status.state === "fenced" || status.state === "stopped")
    return "SERVICE_STATUS_NOT_READY";
  if (
    status.state === "starting" ||
    ((status.state === "running" || status.state === "paused") &&
      (status.startup?.phase !== "ready" || !status.startup.lanesReady))
  )
    return "SERVICE_STATUS_NOT_READY";
  if (
    !Number.isFinite(Date.parse(status.observedAt)) ||
    Math.abs(now - Date.parse(status.observedAt)) > MAXIMUM_STATUS_AGE_MS
  )
    return "SERVICE_STATUS_STALE";
  if (
    desiredRuntimeCommit !== null &&
    status.runtimeRelease?.commit !== desiredRuntimeCommit
  )
    return "SERVICE_RUNTIME_COMMIT_MISMATCH";
  return launchdPid === lock.pid &&
    status.ownerPid === lock.pid &&
    status.runId === lock.runId &&
    status.configDigest === desiredConfigDigest &&
    (status.loadedConfigDigest ?? status.configDigest) === desiredConfigDigest
    ? null
    : "SERVICE_STATUS_IDENTITY_MISMATCH";
}

async function readDesiredRuntimeCommit(
  status: null | z.infer<typeof RuntimeStatusIdentitySchema>,
): Promise<null | string> {
  const manifestPath = status?.runtimeRelease?.manifestPath;
  if (!manifestPath || basename(manifestPath) !== "runtime-release.json")
    return null;
  const releaseDirectory = dirname(manifestPath);
  const releasesDirectory = dirname(releaseDirectory);
  if (basename(releasesDirectory) !== "releases") return null;
  try {
    return DesiredRuntimeManifestSchema.parse(
      JSON.parse(
        await readFile(
          join(dirname(releasesDirectory), "current", "runtime-release.json"),
          "utf8",
        ),
      ),
    ).commit;
  } catch {
    return null;
  }
}

interface LaunchdStatus {
  readonly exitTimeoutSeconds: null | number;
  readonly loaded: boolean;
  readonly pid: null | number;
  readonly running: boolean;
}

async function launchdStatus(target: string): Promise<LaunchdStatus> {
  try {
    const result = await executeFile("/bin/launchctl", ["print", target], {
      encoding: "utf8",
      maxBuffer: 1_048_576,
      timeout: 10_000,
    });
    const exitTimeout = /^\s*exit timeout = (\d+)\s*$/m.exec(
      result.stdout,
    )?.[1];
    const pid = /^\s*pid = (\d+)\s*$/m.exec(result.stdout)?.[1];
    return {
      exitTimeoutSeconds: exitTimeout ? Number(exitTimeout) : null,
      loaded: true,
      pid: pid ? Number(pid) : null,
      running: /^\s*state = running\s*$/m.test(result.stdout),
    };
  } catch (error) {
    if (exitCode(error) !== null)
      return {
        exitTimeoutSeconds: null,
        loaded: false,
        pid: null,
        running: false,
      };
    throw error;
  }
}

function signalVerifiedProcess(
  pid: null | number,
  signal: NodeJS.Signals = "SIGTERM",
): void {
  if (pid === null) throw new Error("LAUNCHD_PROCESS_ID_MISSING");
  try {
    process.kill(pid, signal);
  } catch (error) {
    if (errorCode(error) !== "ESRCH") throw error;
  }
}

async function launchctl(argumentsList: readonly string[]): Promise<void> {
  try {
    await executeFile("/bin/launchctl", [...argumentsList], {
      encoding: "utf8",
      maxBuffer: 1_048_576,
      timeout: 15_000,
    });
  } catch (error) {
    throw new Error(`LAUNCHD_CONTROL_FAILED: ${boundedError(error)}`, {
      cause: error,
    });
  }
}

async function poll<T>(
  timeoutMs: number,
  read: () => Promise<null | T>,
  timeoutCode: string,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    // eslint-disable-next-line no-await-in-loop -- Ordered acknowledgement polling is intentional.
    const value = await read();
    if (value !== null) return value;
    // eslint-disable-next-line no-await-in-loop -- Polling must remain serial and bounded.
    await sleep(POLL_MS);
  }
  throw new Error(timeoutCode);
}

async function acquireControlLock(
  root: string,
  action: LaunchdControlAction,
): Promise<() => Promise<void>> {
  await mkdir(root, { mode: 0o700, recursive: true });
  const path = resolve(root, "CONTROL.lock");
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop -- Lock acquisition attempts must remain serialized.
      const handle = await open(path, "wx", 0o600);
      try {
        // eslint-disable-next-line no-await-in-loop -- The exclusive lock handle must be initialized before it is exposed.
        await handle.writeFile(
          `${JSON.stringify({ action, pid: process.pid, startedAt: new Date().toISOString() })}\n`,
        );
      } catch (error) {
        // eslint-disable-next-line no-await-in-loop -- Failed lock initialization must close before cleanup.
        await handle.close();
        // eslint-disable-next-line no-await-in-loop -- Failed lock initialization cleanup is part of this attempt.
        await rm(path, { force: true });
        throw error;
      }
      return async () => {
        await handle.close();
        await rm(path, { force: true });
      };
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
      // eslint-disable-next-line no-await-in-loop -- Contended lock ownership must be read before recovery.
      const owner = await readControlOwner(path);
      if (owner !== null && processAlive(owner))
        throw new Error(`LAUNCHD_CONTROL_BUSY: pid ${String(owner)}`);
      if (owner === null) {
        try {
          // eslint-disable-next-line no-await-in-loop -- A contended lock must be aged before stale recovery.
          const lockStatus = await stat(path);
          if (
            Date.now() - lockStatus.mtimeMs <
            CONTROL_LOCK_INITIALIZATION_GRACE_MS
          )
            throw new Error("LAUNCHD_CONTROL_BUSY: owner initializing");
        } catch (statusError) {
          if (errorCode(statusError) === "ENOENT") continue;
          throw statusError;
        }
      }
      try {
        // eslint-disable-next-line no-await-in-loop -- Stale-lock quarantine must complete before retrying acquisition.
        await rename(path, `${path}.stale.${String(Date.now())}`);
      } catch (renameError) {
        if (errorCode(renameError) === "ENOENT") continue;
        throw renameError;
      }
    }
  }
  throw new Error("LAUNCHD_CONTROL_LOCK_FAILED");
}

async function readControlOwner(path: string): Promise<null | number> {
  try {
    const value: unknown = JSON.parse(await readFile(path, "utf8"));
    if (typeof value !== "object" || value === null || !("pid" in value))
      return null;
    return typeof value.pid === "number" && Number.isSafeInteger(value.pid)
      ? value.pid
      : null;
  } catch {
    return null;
  }
}

function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return errorCode(error) === "EPERM";
  }
}

function requireUid(): number {
  if (typeof process.getuid !== "function")
    throw new Error("LAUNCHD_REQUIRES_POSIX_UID");
  return process.getuid();
}

function allowedActions(
  actualState: LaunchdControlSnapshot["actualState"],
): readonly LaunchdControlAction[] {
  switch (actualState) {
    case "running":
    case "running_outdated":
    case "starting":
      return ["restart", "status", "stop"];
    case "stopped":
      return ["start", "status"];
    case "fenced":
      return ["status"];
  }
}

function exitCode(error: unknown): null | number {
  if (typeof error !== "object" || error === null || !("code" in error))
    return null;
  return typeof error.code === "number" ? error.code : null;
}

function errorCode(error: unknown): null | string {
  if (typeof error !== "object" || error === null || !("code" in error))
    return null;
  return typeof error.code === "string" ? error.code : null;
}

function boundedError(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error);
  return value.replaceAll(/[\r\n]+/g, " ").slice(0, 1_000);
}
