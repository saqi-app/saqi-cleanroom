import { randomUUID } from "node:crypto";
import {
  access,
  mkdir,
  open,
  readFile,
  rename,
  unlink,
} from "node:fs/promises";
import { dirname, join } from "node:path";

import { z } from "zod";

import { EnrichmentProviderSchema } from "../ports/provider-contract.js";

const MAXIMUM_DIAGNOSTIC_BYTES = 1024 * 1024;
const DEFAULT_HEARTBEAT_MS = 5 * 60_000;
const MAXIMUM_HEALTH_AGE_MS = 15 * 60_000;
const ErrorCodeSchema = z.string().regex(/^[A-Z][A-Z0-9_]{1,127}$/);
const ConfigDigestSchema = z.string().regex(/^[a-f\d]{64}$/);
const HeartbeatIntervalSchema = z
  .number()
  .int()
  .min(1_000)
  .max(24 * 60 * 60_000);
const ModelNameSchema = z.string().trim().min(1).max(128);
const HealthCheckStateSchema = z.enum(["ok", "warning", "blocked"]);
const LaneStateSchema = z.enum([
  "idle",
  "running",
  "waiting",
  "blocked",
  "fenced",
]);
const SolDispositionSchema = z.enum([
  "budget_exhausted",
  "idle",
  "healthy",
  "network_wait",
  "rate_limited",
  "quota_wait",
  "transient_error",
  "neutral",
]);
const ProviderBlockReasonSchema = z.enum([
  "at_capacity",
  "circuit_open",
  "disk_pressure",
  "error_dampener",
  "launch_pacing",
  "paused",
  "provider_wait",
  "quota_wait",
  "rate_limited",
]);
const RecoveryCauseSchema = z.enum([
  "account_switch",
  "authentication",
  "network",
  "provider",
  "quota",
  "rate_limit",
]);
const LocalGrowthStateSchema = z.enum([
  "active",
  "retrying",
  "stalled",
  "disabled",
]);
const ProductionGrowthStateSchema = z.enum([
  "active",
  "ready",
  "gated",
  "stalled",
  "disabled",
]);
const OriginStateSchema = z.enum([
  "challenge_wait",
  "blocked",
  "disabled",
  "disk_wait",
  "healthy",
  "network_wait",
  "rate_wait",
]);
const PipelineStateSchema = z.enum(["healthy", "degraded", "blocked"]);
const QuotaSignalStateSchema = z.enum(["open", "cleared"]);

export const PipelineHealthCheckSchema = z.strictObject({
  code: ErrorCodeSchema,
  detail: z.string().trim().min(1).max(2_000).nullable(),
  retryAt: z.int().nonnegative().nullable(),
  state: HealthCheckStateSchema,
});

export const PipelineLaneHealthSchema = z.strictObject({
  active: z.int().nonnegative(),
  completed: z.int().nonnegative(),
  failed: z.int().nonnegative(),
  lastDurationMs: z.int().nonnegative().nullable(),
  lastErrorCode: ErrorCodeSchema.nullable(),
  lastProgressAt: z.int().nonnegative().nullable(),
  name: z.string().trim().min(1).max(128),
  state: LaneStateSchema,
});

export const PipelineQueueHealthSchema = z.strictObject({
  active: z.int().nonnegative(),
  deadLetter: z.int().nonnegative(),
  kind: z.string().trim().min(1).max(128),
  lastSuccessAt: z.int().nonnegative().nullable().default(null),
  oldestReadyAt: z.int().nonnegative().nullable(),
  pending: z.int().nonnegative(),
  quotaWait: z.int().nonnegative(),
  retryWait: z.int().nonnegative(),
  succeeded: z.int().nonnegative(),
  total: z.int().nonnegative(),
});

export const SolPipelineHealthSchema = z.strictObject({
  accepted: z.int().nonnegative(),
  activeInvocations: z.int().nonnegative(),
  invocationConcurrency: z.int().min(1).max(256),
  lastDisposition: SolDispositionSchema,
  model: z.string().trim().min(1).max(128),
  retryAt: z.int().nonnegative().nullable(),
  selectedConcurrency: z.int().min(1).max(256),
  semanticFailures: z.int().nonnegative(),
});

export const ZPROVIDER_PIPELINE_HEALTH = SolPipelineHealthSchema.extend({
  blockReason: ProviderBlockReasonSchema.nullable().default(null),
  modelKey: z.string().trim().min(1).max(128),
  nextQuotaProbeAt: z.int().nonnegative().nullable().default(null),
  provider: EnrichmentProviderSchema,
  providerErrorCode: ErrorCodeSchema.nullable().default(null),
  quarantinedOperations: z.int().nonnegative().default(0),
  recoverableUnknownOperations: z.int().nonnegative().default(0),
  recoveryCause: RecoveryCauseSchema.nullable().default(null),
  recoveryLeaseUntil: z.int().nonnegative().nullable().default(null),
  unknownOperations: z.int().nonnegative().default(0),
}).strict();

const PipelineStorageHealthSchema = z.strictObject({
  availableBytes: z.int().nonnegative(),
  headroomBytes: z.int(),
  minimumFreeBytes: z.int().nonnegative(),
  writable: z.boolean(),
});

const ProviderHostAdmissionHealthSchema = z
  .strictObject({
    activeProcesses: z.int().nonnegative(),
    maximumProcesses: z.int().positive(),
    remainingProcesses: z.int().nonnegative(),
  })
  .superRefine((value, context) => {
    if (
      value.activeProcesses > value.maximumProcesses ||
      value.remainingProcesses !==
        value.maximumProcesses - value.activeProcesses
    ) {
      context.addIssue({
        code: "custom",
        message: "Provider host admission counts are inconsistent",
      });
    }
  });

const LocalFanoutHealthSchema = z.strictObject({
  cursor: z.int().nonnegative(),
  pendingResolution: z.int().nonnegative(),
  provider: EnrichmentProviderSchema,
  seeded: z.int().nonnegative(),
});

const RetentionHealthSchema = z.strictObject({
  applied: z.boolean(),
  candidates: z.int().nonnegative(),
  provider: EnrichmentProviderSchema,
  projectedArchiveBytes: z.int().nonnegative(),
});

const GrowthHealthSchema = z.strictObject({
  local: z.strictObject({
    enabledLanes: z.int().nonnegative(),
    lastSuccessAt: z.int().nonnegative().nullable(),
    state: LocalGrowthStateSchema,
    viableLanes: z.int().nonnegative(),
  }),
  production: z.strictObject({
    configured: z.boolean(),
    lastSuccessAt: z.int().nonnegative().nullable(),
    state: ProductionGrowthStateSchema,
  }),
});

const PipelineOriginHealthSchema = z.strictObject({
  active: z.boolean(),
  consecutiveFailures: z.int().nonnegative(),
  cooldownUntil: z.int().nonnegative(),
  lastCompletedAt: z.int().nonnegative().nullable(),
  nextAllowedAt: z.int().nonnegative(),
  origin: z.url().max(2_000),
  state: OriginStateSchema.default("healthy"),
  stopReason: ErrorCodeSchema.nullable(),
});

const PipelineHealthBodySchema = z.strictObject({
  checks: z.array(PipelineHealthCheckSchema).max(100),
  configDigest: z.string().regex(/^[a-f\d]{64}$/),
  desiredConfigDigest: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable()
    .default(null),
  growth: GrowthHealthSchema.nullable().default(null),
  heartbeatIntervalMs: z
    .int()
    .min(1_000)
    .max(24 * 60 * 60_000)
    .default(DEFAULT_HEARTBEAT_MS),
  lanes: z.array(PipelineLaneHealthSchema).max(160),
  lastProgressAt: z.int().nonnegative().nullable(),
  localFanout: z.array(LocalFanoutHealthSchema).max(3).default([]),
  origins: z.array(PipelineOriginHealthSchema).max(100).default([]),
  providerHostAdmission:
    ProviderHostAdmissionHealthSchema.nullable().default(null),
  queues: z.array(PipelineQueueHealthSchema).max(1_000),
  retention: z.array(RetentionHealthSchema).max(3).default([]),
  runId: z.string().trim().min(1).max(256),
  providers: z.array(ZPROVIDER_PIPELINE_HEALTH).max(3).default([]),
  sol: SolPipelineHealthSchema.nullable(),
  storage: PipelineStorageHealthSchema.nullable().default(null),
});

export const ZPIPELINE_HEALTH = PipelineHealthBodySchema.extend({
  observedAt: z.int().nonnegative(),
  schemaId: z.literal("saqi.pipeline-health"),
  schemaVersion: z.literal(1),
  state: PipelineStateSchema,
}).strict();

export const SolQuotaSignalSchema = z
  .strictObject({
    configDigest: z.string().regex(/^[a-f\d]{64}$/),
    errorCode: ErrorCodeSchema.nullable(),
    model: z.string().trim().min(1).max(128),
    observedAt: z.int().nonnegative(),
    retryAt: z.int().nonnegative().nullable(),
    schemaId: z.literal("saqi.pipeline-signal"),
    schemaVersion: z.literal(1),
    signal: z.literal("sol_quota"),
    state: QuotaSignalStateSchema,
  })
  .superRefine((signal, context) => {
    const complete = signal.errorCode !== null && signal.retryAt !== null;
    if ((signal.state === "open") !== complete) {
      context.addIssue({
        code: "custom",
        message: "Open quota signals require errorCode and retryAt",
      });
    }
  });

export type PipelineHealth = z.infer<typeof ZPIPELINE_HEALTH>;
export type PipelineHealthInput = z.input<typeof PipelineHealthBodySchema>;
export type SolQuotaSignal = z.infer<typeof SolQuotaSignalSchema>;

export interface PipelineDiagnosticsOptions {
  readonly configDigest: string;
  readonly heartbeatMs?: number;
  readonly model: string;
  readonly now?: () => number;
  readonly stateDirectory: string;
}

export interface DiagnosticWriteResult<T> {
  readonly document: T;
  readonly reason: "heartbeat" | "transition" | "unchanged";
  readonly written: boolean;
}

export interface PipelineHealthEvaluation {
  readonly blocked: boolean;
  readonly blockerCodes: readonly string[];
  readonly exitCode: 0 | 2;
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete diagnostic snapshot writer owns file publication; its options inject the external dependencies.
export class PipelineDiagnostics {
  readonly #configDigest: string;
  readonly #healthPath: string;
  readonly #hourlyHealthRoot: string;
  readonly #heartbeatMs: number;
  readonly #model: string;
  readonly #now: () => number;
  readonly #signalPath: string;
  #initialization: null | Promise<void> = null;

  constructor(options: PipelineDiagnosticsOptions) {
    this.#configDigest = ConfigDigestSchema.parse(options.configDigest);
    this.#heartbeatMs = HeartbeatIntervalSchema.parse(
      options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS,
    );
    this.#model = ModelNameSchema.parse(options.model);
    this.#now = options.now ?? Date.now;
    this.#healthPath = join(options.stateDirectory, "health", "latest.json");
    this.#hourlyHealthRoot = join(options.stateDirectory, "health", "hourly");
    this.#signalPath = join(
      options.stateDirectory,
      "signals",
      "sol-quota.json",
    );
  }

  async initialize(): Promise<void> {
    this.#initialization ??= this.#initialize();
    await this.#initialization;
  }

  async clearQuotaAfterSuccess(
    model: string,
  ): Promise<DiagnosticWriteResult<SolQuotaSignal>> {
    await this.initialize();
    const current = await readSolQuotaSignal(this.#signalPath);
    if (
      model !== this.#model ||
      current.model !== model ||
      (current.state === "open" &&
        current.retryAt !== null &&
        current.retryAt > this.#now())
    ) {
      return { document: current, reason: "unchanged", written: false };
    }
    return this.#commitSignal({
      errorCode: null,
      retryAt: null,
      state: "cleared",
    });
  }

  async clearQuotaAfterProbe(
    model: string,
  ): Promise<DiagnosticWriteResult<SolQuotaSignal>> {
    await this.initialize();
    const current = await readSolQuotaSignal(this.#signalPath);
    if (model !== this.#model || current.model !== model) {
      return { document: current, reason: "unchanged", written: false };
    }
    return this.#commitSignal({
      errorCode: null,
      retryAt: null,
      state: "cleared",
    });
  }

  async recordQuota(
    errorCode: string,
    retryAt: number,
  ): Promise<DiagnosticWriteResult<SolQuotaSignal>> {
    await this.initialize();
    return this.#commitSignal({ errorCode, retryAt, state: "open" });
  }

  async writeHealth(
    input: PipelineHealthInput,
  ): Promise<DiagnosticWriteResult<PipelineHealth>> {
    await this.initialize();
    const body = normalizeHealthBody(PipelineHealthBodySchema.parse(input));
    if (body.configDigest !== this.#configDigest) {
      throw new Error("PIPELINE_DIAGNOSTIC_CONFIG_MISMATCH");
    }
    const observedAt = this.#now();
    const document = ZPIPELINE_HEALTH.parse({
      ...body,
      heartbeatIntervalMs: this.#heartbeatMs,
      observedAt,
      schemaId: "saqi.pipeline-health",
      schemaVersion: 1,
      state: healthState(body, observedAt),
    });
    const current = (await pathExists(this.#healthPath))
      ? await readPipelineHealth(this.#healthPath)
      : null;
    const reason = writeReason(current, document, this.#heartbeatMs);
    if (reason === "unchanged") {
      if (!current) throw new Error("PIPELINE_HEALTH_CURRENT_MISSING");
      await this.#writeHourlySnapshot(current);
      return { document: current, reason, written: false };
    }
    await writeAtomicJson(this.#healthPath, document);
    await this.#writeHourlySnapshot(document);
    return { document, reason, written: true };
  }

  async #initialize(): Promise<void> {
    if (!(await pathExists(this.#signalPath))) {
      await this.#writeSignal({
        configDigest: this.#configDigest,
        errorCode: null,
        model: this.#model,
        observedAt: this.#now(),
        retryAt: null,
        schemaId: "saqi.pipeline-signal",
        schemaVersion: 1,
        signal: "sol_quota",
        state: "cleared",
      });
      return;
    }
    const current = await readSolQuotaSignal(this.#signalPath);
    if (
      current.configDigest !== this.#configDigest ||
      current.model !== this.#model
    ) {
      await this.#writeSignal({
        configDigest: this.#configDigest,
        errorCode: null,
        model: this.#model,
        observedAt: this.#now(),
        retryAt: null,
        schemaId: "saqi.pipeline-signal",
        schemaVersion: 1,
        signal: "sol_quota",
        state: "cleared",
      });
    }
  }

  async #writeHourlySnapshot(document: PipelineHealth): Promise<void> {
    const hour = Math.floor(document.observedAt / (60 * 60_000));
    const path = join(this.#hourlyHealthRoot, `${String(hour)}.json`);
    if (await pathExists(path)) {
      await readPipelineHealth(path);
      return;
    }
    await writeAtomicJson(path, document);
  }

  async #commitSignal(
    change: Pick<SolQuotaSignal, "errorCode" | "retryAt" | "state">,
  ): Promise<DiagnosticWriteResult<SolQuotaSignal>> {
    const observedAt = this.#now();
    const document = SolQuotaSignalSchema.parse({
      ...change,
      configDigest: this.#configDigest,
      model: this.#model,
      observedAt,
      schemaId: "saqi.pipeline-signal",
      schemaVersion: 1,
      signal: "sol_quota",
    });
    const current = await readSolQuotaSignal(this.#signalPath);
    const reason = writeReason(current, document, this.#heartbeatMs);
    if (reason === "unchanged") {
      return { document: current, reason, written: false };
    }
    await this.#writeSignal(document);
    return { document, reason, written: true };
  }

  async #writeSignal(signal: SolQuotaSignal): Promise<void> {
    await writeAtomicJson(this.#signalPath, SolQuotaSignalSchema.parse(signal));
  }
}

export function evaluatePipelineHealth(
  rawHealth: PipelineHealth,
  now = Date.now(),
  rawQuotaSignal?: SolQuotaSignal,
): PipelineHealthEvaluation {
  const health = ZPIPELINE_HEALTH.parse(rawHealth);
  const blockerCodes = new Set(
    health.checks
      .filter((check) => check.state === "blocked")
      .map((check) => check.code),
  );
  if (
    health.desiredConfigDigest !== null &&
    health.desiredConfigDigest !== health.configDigest
  )
    blockerCodes.add("CONFIG_DIGEST_DRIFT");
  const anyGrowthViable = growthViable(health.growth);
  if (health.growth !== null && !anyGrowthViable)
    blockerCodes.add("NO_VIABLE_GROWTH_LANE");
  for (const origin of health.origins) {
    if (origin.state === "blocked" || origin.state === "challenge_wait")
      blockerCodes.add(origin.stopReason ?? "SOURCE_ORIGIN_BLOCKED");
  }
  if (health.growth?.production.state === "stalled")
    blockerCodes.add("PRODUCTION_SUCCESS_STALLED");
  if (
    health.observedAt > now + 60_000 ||
    now - health.observedAt > MAXIMUM_HEALTH_AGE_MS
  ) {
    blockerCodes.add("PIPELINE_HEALTH_STALE");
  }
  if (health.storage && !health.storage.writable)
    blockerCodes.add("ARTIFACT_STORE_DISK_PRESSURE");
  const providerLaneNames = new Set<string>(
    health.providers.map(({ provider }) => provider),
  );
  for (const lane of health.lanes) {
    if (lane.state === "fenced") {
      blockerCodes.add(lane.lastErrorCode ?? "LANE_FENCED");
    } else if (
      lane.state === "blocked" &&
      !(anyGrowthViable && providerLaneNames.has(lane.name))
    ) {
      blockerCodes.add(lane.lastErrorCode ?? "LANE_BLOCKED");
    }
  }
  if (!anyGrowthViable)
    for (const code of providerQuotaBlockerCodes(health.providers, now))
      blockerCodes.add(code);
  if (
    health.providers.length === 0 &&
    health.sol?.lastDisposition === "quota_wait" &&
    health.sol.retryAt !== null &&
    health.sol.retryAt > now
  )
    blockerCodes.add("CODEX_QUOTA_EXHAUSTED");
  if (rawQuotaSignal && !anyGrowthViable) {
    const quotaSignal = SolQuotaSignalSchema.parse(rawQuotaSignal);
    if (
      quotaSignal.configDigest === health.configDigest &&
      quotaSignal.state === "open" &&
      quotaSignal.retryAt !== null &&
      quotaSignal.retryAt > now
    ) {
      blockerCodes.add(quotaSignal.errorCode ?? "CODEX_QUOTA_EXHAUSTED");
    }
  }
  const sorted = [...blockerCodes].toSorted();
  return {
    blocked: sorted.length > 0,
    blockerCodes: sorted,
    exitCode: sorted.length > 0 ? 2 : 0,
  };
}

export async function readPipelineHealth(
  path: string,
): Promise<PipelineHealth> {
  return ZPIPELINE_HEALTH.parse(await readBoundedJson(path));
}

export async function readSolQuotaSignal(
  path: string,
): Promise<SolQuotaSignal> {
  return SolQuotaSignalSchema.parse(await readBoundedJson(path));
}

function healthState(
  health: z.infer<typeof PipelineHealthBodySchema>,
  observedAt: number,
): PipelineHealth["state"] {
  const anyGrowthViable = growthViable(health.growth);
  const providerLaneNames = new Set<string>(
    health.providers.map(({ provider }) => provider),
  );
  const globallyBlockedLane = health.lanes.some(
    (lane) =>
      lane.state === "fenced" ||
      (lane.state === "blocked" &&
        !(anyGrowthViable && providerLaneNames.has(lane.name))),
  );
  const actionableOrigin = health.origins.some(
    ({ state }) => state === "blocked" || state === "challenge_wait",
  );
  if (
    health.checks.some((check) => check.state === "blocked") ||
    globallyBlockedLane ||
    (health.storage !== null && !health.storage.writable) ||
    (health.desiredConfigDigest !== null &&
      health.desiredConfigDigest !== health.configDigest) ||
    actionableOrigin ||
    health.growth?.production.state === "stalled" ||
    (health.growth !== null && !anyGrowthViable) ||
    (!anyGrowthViable &&
      providerQuotaBlockerCodes(health.providers, observedAt).length > 0) ||
    (health.providers.length === 0 &&
      health.sol?.lastDisposition === "quota_wait" &&
      health.sol.retryAt !== null &&
      health.sol.retryAt > observedAt)
  ) {
    return "blocked";
  }
  return health.checks.some((check) => check.state === "warning") ||
    health.lanes.some((lane) => lane.state === "blocked") ||
    (health.growth !== null && health.growth.local.state !== "active") ||
    providerQuotaBlockerCodes(health.providers, observedAt).length > 0 ||
    health.providers.some(
      ({ providerErrorCode, recoverableUnknownOperations }) =>
        providerErrorCode !== null || recoverableUnknownOperations > 0,
    )
    ? "degraded"
    : "healthy";
}

function normalizeHealthBody(
  body: z.infer<typeof PipelineHealthBodySchema>,
): z.infer<typeof PipelineHealthBodySchema> {
  return {
    ...body,
    checks: body.checks.toSorted((left, right) =>
      left.code.localeCompare(right.code),
    ),
    lanes: body.lanes.toSorted((left, right) =>
      left.name.localeCompare(right.name),
    ),
    providers: body.providers.toSorted((left, right) =>
      left.provider.localeCompare(right.provider),
    ),
    localFanout: body.localFanout.toSorted((left, right) =>
      left.provider.localeCompare(right.provider),
    ),
    origins: body.origins.toSorted((left, right) =>
      left.origin.localeCompare(right.origin),
    ),
    queues: body.queues.toSorted((left, right) =>
      left.kind.localeCompare(right.kind),
    ),
    retention: body.retention.toSorted((left, right) =>
      left.provider.localeCompare(right.provider),
    ),
  };
}

function providerQuotaBlockerCodes(
  providers: readonly z.infer<typeof ZPROVIDER_PIPELINE_HEALTH>[],
  now: number,
): string[] {
  return providers.flatMap(({ lastDisposition, retryAt }) => {
    if (lastDisposition !== "quota_wait" || retryAt === null || retryAt <= now)
      return [];
    return ["CODEX_QUOTA_EXHAUSTED"];
  });
}

function growthViable(
  growth: null | z.infer<typeof GrowthHealthSchema>,
): boolean {
  return (
    growth !== null &&
    (["active", "retrying"].includes(growth.local.state) ||
      ["active", "ready"].includes(growth.production.state))
  );
}

async function readBoundedJson(path: string): Promise<unknown> {
  const value = await readFile(path);
  if (value.byteLength > MAXIMUM_DIAGNOSTIC_BYTES) {
    throw new Error("PIPELINE_DIAGNOSTIC_TOO_LARGE");
  }
  const parsed: unknown = JSON.parse(value.toString("utf8"));
  return parsed;
}

function writeReason<T extends { observedAt: number }>(
  current: null | T,
  next: T,
  heartbeatMs: number,
): DiagnosticWriteResult<T>["reason"] {
  if (!current) return "transition";
  const withoutTime = ({ observedAt: _observedAt, ...value }: T) => value;
  if (
    JSON.stringify(withoutTime(current)) !== JSON.stringify(withoutTime(next))
  ) {
    return "transition";
  }
  return next.observedAt - current.observedAt >= heartbeatMs
    ? "heartbeat"
    : "unchanged";
}

async function writeAtomicJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { mode: 0o700, recursive: true });
  const temporary = `${path}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value)}\n`);
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
  const directory = await open(dirname(path), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}
