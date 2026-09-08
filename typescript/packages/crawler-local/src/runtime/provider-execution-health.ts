import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";

import { z } from "zod";

import { EnrichmentProviderSchema } from "../ports/provider-contract.js";

const ErrorCodeSchema = z.string().regex(/^[A-Z][A-Z0-9_]{1,127}$/);
const TimestampSchema = z.int().nonnegative();
const BudgetStateSchema = z.enum(["unarmed", "active", "exhausted", "closed"]);
const RetryStateSchema = z.enum(["ready", "waiting"]);
const ProviderStateSchema = z.enum([
  "ready",
  "network_wait",
  "rate_limited",
  "backoff",
]);
const QuotaStateSchema = z.enum(["clear", "waiting"]);
const SchedulerStateSchema = z.enum([
  "ready",
  "adaptive",
  "at_capacity",
  "launch_pacing",
  "error_dampener",
  "circuit_open",
]);
const ProgressStateSchema = z.enum(["active", "recent", "stalled", "idle"]);
const CoverageStateSchema = z.enum(["backfilling", "complete"]);
const CredentialChangeSchema = z.enum([
  "none",
  "material_refresh",
  "account_switch",
]);
const CredentialStateSchema = z.enum([
  "ready",
  "absent",
  "observation_wait",
  "material_verification",
  "account_switch_wait",
  "account_verification",
]);
const OperatorActionSchema = z.enum([
  "none",
  "resume_all",
  "resume_paid",
  "arm_budget",
  "rearm_budget",
  "restore_auth",
  "enable_provider",
  "inspect_provider",
]);
const RecoveryOwnerSchema = z.enum([
  "automatic",
  "operator",
  "configuration",
  "none",
]);
const AdmissionStateSchema = z.enum([
  "open",
  "limited",
  "waiting",
  "closed",
  "disabled",
]);

const BudgetGateSchema = z
  .strictObject({
    budgetId: z.uuid().nullable(),
    maximumOperations: z.int().nonnegative(),
    remainingOperations: z.int().nonnegative(),
    reservedOperations: z.int().nonnegative(),
    state: BudgetStateSchema,
  })
  .superRefine((budget, context) => {
    if (budget.state === "unarmed") {
      if (
        budget.budgetId !== null ||
        budget.maximumOperations !== 0 ||
        budget.reservedOperations !== 0 ||
        budget.remainingOperations !== 0
      )
        context.addIssue({
          code: "custom",
          message: "An unarmed budget cannot contain an allocation",
        });
      return;
    }
    if (budget.budgetId === null)
      context.addIssue({
        code: "custom",
        message: "An allocated budget requires a budget id",
      });
    if (
      budget.maximumOperations <= 0 ||
      budget.maximumOperations % 3 !== 0 ||
      budget.reservedOperations % 3 !== 0 ||
      budget.remainingOperations !==
        budget.maximumOperations - budget.reservedOperations
    )
      context.addIssue({
        code: "custom",
        message: "Paid-operation budget counts are inconsistent",
      });
    if (budget.state === "active" && budget.remainingOperations < 3)
      context.addIssue({
        code: "custom",
        message: "An active budget must admit one complete claim",
      });
    if (budget.state === "exhausted" && budget.remainingOperations >= 3)
      context.addIssue({
        code: "custom",
        message: "An exhausted budget cannot admit a complete claim",
      });
  });

const RetryGateSchema = z.strictObject({
  errorCode: ErrorCodeSchema.nullable(),
  retryAt: TimestampSchema.nullable(),
  state: RetryStateSchema,
});

const ProviderGateSchema = z.strictObject({
  errorCode: ErrorCodeSchema.nullable(),
  retryAt: TimestampSchema.nullable(),
  state: ProviderStateSchema,
});

const ResourceReasonSchema = z.enum([
  "DISK_PRESSURE",
  "MEMORY_PRESSURE",
  "PROCESS_MEMORY_PRESSURE",
  "FILE_DESCRIPTOR_PRESSURE",
  "RESOURCE_PROBE_FAILED",
]);

const ProviderExecutionGatesSchema = z.strictObject({
  authentication: RetryGateSchema,
  budget: BudgetGateSchema,
  operator: z.strictObject({
    globalPaused: z.boolean(),
    paidWorkPaused: z.boolean(),
  }),
  provider: ProviderGateSchema,
  quota: z.strictObject({
    errorCode: ErrorCodeSchema.nullable(),
    nextProbeAt: TimestampSchema.nullable(),
    retryAt: TimestampSchema.nullable(),
    state: QuotaStateSchema,
  }),
  resources: z.strictObject({
    nextProbeAt: TimestampSchema.nullable(),
    reasons: z.array(ResourceReasonSchema).max(5),
    state: RetryStateSchema,
  }),
  scheduler: z.strictObject({
    activeInvocations: z.int().nonnegative(),
    configuredConcurrency: z.int().min(1).max(256),
    nextWakeAt: TimestampSchema.nullable(),
    selectedConcurrency: z.int().min(1).max(256),
    state: SchedulerStateSchema,
  }),
});

const ProviderProgressSchema = z.strictObject({
  accepted: z.int().nonnegative(),
  activeInvocations: z.int().nonnegative(),
  delayedWork: z.int().nonnegative(),
  lastAcceptedAt: TimestampSchema.nullable(),
  readyWork: z.int().nonnegative(),
  state: ProgressStateSchema,
  terminalWork: z.int().nonnegative(),
});

const PoemMilestoneWindowSchema = z
  .strictObject({
    last15m: z.int().nonnegative(),
    last1h: z.int().nonnegative(),
    last5m: z.int().nonnegative(),
    lastAt: TimestampSchema.nullable(),
  })
  .superRefine((window, context) => {
    if (window.last5m > window.last15m || window.last15m > window.last1h)
      context.addIssue({
        code: "custom",
        message: "Milestone windows must be monotonically nested",
      });
  });

const ProviderPoemThroughputSchema = z
  .strictObject({
    coverage: z.strictObject({
      backfillComplete: z.boolean(),
      highWatermark: z.int().nonnegative(),
      state: CoverageStateSchema,
    }),
    generated: PoemMilestoneWindowSchema,
    published: PoemMilestoneWindowSchema,
    remaining: z.strictObject({
      active: z.int().nonnegative(),
      delayed: z.int().nonnegative(),
      endToEndPublication: z.int().nonnegative(),
      generatedAwaitingPublication: z.int().nonnegative(),
      generation: z.int().nonnegative(),
      ready: z.int().nonnegative(),
      terminalDead: z.int().nonnegative(),
    }),
  })
  .superRefine((throughput, context) => {
    const { coverage, remaining } = throughput;
    if ((coverage.state === "complete") !== coverage.backfillComplete)
      context.addIssue({
        code: "custom",
        message: "Milestone coverage state is inconsistent",
      });
    if (
      remaining.generation !==
      remaining.ready + remaining.delayed + remaining.active
    )
      context.addIssue({
        code: "custom",
        message: "Generation remaining counts are inconsistent",
      });
    if (
      remaining.endToEndPublication !==
      remaining.generation + remaining.generatedAwaitingPublication
    )
      context.addIssue({
        code: "custom",
        message: "End-to-end remaining counts are inconsistent",
      });
  });

const ProviderCredentialHealthSchema = z
  .strictObject({
    accountEpoch: z.int().nonnegative(),
    change: CredentialChangeSchema,
    changedAt: TimestampSchema.nullable(),
    errorCode: ErrorCodeSchema.nullable(),
    lastVerifiedAt: TimestampSchema.nullable(),
    materialEpoch: z.int().nonnegative(),
    retryAt: TimestampSchema.nullable(),
    state: CredentialStateSchema,
  })
  .superRefine((credentials, context) => {
    if (credentials.state === "ready") {
      if (
        credentials.change !== "none" ||
        credentials.errorCode !== null ||
        credentials.retryAt !== null
      )
        context.addIssue({
          code: "custom",
          message: "Ready credentials cannot retain transition metadata",
        });
      return;
    }
    if (credentials.state === "absent") {
      if (
        credentials.change !== "none" ||
        credentials.errorCode === null ||
        credentials.retryAt !== null
      )
        context.addIssue({
          code: "custom",
          message: "Absent credentials require a terminal auth diagnosis",
        });
      return;
    }
    if (credentials.errorCode === null || credentials.retryAt === null)
      context.addIssue({
        code: "custom",
        message: "Credential recovery requires an error and retry time",
      });
    const expectedChange =
      credentials.state === "material_verification"
        ? "material_refresh"
        : credentials.state === "account_switch_wait" ||
            credentials.state === "account_verification"
          ? "account_switch"
          : "none";
    if (credentials.change !== expectedChange)
      context.addIssue({
        code: "custom",
        message: "Credential state and change classification disagree",
      });
    if (credentials.change !== "none" && credentials.changedAt === null)
      context.addIssue({
        code: "custom",
        message: "Credential changes require a transition timestamp",
      });
  });

const ProviderSessionHealthSchema = z.strictObject({
  activeCurrentAccountEpoch: z.int().nonnegative(),
  activePreviousAccountEpoch: z.int().nonnegative(),
  activeUnattributed: z.int().nonnegative(),
});

export const ProviderExecutionAdmissionReasonSchema = z.enum([
  "active",
  "ready",
  "no_ready_work",
  "adaptive_capacity",
  "at_capacity",
  "launch_pacing",
  "error_dampener",
  "operator_paused",
  "paid_work_paused",
  "budget_unarmed",
  "budget_exhausted",
  "resource_wait",
  "auth_wait",
  "codex_quota_wait",
  "network_wait",
  "rate_limit_wait",
  "provider_backoff",
  "circuit_open",
  "disabled",
]);

const ProviderExecutionAdmissionSchema = z.strictObject({
  operatorAction: OperatorActionSchema,
  primaryReason: ProviderExecutionAdmissionReasonSchema,
  recovery: RecoveryOwnerSchema,
  retryAt: TimestampSchema.nullable(),
  state: AdmissionStateSchema,
});

const ProviderExecutionHealthEntryInputSchema = z.strictObject({
  credentials: ProviderCredentialHealthSchema,
  debt: z.strictObject({
    quarantinedOperations: z.int().nonnegative(),
    recoverableUnknownOperations: z.int().nonnegative(),
    semanticFailures: z.int().nonnegative(),
  }),
  enabled: z.boolean(),
  gates: ProviderExecutionGatesSchema,
  model: z.string().trim().min(1).max(128),
  modelKey: z.string().trim().min(1).max(128),
  progress: ProviderProgressSchema,
  provider: EnrichmentProviderSchema,
  sessions: ProviderSessionHealthSchema,
  throughput: ProviderPoemThroughputSchema.nullable()
    .optional()
    .transform((value) => value ?? null),
});

export const ProviderExecutionHealthEntrySchema =
  ProviderExecutionHealthEntryInputSchema.extend({
    admission: ProviderExecutionAdmissionSchema,
  }).superRefine((entry, context) => {
    const { authentication, provider, quota, resources, scheduler } =
      entry.gates;
    if (scheduler.selectedConcurrency > scheduler.configuredConcurrency)
      context.addIssue({
        code: "custom",
        message: "Selected concurrency exceeds the configured ceiling",
      });
    if (scheduler.activeInvocations > scheduler.configuredConcurrency)
      context.addIssue({
        code: "custom",
        message: "Active invocations exceed the configured ceiling",
      });
    if (
      entry.progress.activeInvocations !== scheduler.activeInvocations ||
      (entry.progress.state === "active") !==
        entry.progress.activeInvocations > 0
    )
      context.addIssue({
        code: "custom",
        message: "Progress and scheduler activity are inconsistent",
      });
    const attributedSessions =
      entry.sessions.activeCurrentAccountEpoch +
      entry.sessions.activePreviousAccountEpoch +
      entry.sessions.activeUnattributed;
    if (attributedSessions !== entry.progress.activeInvocations)
      context.addIssue({
        code: "custom",
        message: "Active session attribution does not match progress",
      });
    if (
      entry.credentials.state === "account_switch_wait" &&
      entry.sessions.activePreviousAccountEpoch === 0
    )
      context.addIssue({
        code: "custom",
        message: "Account switch wait requires a previous-account session",
      });
    if (
      entry.credentials.state === "ready" &&
      entry.sessions.activePreviousAccountEpoch > 0
    )
      context.addIssue({
        code: "custom",
        message: "Ready credentials cannot retain previous-account sessions",
      });
    validateRetryGate(authentication, context, "Authentication");
    if (quota.state === "waiting") {
      if (quota.errorCode === null || quota.retryAt === null)
        context.addIssue({
          code: "custom",
          message: "A quota wait requires an error and retry time",
        });
    } else if (
      quota.errorCode !== null ||
      quota.retryAt !== null ||
      quota.nextProbeAt !== null
    )
      context.addIssue({
        code: "custom",
        message: "A clear quota gate cannot retain wait metadata",
      });
    if (provider.state === "ready") {
      if (provider.errorCode !== null || provider.retryAt !== null)
        context.addIssue({
          code: "custom",
          message: "A ready provider cannot retain wait metadata",
        });
    } else if (provider.errorCode === null || provider.retryAt === null)
      context.addIssue({
        code: "custom",
        message: "A provider wait requires an error and retry time",
      });
    if ((resources.state === "waiting") !== resources.reasons.length > 0)
      context.addIssue({
        code: "custom",
        message: "Resource wait state and reasons are inconsistent",
      });
  });

export const ProviderExecutionHealthSchema = z
  .strictObject({
    configDigest: z.string().regex(/^[a-f\d]{64}$/),
    observedAt: TimestampSchema,
    providers: z.array(ProviderExecutionHealthEntrySchema).min(1).max(3),
    runId: z.string().trim().min(1).max(256),
    schemaId: z.literal("saqi.provider-execution-health"),
    schemaVersion: z.literal(1),
  })
  .superRefine((health, context) => {
    const seen = new Set<string>();
    for (const entry of health.providers) {
      if (seen.has(entry.provider))
        context.addIssue({
          code: "custom",
          message: `Duplicate provider execution entry: ${entry.provider}`,
        });
      seen.add(entry.provider);
      // A retry deadline can pass before recovery is observed. Preserve it as
      // due without clearing the gate or invalidating the health snapshot.
      if (
        entry.credentials.changedAt !== null &&
        entry.credentials.changedAt > health.observedAt
      )
        context.addIssue({
          code: "custom",
          message: "Credential transition cannot be observed in the future",
        });
      if (
        entry.credentials.lastVerifiedAt !== null &&
        entry.credentials.lastVerifiedAt > health.observedAt
      )
        context.addIssue({
          code: "custom",
          message: "Credential verification cannot occur in the future",
        });
      for (const lastAt of [
        entry.throughput?.generated.lastAt,
        entry.throughput?.published.lastAt,
      ])
        if (
          lastAt !== null &&
          lastAt !== undefined &&
          lastAt > health.observedAt
        )
          context.addIssue({
            code: "custom",
            message: "Poem milestone cannot occur after observation",
          });
    }
  });

export type ProviderExecutionHealth = z.infer<
  typeof ProviderExecutionHealthSchema
>;
export type ProviderExecutionHealthEntry = z.infer<
  typeof ProviderExecutionHealthEntrySchema
>;
export type ProviderExecutionHealthEntryInput = Omit<
  ProviderExecutionHealthEntry,
  "admission"
>;
export type ProviderExecutionHealthInput = Omit<
  ProviderExecutionHealth,
  "providers" | "schemaId" | "schemaVersion"
> & {
  readonly providers: readonly ProviderExecutionHealthEntryInput[];
};

export function buildProviderExecutionHealthEntry(
  rawInput: ProviderExecutionHealthEntryInput,
): ProviderExecutionHealthEntry {
  const input = ProviderExecutionHealthEntryInputSchema.parse(rawInput);
  return ProviderExecutionHealthEntrySchema.parse({
    ...input,
    admission: classifyAdmission(input),
  });
}

export function buildProviderExecutionHealth(
  input: ProviderExecutionHealthInput,
): ProviderExecutionHealth {
  return ProviderExecutionHealthSchema.parse({
    ...input,
    providers: input.providers
      .map(buildProviderExecutionHealthEntry)
      .toSorted((left, right) => left.provider.localeCompare(right.provider)),
    schemaId: "saqi.provider-execution-health",
    schemaVersion: 1,
  });
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete diagnostic snapshot writer has no independently substituted implementation or consumer port.
export class ProviderExecutionDiagnostics {
  readonly #path: string;

  constructor(stateDirectory: string) {
    this.#path = join(
      stateDirectory,
      "health",
      "provider-execution-latest.json",
    );
  }

  get path(): string {
    return this.#path;
  }

  async write(
    input: ProviderExecutionHealthInput,
  ): Promise<ProviderExecutionHealth> {
    const document = buildProviderExecutionHealth(input);
    await writeAtomicJson(this.#path, document);
    return document;
  }
}

export async function readProviderExecutionHealth(
  path: string,
): Promise<ProviderExecutionHealth> {
  const contents = await readFile(path);
  if (contents.byteLength > 1024 * 1024)
    throw new Error("PROVIDER_EXECUTION_HEALTH_TOO_LARGE");
  return ProviderExecutionHealthSchema.parse(
    JSON.parse(contents.toString("utf8")),
  );
}

function classifyAdmission(
  input: ProviderExecutionHealthEntryInput,
): ProviderExecutionHealthEntry["admission"] {
  const { authentication, budget, operator, provider, quota, resources } =
    input.gates;
  const scheduler = input.gates.scheduler;
  if (!input.enabled)
    return admission(
      "disabled",
      "disabled",
      "configuration",
      "enable_provider",
    );
  if (operator.globalPaused)
    return admission("closed", "operator_paused", "operator", "resume_all");
  if (operator.paidWorkPaused)
    return admission("closed", "paid_work_paused", "operator", "resume_paid");
  if (budget.state === "unarmed")
    return admission("closed", "budget_unarmed", "operator", "arm_budget");
  if (budget.state === "exhausted" || budget.state === "closed")
    return admission("closed", "budget_exhausted", "operator", "rearm_budget");
  if (resources.state === "waiting")
    return admission(
      "closed",
      "resource_wait",
      "automatic",
      "none",
      resources.nextProbeAt,
    );
  if (input.credentials.state !== "ready")
    return admission(
      input.credentials.state === "absent" ? "closed" : "waiting",
      "auth_wait",
      input.credentials.state === "absent" ? "operator" : "automatic",
      input.credentials.state === "absent" ? "restore_auth" : "none",
      input.credentials.retryAt,
    );
  if (authentication.state === "waiting")
    return admission(
      "waiting",
      "auth_wait",
      "automatic",
      "restore_auth",
      authentication.retryAt,
    );
  if (quota.state === "waiting")
    return admission(
      "waiting",
      "codex_quota_wait",
      "automatic",
      "none",
      quota.retryAt,
    );
  if (provider.state !== "ready")
    return admission(
      "waiting",
      provider.state === "network_wait"
        ? "network_wait"
        : provider.state === "rate_limited"
          ? "rate_limit_wait"
          : "provider_backoff",
      "automatic",
      provider.state === "backoff" ? "inspect_provider" : "none",
      provider.retryAt,
    );
  if (scheduler.state === "circuit_open")
    return admission("closed", "circuit_open", "operator", "inspect_provider");
  if (scheduler.state === "error_dampener")
    return admission(
      "waiting",
      "error_dampener",
      "automatic",
      "none",
      scheduler.nextWakeAt,
    );
  if (scheduler.state === "launch_pacing")
    return admission(
      "limited",
      "launch_pacing",
      "automatic",
      "none",
      scheduler.nextWakeAt,
    );
  if (scheduler.state === "at_capacity")
    return admission(
      "limited",
      "at_capacity",
      "none",
      "none",
      scheduler.nextWakeAt,
    );
  if (
    scheduler.state === "adaptive" ||
    scheduler.selectedConcurrency < scheduler.configuredConcurrency
  )
    return admission("limited", "adaptive_capacity", "none", "none");
  if (input.progress.activeInvocations > 0)
    return admission("open", "active", "none", "none");
  if (input.progress.readyWork > 0)
    return admission("open", "ready", "none", "none");
  return admission("open", "no_ready_work", "none", "none");
}

function admission(
  state: ProviderExecutionHealthEntry["admission"]["state"],
  primaryReason: ProviderExecutionHealthEntry["admission"]["primaryReason"],
  recovery: ProviderExecutionHealthEntry["admission"]["recovery"],
  operatorAction: ProviderExecutionHealthEntry["admission"]["operatorAction"],
  retryAt: null | number = null,
): ProviderExecutionHealthEntry["admission"] {
  return { operatorAction, primaryReason, recovery, retryAt, state };
}

function validateRetryGate(
  gate: z.infer<typeof RetryGateSchema>,
  context: z.RefinementCtx,
  label: string,
): void {
  if (gate.state === "waiting") {
    if (gate.errorCode === null || gate.retryAt === null)
      context.addIssue({
        code: "custom",
        message: `${label} wait requires an error and retry time`,
      });
  } else if (gate.errorCode !== null || gate.retryAt !== null)
    context.addIssue({
      code: "custom",
      message: `${label} ready state cannot retain wait metadata`,
    });
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
      // Preserve the original write failure.
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
