import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  evaluatePipelineHealth,
  PipelineDiagnostics,
  type PipelineHealthInput,
  readPipelineHealth,
  readSolQuotaSignal,
} from "../runtime/pipeline-diagnostics.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

const CONFIG_DIGEST = "a".repeat(64);
const MODEL = "gpt-5.6-sol";

function input(change: Partial<PipelineHealthInput> = {}): PipelineHealthInput {
  return {
    checks: [],
    configDigest: CONFIG_DIGEST,
    lanes: [
      {
        active: 0,
        completed: 2,
        failed: 0,
        lastDurationMs: 100,
        lastErrorCode: null,
        lastProgressAt: 900,
        name: "sol-1",
        state: "idle",
      },
    ],
    lastProgressAt: 900,
    queues: [
      {
        active: 0,
        deadLetter: 0,
        kind: "poem-enrichment-sol",
        oldestReadyAt: 500,
        pending: 8,
        quotaWait: 0,
        retryWait: 0,
        succeeded: 2,
        total: 10,
      },
    ],
    runId: "run-1",
    sol: {
      accepted: 2,
      activeInvocations: 0,
      invocationConcurrency: 2,
      lastDisposition: "healthy",
      model: MODEL,
      retryAt: null,
      selectedConcurrency: 2,
      semanticFailures: 0,
    },
    ...change,
  };
}

function fixture(now: () => number) {
  const root = mkdtempSync(join(tmpdir(), "saqi-pipeline-diagnostics-"));
  return {
    diagnostics: new PipelineDiagnostics({
      configDigest: CONFIG_DIGEST,
      heartbeatMs: 5_000,
      model: MODEL,
      now,
      stateDirectory: root,
    }),
    healthPath: join(root, "health", "latest.json"),
    root,
    signalPath: join(root, "signals", "sol-quota.json"),
  };
}

describe("pipeline diagnostics", () => {
  it("accepts the widget's maximum configured and selected concurrency", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const sol = input().sol;
    if (!sol) throw new Error("Expected Sol fixture");
    const { document } = await target.diagnostics.writeHealth(
      input({
        sol: {
          ...sol,
          invocationConcurrency: 256,
          selectedConcurrency: 256,
        },
      }),
    );
    expect(document.sol).toMatchObject({
      invocationConcurrency: 256,
      selectedConcurrency: 256,
    });
  });

  it("publishes internally consistent shared host admission truth", async () => {
    const target = fixture(() => 1_000);
    const { document } = await target.diagnostics.writeHealth(
      input({
        providerHostAdmission: {
          activeProcesses: 31,
          maximumProcesses: 64,
          remainingProcesses: 33,
        },
      }),
    );
    expect(document.providerHostAdmission).toEqual({
      activeProcesses: 31,
      maximumProcesses: 64,
      remainingProcesses: 33,
    });
    await expect(
      target.diagnostics.writeHealth(
        input({
          providerHostAdmission: {
            activeProcesses: 31,
            maximumProcesses: 64,
            remainingProcesses: 34,
          },
        }),
      ),
    ).rejects.toThrow("Provider host admission counts are inconsistent");
  });

  it("creates a durable cleared signal and opens it with the exact retry time", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    await expect(readSolQuotaSignal(target.signalPath)).resolves.toMatchObject({
      errorCode: null,
      model: MODEL,
      observedAt: 1_000,
      retryAt: null,
      state: "cleared",
    });
    expect(statSync(target.signalPath).mode & 0o777).toBe(0o600);

    now = 2_000;
    await expect(
      target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 123_456),
    ).resolves.toMatchObject({ reason: "transition", written: true });
    await expect(readSolQuotaSignal(target.signalPath)).resolves.toMatchObject({
      errorCode: "CODEX_QUOTA_EXHAUSTED",
      observedAt: 2_000,
      retryAt: 123_456,
      state: "open",
    });
    now = 3_000;
    await expect(
      target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 123_456),
    ).resolves.toMatchObject({ reason: "unchanged", written: false });
    now = 7_000;
    await expect(
      target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 123_456),
    ).resolves.toMatchObject({ reason: "heartbeat", written: true });
    expect(readdirSync(join(target.root, "signals"))).toEqual([
      "sol-quota.json",
    ]);
  });

  it("clears quota only after a success from the same model", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    await target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 10_000);

    now = 2_000;
    await expect(
      target.diagnostics.clearQuotaAfterSuccess("another-model"),
    ).resolves.toMatchObject({ reason: "unchanged", written: false });
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited diagnostic document.
    expect((await readSolQuotaSignal(target.signalPath)).state).toBe("open");

    await expect(
      target.diagnostics.clearQuotaAfterSuccess(MODEL),
    ).resolves.toMatchObject({
      reason: "unchanged",
      written: false,
    });
    now = 10_001;
    await expect(
      target.diagnostics.clearQuotaAfterSuccess(MODEL),
    ).resolves.toMatchObject({
      reason: "transition",
      written: true,
    });
    await expect(readSolQuotaSignal(target.signalPath)).resolves.toMatchObject({
      errorCode: null,
      retryAt: null,
      state: "cleared",
    });
  });

  it("lets a matching real-work quota probe supersede a future signal", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    await target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 10_000);

    now = 2_000;
    await expect(
      target.diagnostics.clearQuotaAfterProbe("another-model"),
    ).resolves.toMatchObject({ reason: "unchanged", written: false });
    await expect(
      target.diagnostics.clearQuotaAfterProbe(MODEL),
    ).resolves.toMatchObject({
      reason: "transition",
      written: true,
    });
    await expect(readSolQuotaSignal(target.signalPath)).resolves.toMatchObject({
      errorCode: null,
      retryAt: null,
      state: "cleared",
    });
  });

  it("supersedes a quota signal from a different configuration", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    await target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 10_000);
    now = 2_000;
    const replacement = new PipelineDiagnostics({
      configDigest: "b".repeat(64),
      heartbeatMs: 5_000,
      model: MODEL,
      now: () => now,
      stateDirectory: target.root,
    });
    await replacement.initialize();
    await expect(readSolQuotaSignal(target.signalPath)).resolves.toMatchObject({
      configDigest: "b".repeat(64),
      errorCode: null,
      observedAt: 2_000,
      state: "cleared",
    });
  });

  it("writes health on transitions and five-minute-style heartbeats only", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    await expect(
      target.diagnostics.writeHealth(input()),
    ).resolves.toMatchObject({
      reason: "transition",
      written: true,
    });
    const first = readFileSync(target.healthPath, "utf8");

    now = 2_000;
    await expect(
      target.diagnostics.writeHealth(input()),
    ).resolves.toMatchObject({
      reason: "unchanged",
      written: false,
    });
    expect(readFileSync(target.healthPath, "utf8")).toBe(first);

    now = 6_000;
    await expect(
      target.diagnostics.writeHealth(input()),
    ).resolves.toMatchObject({
      reason: "heartbeat",
      written: true,
    });
    // eslint-disable-next-line unicorn/no-await-expression-member -- Assertion reads one field from the awaited diagnostic document.
    expect((await readPipelineHealth(target.healthPath)).observedAt).toBe(
      6_000,
    );
    expect(readdirSync(join(target.root, "health"))).toEqual([
      "hourly",
      "latest.json",
    ]);
    expect(readdirSync(join(target.root, "health", "hourly"))).toEqual([
      "0.json",
    ]);
  });

  it("publishes its heartbeat contract and source-circuit recovery state", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        origins: [
          {
            active: false,
            consecutiveFailures: 3,
            cooldownUntil: 60_000,
            lastCompletedAt: 500,
            nextAllowedAt: 60_000,
            origin: "https://source.invalid",
            stopReason: "SOURCE_NETWORK_UNAVAILABLE",
          },
        ],
      }),
    );

    expect(document).toMatchObject({
      heartbeatIntervalMs: 5_000,
      origins: [
        {
          consecutiveFailures: 3,
          cooldownUntil: 60_000,
          stopReason: "SOURCE_NETWORK_UNAVAILABLE",
        },
      ],
    });
    expect(document.state).toBe("healthy");
    expect(evaluatePipelineHealth(document, 1_000)).toMatchObject({
      blocked: false,
      blockerCodes: [],
    });
  });

  it("blocks on a source challenge without blocking automatic source waits", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        origins: [
          {
            active: false,
            consecutiveFailures: 1,
            cooldownUntil: 60_000,
            lastCompletedAt: null,
            nextAllowedAt: 60_000,
            origin: "https://source.invalid",
            state: "challenge_wait",
            stopReason: "SOURCE_HUMAN_REQUIRED",
          },
        ],
      }),
    );

    expect(document.state).toBe("blocked");
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: ["SOURCE_HUMAN_REQUIRED"],
      exitCode: 2,
    });
  });

  it("fails closed for stale health and an independently open quota signal", async () => {
    let now = 1_000;
    const target = fixture(() => now);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(input());
    await target.diagnostics.recordQuota("CODEX_QUOTA_EXHAUSTED", 2_000_000);
    expect(
      evaluatePipelineHealth(
        document,
        now,
        await readSolQuotaSignal(target.signalPath),
      ),
    ).toMatchObject({
      blockerCodes: ["CODEX_QUOTA_EXHAUSTED"],
      exitCode: 2,
    });
    now += 15 * 60_000 + 1;
    expect(evaluatePipelineHealth(document, now).blockerCodes).toContain(
      "PIPELINE_HEALTH_STALE",
    );
  });

  it("normalizes repeated fields and evaluates every blocking source", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        checks: [
          {
            code: "DISK_PRESSURE",
            detail: "Free space is below the configured floor",
            retryAt: null,
            state: "blocked",
          },
          {
            code: "AUTH_READY",
            detail: null,
            retryAt: null,
            state: "ok",
          },
        ],
        lanes: [
          {
            active: 0,
            completed: 0,
            failed: 1,
            lastDurationMs: null,
            lastErrorCode: "RUN_LOCK_FENCED",
            lastProgressAt: null,
            name: "collector",
            state: "fenced",
          },
          ...input().lanes,
        ],
        sol: {
          ...input().sol!,
          lastDisposition: "quota_wait",
          retryAt: 10_000,
        },
      }),
    );

    expect(document.state).toBe("blocked");
    expect(document.checks.map(({ code }) => code)).toEqual([
      "AUTH_READY",
      "DISK_PRESSURE",
    ]);
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: [
        "CODEX_QUOTA_EXHAUSTED",
        "DISK_PRESSURE",
        "RUN_LOCK_FENCED",
      ],
      exitCode: 2,
    });
  });

  it("blocks on Codex quota exhaustion", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        providers: [
          {
            ...input().sol!,
            lastDisposition: "quota_wait",
            modelKey: "sol-5.6",
            provider: "sol",
            retryAt: 10_000,
          },
        ],
        sol: null,
      }),
    );

    expect(document.state).toBe("blocked");
    expect(document.providers.map(({ provider }) => provider)).toEqual(["sol"]);
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: ["CODEX_QUOTA_EXHAUSTED"],
      exitCode: 2,
    });
  });

  it("degrades for a blocked provider while another growth lane remains viable", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        growth: {
          local: {
            enabledLanes: 2,
            lastSuccessAt: 900,
            state: "active",
            viableLanes: 1,
          },
          production: {
            configured: false,
            lastSuccessAt: null,
            state: "disabled",
          },
        },
        lanes: [
          {
            ...input().lanes[0]!,
            lastErrorCode: "CODEX_AUTHENTICATION_REQUIRED",
            name: "sol",
            state: "blocked",
          },
        ],
        providers: [
          {
            ...input().sol!,
            lastDisposition: "quota_wait",
            modelKey: "sol-5.6",
            provider: "sol",
            providerErrorCode: "CODEX_AUTHENTICATION_REQUIRED",
            retryAt: 10_000,
          },
        ],
        sol: null,
      }),
    );

    expect(document.state).toBe("degraded");
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: false,
      blockerCodes: [],
      exitCode: 0,
    });
  });

  it("blocks on desired config drift and when no local growth lane is viable", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        desiredConfigDigest: "b".repeat(64),
        growth: {
          local: {
            enabledLanes: 2,
            lastSuccessAt: 0,
            state: "stalled",
            viableLanes: 0,
          },
          production: {
            configured: true,
            lastSuccessAt: null,
            state: "gated",
          },
        },
        lanes: [],
      }),
    );

    expect(document.state).toBe("blocked");
    expect(document.growth).toMatchObject({
      local: { state: "stalled", viableLanes: 0 },
      production: { configured: true, state: "gated" },
    });
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: ["CONFIG_DIGEST_DRIFT", "NO_VIABLE_GROWTH_LANE"],
      exitCode: 2,
    });
  });

  it("reports publication readiness without claiming a production success", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        growth: {
          local: {
            enabledLanes: 0,
            lastSuccessAt: null,
            state: "disabled",
            viableLanes: 0,
          },
          production: {
            configured: true,
            lastSuccessAt: null,
            state: "ready",
          },
        },
        lanes: [],
      }),
    );

    expect(document).toMatchObject({
      growth: {
        production: { lastSuccessAt: null, state: "ready" },
      },
      state: "degraded",
    });
    expect(evaluatePipelineHealth(document, 1_000).blocked).toBe(false);
  });

  it("blocks a silent production stall while local growth remains active", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        growth: {
          local: {
            enabledLanes: 1,
            lastSuccessAt: 900,
            state: "active",
            viableLanes: 1,
          },
          production: {
            configured: true,
            lastSuccessAt: null,
            state: "stalled",
          },
        },
      }),
    );

    expect(document.state).toBe("blocked");
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: ["PRODUCTION_SUCCESS_STALLED"],
      exitCode: 2,
    });
  });

  it("degrades for a provider-local unknown outcome while healthy lanes continue", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        checks: [
          {
            code: "CODEX_OPERATION_OUTCOME_UNKNOWN",
            detail: "1 Codex operation outcome requires reconciliation",
            retryAt: null,
            state: "warning",
          },
        ],
        providers: [
          {
            ...input().sol!,
            modelKey: "sol-5.6",
            provider: "sol",
            semanticFailures: 7,
            unknownOperations: 1,
          },
        ],
        sol: null,
      }),
    );

    expect(document.state).toBe("degraded");
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: false,
      blockerCodes: [],
      exitCode: 0,
    });
    expect(
      document.providers.map(
        ({ provider, semanticFailures, unknownOperations }) => ({
          provider,
          semanticFailures,
          unknownOperations,
        }),
      ),
    ).toEqual([{ provider: "sol", semanticFailures: 7, unknownOperations: 1 }]);
  });

  it("keeps terminal paid-operation debt visible without false degradation", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        providers: [
          {
            ...input().sol!,
            modelKey: "sol-5.6",
            provider: "sol",
            quarantinedOperations: 332,
            recoverableUnknownOperations: 0,
            unknownOperations: 332,
          },
        ],
        sol: null,
      }),
    );

    expect(document.state).toBe("healthy");
    expect(document.providers[0]).toMatchObject({
      quarantinedOperations: 332,
      recoverableUnknownOperations: 0,
      unknownOperations: 332,
    });
  });

  it("blocks on the actual artifact-store write fence", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    const { document } = await target.diagnostics.writeHealth(
      input({
        storage: {
          availableBytes: 900,
          headroomBytes: -100,
          minimumFreeBytes: 1_000,
          writable: false,
        },
      }),
    );

    expect(document.state).toBe("blocked");
    expect(evaluatePipelineHealth(document, 1_000)).toEqual({
      blocked: true,
      blockerCodes: ["ARTIFACT_STORE_DISK_PRESSURE"],
      exitCode: 2,
    });
  });

  it("rejects corrupt existing diagnostics instead of silently replacing them", async () => {
    const target = fixture(() => 1_000);
    await target.diagnostics.initialize();
    expect(existsSync(target.signalPath)).toBe(true);
    await expect(readPipelineHealth(target.signalPath)).rejects.toThrow();
    await expect(
      target.diagnostics.writeHealth(input({ configDigest: "b".repeat(64) })),
    ).rejects.toThrow("PIPELINE_DIAGNOSTIC_CONFIG_MISMATCH");
  });
});
