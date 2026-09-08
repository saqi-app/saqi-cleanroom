import { describe, expect, it } from "vitest";

import {
  buildProviderExecutionHealth,
  buildProviderExecutionHealthEntry,
  type ProviderExecutionHealthEntryInput,
  ProviderExecutionHealthSchema,
} from "../runtime/provider-execution-health.js";

const NOW = 1_000;

function input(
  change: Partial<ProviderExecutionHealthEntryInput> = {},
): ProviderExecutionHealthEntryInput {
  return {
    credentials: {
      accountEpoch: 1,
      change: "none",
      changedAt: null,
      errorCode: null,
      lastVerifiedAt: 900,
      materialEpoch: 1,
      retryAt: null,
      state: "ready",
    },
    debt: {
      quarantinedOperations: 0,
      recoverableUnknownOperations: 0,
      semanticFailures: 0,
    },
    enabled: true,
    gates: {
      authentication: { errorCode: null, retryAt: null, state: "ready" },
      budget: {
        budgetId: "00000000-0000-4000-8000-000000000001",
        maximumOperations: 12,
        remainingOperations: 9,
        reservedOperations: 3,
        state: "active",
      },
      operator: { globalPaused: false, paidWorkPaused: false },
      provider: { errorCode: null, retryAt: null, state: "ready" },
      quota: {
        errorCode: null,
        nextProbeAt: null,
        retryAt: null,
        state: "clear",
      },
      resources: { nextProbeAt: 2_000, reasons: [], state: "ready" },
      scheduler: {
        activeInvocations: 0,
        configuredConcurrency: 4,
        nextWakeAt: null,
        selectedConcurrency: 4,
        state: "ready",
      },
    },
    model: "gpt-5.6-sol",
    modelKey: "sol-5.6",
    progress: {
      accepted: 20,
      activeInvocations: 0,
      delayedWork: 1,
      lastAcceptedAt: 900,
      readyWork: 100,
      state: "recent",
      terminalWork: 3,
    },
    provider: "sol",
    throughput: null,
    sessions: {
      activeCurrentAccountEpoch: 0,
      activePreviousAccountEpoch: 0,
      activeUnattributed: 0,
    },
    ...change,
  };
}

describe("provider execution health", () => {
  it.each([
    [
      "disabled",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        enabled: false,
      }),
    ],
    [
      "operator_paused",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          operator: { globalPaused: true, paidWorkPaused: true },
        },
      }),
    ],
    [
      "paid_work_paused",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          operator: { globalPaused: false, paidWorkPaused: true },
        },
      }),
    ],
    [
      "budget_unarmed",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          budget: {
            budgetId: null,
            maximumOperations: 0,
            remainingOperations: 0,
            reservedOperations: 0,
            state: "unarmed" as const,
          },
        },
      }),
    ],
    [
      "budget_exhausted",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          budget: {
            ...value.gates.budget,
            remainingOperations: 0,
            reservedOperations: 12,
            state: "exhausted" as const,
          },
        },
      }),
    ],
    [
      "resource_wait",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          resources: {
            nextProbeAt: 2_000,
            reasons: ["MEMORY_PRESSURE" as const],
            state: "waiting" as const,
          },
        },
      }),
    ],
    [
      "auth_wait",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          authentication: {
            errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
            retryAt: 2_000,
            state: "waiting" as const,
          },
        },
      }),
    ],
    [
      "codex_quota_wait",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          quota: {
            errorCode: "CODEX_QUOTA_EXHAUSTED",
            nextProbeAt: 1_500,
            retryAt: 2_000,
            state: "waiting" as const,
          },
        },
      }),
    ],
    [
      "network_wait",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          provider: {
            errorCode: "ENRICHMENT_NETWORK_UNAVAILABLE",
            retryAt: 2_000,
            state: "network_wait" as const,
          },
        },
      }),
    ],
    [
      "rate_limit_wait",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          provider: {
            errorCode: "CODEX_RATE_LIMITED",
            retryAt: 2_000,
            state: "rate_limited" as const,
          },
        },
      }),
    ],
    [
      "provider_backoff",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          provider: {
            errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE",
            retryAt: 2_000,
            state: "backoff" as const,
          },
        },
      }),
    ],
    [
      "at_capacity",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          scheduler: {
            ...value.gates.scheduler,
            state: "at_capacity" as const,
          },
        },
      }),
    ],
    [
      "adaptive_capacity",
      (value: ProviderExecutionHealthEntryInput) => ({
        ...value,
        gates: {
          ...value.gates,
          scheduler: {
            ...value.gates.scheduler,
            selectedConcurrency: 1,
            state: "adaptive" as const,
          },
        },
      }),
    ],
  ] as const)("uses deterministic precedence for %s", (reason, change) => {
    expect(
      buildProviderExecutionHealthEntry(change(input())).admission
        .primaryReason,
    ).toBe(reason);
  });

  it("keeps active progress visible when future admission is budget-fenced", () => {
    const value = input();
    const entry = buildProviderExecutionHealthEntry({
      ...value,
      gates: {
        ...value.gates,
        budget: {
          ...value.gates.budget,
          remainingOperations: 0,
          reservedOperations: 12,
          state: "exhausted",
        },
        scheduler: {
          ...value.gates.scheduler,
          activeInvocations: 1,
          state: "at_capacity",
        },
      },
      progress: { ...value.progress, activeInvocations: 1, state: "active" },
      sessions: {
        activeCurrentAccountEpoch: 1,
        activePreviousAccountEpoch: 0,
        activeUnattributed: 0,
      },
    });
    expect(entry.progress.state).toBe("active");
    expect(entry.admission).toMatchObject({
      primaryReason: "budget_exhausted",
      state: "closed",
    });
  });

  it("keeps historical terminal debt out of admission classification", () => {
    expect(
      buildProviderExecutionHealthEntry({
        ...input(),
        debt: {
          quarantinedOperations: 733,
          recoverableUnknownOperations: 1,
          semanticFailures: 54,
        },
      }).admission,
    ).toMatchObject({ primaryReason: "ready", state: "open" });
  });

  it("rejects inconsistent budget and activity", () => {
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        gates: {
          ...input().gates,
          budget: { ...input().gates.budget, remainingOperations: 8 },
        },
      }),
    ).toThrow();
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        progress: { ...input().progress, activeInvocations: 1 },
      }),
    ).toThrow();
  });

  it.each([NOW - 1, NOW, NOW + 1])(
    "preserves pending recovery gates across retry deadline %s",
    (retryAt) => {
      const value = input();
      const cases: readonly [ProviderExecutionHealthEntryInput, string][] = [
        [
          {
            ...value,
            gates: {
              ...value.gates,
              authentication: {
                errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
                retryAt,
                state: "waiting",
              },
            },
          },
          "auth_wait",
        ],
        [
          {
            ...value,
            gates: {
              ...value.gates,
              quota: {
                errorCode: "CODEX_QUOTA_EXHAUSTED",
                nextProbeAt: retryAt,
                retryAt,
                state: "waiting",
              },
            },
          },
          "codex_quota_wait",
        ],
        ...(["network_wait", "rate_limited", "backoff"] as const).map(
          (state) =>
            [
              {
                ...value,
                gates: {
                  ...value.gates,
                  provider: {
                    errorCode: "ENRICHMENT_PROVIDER_UNAVAILABLE",
                    retryAt,
                    state,
                  },
                },
              },
              state === "rate_limited"
                ? "rate_limit_wait"
                : state === "backoff"
                  ? "provider_backoff"
                  : "network_wait",
            ] as const,
        ),
        ...(
          [
            "observation_wait",
            "material_verification",
            "account_verification",
            "account_switch_wait",
          ] as const
        ).map(
          (state) =>
            [
              {
                ...value,
                credentials: {
                  ...value.credentials,
                  change:
                    state === "material_verification"
                      ? ("material_refresh" as const)
                      : state === "observation_wait"
                        ? ("none" as const)
                        : ("account_switch" as const),
                  changedAt: NOW - 10,
                  errorCode: "ENRICHMENT_AUTH_REQUIRED",
                  retryAt,
                  state,
                },
                ...(state === "account_switch_wait"
                  ? {
                      gates: {
                        ...value.gates,
                        scheduler: {
                          ...value.gates.scheduler,
                          activeInvocations: 1,
                        },
                      },
                      progress: {
                        ...value.progress,
                        activeInvocations: 1,
                        state: "active" as const,
                      },
                      sessions: {
                        ...value.sessions,
                        activePreviousAccountEpoch: 1,
                      },
                    }
                  : {}),
              },
              "auth_wait",
            ] as const,
        ),
      ];
      for (const [provider, primaryReason] of cases) {
        const document = buildProviderExecutionHealth({
          configDigest: "a".repeat(64),
          observedAt: NOW,
          providers: [provider],
          runId: "run-1",
        });
        expect(document.providers[0]).toMatchObject({
          ...provider,
          admission: { primaryReason, retryAt, state: "waiting" },
        });
        expect(ProviderExecutionHealthSchema.parse(document)).toEqual(document);
        for (const [gates, reason] of [
          [
            {
              ...provider.gates,
              operator: { globalPaused: true, paidWorkPaused: true },
            },
            "operator_paused",
          ],
          [
            {
              ...provider.gates,
              budget: {
                ...provider.gates.budget,
                remainingOperations: 0,
                reservedOperations: 12,
                state: "exhausted" as const,
              },
            },
            "budget_exhausted",
          ],
        ] as const) {
          const fenced = buildProviderExecutionHealth({
            configDigest: "a".repeat(64),
            observedAt: NOW,
            providers: [{ ...provider, gates }],
            runId: "run-1",
          });
          expect(fenced.providers[0]?.admission).toMatchObject({
            primaryReason: reason,
            state: "closed",
          });
        }
      }
    },
  );

  it("publishes a same-account material refresh without account identity", () => {
    const document = buildProviderExecutionHealth({
      configDigest: "a".repeat(64),
      observedAt: NOW,
      providers: [
        {
          ...input(),
          credentials: {
            accountEpoch: 4,
            change: "material_refresh",
            changedAt: 950,
            errorCode: "ENRICHMENT_AUTH_REQUIRED",
            lastVerifiedAt: 900,
            materialEpoch: 9,
            retryAt: 1_100,
            state: "material_verification",
          },
        },
      ],
      runId: "run-1",
    });
    expect(document.providers[0]?.credentials).toEqual({
      accountEpoch: 4,
      change: "material_refresh",
      changedAt: 950,
      errorCode: "ENRICHMENT_AUTH_REQUIRED",
      lastVerifiedAt: 900,
      materialEpoch: 9,
      retryAt: 1_100,
      state: "material_verification",
    });
    expect(JSON.stringify(document)).not.toMatch(
      /accountGeneration|materialGeneration|accountIdentity/,
    );
  });

  it("attributes an account-switch barrier to previous-account sessions", () => {
    const value = input();
    const entry = buildProviderExecutionHealthEntry({
      ...value,
      credentials: {
        accountEpoch: 5,
        change: "account_switch",
        changedAt: 950,
        errorCode: "ENRICHMENT_AUTH_REQUIRED",
        lastVerifiedAt: 900,
        materialEpoch: 10,
        retryAt: 1_100,
        state: "account_switch_wait",
      },
      gates: {
        ...value.gates,
        scheduler: {
          ...value.gates.scheduler,
          activeInvocations: 2,
          state: "at_capacity",
        },
      },
      progress: { ...value.progress, activeInvocations: 2, state: "active" },
      sessions: {
        activeCurrentAccountEpoch: 0,
        activePreviousAccountEpoch: 2,
        activeUnattributed: 0,
      },
    });
    expect(entry.sessions).toEqual({
      activeCurrentAccountEpoch: 0,
      activePreviousAccountEpoch: 2,
      activeUnattributed: 0,
    });
  });

  it("rejects misleading session attribution and raw generations", () => {
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        sessions: {
          activeCurrentAccountEpoch: 1,
          activePreviousAccountEpoch: 0,
          activeUnattributed: 0,
        },
      }),
    ).toThrow();
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        sessions: {
          activeCurrentAccountEpoch: 0,
          activePreviousAccountEpoch: 1,
          activeUnattributed: 0,
        },
      }),
    ).toThrow();
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        credentials: {
          ...input().credentials,
          accountGeneration: "secret-generation",
        },
      } as ProviderExecutionHealthEntryInput),
    ).toThrow();
  });

  it("builds a strict, deterministic provider document", () => {
    const document = buildProviderExecutionHealth({
      configDigest: "a".repeat(64),
      observedAt: NOW,
      providers: [input()],
      runId: "run-1",
    });
    expect(ProviderExecutionHealthSchema.parse(document)).toEqual(document);
    expect(document).toMatchObject({
      schemaId: "saqi.provider-execution-health",
      schemaVersion: 1,
    });
    expect(() =>
      ProviderExecutionHealthSchema.parse({ ...document, legacyHealth: true }),
    ).toThrow();
  });

  it("preserves exact poem throughput while accepting legacy entries", () => {
    expect(buildProviderExecutionHealthEntry(input()).throughput).toBeNull();
    const throughput = {
      coverage: {
        backfillComplete: true,
        highWatermark: 42,
        state: "complete" as const,
      },
      generated: { last15m: 7, last1h: 15, last5m: 3, lastAt: 990 },
      published: { last15m: 5, last1h: 12, last5m: 2, lastAt: 980 },
      remaining: {
        active: 2,
        delayed: 3,
        endToEndPublication: 16,
        generatedAwaitingPublication: 10,
        generation: 6,
        ready: 1,
        terminalDead: 4,
      },
    };
    expect(
      buildProviderExecutionHealthEntry(input({ throughput })),
    ).toMatchObject({ throughput });
  });

  it("rejects inconsistent poem throughput accounting", () => {
    const throughput = {
      coverage: {
        backfillComplete: true,
        highWatermark: 42,
        state: "backfilling" as const,
      },
      generated: { last15m: 7, last1h: 15, last5m: 3, lastAt: 990 },
      published: { last15m: 5, last1h: 12, last5m: 2, lastAt: 980 },
      remaining: {
        active: 2,
        delayed: 3,
        endToEndPublication: 16,
        generatedAwaitingPublication: 10,
        generation: 6,
        ready: 1,
        terminalDead: 4,
      },
    };
    expect(() =>
      buildProviderExecutionHealthEntry(input({ throughput })),
    ).toThrow("Milestone coverage state is inconsistent");
    expect(() =>
      buildProviderExecutionHealthEntry({
        ...input(),
        throughput: {
          ...throughput,
          coverage: { ...throughput.coverage, state: "complete" },
          remaining: { ...throughput.remaining, generation: 5 },
        },
      }),
    ).toThrow("Generation remaining counts are inconsistent");
  });
});
