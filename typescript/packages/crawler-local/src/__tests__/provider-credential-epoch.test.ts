import { hash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { ProviderCredentialEpochTracker } from "../enrichment/provider-credential-epoch.js";
import type { ProviderCredentialSnapshot } from "../enrichment/provider-credential-generation.js";
import {
  QuotaAwareSolLaneScheduler,
  type SolSchedulerStateStore,
  type SolTaskOutcome,
} from "../enrichment/sol-lane-scheduler.js";
import { Ledger } from "../persistence/ledger.js";

const OPEN = {
  circuitOpen: false,
  diskWritable: true,
  paused: false,
  quotaWaitUntil: null,
} as const;

function observed(
  account: string,
  material: string,
): ProviderCredentialSnapshot {
  return {
    accountGeneration: account.repeat(64),
    materialGeneration: material.repeat(64),
    state: "observed",
  };
}

function memoryStateStore(
  initial: {
    readonly digest: string;
    readonly serialized: string;
  } | null = null,
) {
  let stored = initial;
  let writes = 0;
  const port: SolSchedulerStateStore = {
    loadSchedulerState: () => stored,
    saveSchedulerState: (
      _stateKey,
      serialized,
      stateDigest,
      expectedDigest,
    ) => {
      if ((stored?.digest ?? null) !== expectedDigest) return false;
      stored = { digest: stateDigest, serialized };
      writes += 1;
      return true;
    },
  };
  return {
    port,
    replace: (replacement: typeof stored) => {
      stored = replacement;
    },
    snapshot: () => stored,
    writes: () => writes,
  };
}

function digest(serialized: string): string {
  return hash("sha256", serialized, "hex");
}

describe("provider credential epochs", () => {
  it.each([true, false])(
    "fences old material completions with intervening snapshot=%s",
    async (refreshFirst) => {
      const ledger = Ledger.initialize(":memory:");
      let credential = observed("a", "b");
      let now = 100;
      const scheduler = new QuotaAwareSolLaneScheduler({
        ceiling: 2,
        initialConcurrency: 2,
        configDigest: "f".repeat(64),
        credentialGeneration: () => "a".repeat(64),
        credentialSnapshot: () => credential,
        now: () => now,
        stateKey: "material-race",
        legacyCurrentStatePath: "unused",
        stateStore: ledger,
      });
      const success = await scheduler.acquire(OPEN);
      const failure = await scheduler.acquire(OPEN);
      if (!success || !failure) throw new Error("Expected two permits");
      credential = observed("a", "c");
      now = 200;
      if (refreshFirst) await scheduler.snapshot(OPEN);
      await success.complete({ kind: "success" });
      await expect(
        failure.complete({
          kind: "provider_wait",
          errorCode: "CODEX_OAUTH_TOKEN_REVOKED",
        }),
      ).resolves.toMatchObject({ accepted: false });
      await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
        credentialMaterialEpoch: 2,
        credentialChange: "material_refresh",
        providerErrorCode: null,
      });
      const current = await scheduler.acquire(OPEN);
      if (!current) throw new Error("Expected refreshed permit");
      await current.complete({
        kind: "provider_wait",
        errorCode: "CODEX_OAUTH_TOKEN_REVOKED",
      });
      await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
        providerErrorCode: "CODEX_OAUTH_TOKEN_REVOKED",
        blockReason: "provider_wait",
      });
      ledger.close();
    },
  );

  it.each<SolTaskOutcome>([
    { kind: "idle" },
    { kind: "budget_exhausted" },
    { kind: "error" },
    { kind: "task_failure" },
  ])("does not verify credentials from $kind", async (outcome) => {
    const ledger = Ledger.initialize(":memory:");
    let credential = observed("a", "b");
    const scheduler = new QuotaAwareSolLaneScheduler({
      ceiling: 1,
      initialConcurrency: 1,
      configDigest: "f".repeat(64),
      credentialGeneration: () => "a".repeat(64),
      credentialSnapshot: () => credential,
      now: () => 100,
      stateKey: "non-verification",
      legacyCurrentStatePath: "unused",
      stateStore: ledger,
    });
    await scheduler.snapshot(OPEN);
    credential = observed("a", "c");
    const permit = await scheduler.acquire(OPEN);
    if (!permit) throw new Error("Expected permit");
    await permit.complete(outcome);
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      credentialChange: "material_refresh",
    });
    ledger.close();
  });

  it("rejects verification of material changed since admission without a prior observation", async () => {
    let credential = observed("a", "b");
    const tracker = new ProviderCredentialEpochTracker({
      credentialSnapshot: () => credential,
      now: () => 100,
      stateKey: "verify-fence",
      stateStore: memoryStateStore().port,
    });
    const admitted = await tracker.observe();
    credential = observed("a", "c");
    await expect(tracker.markVerified(admitted)).resolves.toMatchObject({
      materialEpoch: 2,
      change: "material_refresh",
      lastVerifiedAt: null,
    });
    await expect(
      tracker.markVerified(await tracker.observe()),
    ).resolves.toMatchObject({ change: "none", lastVerifiedAt: 100 });
  });

  it("preserves a newer persisted epoch when stale verification loses CAS", async () => {
    const store = memoryStateStore();
    const create = (material: string) =>
      new ProviderCredentialEpochTracker({
        credentialSnapshot: () => observed("a", material),
        now: () => 100,
        stateKey: "verification-conflict",
        stateStore: store.port,
      });
    const stale = create("b");
    const admitted = await stale.observe();
    await create("c").observe();
    await expect(stale.markVerified(admitted)).resolves.toMatchObject({
      materialEpoch: 2,
      change: "material_refresh",
      lastVerifiedAt: null,
    });
    await expect(create("c").observe()).resolves.toMatchObject({
      materialEpoch: 2,
      change: "material_refresh",
      lastVerifiedAt: null,
    });
  });

  it("distinguishes a material refresh from an account switch durably", async () => {
    const ledger = Ledger.initialize(":memory:");
    let now = 100;
    let credential: ProviderCredentialSnapshot = observed("a", "b");
    const create = () =>
      new ProviderCredentialEpochTracker({
        credentialSnapshot: () => credential,
        now: () => now,
        stateKey: "credential-test",
        stateStore: ledger,
      });
    const tracker = create();
    await expect(tracker.observe()).resolves.toMatchObject({
      accountEpoch: 1,
      change: "none",
      materialEpoch: 1,
      observation: "observed",
    });

    now = 200;
    credential = observed("a", "c");
    await expect(tracker.observe()).resolves.toMatchObject({
      accountEpoch: 1,
      change: "material_refresh",
      changedAt: 200,
      materialEpoch: 2,
    });
    await tracker.markVerified(await tracker.observe());

    now = 300;
    credential = observed("d", "e");
    const switched = await tracker.observe();
    expect(switched).toMatchObject({
      accountEpoch: 2,
      change: "account_switch",
      changedAt: 300,
      materialEpoch: 3,
    });
    expect(switched).not.toHaveProperty("accountGeneration");
    expect(switched).not.toHaveProperty("materialGeneration");

    const restarted = create();
    await expect(restarted.observe()).resolves.toEqual(switched);
    ledger.close();
  });

  it("preserves epochs across transient and absent observations", async () => {
    const ledger = Ledger.initialize(":memory:");
    let credential: ProviderCredentialSnapshot = observed("a", "b");
    const tracker = new ProviderCredentialEpochTracker({
      credentialSnapshot: () => credential,
      now: () => 100,
      stateKey: "fallback-test",
      stateStore: ledger,
    });
    await tracker.observe();
    credential = { state: "transient_unavailable" };
    await expect(tracker.observe()).resolves.toMatchObject({
      accountEpoch: 1,
      materialEpoch: 1,
      observation: "transient_unavailable",
    });
    credential = { state: "absent" };
    await expect(tracker.observe()).resolves.toMatchObject({
      accountEpoch: 1,
      materialEpoch: 1,
      observation: "absent",
    });
    ledger.close();
  });

  it("accepts a valid digest and restores the exact epoch state", async () => {
    const store = memoryStateStore();
    let credential: ProviderCredentialSnapshot = observed("a", "b");
    const create = () =>
      new ProviderCredentialEpochTracker({
        credentialSnapshot: () => credential,
        now: () => 100,
        stateKey: "valid-restart",
        stateStore: store.port,
      });
    const first = create();
    await first.observe();
    credential = observed("a", "c");
    const expected = await first.observe();
    const persisted = store.snapshot();
    expect(persisted).not.toBeNull();
    expect(persisted?.digest).toBe(digest(persisted?.serialized ?? ""));

    await expect(create().observe()).resolves.toEqual(expected);
  });

  it("fails closed without overwriting malformed state with a valid digest", async () => {
    const serialized = "{\n";
    const store = memoryStateStore({
      digest: digest(serialized),
      serialized,
    });
    const tracker = new ProviderCredentialEpochTracker({
      credentialSnapshot: () => observed("a", "b"),
      now: () => 100,
      stateKey: "corrupt-state",
      stateStore: store.port,
    });

    await expect(tracker.observe()).rejects.toThrow(
      "PROVIDER_CREDENTIAL_EPOCH_STATE_INVALID",
    );
    expect(store.writes()).toBe(0);
    expect(store.snapshot()).toEqual({
      digest: digest(serialized),
      serialized,
    });
  });

  it("rejects restart state whose serialized value was tampered after hashing", async () => {
    const store = memoryStateStore();
    const create = () =>
      new ProviderCredentialEpochTracker({
        credentialSnapshot: () => observed("a", "b"),
        now: () => 100,
        stateKey: "tampered-restart",
        stateStore: store.port,
      });
    await create().observe();
    const valid = store.snapshot();
    if (!valid) throw new Error("Expected persisted credential epoch state");
    store.replace({
      digest: valid.digest,
      serialized: `${valid.serialized} `,
    });

    await expect(create().observe()).rejects.toThrow(
      "PROVIDER_CREDENTIAL_EPOCH_STATE_INVALID",
    );
    expect(store.writes()).toBe(1);
  });

  it("keeps an in-flight old-account permit attributed to its epoch", async () => {
    const ledger = Ledger.initialize(":memory:");
    let account = "a";
    let credential: ProviderCredentialSnapshot = observed(account, "b");
    const scheduler = new QuotaAwareSolLaneScheduler({
      ceiling: 2,
      configDigest: "f".repeat(64),
      credentialGeneration: () => account.repeat(64),
      credentialSnapshot: () => credential,
      initialConcurrency: 2,
      now: () => 100,
      stateKey: "provider-test",
      legacyCurrentStatePath: "unused",
      stateStore: ledger,
    });
    await scheduler.snapshot(OPEN);
    const permit = await scheduler.acquire(OPEN);
    if (!permit) throw new Error("Expected provider permit");
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      activeCurrentAccountEpoch: 1,
      activePreviousAccountEpoch: 0,
    });

    account = "c";
    credential = observed(account, "d");
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      activeCurrentAccountEpoch: 0,
      activePreviousAccountEpoch: 1,
      credentialAccountEpoch: 2,
      credentialChange: "account_switch",
    });
    await permit.complete({ kind: "success" }, 101);
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      activeCurrentAccountEpoch: 0,
      activePreviousAccountEpoch: 0,
      credentialChange: "account_switch",
    });
    ledger.close();
  });

  it("clears an authentication wait on same-account material refresh", async () => {
    const ledger = Ledger.initialize(":memory:");
    let now = 100;
    const account = "a";
    let credential: ProviderCredentialSnapshot = observed(account, "b");
    const createScheduler = () =>
      new QuotaAwareSolLaneScheduler({
        ceiling: 1,
        configDigest: "e".repeat(64),
        credentialGeneration: () => account.repeat(64),
        credentialSnapshot: () => credential,
        initialConcurrency: 1,
        now: () => now,
        stateKey: "material-healing-test",
        legacyCurrentStatePath: "unused",
        stateStore: ledger,
      });
    const scheduler = createScheduler();
    await scheduler.snapshot(OPEN);
    const permit = await scheduler.acquire(OPEN);
    if (!permit) throw new Error("Expected provider permit");
    now = 101;
    await permit.complete(
      {
        errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
        kind: "provider_wait",
        retryAt: 1_000,
      },
      now,
    );
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
    });
    const restarted = createScheduler();
    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
    });

    now = 200;
    credential = observed(account, "c");
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      credentialAccountEpoch: 1,
      credentialChange: "material_refresh",
      credentialMaterialEpoch: 2,
      providerErrorCode: null,
    });
    const healed = await scheduler.snapshot(OPEN);
    expect(healed.blockReason).not.toBe("provider_wait");
    const refreshedPermit = await scheduler.acquire(OPEN);
    if (!refreshedPermit) throw new Error("Expected refreshed provider permit");
    now = 201;
    await refreshedPermit.complete({
      errorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
      kind: "provider_wait",
      retryAt: 1_000,
    });
    await expect(scheduler.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
    });
    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
    });
    now = 300;
    await expect(createScheduler().snapshot(OPEN)).resolves.toMatchObject({
      blockReason: "provider_wait",
      providerErrorCode: "CODEX_CHATGPT_AUTH_REQUIRED",
    });
    credential = observed(account, "d");
    await expect(restarted.snapshot(OPEN)).resolves.toMatchObject({
      providerErrorCode: null,
      credentialMaterialEpoch: 3,
    });
    ledger.close();
  });
});
