import { hash, timingSafeEqual } from "node:crypto";

import { z } from "zod";

import type { ProviderCredentialSnapshot } from "./provider-credential-generation.js";
import type { SolSchedulerStateStore } from "./sol-lane-scheduler.js";

const CredentialChangeSchema = z.enum([
  "none",
  "material_refresh",
  "account_switch",
]);
const CredentialObservationSchema = z.enum([
  "observed",
  "absent",
  "transient_unavailable",
]);
const StateSchema = z.strictObject({
  accountEpoch: z.int().nonnegative(),
  accountGeneration: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable(),
  change: CredentialChangeSchema,
  changedAt: z.int().nonnegative().nullable(),
  lastVerifiedAt: z.int().nonnegative().nullable(),
  materialEpoch: z.int().nonnegative(),
  materialGeneration: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable(),
  observation: CredentialObservationSchema,
  schemaVersion: z.literal(1),
});
const StateDigestSchema = z.string().regex(/^[a-f\d]{64}$/);

type State = z.infer<typeof StateSchema>;

export interface ProviderCredentialEpochSnapshot {
  readonly accountEpoch: number;
  readonly change: State["change"];
  readonly changedAt: null | number;
  readonly lastVerifiedAt: null | number;
  readonly materialEpoch: number;
  readonly observation: State["observation"];
}

export interface ProviderCredentialEpochPort {
  markVerified(
    expected: ProviderCredentialEpochSnapshot,
  ): Promise<ProviderCredentialEpochSnapshot>;
  observe(): Promise<ProviderCredentialEpochSnapshot>;
}

export class ProviderCredentialEpochTracker implements ProviderCredentialEpochPort {
  readonly #credentialSnapshot: () =>
    Promise<ProviderCredentialSnapshot> | ProviderCredentialSnapshot;
  readonly #now: () => number;
  readonly #stateKey: string;
  readonly #stateStore: SolSchedulerStateStore;
  #digest: null | string = null;
  #state: State = freshState();
  readonly #ready: Promise<void>;

  constructor(options: {
    readonly credentialSnapshot: () =>
      Promise<ProviderCredentialSnapshot> | ProviderCredentialSnapshot;
    readonly now: () => number;
    readonly stateKey: string;
    readonly stateStore: SolSchedulerStateStore;
  }) {
    this.#credentialSnapshot = options.credentialSnapshot;
    this.#now = options.now;
    this.#stateKey = options.stateKey;
    this.#stateStore = options.stateStore;
    this.#ready = this.#initialize();
  }

  async observe(): Promise<ProviderCredentialEpochSnapshot> {
    await this.#ready;
    const observed = await this.#credentialSnapshot();
    const now = this.#now();
    if (observed.state !== "observed") {
      if (this.#state.observation !== observed.state) {
        this.#state = { ...this.#state, observation: observed.state };
        await this.#persist();
      }
      return publicSnapshot(this.#state);
    }
    const accountChanged =
      this.#state.accountGeneration !== null &&
      this.#state.accountGeneration !== observed.accountGeneration;
    const materialChanged =
      this.#state.materialGeneration !== null &&
      this.#state.materialGeneration !== observed.materialGeneration;
    const firstObservation = this.#state.accountGeneration === null;
    if (
      firstObservation ||
      accountChanged ||
      materialChanged ||
      this.#state.observation !== "observed"
    ) {
      this.#state = {
        ...this.#state,
        accountEpoch:
          firstObservation || accountChanged
            ? this.#state.accountEpoch + 1
            : this.#state.accountEpoch,
        accountGeneration: observed.accountGeneration,
        change: accountChanged
          ? "account_switch"
          : materialChanged
            ? "material_refresh"
            : this.#state.change,
        changedAt:
          accountChanged || materialChanged ? now : this.#state.changedAt,
        materialEpoch:
          firstObservation || materialChanged || accountChanged
            ? this.#state.materialEpoch + 1
            : this.#state.materialEpoch,
        materialGeneration: observed.materialGeneration,
        observation: "observed",
      };
      await this.#persist();
    }
    return publicSnapshot(this.#state);
  }

  async markVerified(
    expected: ProviderCredentialEpochSnapshot,
  ): Promise<ProviderCredentialEpochSnapshot> {
    const current = await this.observe();
    if (
      current.observation !== "observed" ||
      current.accountEpoch !== expected.accountEpoch ||
      current.materialEpoch !== expected.materialEpoch
    )
      return current;
    this.#state = {
      ...this.#state,
      change: "none",
      lastVerifiedAt: this.#now(),
    };
    await this.#persist();
    return publicSnapshot(this.#state);
  }

  async #initialize(): Promise<void> {
    const stored = await this.#stateStore.loadSchedulerState(this.#stateKey);
    if (!stored) return;
    try {
      this.#state = parseStoredState(stored);
      this.#digest = stored.digest;
    } catch (error) {
      throw new Error("PROVIDER_CREDENTIAL_EPOCH_STATE_INVALID", {
        cause: error,
      });
    }
  }

  async #persist(): Promise<void> {
    const serialized = `${JSON.stringify(this.#state)}\n`;
    const saved = await this.#stateStore.saveSchedulerState(
      this.#stateKey,
      serialized,
      // The store verifies this digest against the serialized value.
      sha256(serialized),
      this.#digest,
      this.#now(),
    );
    if (!saved) {
      const winner = await this.#stateStore.loadSchedulerState(this.#stateKey);
      if (!winner) throw new Error("PROVIDER_CREDENTIAL_EPOCH_WRITE_CONFLICT");
      try {
        this.#state = parseStoredState(winner);
        this.#digest = winner.digest;
      } catch (error) {
        throw new Error("PROVIDER_CREDENTIAL_EPOCH_STATE_INVALID", {
          cause: error,
        });
      }
      return;
    }
    this.#digest = sha256(serialized);
  }
}

function freshState(): State {
  return {
    accountEpoch: 0,
    accountGeneration: null,
    change: "none",
    changedAt: null,
    lastVerifiedAt: null,
    materialEpoch: 0,
    materialGeneration: null,
    observation: "transient_unavailable",
    schemaVersion: 1,
  };
}

function publicSnapshot(state: State): ProviderCredentialEpochSnapshot {
  return {
    accountEpoch: state.accountEpoch,
    change: state.change,
    changedAt: state.changedAt,
    lastVerifiedAt: state.lastVerifiedAt,
    materialEpoch: state.materialEpoch,
    observation: state.observation,
  };
}

function sha256(value: string): string {
  return hash("sha256", value, "hex");
}

function parseStoredState(stored: {
  readonly digest: string;
  readonly serialized: string;
}): State {
  const expectedDigest = StateDigestSchema.safeParse(stored.digest);
  const actualDigest = sha256(stored.serialized);
  if (
    !expectedDigest.success ||
    !timingSafeEqual(
      Buffer.from(expectedDigest.data, "hex"),
      Buffer.from(actualDigest, "hex"),
    )
  )
    throw new Error("PROVIDER_CREDENTIAL_EPOCH_STATE_DIGEST_INVALID");
  return StateSchema.parse(JSON.parse(stored.serialized));
}
