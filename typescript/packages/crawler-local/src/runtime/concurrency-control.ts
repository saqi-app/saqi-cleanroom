import { z } from "zod";

import { ConcurrencyStore } from "./concurrency-store.js";
import type {
  controlLaunchdService,
  LaunchdControlSnapshot,
} from "./launchd-control.js";
import { withLaunchdControlLock } from "./launchd-control.js";
import {
  loadScraperOperationConfig,
  operationConfigDigest,
} from "./operations-contract.js";

export const AllowedConcurrencySchema = z.literal([2, 4, 8, 16, 32, 128, 256]);

export type AllowedConcurrency = z.infer<typeof AllowedConcurrencySchema>;
export const ConcurrencyProviderSchema = z.literal("sol");
export type ConcurrencyProvider = z.infer<typeof ConcurrencyProviderSchema>;

export interface ConcurrencyUpdate {
  readonly configDigest: string;
  readonly configPath: string;
  readonly initialValue: AllowedConcurrency;
  readonly previousInitialValue: number;
  readonly previousValue: number;
  readonly provider: ConcurrencyProvider;
  readonly value: AllowedConcurrency;
}

export interface ConcurrencyApplication {
  readonly applicationState:
    "applied" | "pending_start" | "restart_failed" | "status_unavailable";
  readonly concurrency: ConcurrencyUpdate;
  readonly restartError: null | string;
  readonly service: LaunchdControlSnapshot | null;
}

type ServiceController = typeof controlLaunchdService;

/** Keeps the mutation lock until the running process acknowledges the digest. */
export async function applyRigConcurrency(
  configPath: string,
  provider: ConcurrencyProvider,
  value: AllowedConcurrency,
  label?: string,
  control?: ServiceController,
  initialValueInput: unknown = value,
): Promise<ConcurrencyApplication> {
  const initialValue = AllowedConcurrencySchema.parse(initialValueInput);
  if (initialValue > value)
    throw new Error("INITIAL_CONCURRENCY_EXCEEDS_TARGET");
  return withLaunchdControlLock(
    configPath,
    "restart",
    async (lockedControl) => {
      const serviceControl = control ?? lockedControl;
      const serviceOptions = label === undefined ? {} : { label };
      const before = await serviceControl({
        action: "status",
        configPath,
        ...serviceOptions,
      });
      const expectedConfiguration =
        await loadScraperOperationConfig(configPath);
      const expected = expectedConfiguration.configDigest;
      const concurrency = await updateRigConcurrency(
        configPath,
        provider,
        value,
        expected,
        initialValue,
      );
      if (before.actualState === "stopped" && !before.serviceEnabled) {
        const service = await serviceControl({
          action: "status",
          configPath,
          ...serviceOptions,
        });
        return {
          applicationState: "pending_start",
          concurrency,
          restartError: null,
          service,
        };
      }
      try {
        const service = await serviceControl({
          action: "restart",
          configPath,
          ...serviceOptions,
        });
        const applied =
          service.actualState === "running" &&
          service.loadedConfigDigest === concurrency.configDigest;
        return {
          applicationState: applied ? "applied" : "restart_failed",
          concurrency,
          restartError: applied ? null : "SERVICE_CONFIG_NOT_ACKNOWLEDGED",
          service,
        };
      } catch (error) {
        const restartError = boundedError(error);
        try {
          const service = await serviceControl({
            action: "status",
            configPath,
            ...serviceOptions,
          });
          return {
            applicationState: "restart_failed",
            concurrency,
            restartError,
            service,
          };
        } catch (statusError) {
          return {
            applicationState: "status_unavailable",
            concurrency,
            restartError: `${restartError}; status: ${boundedError(statusError)}`,
            service: null,
          };
        }
      }
    },
  );
}

/** Atomically applies one provider's initial and target concurrency. */
export async function updateRigConcurrency(
  configPathInput: string,
  providerInput: unknown,
  valueInput: unknown,
  expectedConfigDigest?: string,
  initialValueInput: unknown = valueInput,
): Promise<ConcurrencyUpdate> {
  const value = AllowedConcurrencySchema.parse(valueInput);
  const initialValue = AllowedConcurrencySchema.parse(initialValueInput);
  if (initialValue > value)
    throw new Error("INITIAL_CONCURRENCY_EXCEEDS_TARGET");
  const provider = ConcurrencyProviderSchema.parse(providerInput);
  const loaded = await loadScraperOperationConfig(configPathInput);
  const { configPath } = loaded;
  if (
    expectedConfigDigest !== undefined &&
    loaded.configDigest !== expectedConfigDigest
  ) {
    throw new Error("CONCURRENCY_CONFIG_CHANGED");
  }
  const authority = ConcurrencyStore.withDatabase(
    loaded.config.stateDirectory,
    false,
    (store) => store.initialize(loaded.config.sol),
  );
  if (
    authority.concurrency !== loaded.config.sol.concurrency ||
    authority.initialConcurrency !== loaded.config.sol.initialConcurrency
  ) {
    throw new Error("CONCURRENCY_CONFIG_CHANGED");
  }
  const previousValue = loaded.config[provider].concurrency;
  const previousInitialValue = loaded.config[provider].initialConcurrency;
  const nextProvider = {
    ...loaded.config[provider],
    concurrency: value,
    // Equal values select fixed operating mode; a lower initial value lets the
    // scheduler prove capacity before promoting toward the target ceiling.
    initialConcurrency: initialValue,
  };
  const next = {
    ...loaded.config,
    [provider]: nextProvider,
  };
  ConcurrencyStore.withDatabase(loaded.config.stateDirectory, false, (store) =>
    store.update(authority.revision, {
      concurrency: value,
      initialConcurrency: initialValue,
    }),
  );
  return {
    configDigest: operationConfigDigest(next),
    configPath,
    initialValue,
    previousInitialValue,
    previousValue,
    provider,
    value,
  };
}

function boundedError(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error);
  return value.replaceAll(/[\r\n]+/g, " ").slice(0, 1_000);
}
