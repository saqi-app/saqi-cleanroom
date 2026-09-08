import { hash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";

import { currentSource, type SourceConfiguration } from "@saqi/source-adapter";
import { z } from "zod";

import {
  D1CapacityPreflightInputSchema,
  SAQI_CURRENT_D1_BYTES,
  SAQI_CURRENT_POEMS,
} from "../publication/d1-capacity-preflight.js";
import { PublicationAuthConfigSchema } from "../publication/publication-auth-client.js";
import { ConcurrencyStore } from "./concurrency-store.js";

const OptionalPathSchema = z.string().min(1).max(4_096).nullable();
const BootstrapConfigSchema = z.looseObject({
  stateDirectory: z.string().min(1).max(4_096),
});
const ProviderConfigRecordSchema = z.record(z.string(), z.unknown());
const MAXIMUM_PROVIDER_ENRICHMENT_CONCURRENCY = 256;
function providerTuningShape(enabledByDefault: boolean) {
  // Keep this property order stable: operationConfigDigest intentionally hashes
  // JSON.stringify(config), so even a schema-only reorder would force a
  // needless runtime/config acknowledgement change.
  return {
    concurrency: z
      .int()
      .min(1)
      .max(MAXIMUM_PROVIDER_ENRICHMENT_CONCURRENCY)
      .default(2),
    enabled: z.boolean().default(enabledByDefault),
    initialConcurrency: z
      .int()
      .min(1)
      .max(MAXIMUM_PROVIDER_ENRICHMENT_CONCURRENCY)
      .default(1),
    minimumLaunchIntervalMs: z.int().min(0).max(60_000).default(250),
    maximumPerCyclePerLane: z.int().min(1).max(100).default(2),
    promotionSuccesses: z.int().min(2).max(10_000).default(8),
  };
}

const SolProviderConfigSchema = z
  .object(providerTuningShape(true))
  .strict()
  .prefault({});

const D1_DEFAULT = Object.freeze({
  actions: [
    {
      fixedQueries: 4,
      maximumRecords: 50,
      name: "stage_and_plan",
      queriesPerRecord: 7,
      rowsWrittenPerRecord: 8,
    },
    {
      fixedQueries: 3,
      maximumRecords: 50,
      name: "seal_and_promote",
      queriesPerRecord: 3,
      rowsWrittenPerRecord: 4,
    },
  ],
  backupProof: null,
  currentDatabaseBytes: SAQI_CURRENT_D1_BYTES,
  currentEnrichments: 0,
  currentPoems: SAQI_CURRENT_POEMS,
  estimates: {
    averageEnrichmentPayloadBytes: 12_000,
    enrichmentMetadataBytes: 1_200,
    indexExpansionBytes: 40_000_000,
    indexOverheadBasisPoints: 2_000,
    migrationFixedBytes: 2_000_000,
    reviewBytesPerEnrichment: 2_000,
  },
  observedAt: 0,
  safetyMarginBasisPoints: 2_000,
  targetEnrichments: SAQI_CURRENT_POEMS,
  tier: "unknown" as const,
});

const ConfigSchema = z
  .object({
    continuousFreePublication: z.boolean().default(false),
    collector: z
      .object({
        detailBurst: z.int().min(1).max(10_000).default(100),
        enabled: z.boolean().default(true),
        headless: z.boolean().default(false),
        maximumPerCycle: z.int().min(1).max(1_000).default(25),
        minimumSourceGapMs: z.int().min(13_000).max(3_600_000).default(13_000),
        recovery: z
          .object({
            enabled: z.boolean().default(false),
          })
          .strict()
          .prefault({}),
      })
      .strict()
      .prefault({}),
    baseline: z
      .object({
        authorsPath: OptionalPathSchema.default(null),
        batchSize: z.int().min(1).max(10_000).default(500),
        enabled: z.boolean().default(false),
        poemsPath: OptionalPathSchema.default(null),
      })
      .strict()
      .prefault({}),
    fanout: z
      .object({
        batchSize: z.int().min(1).max(1_000).default(250),
        enabled: z.boolean().default(false),
        refresh: z
          .object({
            cachePath: OptionalPathSchema.default(null),
            demandDriven: z.literal(true).default(true),
            enabled: z.boolean().default(false),
            endpoint: z.url().nullable().default(null),
            requestPath: OptionalPathSchema.default(null),
          })
          .strict()
          .prefault({}),
        resolutionFormat: z.literal("sqlite-v1").nullable().default(null),
        resolutionPath: OptionalPathSchema.default(null),
      })
      .strict()
      .prefault({}),
    localFanout: z
      .object({
        batchSize: z.int().min(1).max(1_000).default(250),
        enabled: z.boolean().default(false),
        resolutionFormat: z.literal("sqlite-v1").nullable().default(null),
        resolutionPath: OptionalPathSchema.default(null),
      })
      .strict()
      .prefault({}),
    inventory: z
      .object({
        enabled: z.boolean().default(false),
        productionAuthorsPath: OptionalPathSchema.default(null),
        refreshGeneration: z
          .string()
          .regex(/^[\w.-]{1,64}$/)
          .nullable()
          .default(null),
        statusPath: OptionalPathSchema.default(null),
      })
      .strict()
      .prefault({}),
    logging: z
      .object({
        maximumBytes: z
          .int()
          .min(65_536)
          .max(1_073_741_824)
          .default(25_165_824),
        retainedFiles: z.int().min(1).max(30).default(7),
      })
      .strict()
      .prefault({}),
    restart: z
      .object({
        errorBackoffMs: z.int().min(1_000).max(3_600_000).default(30_000),
        idlePollMs: z.int().min(1_000).max(300_000).default(30_000),
        shutdownGraceMs: z.int().min(1_000).max(3_600_000).default(120_000),
        startupTimeoutMs: z.int().min(30_000).max(3_600_000).default(900_000),
      })
      .strict()
      .prefault({}),
    startupReconciliation: z
      .object({
        enabled: z.boolean().default(true),
      })
      .strict()
      .prefault({}),
    resources: z
      .object({
        enabled: z.boolean().default(true),
        maximumProviderBufferedOutputBytes: z
          .int()
          .min(6_291_456)
          .max(17_179_869_184)
          .default(402_653_184),
        maximumProviderProcesses: z.int().min(1).max(768).default(64),
        maximumProviderReservedMemoryBytes: z
          .int()
          .min(268_435_456)
          .max(1_099_511_627_776)
          .default(17_179_869_184),
        maximumOpenFileDescriptors: z.int().min(64).max(1_000_000).default(768),
        maximumProcessRssBytes: z
          .int()
          .min(67_108_864)
          .max(1_099_511_627_776)
          .default(4_294_967_296),
        providerBufferedOutputReservationBytes: z
          .int()
          .min(1_048_576)
          .max(67_108_864)
          .default(6_291_456),
        providerProcessMemoryReservationBytes: z
          .int()
          .min(67_108_864)
          .max(17_179_869_184)
          .default(268_435_456),
        minimumFreeDiskBytes: z
          .int()
          .min(0)
          .max(1_099_511_627_776)
          .default(2_147_483_648),
        minimumFreeMemoryBytes: z
          .int()
          .min(0)
          .max(1_099_511_627_776)
          // os.freemem excludes reclaimable cache on several platforms; this
          // is an emergency floor, while process RSS and descriptor limits
          // provide earlier deterministic admission controls.
          .default(268_435_456),
        minimumAvailableMemoryBasisPoints: z
          .int()
          .min(0)
          .max(5_000)
          .default(1_000),
        probeIntervalMs: z.int().min(1_000).max(300_000).default(5_000),
        resumeHysteresisBasisPoints: z.int().min(0).max(10_000).default(1_250),
      })
      .strict()
      .superRefine((resources, context) => {
        if (
          resources.providerBufferedOutputReservationBytes >
          resources.maximumProviderBufferedOutputBytes
        ) {
          context.addIssue({
            code: "custom",
            message:
              "Provider output reservation must not exceed its aggregate budget",
          });
        }
        if (
          resources.providerProcessMemoryReservationBytes >
          resources.maximumProviderReservedMemoryBytes
        ) {
          context.addIssue({
            code: "custom",
            message:
              "Provider memory reservation must not exceed its aggregate budget",
          });
        }
      })
      .prefault({}),
    publication: z
      .object({
        auth: PublicationAuthConfigSchema.prefault({
          allowedOrigins: ["https://ops.saqi.app"],
          identityEndpoint: null,
          mode: "service_token",
        }),
        allowInsecureLocalhost: z.boolean().default(false),
        d1: D1CapacityPreflightInputSchema.prefault(D1_DEFAULT),
        enabled: z.boolean().default(false),
        endpoint: z.url().nullable().default(null),
        maximumPerCycle: z.int().min(1).max(1_000).default(10),
      })
      .strict()
      .prefault({}),
    retention: z
      .object({
        apply: z.boolean().default(false),
        enabled: z.boolean().default(true),
        maximumAttemptsPerCycle: z.int().min(1).max(1_000).default(25),
        maximumInputBytesPerCycle: z
          .int()
          .min(65_536)
          .max(1_073_741_824)
          .default(67_108_864),
        minimumFreeBytes: z
          .int()
          .min(0)
          .max(1_099_511_627_776)
          .default(2_147_483_648),
        purgeArchivedDiagnostics: z.boolean().default(false),
        safetyAgeMs: z
          .int()
          .min(86_400_000)
          .max(31_536_000_000)
          .default(604_800_000),
      })
      .strict()
      .prefault({}),
    schemaId: z.literal("saqi.unified-rig").default("saqi.unified-rig"),
    schemaVersion: z.literal(1),
    sol: SolProviderConfigSchema,
    stateDirectory: z.string().min(1).max(4_096),
  })
  .strict()
  .refine(
    ({ collector, fanout, localFanout, publication, sol }) =>
      collector.enabled ||
      sol.enabled ||
      fanout.enabled ||
      localFanout.enabled ||
      publication.enabled,
    {
      message: "At least one processing lane must be enabled",
    },
  )
  .superRefine((configuration, context) => {
    if (configuration.sol.initialConcurrency > configuration.sol.concurrency) {
      context.addIssue({
        code: "custom",
        message: "sol initialConcurrency must not exceed concurrency",
      });
    }
    // Provider concurrency is a desired ceiling, not a promise to spawn every
    // descendant at once. ProviderHostAdmission applies the process, memory,
    // and buffered-output budgets dynamically. Rejecting a high ceiling here
    // would make the persisted control plane disagree with the runtime and
    // prevent safe adaptive ramping on smaller hosts.
    if (
      configuration.baseline.enabled &&
      (!configuration.baseline.authorsPath || !configuration.baseline.poemsPath)
    ) {
      context.addIssue({
        code: "custom",
        message: "Enabled baseline requires authorsPath and poemsPath",
      });
    }
    if (
      configuration.publication.enabled &&
      !configuration.publication.endpoint
    ) {
      context.addIssue({
        code: "custom",
        message: "Enabled publication requires endpoint",
      });
    }
    if (configuration.publication.endpoint) {
      const endpointOrigin = new URL(configuration.publication.endpoint).origin;
      if (
        !configuration.publication.auth.allowedOrigins.includes(endpointOrigin)
      ) {
        context.addIssue({
          code: "custom",
          message: "Publication endpoint origin is not auth-allowlisted",
        });
      }
    }
    if (
      configuration.fanout.enabled &&
      (!configuration.publication.enabled ||
        (!configuration.fanout.refresh.enabled &&
          !configuration.fanout.resolutionPath))
    ) {
      context.addIssue({
        code: "custom",
        message:
          "Enabled fanout requires publication and either demand refresh or a resolutionPath",
      });
    }
    if (
      configuration.fanout.refresh.enabled &&
      (!configuration.publication.enabled ||
        !configuration.fanout.refresh.cachePath)
    ) {
      context.addIssue({
        code: "custom",
        message:
          "Enabled resolution refresh requires publication and a demand-driven cachePath",
      });
    }
    const refreshEndpoint = configuration.fanout.refresh.endpoint;
    if (
      refreshEndpoint &&
      !configuration.publication.auth.allowedOrigins.includes(
        new URL(refreshEndpoint).origin,
      )
    ) {
      context.addIssue({
        code: "custom",
        message: "Resolution refresh endpoint origin is not auth-allowlisted",
      });
    }
    const staticResolutionLanes = [
      configuration.localFanout,
      ...(configuration.fanout.refresh.enabled ? [] : [configuration.fanout]),
    ];
    for (const lane of staticResolutionLanes) {
      if (!lane.enabled) continue;
      if (lane.resolutionPath?.toLowerCase().endsWith(".json")) {
        context.addIssue({
          code: "custom",
          message:
            "RESOLUTION_JSON_V1_RETIRED: run export-resolution to create a SQLite snapshot and set resolutionFormat to sqlite-v1",
        });
      } else if (lane.resolutionFormat !== "sqlite-v1") {
        context.addIssue({
          code: "custom",
          message:
            "RESOLUTION_FORMAT_REQUIRED: enabled resolution lanes require resolutionFormat sqlite-v1",
        });
      }
    }
    if (
      configuration.localFanout.enabled &&
      !configuration.localFanout.resolutionPath
    ) {
      context.addIssue({
        code: "custom",
        message: "Enabled localFanout requires a resolutionPath",
      });
    }
    if (
      configuration.inventory.enabled &&
      (!configuration.collector.enabled ||
        !configuration.inventory.productionAuthorsPath ||
        !configuration.inventory.refreshGeneration)
    ) {
      context.addIssue({
        code: "custom",
        message:
          "Enabled inventory requires collector, productionAuthorsPath, and refreshGeneration",
      });
    }
    if (
      configuration.collector.recovery.enabled &&
      !configuration.collector.enabled
    ) {
      context.addIssue({
        code: "custom",
        message: "Enabled collector recovery requires collector",
      });
    }
    if (
      configuration.collector.recovery.enabled &&
      configuration.inventory.enabled
    ) {
      context.addIssue({
        code: "custom",
        message: "Collector recovery cannot run with author inventory",
      });
    }
    if (configuration.continuousFreePublication) {
      if (
        configuration.collector.enabled ||
        configuration.sol.enabled ||
        configuration.localFanout.enabled ||
        configuration.baseline.enabled ||
        configuration.inventory.enabled ||
        configuration.startupReconciliation.enabled
      ) {
        context.addIssue({
          code: "custom",
          message:
            "Continuous free publication forbids collection, provider, local fanout, baseline, inventory, and startup reconciliation lanes",
        });
      }
      if (
        !configuration.fanout.enabled ||
        !configuration.fanout.refresh.enabled ||
        !configuration.publication.enabled
      ) {
        context.addIssue({
          code: "custom",
          message:
            "Continuous free publication requires fanout, demand-driven resolution refresh, and publication",
        });
      }
      if (
        configuration.fanout.batchSize > 25 ||
        configuration.publication.maximumPerCycle > 50 ||
        configuration.retention.maximumAttemptsPerCycle > 25 ||
        configuration.restart.idlePollMs < 30_000
      ) {
        context.addIssue({
          code: "custom",
          message:
            "Continuous free publication requires fanout batchSize <= 25, publication maximumPerCycle <= 50, retention maximumAttemptsPerCycle <= 25, and idlePollMs >= 30000",
        });
      }
    }
  });

export const ScraperOperationConfigSchema = ConfigSchema.transform(
  (configuration) => ({
    ...configuration,
    stateDirectory: resolve(configuration.stateDirectory),
  }),
).refine((configuration) => isAbsolute(configuration.stateDirectory), {
  message: "stateDirectory must resolve to an absolute path",
});

export type ScraperOperationConfig = z.infer<
  typeof ScraperOperationConfigSchema
>;

export function parseScraperOperationConfig(
  input: unknown,
  baseDirectory = process.cwd(),
): ScraperOperationConfig {
  const parsed = ConfigSchema.parse(input);
  return ScraperOperationConfigSchema.parse({
    ...parsed,
    baseline: {
      ...parsed.baseline,
      authorsPath:
        parsed.baseline.authorsPath === null
          ? null
          : resolve(baseDirectory, parsed.baseline.authorsPath),
      poemsPath:
        parsed.baseline.poemsPath === null
          ? null
          : resolve(baseDirectory, parsed.baseline.poemsPath),
    },
    fanout: {
      ...parsed.fanout,
      refresh: {
        ...parsed.fanout.refresh,
        cachePath:
          parsed.fanout.refresh.cachePath === null
            ? null
            : resolve(baseDirectory, parsed.fanout.refresh.cachePath),
        requestPath:
          parsed.fanout.refresh.requestPath === null
            ? null
            : resolve(baseDirectory, parsed.fanout.refresh.requestPath),
      },
      resolutionPath:
        parsed.fanout.resolutionPath === null
          ? null
          : resolve(baseDirectory, parsed.fanout.resolutionPath),
    },
    localFanout: {
      ...parsed.localFanout,
      resolutionPath:
        parsed.localFanout.resolutionPath === null
          ? null
          : resolve(baseDirectory, parsed.localFanout.resolutionPath),
    },
    inventory: {
      ...parsed.inventory,
      productionAuthorsPath:
        parsed.inventory.productionAuthorsPath === null
          ? null
          : resolve(baseDirectory, parsed.inventory.productionAuthorsPath),
      statusPath:
        parsed.inventory.statusPath === null
          ? null
          : resolve(baseDirectory, parsed.inventory.statusPath),
    },
    publication: {
      ...parsed.publication,
      auth: {
        ...parsed.publication.auth,
        identityEndpoint:
          parsed.publication.enabled &&
          parsed.publication.endpoint !== null &&
          parsed.publication.auth.identityEndpoint === null
            ? `${new URL(parsed.publication.endpoint).origin}/api/corpus-import`
            : parsed.publication.auth.identityEndpoint,
      },
    },
    stateDirectory: resolve(baseDirectory, parsed.stateDirectory),
  });
}

export interface LoadedOperationConfig {
  readonly config: ScraperOperationConfig;
  readonly configDigest: string;
  readonly configPath: string;
}

export async function loadScraperOperationConfig(
  path: string,
  source?: SourceConfiguration,
): Promise<LoadedOperationConfig> {
  const configPath = resolve(path);
  const input: unknown = JSON.parse(await readFile(configPath, "utf8"));
  return loadScraperOperationConfigFromInput(input, configPath, source);
}

export function loadScraperOperationConfigFromInput(
  input: unknown,
  path: string,
  source?: SourceConfiguration,
): LoadedOperationConfig {
  const configPath = resolve(path);
  const bootstrap = BootstrapConfigSchema.parse(input);
  const desired = ConcurrencyStore.inspect(
    resolve(dirname(configPath), bootstrap.stateDirectory),
  );
  const effectiveInput =
    desired === null
      ? bootstrap
      : {
          ...bootstrap,
          sol: {
            ...ProviderConfigRecordSchema.parse(bootstrap["sol"] ?? {}),
            concurrency: desired.concurrency,
            initialConcurrency: desired.initialConcurrency,
          },
        };
  const config = parseScraperOperationConfig(
    effectiveInput,
    dirname(configPath),
  );
  return {
    config,
    configDigest:
      source === undefined
        ? operationConfigDigest(config)
        : operationConfigDigestForSource(config, source),
    configPath,
  };
}

export function operationConfigDigest(config: ScraperOperationConfig): string {
  return operationConfigDigestForSource(config, currentSource());
}

/** Binds the normalized source namespace to every runtime acknowledgement. */
export function operationConfigDigestForSource(
  config: ScraperOperationConfig,
  source: SourceConfiguration,
): string {
  return hash("sha256", JSON.stringify({ config, source }), "hex");
}
