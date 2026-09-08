import {
  ENRICHMENT_INPUT_SCHEMA_ID,
  ENRICHMENT_INPUT_SCHEMA_VERSION,
} from "@saqi/precedent-iso";
import { z } from "zod";

import type { Ledger } from "../persistence/ledger.js";
import { EnrichmentProviderSchema } from "../ports/provider-contract.js";
import { SOL_ENRICHMENT_WORK_KIND } from "./sol-coordinator.js";
import {
  ENRICHMENT_PROVIDER_SPECS,
  type EnrichmentProvider,
  SOL_PIPELINE_VERSION,
} from "./sol-runner.js";

const SOURCE_SCHEMA_VERSION = `${ENRICHMENT_INPUT_SCHEMA_ID}@${String(ENRICHMENT_INPUT_SCHEMA_VERSION)}`;
const DEFAULT_BATCH_SIZE = 500;
const MAXIMUM_RECONCILED_INPUTS = 250_000;
const LEGACY_SOL_PIPELINE_VERSION = "sol-enrichment-v1";

const BatchSizeSchema = z.number().int().min(1).max(1_000);
const MaximumInputsSchema = z
  .number()
  .int()
  .min(1)
  .max(MAXIMUM_RECONCILED_INPUTS);
const ReconciliationProvidersSchema = z
  .array(EnrichmentProviderSchema)
  .max(1)
  .refine((items) => new Set(items).size === items.length, {
    message: "Reconciliation providers must be unique",
  });

const ProfileSchema = z.strictObject({
  implementationVersion: z.string().trim().min(1).max(100),
  kind: z.string().trim().min(1).max(100),
  provider: EnrichmentProviderSchema,
});

const EnrichmentStartupReconciliationReportSchema = z.strictObject({
  duplicates: z.int().nonnegative(),
  profiles: z.array(
    ProfileSchema.extend({
      duplicates: z.int().nonnegative(),
      inserted: z.int().nonnegative(),
    }).strict(),
  ),
  scannedInputs: z.int().nonnegative(),
  schemaId: z.literal("saqi.enrichment-startup-reconciliation"),
  schemaVersion: z.literal(2),
  seeded: z.int().nonnegative(),
  sourceImplementationVersion: z.literal(SOL_PIPELINE_VERSION),
  sourceImplementationVersions: z.tuple([
    z.literal(LEGACY_SOL_PIPELINE_VERSION),
    z.literal(SOL_PIPELINE_VERSION),
  ]),
  sourceKind: z.literal(SOL_ENRICHMENT_WORK_KIND),
  sourceSchemaVersion: z.literal(SOURCE_SCHEMA_VERSION),
});

export type EnrichmentStartupReconciliationReport = z.infer<
  typeof EnrichmentStartupReconciliationReportSchema
>;

export function reconcileExistingEnrichmentInputs(options: {
  readonly batchSize?: number;
  readonly ledger: Ledger;
  readonly maximumInputs?: number;
  readonly providers: readonly EnrichmentProvider[];
}): EnrichmentStartupReconciliationReport {
  BatchSizeSchema.parse(options.batchSize ?? DEFAULT_BATCH_SIZE);
  MaximumInputsSchema.parse(options.maximumInputs ?? MAXIMUM_RECONCILED_INPUTS);
  const providers = ReconciliationProvidersSchema.parse(options.providers);
  if (providers.length === 0) {
    return EnrichmentStartupReconciliationReportSchema.parse({
      duplicates: 0,
      profiles: [],
      scannedInputs: 0,
      schemaId: "saqi.enrichment-startup-reconciliation",
      schemaVersion: 2,
      seeded: 0,
      sourceImplementationVersion: SOL_PIPELINE_VERSION,
      sourceImplementationVersions: [
        LEGACY_SOL_PIPELINE_VERSION,
        SOL_PIPELINE_VERSION,
      ],
      sourceKind: SOL_ENRICHMENT_WORK_KIND,
      sourceSchemaVersion: SOURCE_SCHEMA_VERSION,
    });
  }
  const profileCounts = new Map(
    providers.map((provider) => [provider, { duplicates: 0, inserted: 0 }]),
  );
  // A profile release is not authorization to translate the historical corpus
  // again. Existing definitions retain their immutable version and checkpoints;
  // only newly admitted inputs use the current profile.
  const profiles = providers.map((provider) => {
    const spec = ENRICHMENT_PROVIDER_SPECS[provider];
    const counts = profileCounts.get(provider);
    if (!counts) throw new Error("ENRICHMENT_PROFILE_COUNTS_MISSING");
    return {
      ...counts,
      implementationVersion: spec.pipelineVersion,
      kind: SOL_ENRICHMENT_WORK_KIND,
      provider,
    };
  });
  return EnrichmentStartupReconciliationReportSchema.parse({
    duplicates: profiles.reduce((sum, profile) => sum + profile.duplicates, 0),
    profiles,
    scannedInputs: 0,
    schemaId: "saqi.enrichment-startup-reconciliation",
    schemaVersion: 2,
    seeded: profiles.reduce((sum, profile) => sum + profile.inserted, 0),
    sourceImplementationVersion: SOL_PIPELINE_VERSION,
    sourceImplementationVersions: [
      LEGACY_SOL_PIPELINE_VERSION,
      SOL_PIPELINE_VERSION,
    ],
    sourceKind: SOL_ENRICHMENT_WORK_KIND,
    sourceSchemaVersion: SOURCE_SCHEMA_VERSION,
  });
}
