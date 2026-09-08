import { z } from "zod";

/** The sole executable enrichment provider. */
export const EnrichmentProviderSchema = z.literal("sol");

export type EnrichmentProvider = z.infer<typeof EnrichmentProviderSchema>;

/** Data-only compatibility for already-persisted historical provenance. */
export const HistoricalEnrichmentProviderSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(/^[a-z][a-z0-9._-]*$/u);
