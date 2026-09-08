import { z } from "zod";

import {
  type LegacySolIntentProvenance,
  LegacySolIntentProvenanceSchema,
} from "./sol-legacy-intent-provenance.js";
import { LegacySolIntentSchema } from "./sol-operation-import-schema.js";
import { canonicalJson, sha256 } from "./work-key.js";

const OperationKeySchema = z.string().regex(/^[a-f\d]{64}$/);
const SolOnlyIntentV1Schema = LegacySolIntentSchema.omit({
  provider: true,
  modelKey: true,
}).extend({
  model: z.literal("gpt-5.6-sol"),
  pipelineVersion: z.literal("sol-enrichment-v1"),
  reasoningEffort: z.literal("high"),
});
const Provenance = LegacySolIntentProvenanceSchema.parse({
  strategy: "sol_only_intent_v1",
  sourceRevision: "1a2f1c34d0f8df55db13df638cc03e0b5e2c0750",
  mappingRevision: "58d65d3d89dec6d8d2d1cb457bdb1d702a99e7d3",
  sourceShape: "attemptId,inputHash,kind,model,pipelineVersion,reasoningEffort",
  provider: "sol",
  modelKey: "sol-5.6",
});

export interface ParsedLegacySolIntent {
  readonly intent: z.infer<typeof LegacySolIntentSchema>;
  readonly provenance?: LegacySolIntentProvenance;
}

/** Accepts only the exact pre-multiprovider shape and its original operation key. */
export function parseLegacySolIntent(
  raw: unknown,
  operationKey: string,
): ParsedLegacySolIntent {
  OperationKeySchema.parse(operationKey);
  const modern = LegacySolIntentSchema.safeParse(raw);
  if (modern.success) return { intent: modern.data };
  const legacy = SolOnlyIntentV1Schema.parse(raw);
  const expectedKey = sha256(
    canonicalJson({ inputHash: legacy.inputHash, kind: legacy.kind }),
  );
  if (operationKey !== expectedKey)
    throw new Error("SOL_IMPORT_LEGACY_INTENT_KEY_MISMATCH");
  return {
    intent: LegacySolIntentSchema.parse({
      ...legacy,
      provider: Provenance.provider,
      modelKey: Provenance.modelKey,
    }),
    provenance: { ...Provenance },
  };
}
