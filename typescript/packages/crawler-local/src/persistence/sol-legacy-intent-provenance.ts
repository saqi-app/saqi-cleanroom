import { z } from "zod";

/** Immutable source revisions prove this mapping; it is not a current-profile default. */
export const LegacySolIntentProvenanceSchema = z.strictObject({
  strategy: z.literal("sol_only_intent_v1"),
  sourceRevision: z.literal("1a2f1c34d0f8df55db13df638cc03e0b5e2c0750"),
  mappingRevision: z.literal("58d65d3d89dec6d8d2d1cb457bdb1d702a99e7d3"),
  sourceShape: z.literal(
    "attemptId,inputHash,kind,model,pipelineVersion,reasoningEffort",
  ),
  provider: z.literal("sol"),
  modelKey: z.literal("sol-5.6"),
});
export type LegacySolIntentProvenance = z.infer<
  typeof LegacySolIntentProvenanceSchema
>;
