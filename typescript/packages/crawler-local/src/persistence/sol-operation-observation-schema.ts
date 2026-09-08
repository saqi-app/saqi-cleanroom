import { z } from "zod";

import { SolCredentialObservationSchema } from "../enrichment/sol-credential-observation.js";
import { LegacySolIntentProvenanceSchema } from "./sol-legacy-intent-provenance.js";
import {
  LegacyArtifactReconciliationEvidenceSchema,
  LegacyContinuationEvidenceSchema,
  LegacySessionTimestampSchema,
} from "./sol-legacy-observation-schema.js";

const TimeSchema = z.int().nonnegative();
export const SolSessionCleanupSchema = z.strictObject({
  cleanedAt: TimeSchema,
  sessionId: z.uuid(),
  state: z.literal("removed_after_result_promotion"),
});
export const SolSessionCleanupPendingSchema = z.strictObject({
  code: z.literal("CODEX_SESSION_CLEANUP_PENDING"),
  detail: z.string().max(500),
  observedAt: TimeSchema,
  sessionId: z.uuid(),
});
export const SolReconciliationSchema = z.strictObject({
  checkedAt: TimeSchema,
  state: z.literal("unresolved_no_valid_retained_artifact"),
  strategy: z.literal("artifact_only_v2"),
});
const SolLegacyImportQuarantineSchema = z.strictObject({
  strategy: z.literal("legacy_rejection_quarantine_v1"),
  originalState: z.literal("known_rejection"),
  reasons: z
    .strictObject({
      missingExitCode: z.boolean(),
      signaled: z.boolean(),
      turnStarted: z.boolean(),
    })
    .refine(
      (reasons) =>
        reasons.missingExitCode || reasons.signaled || reasons.turnStarted,
    ),
});
export const SolOperationObservationsSchema = z.strictObject({
  legacyContinuationEvidence: LegacyContinuationEvidenceSchema.optional(),
  legacyArtifactReconciliationEvidence:
    LegacyArtifactReconciliationEvidenceSchema.optional(),
  legacySessionTimestamp: LegacySessionTimestampSchema.optional(),
  legacyIntentNormalization: LegacySolIntentProvenanceSchema.optional(),
  legacyImportQuarantine: SolLegacyImportQuarantineSchema.optional(),
  credential: SolCredentialObservationSchema.nullable().default(null),
  cleanup: SolSessionCleanupSchema.nullable().default(null),
  cleanupPending: SolSessionCleanupPendingSchema.nullable().default(null),
  reconciliation: SolReconciliationSchema.nullable().default(null),
});
export type SolOperationObservations = z.infer<
  typeof SolOperationObservationsSchema
>;
