import { z } from "zod";

import { SolOperationObservationsSchema } from "./sol-operation-observation-schema.js";
import { SolOperationClaimSchema } from "./sol-operation-store.js";

export {
  type SolImportReceipt,
  SolImportReceiptSchema,
} from "./sol-operation-receipt-schema.js";

const TimeSchema = z.int().nonnegative();
const LegacyTerminalStateSchema = z.enum([
  "unknown",
  "known_success",
  "known_invalid",
  "known_rejection",
  "completed",
]);
const ImportedStateSchema = z.enum([
  "intent",
  "unknown",
  "known_success",
  "known_invalid",
  "known_rejection",
]);
export const LegacySolIntentSchema = SolOperationClaimSchema.omit({
  operationKey: true,
});
export const SolLegacyObservationsSchema = SolOperationObservationsSchema;
export const LegacySolTerminalSchema = z.strictObject({
  exitCode: z.int().nullable(),
  finishedAt: TimeSchema,
  signal: z.string().max(100).nullable(),
  state: LegacyTerminalStateSchema,
});
export const LegacySolSessionSchema = z.strictObject({
  observedAt: TimeSchema,
  sessionId: z.uuid(),
});
export const LegacySolTurnSchema = z.strictObject({ observedAt: TimeSchema });
export const SolImportedAttemptSchema = SolOperationClaimSchema.extend({
  createdAt: TimeSchema,
  exitCode: z.int().nullable(),
  finishedAt: TimeSchema.nullable(),
  signal: z.string().max(100).nullable(),
  state: ImportedStateSchema,
  turnStartedAt: TimeSchema.nullable(),
  sessionId: z.uuid().nullable(),
  sessionObservedAt: TimeSchema.nullable(),
  observations: SolLegacyObservationsSchema,
});
export type SolImportedAttempt = z.infer<typeof SolImportedAttemptSchema>;
export interface SolLegacyScan {
  readonly legacyIntentNormalizations: number;
  readonly legacyObservations: {
    readonly continuations: number;
    readonly artifactReconciliations: number;
    readonly fractionalSessionTimestamps: number;
  };
  readonly quarantine: {
    readonly records: number;
    readonly missingExitCode: number;
    readonly signaled: number;
    readonly turnStarted: number;
  };
  readonly records: readonly SolImportedAttempt[];
  readonly sourceBytes: number;
  readonly sourceDigest: string;
}
