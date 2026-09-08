import { z } from "zod";

const TimeSchema = z.int().nonnegative();
const SourceHashSchema = z.string().regex(/^[a-f\d]{64}$/);
const HistoricalWriterSchema = z.literal(
  "19018523d8473a1ea3f124fbe94d833ff0f49a00",
);
const LegacyContinuationStateSchema = z.enum([
  "known_success",
  "predispatch_retryable",
  "unresolved_after_turn_started",
  "known_failure",
]);
const LegacyContinuationTerminalSchema = z
  .strictObject({
    exitCode: z.int().nullable(),
    finishedAt: TimeSchema,
    signal: z.string().max(100).nullable(),
    state: LegacyContinuationStateSchema,
  })
  .refine(
    (terminal) =>
      terminal.state !== "known_success" ||
      (terminal.exitCode === 0 && terminal.signal === null),
  );
const LegacyContinuationIntentSchema = z.strictObject({
  finishedAt: TimeSchema,
  state: z.literal("unresolved_continuation_intent"),
  strategy: z.literal("codex_exec_resume_v1"),
});
const LegacyArtifactStateSchema = z.enum([
  "unresolved_intent_without_terminal",
  "unavailable_no_persisted_session",
]);
export const LegacyArtifactReconciliationSchema = z.strictObject({
  finishedAt: TimeSchema,
  state: LegacyArtifactStateSchema,
  strategy: z.literal("artifact_only_v1"),
});
export const LegacyContinuationObservationSchema = z.union([
  LegacyContinuationTerminalSchema,
  LegacyContinuationIntentSchema,
]);
export const LegacyContinuationEvidenceSchema = z.strictObject({
  strategy: z.literal("legacy_paid_continuation_evidence_v1"),
  sourceRevision: HistoricalWriterSchema,
  sourceSha256: SourceHashSchema,
  original: LegacyContinuationObservationSchema,
});
export const LegacyArtifactReconciliationEvidenceSchema = z.strictObject({
  strategy: z.literal("legacy_artifact_reconciliation_evidence_v1"),
  sourceRevision: HistoricalWriterSchema,
  sourceSha256: SourceHashSchema,
  original: LegacyArtifactReconciliationSchema,
});
export const LegacyFractionalSessionSchema = z.strictObject({
  observedAt: z
    .number()
    .nonnegative()
    .max(Number.MAX_SAFE_INTEGER)
    .refine((value) => !Number.isSafeInteger(value)),
  sessionId: z.uuid(),
});
export const LegacySessionTimestampSchema = z.strictObject({
  strategy: z.literal("legacy_mtime_ms_floor_v1"),
  sourceRevision: HistoricalWriterSchema,
  sourceSha256: SourceHashSchema,
  original: LegacyFractionalSessionSchema,
});
