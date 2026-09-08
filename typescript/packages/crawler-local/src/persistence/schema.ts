import { z } from "zod";

export const WORK_STATES = [
  "pending",
  "running",
  "retry_wait",
  "quota_wait",
  "succeeded",
  "dead_letter",
  "imported",
] as const;

export const WorkStateSchema = z.enum(WORK_STATES);
export type WorkState = z.infer<typeof WorkStateSchema>;

export const WorkDefinitionSchema = z
  .object({
    kind: z.string().trim().min(1).max(100),
    input: z.record(z.string(), z.unknown()),
    inputHash: z.string().regex(/^[a-f\d]{64}$/),
    schemaVersion: z.string().trim().min(1).max(100),
    implementationVersion: z.string().trim().min(1).max(100),
    priority: z.int().min(-1_000_000).max(1_000_000).default(0),
  })
  .strict();

export type WorkDefinition = z.infer<typeof WorkDefinitionSchema>;

export interface WorkItem extends WorkDefinition {
  readonly attemptCount: number;
  readonly availableAt: number;
  readonly createdAt: number;
  readonly lastErrorCode: null | string;
  readonly leaseEpoch: number;
  readonly leaseExpiresAt: null | number;
  readonly leaseOwner: null | string;
  readonly leaseToken: null | string;
  readonly outputArtifactHash: null | string;
  readonly state: WorkState;
  readonly updatedAt: number;
  readonly workKey: string;
}

export interface WorkClaim {
  readonly attemptId: string;
  readonly leaseEpoch: number;
  readonly leaseToken: string;
  readonly work: WorkItem;
}

export const CheckpointSchema = z
  .object({
    artifactHash: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullable()
      .default(null),
    kind: z.string().trim().min(1).max(100),
    payload: z.record(z.string(), z.unknown()),
  })
  .strict();

export type Checkpoint = z.infer<typeof CheckpointSchema>;

export interface StoredCheckpoint extends Checkpoint {
  readonly attemptId: string;
  readonly createdAt: number;
  readonly leaseEpoch: number;
  readonly sequence: number;
  readonly workKey: string;
}

export interface HealthReport {
  readonly coolingOrigins: number;
  readonly integrity: string;
  readonly integrityScope: "full" | "schema";
  readonly journalMode: string;
  readonly schemaVersion: number;
  readonly staleRunning: number;
  readonly stoppedOrigins: number;
}

export interface KindProgress {
  readonly byState: Readonly<Record<WorkState, number>>;
  readonly completed: number;
  readonly kind: string;
  readonly lastSuccessAt: null | number;
  readonly terminal: number;
  readonly total: number;
}

export interface OriginStatus {
  readonly active: boolean;
  readonly consecutiveFailures: number;
  readonly cooldownUntil: number;
  readonly lastCompletedAt: null | number;
  readonly nextAllowedAt: number;
  readonly origin: string;
  readonly stopReason: null | string;
}

export interface LedgerStatus {
  readonly affectedByErrorCode: readonly Readonly<{
    code: string;
    count: number;
  }>[];
  readonly affectedByKindAndErrorCode: readonly Readonly<{
    code: string;
    count: number;
    kind: string;
  }>[];
  readonly byState: Readonly<Record<WorkState, number>>;
  readonly earliestWorkAvailableAt: null | number;
  readonly failureEventsByCode: readonly Readonly<{
    code: string;
    count: number;
  }>[];
  readonly failureEventWindow: number;
  readonly kindProgress: readonly KindProgress[];
  readonly lastFailureAt: null | number;
  readonly lastSuccessAt: null | number;
  readonly origins: readonly OriginStatus[];
  readonly ready: number;
  readonly schemaVersion: number;
  readonly total: number;
  readonly truncated: boolean;
}
