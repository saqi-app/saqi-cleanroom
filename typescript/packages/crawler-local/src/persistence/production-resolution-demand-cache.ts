import { hash, randomUUID, timingSafeEqual } from "node:crypto";

import {
  CANONICAL_POEM_BINDING_SCHEMA_ID,
  CANONICAL_POEM_BINDING_SCHEMA_VERSION,
  canonicalPoemBindingIdBody,
  type CanonicalPoemBindingV1,
  CanonicalPoemBindingV1Schema,
  isProductionResolutionFingerprintTarget,
  isProductionResolutionSourceTarget,
  MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES,
  MAX_PRODUCTION_RESOLUTION_TARGETS,
  normalizeProductionResolutionRequest,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  type ProductionResolutionFingerprintTarget,
  type ProductionResolutionRequest,
  ProductionResolutionRequestSchema,
  ProductionResolutionRequestV1Schema,
  ProductionResolutionRequestV2Schema,
  ProductionResolutionRequestV3Schema,
  type ProductionResolutionResponse,
  ProductionResolutionResponseSchema,
  type ProductionResolutionTarget,
  SAQI_PRODUCTION_DATABASE_ID,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { currentSource } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { z } from "zod";

import { assertCollectedArtifactBinding } from "../enrichment/local-enrichment-fanout.js";
import type { WorkItem } from "./schema.js";
import { validateFetchedProductionResolutionResponse } from "./scoped-production-resolution.js";
import {
  queryMany,
  queryOptional,
  queryRequired,
  queryScalar,
  SqliteSafeIntegerSchema,
} from "./sqlite-query.js";
import { canonicalJson, sha256 } from "./work-key.js";

const MAX_CANONICAL_CLAIM_BURST = 4;
const MAX_CANONICAL_MODE_CLAIM_BURST = 4;
const MAX_FINGERPRINT_SPLIT_CLAIM_BURST = 2;
// Fingerprint resolution is currently an all-or-nothing endpoint and the
// reconciliation backlog is overwhelmingly unmatched. A singleton avoids the
// near-2x request amplification of recursively bisecting rejected batches and
// publishes resolvable identities without waiting for unrelated bad targets.
const MAX_FINGERPRINT_RESOLUTION_TARGETS = 1;
const MAX_SOURCE_CLAIM_BURST = 4;
const DEFAULT_LEASE_MS = 5 * 60_000;
const DEMAND_TOUCH_INTERVAL_MS = 5 * 60_000;
const DELETE_PUBLICATION_WAKEUP_SQL =
  "DELETE FROM publication_wakeup WHERE work_key = ?";
const DELETE_FINGERPRINT_WAKEUP_SQL =
  "DELETE FROM resolution_fingerprint_wakeup WHERE work_key = ?";
const DELETE_FINGERPRINT_WAITER_SQL =
  "DELETE FROM resolution_fingerprint_waiter WHERE work_key = ?";
const UPSERT_SPLIT_REQUEST_SQL = `INSERT INTO resolution_request (
  scope_hash, request_json, state, retry_at, attempt_count,
  lease_epoch, created_at, updated_at
) VALUES (?, ?, 'retry_wait', ?, 0, 0, ?, ?)
ON CONFLICT(scope_hash) DO UPDATE SET
  request_json = excluded.request_json,
  state = 'retry_wait',
  retry_at = excluded.retry_at,
  last_error_code = NULL,
  updated_at = excluded.updated_at
WHERE resolution_request.state IN ('succeeded', 'superseded')`;
const INSERT_REQUEST_MEMBER_SQL = `INSERT OR IGNORE INTO resolution_request_member (
  scope_hash, source_poem_id, source_author_slug, model_key
) VALUES (?, ?, ?, ?)`;
const INSERT_CANONICAL_REQUEST_MEMBER_SQL = `INSERT OR IGNORE INTO resolution_canonical_request_member (
  scope_hash, poem_id, source_revision_id, model_key
) VALUES (?, ?, ?, ?)`;
const INSERT_FINGERPRINT_REQUEST_MEMBER_SQL = `INSERT OR IGNORE INTO resolution_fingerprint_request_member (
  scope_hash, algorithm, line_nfc_hash, prompt_material_hash, model_key
) VALUES (?, ?, ?, ?, ?)`;
const UPSERT_PENDING_REQUEST_SQL = `INSERT INTO resolution_request (
  scope_hash, request_json, state, retry_at, attempt_count,
  lease_epoch, created_at, updated_at
) VALUES (?, ?, 'pending', ?, 0, 0, ?, ?)
ON CONFLICT(scope_hash) DO UPDATE SET
  request_json = excluded.request_json,
  state = CASE WHEN resolution_request.state IN ('succeeded', 'superseded')
               THEN 'pending' ELSE resolution_request.state END,
  retry_at = CASE WHEN resolution_request.state IN ('succeeded', 'superseded')
                  THEN excluded.retry_at ELSE resolution_request.retry_at END,
  updated_at = excluded.updated_at`;
const CollectedIdentitySchema = z.looseObject({
  source: z.looseObject({
    author: z.looseObject({ slug: z.string().min(1).max(1_000) }),
    lines: z.array(z.string()).min(1).max(2_000),
    numericId: z.string().regex(/^[1-9]\d*$/),
    title: z.string().min(1),
  }),
});
const Sha256Schema = z.string().regex(/^[a-f\d]{64}$/);
const CountSchema = SqliteSafeIntegerSchema.nonnegative();
const SourceResolutionRefreshTargetRowSchema = z
  .strictObject({
    source_author_slug: z.string().min(1).max(1_000),
    source_poem_id: z.string().regex(/^[1-9]\d*$/),
  })
  .transform(({ source_author_slug, source_poem_id }) => ({
    sourceAuthorSlug: source_author_slug,
    sourcePoemId: source_poem_id,
  }));
const RequestTableInfoRowSchema = z
  .strictObject({
    cid: SqliteSafeIntegerSchema,
    dflt_value: z.unknown().nullable(),
    hidden: SqliteSafeIntegerSchema,
    name: z.string().min(1),
    notnull: SqliteSafeIntegerSchema,
    pk: SqliteSafeIntegerSchema,
    type: z.string(),
  })
  .transform(({ name }) => name);
const DemandRowSchema = z.strictObject({
  first_seen_at: SqliteSafeIntegerSchema.nonnegative(),
  model_key: z.string().min(1),
  priority: SqliteSafeIntegerSchema,
  source_author_slug: z.string().min(1),
  source_poem_id: z.string().regex(/^[1-9]\d*$/),
});
const SourceResolutionRefreshSchema = z.strictObject({
  eventId: z.string().regex(/^[a-f\d]{64}$/),
  targets: z
    .array(
      z.strictObject({
        sourceAuthorSlug: z.string().min(1).max(1_000),
        sourcePoemId: z.string().regex(/^[1-9]\d*$/),
      }),
    )
    .min(1)
    .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
});
type SourceResolutionRefreshTarget = z.infer<
  typeof SourceResolutionRefreshSchema
>["targets"][number];

function compareSourceResolutionRefreshTargets(
  left: SourceResolutionRefreshTarget,
  right: SourceResolutionRefreshTarget,
): number {
  return (
    left.sourcePoemId.localeCompare(right.sourcePoemId) ||
    left.sourceAuthorSlug.localeCompare(right.sourceAuthorSlug)
  );
}
const CanonicalDemandRowSchema = z.strictObject({
  first_seen_at: SqliteSafeIntegerSchema.nonnegative(),
  model_key: z.string().min(1),
  poem_id: z.string().min(1).max(512),
  priority: SqliteSafeIntegerSchema,
  source_revision_id: z.string().regex(/^[a-f\d]{64}$/),
});
const FingerprintDemandRowSchema = z.strictObject({
  algorithm: z.literal(PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM),
  first_seen_at: SqliteSafeIntegerSchema.nonnegative(),
  line_nfc_hash: z.string().regex(/^[a-f\d]{64}$/),
  model_key: z.string().min(1),
  priority: SqliteSafeIntegerSchema,
  prompt_material_hash: z.string().regex(/^[a-f\d]{64}$/),
});
const RequestRowSchema = z.strictObject({
  lease_epoch: SqliteSafeIntegerSchema.nonnegative(),
  request_json: z.string(),
  scope_hash: z.string().regex(/^[a-f\d]{64}$/),
});
const MappingRowSchema = z.strictObject({
  author_id: z.string().min(1),
  author_name_arabic: z.string().min(1),
  current_source_nfc_sha256: z.string().regex(/^[a-f\d]{64}$/),
  current_source_revision_id: z.string().regex(/^[a-f\d]{64}$/),
  observed_at: SqliteSafeIntegerSchema.nonnegative(),
  poem_id: z.string().min(1),
  source_author_slug: z.string().min(1),
  source_poem_id: z.string().regex(/^[1-9]\d*$/),
  source_pointer_version: SqliteSafeIntegerSchema.positive(),
  writer_epoch: SqliteSafeIntegerSchema.positive(),
});
const PublicationRowSchema = z.strictObject({
  current_source_nfc_sha256: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable(),
  current_source_revision_id: z.string().nullable(),
  pointer_version: SqliteSafeIntegerSchema.positive().nullable(),
  writer_epoch: SqliteSafeIntegerSchema.positive(),
});
const PublicationSourceTargetRowSchema = z.strictObject({
  source_author_slug: z.string().min(1),
  source_poem_id: z.string().regex(/^[1-9]\d*$/),
});
const WriterEpochRowSchema = z.strictObject({
  writer_epoch: SqliteSafeIntegerSchema.positive(),
});
const PublicationSourceRowSchema = z.strictObject({
  source_author_slug: z.string().min(1),
  source_poem_id: z.string().regex(/^[1-9]\d*$/),
});
const CanonicalMappingRowSchema = MappingRowSchema.extend({
  model_key: z.string().min(1),
});
const FingerprintMappingRowSchema = CanonicalMappingRowSchema.extend({
  fingerprint_algorithm: z.literal(PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM),
  prompt_material_hash: z.string().regex(/^[a-f\d]{64}$/),
});
const ResolutionRequestStateSchema = z.enum([
  "pending",
  "running",
  "retry_wait",
  "succeeded",
  "superseded",
]);
const ResolutionModeSchema = z.enum(["canonical", "fingerprint"]);
const ResolutionLeaseMsSchema = z
  .number()
  .int()
  .min(1_000)
  .max(60 * 60_000);
const ScopePruneLimitSchema = z.number().int().min(1).max(100);
const WakeupLimitSchema = z.number().int().min(1).max(1_000);
const ActiveModelKeysSchema = z
  .array(z.string().trim().min(1).max(100))
  .min(1)
  .max(32)
  .refine((values) => new Set(values).size === values.length);
const ResolutionErrorCodeSchema = z.string().regex(/^[A-Z][A-Z\d_:.-]{0,255}$/);
const WaiterWorkKeySchema = z.string().regex(/^[a-f\d]{64}$/);
const WakeupWorkKeysSchema = z.array(WaiterWorkKeySchema).max(1_000);
const PendingRefreshFlagSchema = z.literal(1);

const LeaseRowSchema = z.strictObject({
  lease_epoch: SqliteSafeIntegerSchema.nonnegative(),
  lease_token: z.string().nullable(),
  state: ResolutionRequestStateSchema,
});
const SchedulerRowSchema = z.strictObject({
  canonical_burst: SqliteSafeIntegerSchema.min(0).max(
    MAX_CANONICAL_CLAIM_BURST,
  ),
  canonical_mode_burst: SqliteSafeIntegerSchema.min(0).max(
    MAX_CANONICAL_MODE_CLAIM_BURST,
  ),
  fingerprint_split_burst: SqliteSafeIntegerSchema.min(0).max(
    MAX_FINGERPRINT_SPLIT_CLAIM_BURST,
  ),
  source_burst: SqliteSafeIntegerSchema.min(0).max(MAX_SOURCE_CLAIM_BURST),
});
const PublicationWaiterInputSchema = z.strictObject({
  modelKey: z.string().min(1),
  poemId: z.string().min(1).max(512),
  priority: z.number().int(),
  sourceNfcSha256: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .optional(),
  sourceRevisionId: z.string().regex(/^[a-f\d]{64}$/),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
const PublicationWakeupRowSchema = z.strictObject({
  expected_pointer_version: SqliteSafeIntegerSchema.positive().nullable(),
  model_key: z.string().min(1),
  poem_id: z.string().min(1),
  source_revision_id: z.string().regex(/^[a-f\d]{64}$/),
  work_key: z.string().regex(/^[a-f\d]{64}$/),
  writer_epoch: SqliteSafeIntegerSchema.positive(),
});
const PublicationWaiterIdentityRowSchema = z.strictObject({
  model_key: z.string().min(1),
  poem_id: z.string().min(1).max(512),
  source_nfc_sha256: z
    .string()
    .regex(/^[a-f\d]{64}$/)
    .nullable(),
  source_revision_id: z.string().regex(/^[a-f\d]{64}$/),
});
const ResolutionFailureRowSchema = z.strictObject({
  last_error_code: z.string().regex(/^[A-Z][A-Z\d_:.-]{0,255}$/),
});
const BoundModeRowSchema = z.strictObject({
  mode: ResolutionModeSchema,
});
const FingerprintWaiterInputSchema = z.strictObject({
  input: PoemEnrichmentInputSchema,
  modelKey: z.string().min(1),
  priority: z.number().int(),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
const FingerprintWaiterIdentityRowSchema = z.strictObject({
  algorithm: z.literal(PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM),
  line_nfc_hash: z.string().regex(/^[a-f\d]{64}$/),
  model_key: z.string().min(1),
  prompt_material_hash: z.string().regex(/^[a-f\d]{64}$/),
});
const FingerprintWakeupRowSchema = z.strictObject({
  work_key: z.string().regex(/^[a-f\d]{64}$/),
});

export interface PublicationResolution {
  readonly expectedPointerVersion: null | number;
  readonly sourceRevisionId: string;
  readonly writerEpoch: number;
}

export interface SourceResolutionTarget {
  readonly sourceAuthorSlug: string;
  readonly sourcePoemId: string;
}

export type CanonicalEnrichmentResolution =
  | {
      readonly binding: CanonicalPoemBindingV1;
      readonly status: "resolved";
    }
  | {
      readonly code: "SOURCE_BINDING_CONTENT_MISMATCH";
      readonly status: "conflict";
    }
  | { readonly status: "pending" };

export type FingerprintEnrichmentResolution =
  | {
      readonly binding: CanonicalPoemBindingV1;
      readonly status: "resolved";
    }
  | { readonly status: "pending" };

export interface FingerprintResolutionWaiterInput {
  readonly input: PoemEnrichmentInput;
  readonly modelKey: string;
  readonly priority: number;
  readonly workKey: string;
}

export type FingerprintResolutionRegistration =
  | {
      readonly binding: CanonicalPoemBindingV1;
      readonly status: "resolved";
    }
  | { readonly status: "waiting" };

export interface PublicationResolutionWaiterInput {
  readonly modelKey: string;
  readonly poemId: string;
  readonly priority: number;
  readonly sourceNfcSha256?: string;
  readonly sourceRevisionId: string;
  readonly workKey: string;
}

export interface PublicationWakeup {
  readonly modelKey: string;
  readonly poemId: string;
  readonly resolution: PublicationResolution;
  readonly workKey: string;
}

export type PublicationResolutionRegistration =
  | { readonly resolution: PublicationResolution; readonly status: "resolved" }
  | { readonly status: "waiting" };

export interface ProductionResolutionDemandClaim {
  readonly leaseEpoch: number;
  readonly leaseToken: string;
  readonly request: ProductionResolutionRequest;
  readonly scopeHash: string;
}

export interface ProductionResolutionDemandCacheOptions {
  readonly leaseMs?: number;
  readonly now?: () => number;
  readonly owner?: string;
  readonly path: string;
}

export interface ExpiredResolutionScopePruneResult {
  readonly models: number;
  readonly scopes: number;
  readonly targets: number;
}

export interface InactiveResolutionModelRetirementResult {
  readonly canonicalDemands: number;
  readonly fingerprintDemands: number;
  readonly fingerprintWaiters: number;
  readonly publicationWaiters: number;
  readonly sourceDemands: number;
  readonly supersededRequests: number;
}

const DEFAULT_EXPIRED_SCOPE_PRUNE_LIMIT = 10;

interface ProductionResolutionDemandCounts {
  readonly demands: number;
  readonly requests: number;
  readonly scopes: number;
}

interface ProductionResolutionDemandMetrics {
  readonly canonicalBurst: number;
  readonly canonicalModeBurst: number;
  readonly fingerprintSplitBurst: number;
  readonly retryWaitRequests: number;
  readonly sourceBurst: number;
  readonly supersededRequests: number;
}

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete SQLite cache owns transactions and durable waiters; consumer-specific projections already define the required subsets.
export class ProductionResolutionDemandCache {
  readonly #database: Database.Database;
  readonly #leaseMs: number;
  readonly #now: () => number;
  readonly #owner: string;

  constructor(options: ProductionResolutionDemandCacheOptions) {
    this.#leaseMs = ResolutionLeaseMsSchema.parse(
      options.leaseMs ?? DEFAULT_LEASE_MS,
    );
    this.#now = options.now ?? Date.now;
    this.#owner = options.owner ?? `resolution-${String(process.pid)}`;
    this.#database = new Database(options.path);
    this.#database.pragma("busy_timeout = 5000");
    this.#database.pragma("foreign_keys = ON");
    this.#database.pragma("journal_mode = WAL");
    this.#migrate();
  }

  close(): void {
    this.#database.close();
  }

  /**
   * Reclaims a deterministic, bounded page of resolution material that can no
   * longer satisfy any lookup. Request rows remain as durable scheduling
   * history, and their existing upsert path can reissue the exact scope.
   */
  pruneExpiredScopes(
    limit = DEFAULT_EXPIRED_SCOPE_PRUNE_LIMIT,
  ): ExpiredResolutionScopePruneResult {
    const bounded = ScopePruneLimitSchema.parse(limit);
    const now = this.#now();
    return this.#database.transaction(() => {
      const scopeHashes = queryMany(
        { operation: "productionResolutionDemand.expiredScopes" },
        () =>
          this.#database
            .prepare(
              `SELECT scope_hash FROM resolution_scope
            WHERE expires_at <= ?
            ORDER BY expires_at, scope_hash LIMIT ?`,
            )
            .pluck()
            .all(now, bounded),
        Sha256Schema,
      );
      if (scopeHashes.length === 0) return { models: 0, scopes: 0, targets: 0 };
      const placeholders = scopeHashes.map(() => "?").join(",");
      const models = queryRequired(
        { operation: "productionResolutionDemand.expiredModelCount" },
        () =>
          this.#database
            .prepare(
              `SELECT COUNT(*) FROM resolution_model
            WHERE scope_hash IN (${placeholders})`,
            )
            .pluck()
            .get(...scopeHashes),
        CountSchema,
      );
      const targets = queryRequired(
        { operation: "productionResolutionDemand.expiredTargetCount" },
        () =>
          this.#database
            .prepare(
              `SELECT COUNT(*) FROM resolution_target
            WHERE scope_hash IN (${placeholders})`,
            )
            .pluck()
            .get(...scopeHashes),
        CountSchema,
      );
      const remove = this.#database.prepare(
        "DELETE FROM resolution_scope WHERE scope_hash = ?",
      );
      let scopes = 0;
      for (const scopeHash of scopeHashes)
        scopes += remove.run(scopeHash).changes;
      if (scopes !== scopeHashes.length)
        throw new Error("EXPIRED_RESOLUTION_SCOPE_DELETE_MISMATCH");
      return { models, scopes, targets };
    })();
  }

  /**
   * Retires canonical lookup work that cannot wake a publication. This is
   * intentionally narrower than clearing the derived cache: source and
   * fingerprint demand remain available, and every canonical demand backed by
   * a durable publication waiter is preserved.
   */
  retireUnboundCanonicalDemands(): number {
    const now = this.#now();
    return this.#database.transaction(() => {
      const retired = this.#database
        .prepare(
          `DELETE FROM resolution_canonical_demand AS demand
            WHERE NOT EXISTS (
              SELECT 1 FROM publication_waiter waiter
               WHERE waiter.poem_id = demand.poem_id
                 AND waiter.model_key = demand.model_key
                 AND waiter.source_revision_id = demand.source_revision_id
            )`,
        )
        .run().changes;
      this.#supersedeOrphanedCanonicalRequests(now);
      return retired;
    })();
  }

  /**
   * Retires source-refresh work when collection and providers are fenced off.
   * Source requests are derived scheduling state; canonical and fingerprint
   * publication demand remain untouched.
   */
  retireSourceDemands(): number {
    const now = this.#now();
    return this.#database.transaction(() => {
      const retired = this.#database
        .prepare("DELETE FROM resolution_demand")
        .run().changes;
      this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'superseded', lease_owner = NULL,
                  lease_token = NULL, lease_expires_at = NULL, updated_at = ?
            WHERE state IN ('pending', 'retry_wait')
              AND EXISTS (
                SELECT 1 FROM resolution_request_member member
                LEFT JOIN resolution_demand demand
                  ON demand.source_poem_id = member.source_poem_id
                 AND demand.source_author_slug = member.source_author_slug
                 AND demand.model_key = member.model_key
               WHERE member.scope_hash = resolution_request.scope_hash
                 AND demand.source_poem_id IS NULL
              )`,
        )
        .run(now);
      return retired;
    })();
  }

  /**
   * Retires derived scheduling state for models that are no longer executable.
   * Historical successful scopes and responses remain immutable evidence.
   */
  retireInactiveModelState(
    activeModelKeys: readonly string[],
  ): InactiveResolutionModelRetirementResult {
    const keys = ActiveModelKeysSchema.parse(activeModelKeys);
    const placeholders = keys.map(() => "?").join(",");
    const now = this.#now();
    return this.#database.transaction(() => {
      const supersededRequests = this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'superseded', lease_owner = NULL,
                  lease_token = NULL, lease_expires_at = NULL, updated_at = ?
            WHERE state IN ('pending', 'running', 'retry_wait') AND (
              EXISTS (
                SELECT 1 FROM resolution_request_member member
                 WHERE member.scope_hash = resolution_request.scope_hash
                   AND member.model_key NOT IN (${placeholders})
              ) OR EXISTS (
                SELECT 1 FROM resolution_canonical_request_member member
                 WHERE member.scope_hash = resolution_request.scope_hash
                   AND member.model_key NOT IN (${placeholders})
              ) OR EXISTS (
                SELECT 1 FROM resolution_fingerprint_request_member member
                 WHERE member.scope_hash = resolution_request.scope_hash
                   AND member.model_key NOT IN (${placeholders})
              )
            )`,
        )
        .run(now, ...keys, ...keys, ...keys).changes;
      const canonicalDemands = this.#database
        .prepare(
          `DELETE FROM resolution_canonical_demand
            WHERE model_key NOT IN (${placeholders})`,
        )
        .run(...keys).changes;
      const fingerprintDemands = this.#database
        .prepare(
          `DELETE FROM resolution_fingerprint_demand
            WHERE model_key NOT IN (${placeholders})`,
        )
        .run(...keys).changes;
      const fingerprintWaiters = this.#database
        .prepare(
          `DELETE FROM resolution_fingerprint_waiter
            WHERE model_key NOT IN (${placeholders})`,
        )
        .run(...keys).changes;
      const publicationWaiters = this.#database
        .prepare(
          `DELETE FROM publication_waiter
            WHERE model_key NOT IN (${placeholders})`,
        )
        .run(...keys).changes;
      const sourceDemands = this.#database
        .prepare(
          `DELETE FROM resolution_demand
            WHERE model_key NOT IN (${placeholders})`,
        )
        .run(...keys).changes;
      return {
        canonicalDemands,
        fingerprintDemands,
        fingerprintWaiters,
        publicationWaiters,
        sourceDemands,
        supersededRequests,
      };
    })();
  }

  registerCollectedDemand(
    source: WorkItem,
    artifact: unknown,
    modelKeys: readonly string[],
    priority = source.priority,
  ): void {
    assertCollectedArtifactBinding(source, artifact);
    const identity = CollectedIdentitySchema.parse(artifact).source;
    this.registerDemand({
      modelKeys,
      priority,
      sourceAuthorSlug: identity.author.slug,
      sourcePoemId: identity.numericId,
    });
  }

  registerDemand(input: {
    readonly modelKeys: readonly string[];
    readonly priority: number;
    readonly sourceAuthorSlug: string;
    readonly sourcePoemId: string;
  }): void {
    const request = ProductionResolutionRequestV1Schema.parse({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 1,
      targets: [
        {
          modelKeys: input.modelKeys,
          sourceAuthorSlug: input.sourceAuthorSlug,
          sourcePoemId: input.sourcePoemId,
        },
      ],
    });
    const target = request.targets[0];
    if (!target || !isProductionResolutionSourceTarget(target))
      throw new Error("PRODUCTION_RESOLUTION_DEMAND_EMPTY");
    const now = this.#now();
    const statement = this.#database.prepare(
      `INSERT INTO resolution_demand (
         source_poem_id, source_author_slug, model_key, priority,
         first_seen_at, last_seen_at
       ) VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(source_poem_id, source_author_slug, model_key) DO UPDATE SET
         priority = max(resolution_demand.priority, excluded.priority),
         last_seen_at = excluded.last_seen_at
       WHERE excluded.priority > resolution_demand.priority
          OR excluded.last_seen_at >= resolution_demand.last_seen_at + ?`,
    );
    const write = this.#database.transaction(() => {
      for (const modelKey of target.modelKeys)
        statement.run(
          target.sourcePoemId,
          target.sourceAuthorSlug,
          modelKey,
          input.priority,
          now,
          now,
          DEMAND_TOUCH_INTERVAL_MS,
        );
    });
    write();
  }

  wakeSourceResolution(input: {
    readonly eventId: string;
    readonly targets: readonly {
      readonly sourceAuthorSlug: string;
      readonly sourcePoemId: string;
    }[];
  }): boolean {
    const parsed = SourceResolutionRefreshSchema.parse(input);
    const targets = parsed.targets.toSorted(
      compareSourceResolutionRefreshTargets,
    );
    if (
      new Set(targets.map((target) => canonicalJson(target))).size !==
      targets.length
    )
      throw new Error("SOURCE_RESOLUTION_REFRESH_TARGET_DUPLICATE");
    const now = this.#now();
    const inserted = this.#database.transaction(() => {
      const event = this.#database
        .prepare(
          `INSERT INTO source_resolution_refresh_event(
             event_id, created_at, applied_at
           ) VALUES(?, ?, NULL) ON CONFLICT(event_id) DO NOTHING`,
        )
        .run(parsed.eventId, now);
      const insertTarget = this.#database.prepare(
        `INSERT INTO source_resolution_refresh_target(
           event_id, source_poem_id, source_author_slug
         ) VALUES(?, ?, ?) ON CONFLICT DO NOTHING`,
      );
      if (event.changes === 1)
        for (const target of targets)
          insertTarget.run(
            parsed.eventId,
            target.sourcePoemId,
            target.sourceAuthorSlug,
          );
      const stored = queryMany(
        { operation: "productionResolutionDemand.refreshTargets" },
        () =>
          this.#database
            .prepare(
              `SELECT source_poem_id, source_author_slug
             FROM source_resolution_refresh_target WHERE event_id = ?
            ORDER BY source_poem_id, source_author_slug`,
            )
            .all(parsed.eventId),
        SourceResolutionRefreshTargetRowSchema,
      ).toSorted(compareSourceResolutionRefreshTargets);
      if (canonicalJson(stored) !== canonicalJson(targets))
        throw new Error("SOURCE_RESOLUTION_REFRESH_EVENT_CONFLICT");
      this.#applySourceResolutionRefreshes(now);
      return event.changes === 1;
    })();
    return inserted;
  }

  registerPublicationDemand(
    poemId: string,
    modelKey: string,
    sourceRevisionId: string,
    priority: number,
  ): boolean {
    const source = queryOptional(
      { operation: "productionResolutionDemand.publicationSource" },
      () =>
        this.#database
          .prepare(
            `SELECT target.source_poem_id, target.source_author_slug
           FROM resolution_target target
           JOIN resolution_scope scope USING(scope_hash)
          WHERE target.poem_id = ? AND target.current_source_revision_id = ?
            AND scope.expires_at > ?
          ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(poemId, sourceRevisionId, this.#now()),
      PublicationSourceRowSchema,
    );
    if (source) {
      this.registerDemand({
        modelKeys: [modelKey],
        priority,
        sourceAuthorSlug: source.source_author_slug,
        sourcePoemId: source.source_poem_id,
      });
      return true;
    }
    const target = ProductionResolutionRequestV2Schema.parse({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 2,
      targets: [{ modelKeys: [modelKey], poemId, sourceRevisionId }],
    }).targets[0];
    if (!target || isProductionResolutionSourceTarget(target))
      throw new Error("PRODUCTION_RESOLUTION_CANONICAL_DEMAND_INVALID");
    const now = this.#now();
    const statement = this.#database.prepare(
      `INSERT INTO resolution_canonical_demand (
         poem_id, source_revision_id, model_key, priority,
         first_seen_at, last_seen_at
       ) VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(poem_id, source_revision_id, model_key) DO UPDATE SET
         priority = max(resolution_canonical_demand.priority, excluded.priority),
         last_seen_at = excluded.last_seen_at
       WHERE excluded.priority > resolution_canonical_demand.priority
          OR excluded.last_seen_at >= resolution_canonical_demand.last_seen_at + ?`,
    );
    const insert = this.#database.transaction(() => {
      for (const key of target.modelKeys) {
        statement.run(
          target.poemId,
          target.sourceRevisionId,
          key,
          priority,
          now,
          now,
          DEMAND_TOUCH_INTERVAL_MS,
        );
      }
    });
    insert();
    return true;
  }

  registerFingerprintDemand(
    input: PoemEnrichmentInput,
    modelKey: string,
    priority: number,
  ): void {
    const lineNfcHash = sha256(sourceLineNfcHashBody(input.linesArabic));
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: input.authorArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    );
    const target = ProductionResolutionRequestV3Schema.parse({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 3,
      targets: [
        {
          fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash,
          modelKeys: [modelKey],
          promptMaterialHash,
        },
      ],
    }).targets[0];
    if (!target)
      throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_DEMAND_INVALID");
    const now = this.#now();
    const statement = this.#database.prepare(
      `INSERT INTO resolution_fingerprint_demand (
         algorithm, line_nfc_hash, prompt_material_hash, model_key,
         priority, first_seen_at, last_seen_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(algorithm, line_nfc_hash, prompt_material_hash, model_key)
       DO UPDATE SET
         priority = max(resolution_fingerprint_demand.priority, excluded.priority),
         last_seen_at = excluded.last_seen_at
       WHERE excluded.priority > resolution_fingerprint_demand.priority
          OR excluded.last_seen_at >= resolution_fingerprint_demand.last_seen_at + ?`,
    );
    for (const key of target.modelKeys)
      statement.run(
        target.fingerprintAlgorithm,
        target.lineNfcHash,
        target.promptMaterialHash,
        key,
        priority,
        now,
        now,
        DEMAND_TOUCH_INTERVAL_MS,
      );
  }

  terminalFingerprintFailure(
    input: PoemEnrichmentInput,
    modelKey: string,
  ): null | string {
    const lineNfcHash = sha256(sourceLineNfcHashBody(input.linesArabic));
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: input.authorArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    );
    const row = queryOptional(
      { operation: "productionResolutionDemand.fingerprintFailure" },
      () =>
        this.#database
          .prepare(
            `SELECT request.last_error_code
             FROM resolution_fingerprint_request_member member
             JOIN resolution_request request USING(scope_hash)
            WHERE member.algorithm = ? AND member.line_nfc_hash = ?
              AND member.prompt_material_hash = ? AND member.model_key = ?
              AND request.state IN ('retry_wait', 'superseded')
              AND request.last_error_code IN (
                'PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED',
                'PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE',
                'PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS',
                'PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT'
              )
              AND json_array_length(request.request_json, '$.targets') = 1
            ORDER BY request.updated_at DESC LIMIT 1`,
          )
          .get(
            PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
            lineNfcHash,
            promptMaterialHash,
            modelKey,
          ),
      ResolutionFailureRowSchema,
    );
    return row?.last_error_code ?? null;
  }

  retireFingerprintDemand(input: PoemEnrichmentInput, modelKey: string): void {
    const target = {
      fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
      lineNfcHash: sha256(sourceLineNfcHashBody(input.linesArabic)),
      promptMaterialHash: sha256(
        sourcePromptMaterialHashBody({
          authorArabic: input.authorArabic,
          linesArabic: input.linesArabic,
          titleArabic: input.titleArabic,
        }),
      ),
    } as const;
    this.#database.transaction(() => {
      this.#retireFingerprintTargetDemand(target, modelKey, this.#now());
    })();
  }

  terminalCanonicalFailure(
    poemId: string,
    modelKey: string,
    sourceRevisionId: string,
  ): null | string {
    const row = queryOptional(
      { operation: "productionResolutionDemand.canonicalFailure" },
      () =>
        this.#database
          .prepare(
            `SELECT request.last_error_code
             FROM resolution_canonical_request_member member
             JOIN resolution_request request USING(scope_hash)
            WHERE member.poem_id = ? AND member.model_key = ?
              AND member.source_revision_id = ?
              AND request.state IN ('retry_wait', 'superseded')
              AND request.last_error_code IN (
                'PRODUCTION_RESOLUTION_TARGET_UNRESOLVED',
                'PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT',
                'PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT'
              )
              AND json_array_length(request.request_json, '$.targets') = 1
            ORDER BY request.updated_at DESC LIMIT 1`,
          )
          .get(poemId, modelKey, sourceRevisionId),
      ResolutionFailureRowSchema,
    );
    return row?.last_error_code ?? null;
  }

  retireCanonicalDemand(
    poemId: string,
    modelKey: string,
    sourceRevisionId: string,
  ): void {
    const now = this.#now();
    this.#database.transaction(() => {
      this.#database
        .prepare(
          `DELETE FROM resolution_canonical_demand
            WHERE poem_id = ? AND model_key = ? AND source_revision_id = ?`,
        )
        .run(poemId, modelKey, sourceRevisionId);
      this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'superseded', lease_owner = NULL,
                  lease_token = NULL, lease_expires_at = NULL, updated_at = ?
            WHERE state IN ('pending','retry_wait') AND scope_hash IN (
              SELECT scope_hash FROM resolution_canonical_request_member
               WHERE poem_id = ? AND model_key = ? AND source_revision_id = ?
            )`,
        )
        .run(now, poemId, modelKey, sourceRevisionId);
    })();
  }

  hasDueWork(): boolean {
    const now = this.#now();
    const pending = queryScalar(
      { operation: "productionResolutionDemand.hasDueWork" },
      () =>
        this.#database
          .prepare(
            `SELECT 1 FROM resolution_request
              WHERE state IN ('pending', 'retry_wait') AND retry_at <= ?
             UNION ALL
             SELECT 1 FROM resolution_request
              WHERE state = 'running' AND lease_expires_at <= ?
             UNION ALL
             SELECT 1 FROM source_resolution_refresh_event
              WHERE applied_at IS NULL
             LIMIT 1`,
          )
          .pluck()
          .get(now, now),
      PendingRefreshFlagSchema,
    );
    return (
      pending !== undefined ||
      this.#buildRequest(now, "all", 1) !== null ||
      this.#buildFingerprintRequest(now, 1) !== null
    );
  }

  claim(): null | ProductionResolutionDemandClaim {
    const now = this.#now();
    this.#database
      .prepare(
        `UPDATE resolution_request
            SET state = 'retry_wait', lease_owner = NULL, lease_token = NULL,
                lease_expires_at = NULL, retry_at = ?, updated_at = ?
          WHERE state = 'running' AND lease_expires_at <= ?`,
      )
      .run(now, now, now);
    this.#applySourceResolutionRefreshes(now);
    this.#supersedeOrphanedCanonicalRequests(now);
    // A split request has already paid its turn in both fairness schedulers.
    // Drain its finite, unattempted schema-v3 children before admitting fresh
    // source/canonical work; otherwise every bisection resets both bursts and
    // the nested schedulers can insert up to 24 unrelated network calls between
    // a rejected batch and its first resolvable child. New requests are inserted
    // as `pending` and claimed immediately, so retry_wait + attempt_count=0
    // identifies only durable bisection children (including after restart).
    const splitBurstAtLimit =
      this.#fingerprintSplitBurst() >= MAX_FINGERPRINT_SPLIT_CLAIM_BURST;
    if (!splitBurstAtLimit) {
      const boundSplit = this.#oldestDueBoundSplitRequest(now);
      if (boundSplit) return this.#claimRow(boundSplit, now, true);
    }
    if (this.#sourceBurst() < MAX_SOURCE_CLAIM_BURST) {
      const dueSource = this.#oldestDueSourceRequest(now);
      if (dueSource) return this.#claimRow(dueSource, now);
      const sourceRequest = this.#buildRequest(now, "source");
      if (sourceRequest) return this.#insertAndClaim(sourceRequest, now);
    }
    // Once two split children have continued, force one available canonical
    // turn before reopening the continuation burst. Split children preserve
    // the normal source/canonical counters, so sustained source work reaches
    // its own burst limit instead of taking every competing turn forever.
    if (splitBurstAtLimit) {
      // Cross-mode fairness remains the outer bound. If canonical/source work
      // has already consumed its full burst, service an ordinary fingerprint
      // request before granting another canonical turn. Otherwise a durable
      // queue of split children can repeatedly reset the split counter through
      // canonical claims and postpone attempted fingerprint retries forever.
      if (this.#canonicalModeBurst() >= MAX_CANONICAL_MODE_CLAIM_BURST) {
        const fingerprintRequest = this.#buildRequest(now, "fingerprint");
        if (fingerprintRequest)
          return this.#insertAndClaim(fingerprintRequest, now);
        const dueFingerprint = this.#oldestDueBoundRequest(now, "fingerprint");
        if (dueFingerprint) return this.#claimRow(dueFingerprint, now);
      }
      const canonicalRequest = this.#buildRequest(now, "canonical");
      if (canonicalRequest) return this.#insertAndClaim(canonicalRequest, now);
      const dueCanonical = this.#oldestDueBoundRequest(now, "canonical");
      if (dueCanonical) return this.#claimRow(dueCanonical, now);
      // Nothing from the competing lanes is eligible. Continue without
      // resetting the saturated burst so work arriving before the next claim
      // still receives the next turn.
      const boundSplit = this.#oldestDueBoundSplitRequest(now);
      if (boundSplit) return this.#claimRow(boundSplit, now, true);
    }
    // Fingerprint work is deliberately a distinct identity mode. Cross-mode
    // fairness overrides priority after a bounded non-fingerprint burst;
    // ordering within each mode remains priority-first. The persisted column
    // retains its historical name, but both legacy source and canonical claims
    // count toward the bound so their nested schedulers cannot multiply it.
    if (this.#canonicalModeBurst() >= MAX_CANONICAL_MODE_CLAIM_BURST) {
      const fingerprintRequest = this.#buildRequest(now, "fingerprint");
      if (fingerprintRequest)
        return this.#insertAndClaim(fingerprintRequest, now);
      const dueFingerprint = this.#oldestDueBoundRequest(now, "fingerprint");
      if (dueFingerprint) return this.#claimRow(dueFingerprint, now);
    }
    // Admit uncovered publication demand before replaying the historical retry
    // queue. Covered demand is excluded by #buildRequest, so a failed singleton
    // still observes its retry deadline without letting thousands of unrelated
    // old retries starve a newly arrived poem forever.
    const preferredBoundMode = this.#preferredUncoveredBoundMode(now);
    if (preferredBoundMode) {
      const boundRequest = this.#buildRequest(now, preferredBoundMode);
      if (boundRequest) return this.#insertAndClaim(boundRequest, now);
    }
    const dueBound = this.#oldestDueBoundRequest(now);
    if (dueBound) return this.#claimRow(dueBound, now);
    const dueSource = this.#oldestDueSourceRequest(now);
    if (dueSource) return this.#claimRow(dueSource, now);
    const sourceRequest = this.#buildRequest(now, "source");
    if (sourceRequest) return this.#insertAndClaim(sourceRequest, now);
    const existing = this.#oldestDueRequest(now);
    if (existing) return this.#claimRow(existing, now);
    const request = this.#buildRequest(now, "all");
    if (!request) return null;
    return this.#insertAndClaim(request, now);
  }

  complete(
    claim: ProductionResolutionDemandClaim,
    responseInput: ProductionResolutionResponse,
  ): "merged" | "replayed" {
    const response = ProductionResolutionResponseSchema.parse(responseInput);
    const request = claim.request;
    const now = this.#now();
    validateFetchedProductionResolutionResponse(request, response, now);
    const result = this.#database.transaction(() => {
      const row = queryOptional(
        { operation: "productionResolutionDemand.completionLease" },
        () =>
          this.#database
            .prepare(
              `SELECT state, lease_token, lease_epoch FROM resolution_request
            WHERE scope_hash = ?`,
            )
            .get(claim.scopeHash),
        LeaseRowSchema,
      );
      if (row?.state === "succeeded") {
        const stored = queryRequired(
          { operation: "productionResolutionDemand.completedManifest" },
          () =>
            this.#database
              .prepare(
                `SELECT manifest_hash FROM resolution_scope WHERE scope_hash = ?`,
              )
              .pluck()
              .get(claim.scopeHash),
          Sha256Schema,
        );
        if (stored === response.manifestHash) return "replayed" as const;
        throw new Error("PRODUCTION_RESOLUTION_REPLAY_CONFLICT");
      }
      if (
        row?.state !== "running" ||
        row.lease_token === null ||
        !secretsEqual(row.lease_token, claim.leaseToken) ||
        row.lease_epoch !== claim.leaseEpoch
      )
        throw new Error("PRODUCTION_RESOLUTION_LEASE_LOST");
      this.#database
        .prepare(
          `INSERT INTO resolution_scope (
             scope_hash, manifest_hash, observed_at, expires_at,
             writer_epoch, response_json, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(scope_hash) DO UPDATE SET
             manifest_hash = excluded.manifest_hash,
             observed_at = excluded.observed_at,
             expires_at = excluded.expires_at,
             writer_epoch = excluded.writer_epoch,
             response_json = excluded.response_json,
             updated_at = excluded.updated_at`,
        )
        .run(
          claim.scopeHash,
          response.manifestHash,
          Date.parse(response.observedAt),
          Date.parse(response.expiresAt),
          response.writerEpoch,
          canonicalJson(response),
          now,
        );
      this.#database
        .prepare("DELETE FROM resolution_target WHERE scope_hash = ?")
        .run(claim.scopeHash);
      const targetStatement = this.#database.prepare(
        `INSERT INTO resolution_target (
           scope_hash, source_poem_id, source_author_slug, poem_id, author_id,
           author_name_arabic, current_source_revision_id,
           source_pointer_version, current_source_nfc_sha256,
           fingerprint_algorithm, prompt_material_hash
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      );
      const modelStatement = this.#database.prepare(
        `INSERT INTO resolution_model (
           scope_hash, source_poem_id, source_author_slug, model_key,
           pointer_version
         ) VALUES (?, ?, ?, ?, ?)`,
      );
      const fingerprintWakeStatement = this.#database.prepare(
        `INSERT INTO resolution_fingerprint_wakeup (work_key, created_at)
         SELECT waiter.work_key, ?
           FROM resolution_fingerprint_waiter waiter
          WHERE waiter.algorithm = ? AND waiter.line_nfc_hash = ?
            AND waiter.prompt_material_hash = ? AND waiter.model_key = ?
         ON CONFLICT(work_key) DO UPDATE SET created_at = excluded.created_at`,
      );
      const wakeStatement = this.#database.prepare(
        `INSERT INTO publication_wakeup (
           work_key, poem_id, model_key, source_revision_id,
           expected_pointer_version, writer_epoch, created_at
         )
         SELECT waiter.work_key, waiter.poem_id, waiter.model_key, ?, ?, ?, ?
           FROM publication_waiter waiter
          WHERE waiter.poem_id = ? AND waiter.model_key = ?
            AND (waiter.source_revision_id = ?
              OR (waiter.source_nfc_sha256 IS NOT NULL
                AND waiter.source_nfc_sha256 = ?))
         ON CONFLICT(work_key) DO UPDATE SET
           poem_id = excluded.poem_id,
           model_key = excluded.model_key,
           source_revision_id = excluded.source_revision_id,
           expected_pointer_version = excluded.expected_pointer_version,
           writer_epoch = excluded.writer_epoch,
           created_at = excluded.created_at`,
      );
      const requested = new Set<
        ProductionResolutionFingerprintTarget | ProductionResolutionTarget
      >(request.targets);
      for (const target of response.targets) {
        targetStatement.run(
          claim.scopeHash,
          target.sourcePoemId,
          target.sourceAuthorSlug,
          target.poemId,
          target.authorId,
          target.authorNameArabic,
          target.currentSourceRevisionId,
          target.sourcePointerVersion,
          target.currentSourceNfcSha256 ?? null,
          "activeSourceFingerprint" in target
            ? target.activeSourceFingerprint.algorithm
            : null,
          "activeSourceFingerprint" in target
            ? target.activeSourceFingerprint.promptMaterialHash
            : null,
        );
        const demand = [...requested].find((candidate) =>
          isProductionResolutionFingerprintTarget(candidate)
            ? "activeSourceFingerprint" in target &&
              candidate.lineNfcHash ===
                target.activeSourceFingerprint.lineNfcHash &&
              candidate.promptMaterialHash ===
                target.activeSourceFingerprint.promptMaterialHash
            : isProductionResolutionSourceTarget(candidate)
              ? candidate.sourcePoemId === target.sourcePoemId &&
                candidate.sourceAuthorSlug === target.sourceAuthorSlug
              : candidate.poemId === target.poemId,
        );
        if (!demand)
          throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
        requested.delete(demand);
        const pointers = new Map(
          target.modelPointers.map(({ modelKey, pointerVersion }) => [
            modelKey,
            pointerVersion,
          ]),
        );
        for (const modelKey of demand.modelKeys)
          modelStatement.run(
            claim.scopeHash,
            target.sourcePoemId,
            target.sourceAuthorSlug,
            modelKey,
            pointers.get(modelKey) ?? null,
          );
        for (const modelKey of demand.modelKeys) {
          const pointerVersion = pointers.get(modelKey) ?? null;
          if (target.currentSourceRevisionId !== null)
            wakeStatement.run(
              target.currentSourceRevisionId,
              pointerVersion,
              response.writerEpoch,
              now,
              target.poemId,
              modelKey,
              target.currentSourceRevisionId,
              target.currentSourceNfcSha256 ?? null,
            );
          if (isProductionResolutionFingerprintTarget(demand)) {
            fingerprintWakeStatement.run(
              now,
              demand.fingerprintAlgorithm,
              demand.lineNfcHash,
              demand.promptMaterialHash,
              modelKey,
            );
            this.#retireFingerprintTargetDemand(demand, modelKey, now);
          } else {
            this.#retireCompatibleCanonicalDemand(
              target.poemId,
              modelKey,
              target.currentSourceRevisionId,
              target.currentSourceNfcSha256 ?? null,
              now,
            );
          }
        }
      }
      this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'succeeded', lease_owner = NULL, lease_token = NULL,
                  lease_expires_at = NULL, updated_at = ?
            WHERE scope_hash = ?`,
        )
        .run(now, claim.scopeHash);
      return "merged" as const;
    })();
    return result;
  }

  retry(
    claim: ProductionResolutionDemandClaim,
    errorCode: string,
    retryAt: number,
  ): void {
    const parsedCode = ResolutionErrorCodeSchema.parse(errorCode);
    const result = this.#database.transaction(() => {
      const updated = this.#database
        .prepare(
          `UPDATE resolution_request
            SET state = 'retry_wait', retry_at = ?, last_error_code = ?,
                lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
                updated_at = ?
          WHERE scope_hash = ? AND state = 'running' AND lease_token = ?
            AND lease_epoch = ?`,
        )
        .run(
          retryAt,
          parsedCode,
          this.#now(),
          claim.scopeHash,
          claim.leaseToken,
          claim.leaseEpoch,
        );
      if (
        updated.changes === 1 &&
        claim.request.schemaVersion === 3 &&
        claim.request.targets.length === 1 &&
        isTerminalFingerprintFailure(parsedCode)
      ) {
        const target = claim.request.targets[0];
        if (!target)
          throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_TARGET_MISSING");
        const wake = this.#database.prepare(
          `INSERT INTO resolution_fingerprint_wakeup (work_key, created_at)
           SELECT waiter.work_key, ?
             FROM resolution_fingerprint_waiter waiter
            WHERE waiter.algorithm = ? AND waiter.line_nfc_hash = ?
              AND waiter.prompt_material_hash = ? AND waiter.model_key = ?
           ON CONFLICT(work_key) DO UPDATE SET created_at = excluded.created_at`,
        );
        for (const modelKey of target.modelKeys)
          wake.run(
            this.#now(),
            target.fingerprintAlgorithm,
            target.lineNfcHash,
            target.promptMaterialHash,
            modelKey,
          );
      }
      if (
        updated.changes === 1 &&
        claim.request.schemaVersion === 2 &&
        claim.request.targets.length === 1 &&
        isTerminalCanonicalFailure(parsedCode)
      ) {
        const target = claim.request.targets[0];
        if (!target)
          throw new Error("PRODUCTION_RESOLUTION_CANONICAL_TARGET_MISSING");
        if (isProductionResolutionSourceTarget(target))
          throw new Error("PRODUCTION_RESOLUTION_CANONICAL_TARGET_INVALID");
        const wake = this.#database.prepare(
          `INSERT INTO resolution_fingerprint_wakeup (work_key, created_at)
           SELECT fingerprint.work_key, ?
             FROM publication_waiter publication
             JOIN resolution_fingerprint_waiter fingerprint USING(work_key)
            WHERE publication.poem_id = ?
              AND publication.source_revision_id = ?
              AND publication.model_key = ?
           ON CONFLICT(work_key) DO UPDATE SET created_at = excluded.created_at`,
        );
        for (const modelKey of target.modelKeys)
          wake.run(
            this.#now(),
            target.poemId,
            target.sourceRevisionId,
            modelKey,
          );
      }
      return updated;
    })();
    if (result.changes !== 1)
      throw new Error("PRODUCTION_RESOLUTION_LEASE_LOST");
  }

  bisect(
    claim: ProductionResolutionDemandClaim,
    retryAt: number,
    singletonErrorCode = "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
  ): number {
    if (claim.request.targets.length === 1) {
      this.retry(claim, singletonErrorCode, retryAt);
      return 0;
    }
    const midpoint = Math.ceil(claim.request.targets.length / 2);
    const requests = [
      claim.request.targets.slice(0, midpoint),
      claim.request.targets.slice(midpoint),
    ].map((targets) =>
      normalizeProductionResolutionRequest({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: claim.request.schemaVersion,
        targets,
      }),
    );
    const now = this.#now();
    return this.#database.transaction(() => {
      const updated = this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'superseded', lease_owner = NULL, lease_token = NULL,
                  lease_expires_at = NULL, updated_at = ?
            WHERE scope_hash = ? AND state = 'running' AND lease_token = ?
              AND lease_epoch = ?`,
        )
        .run(now, claim.scopeHash, claim.leaseToken, claim.leaseEpoch);
      if (updated.changes !== 1)
        throw new Error("PRODUCTION_RESOLUTION_LEASE_LOST");
      const insertRequest = this.#database.prepare(UPSERT_SPLIT_REQUEST_SQL);
      for (const request of requests) {
        const requestJson = canonicalJson(request);
        const scopeHash = sha256(requestJson);
        insertRequest.run(scopeHash, requestJson, now, now, now);
        this.#insertRequestMembers(scopeHash, request);
      }
      return requests.length;
    })();
  }

  resolveCollected(source: WorkItem, artifact: unknown) {
    assertCollectedArtifactBinding(source, artifact);
    const identity = CollectedIdentitySchema.parse(artifact).source;
    const row = queryOptional(
      { operation: "productionResolutionDemand.collectedMapping" },
      () =>
        this.#database
          .prepare(
            `SELECT target.source_poem_id, target.source_author_slug,
                  target.poem_id, target.author_id, target.author_name_arabic,
                  target.current_source_revision_id,
                  target.current_source_nfc_sha256,
                  target.source_pointer_version,
                  scope.observed_at, scope.writer_epoch
             FROM resolution_target target
             JOIN resolution_scope scope USING(scope_hash)
            WHERE target.source_poem_id = ? AND target.source_author_slug = ?
              AND scope.expires_at > ?
            ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(identity.numericId, identity.author.slug, this.#now()),
      MappingRowSchema,
    );
    if (!row) return null;
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: row.author_name_arabic,
        linesArabic: identity.lines,
        titleArabic: identity.title,
      }),
    );
    const lineNfcHash = sha256(sourceLineNfcHashBody(identity.lines));
    if (lineNfcHash !== row.current_source_nfc_sha256) return null;
    const bindingIdentity: Omit<
      CanonicalPoemBindingV1,
      "admissionEvidence" | "bindingId"
    > = {
      authorId: row.author_id,
      authorNameArabic: row.author_name_arabic,
      externalPoemId: row.source_poem_id,
      lineNfcHash,
      poemId: row.poem_id,
      promptMaterialHash,
      schemaId: CANONICAL_POEM_BINDING_SCHEMA_ID,
      schemaVersion: CANONICAL_POEM_BINDING_SCHEMA_VERSION,
      sourceName: currentSource().name,
      sourceRevisionId: row.current_source_revision_id,
    };
    return {
      binding: CanonicalPoemBindingV1Schema.parse({
        ...bindingIdentity,
        admissionEvidence: {
          databaseId: SAQI_PRODUCTION_DATABASE_ID,
          issuedAt: new Date(row.observed_at).toISOString(),
          sourcePointerVersion: row.source_pointer_version,
        },
        bindingId: sha256(canonicalPoemBindingIdBody(bindingIdentity)),
      }),
      mapping: {
        authorId: row.author_id,
        authorNameArabic: row.author_name_arabic,
        canonicalPoemId: row.poem_id,
        poemId: row.poem_id,
        sourceAuthorSlug: row.source_author_slug,
        sourcePoemId: row.source_poem_id,
      },
      observedAt: new Date(row.observed_at).toISOString(),
      writerEpoch: row.writer_epoch,
    };
  }

  resolveCanonicalEnrichment(
    input: PoemEnrichmentInput,
    modelKey: string,
  ): CanonicalEnrichmentResolution {
    const row = queryOptional(
      { operation: "productionResolutionDemand.canonicalMapping" },
      () =>
        this.#database
          .prepare(
            `SELECT target.source_poem_id, target.source_author_slug,
                  target.poem_id, target.author_id, target.author_name_arabic,
                  target.current_source_revision_id,
                  target.current_source_nfc_sha256,
                  target.source_pointer_version,
                  model.model_key, scope.observed_at, scope.writer_epoch
             FROM resolution_target target
             CROSS JOIN resolution_scope scope
             CROSS JOIN resolution_model model
            WHERE target.poem_id = ?
              AND scope.scope_hash = target.scope_hash
              AND model.scope_hash = target.scope_hash
              AND model.source_poem_id = target.source_poem_id
              AND model.source_author_slug = target.source_author_slug
              AND model.model_key = ? AND scope.expires_at > ?
            ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(input.poemId, modelKey, this.#now()),
      CanonicalMappingRowSchema,
    );
    if (!row) return { status: "pending" };
    const lineNfcHash = sha256(sourceLineNfcHashBody(input.linesArabic));
    if (lineNfcHash !== row.current_source_nfc_sha256)
      return { code: "SOURCE_BINDING_CONTENT_MISMATCH", status: "conflict" };
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: row.author_name_arabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    );
    const identity: Omit<
      CanonicalPoemBindingV1,
      "admissionEvidence" | "bindingId"
    > = {
      authorId: row.author_id,
      authorNameArabic: row.author_name_arabic,
      externalPoemId: row.source_poem_id,
      lineNfcHash,
      poemId: row.poem_id,
      promptMaterialHash,
      schemaId: CANONICAL_POEM_BINDING_SCHEMA_ID,
      schemaVersion: CANONICAL_POEM_BINDING_SCHEMA_VERSION,
      sourceName: currentSource().name,
      sourceRevisionId: row.current_source_revision_id,
    };
    return {
      binding: CanonicalPoemBindingV1Schema.parse({
        ...identity,
        admissionEvidence: {
          databaseId: SAQI_PRODUCTION_DATABASE_ID,
          issuedAt: new Date(row.observed_at).toISOString(),
          sourcePointerVersion: row.source_pointer_version,
        },
        bindingId: sha256(canonicalPoemBindingIdBody(identity)),
      }),
      status: "resolved",
    };
  }

  resolveFingerprintEnrichment(
    input: PoemEnrichmentInput,
    modelKey: string,
  ): FingerprintEnrichmentResolution {
    const lineNfcHash = sha256(sourceLineNfcHashBody(input.linesArabic));
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: input.authorArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    );
    const row = queryOptional(
      { operation: "productionResolutionDemand.fingerprintMapping" },
      () =>
        this.#database
          .prepare(
            `SELECT target.source_poem_id, target.source_author_slug,
                  target.poem_id, target.author_id, target.author_name_arabic,
                  target.current_source_revision_id,
                  target.current_source_nfc_sha256,
                  target.source_pointer_version,
                  target.fingerprint_algorithm,
                  target.prompt_material_hash,
                  model.model_key, scope.observed_at, scope.writer_epoch
             FROM resolution_target target
             JOIN resolution_scope scope USING(scope_hash)
             JOIN resolution_model model
               ON model.scope_hash = target.scope_hash
              AND model.source_poem_id = target.source_poem_id
              AND model.source_author_slug = target.source_author_slug
            WHERE target.fingerprint_algorithm = ?
              AND target.current_source_nfc_sha256 = ?
              AND target.prompt_material_hash = ?
              AND model.model_key = ? AND scope.expires_at > ?
            ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(
            PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
            lineNfcHash,
            promptMaterialHash,
            modelKey,
            this.#now(),
          ),
      FingerprintMappingRowSchema,
    );
    if (!row) return { status: "pending" };
    const identity: Omit<
      CanonicalPoemBindingV1,
      "admissionEvidence" | "bindingId"
    > = {
      authorId: row.author_id,
      authorNameArabic: row.author_name_arabic,
      externalPoemId: row.source_poem_id,
      lineNfcHash: row.current_source_nfc_sha256,
      poemId: row.poem_id,
      promptMaterialHash: row.prompt_material_hash,
      schemaId: CANONICAL_POEM_BINDING_SCHEMA_ID,
      schemaVersion: CANONICAL_POEM_BINDING_SCHEMA_VERSION,
      sourceName: currentSource().name,
      sourceRevisionId: row.current_source_revision_id,
    };
    return {
      binding: CanonicalPoemBindingV1Schema.parse({
        ...identity,
        admissionEvidence: {
          databaseId: SAQI_PRODUCTION_DATABASE_ID,
          issuedAt: new Date(row.observed_at).toISOString(),
          sourcePointerVersion: row.source_pointer_version,
        },
        bindingId: sha256(canonicalPoemBindingIdBody(identity)),
      }),
      status: "resolved",
    };
  }

  resolveOrRegisterFingerprintEnrichment(
    input: FingerprintResolutionWaiterInput,
  ): FingerprintResolutionRegistration {
    const parsed = FingerprintWaiterInputSchema.parse(input);
    const lineNfcHash = sha256(sourceLineNfcHashBody(parsed.input.linesArabic));
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: parsed.input.authorArabic,
        linesArabic: parsed.input.linesArabic,
        titleArabic: parsed.input.titleArabic,
      }),
    );
    return this.#database.transaction(() => {
      const existing = queryOptional(
        { operation: "productionResolutionDemand.fingerprintWaiter" },
        () =>
          this.#database
            .prepare(
              `SELECT algorithm, line_nfc_hash, prompt_material_hash, model_key
               FROM resolution_fingerprint_waiter WHERE work_key = ?`,
            )
            .get(parsed.workKey),
        FingerprintWaiterIdentityRowSchema,
      );
      if (
        existing &&
        (existing.line_nfc_hash !== lineNfcHash ||
          existing.prompt_material_hash !== promptMaterialHash ||
          existing.model_key !== parsed.modelKey)
      )
        throw new Error("PRODUCTION_RESOLUTION_WAITER_IDENTITY_CONFLICT");
      const resolution = this.resolveFingerprintEnrichment(
        parsed.input,
        parsed.modelKey,
      );
      if (resolution.status === "resolved") {
        this.#database
          .prepare(DELETE_FINGERPRINT_WAKEUP_SQL)
          .run(parsed.workKey);
        this.#database
          .prepare(DELETE_FINGERPRINT_WAITER_SQL)
          .run(parsed.workKey);
        return resolution;
      }
      const now = this.#now();
      this.#database
        .prepare(
          `INSERT INTO resolution_fingerprint_waiter (
             work_key, algorithm, line_nfc_hash, prompt_material_hash,
             model_key, priority, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(work_key) DO UPDATE SET
             priority = max(resolution_fingerprint_waiter.priority,
                            excluded.priority),
             updated_at = excluded.updated_at`,
        )
        .run(
          parsed.workKey,
          PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash,
          promptMaterialHash,
          parsed.modelKey,
          parsed.priority,
          now,
          now,
        );
      this.registerFingerprintDemand(
        parsed.input,
        parsed.modelKey,
        parsed.priority,
      );
      return { status: "waiting" as const };
    })();
  }

  listFingerprintWakeups(limit = 100): readonly { readonly workKey: string }[] {
    const bounded = WakeupLimitSchema.parse(limit);
    return queryMany(
      { operation: "productionResolutionDemand.fingerprintWakeups" },
      () =>
        this.#database
          .prepare(
            `SELECT wakeup.work_key
           FROM resolution_fingerprint_wakeup wakeup
           JOIN resolution_fingerprint_waiter waiter USING(work_key)
          ORDER BY waiter.priority DESC, wakeup.created_at, wakeup.work_key
          LIMIT ?`,
          )
          .all(bounded),
      FingerprintWakeupRowSchema,
    ).map(({ work_key }) => ({ workKey: work_key }));
  }

  /**
   * Returns canonical-only waiters whose lookup is terminal so their fanout
   * jobs can immediately register the fingerprint fallback. The terminal
   * request is itself durable, so this notification remains restart-safe
   * without another acknowledgement table.
   */
  listTerminalCanonicalWakeups(
    limit = 100,
  ): readonly { readonly workKey: string }[] {
    const bounded = WakeupLimitSchema.parse(limit);
    return queryMany(
      { operation: "productionResolutionDemand.terminalCanonicalWakeups" },
      () =>
        this.#database
          .prepare(
            `SELECT DISTINCT waiter.work_key
           FROM publication_waiter waiter
           JOIN resolution_canonical_request_member member
             ON member.poem_id = waiter.poem_id
            AND member.model_key = waiter.model_key
            AND member.source_revision_id = waiter.source_revision_id
           JOIN resolution_request request USING(scope_hash)
          WHERE request.state = 'retry_wait'
            AND request.last_error_code IN (
              'PRODUCTION_RESOLUTION_TARGET_UNRESOLVED',
              'PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT',
              'PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT'
            )
            AND json_array_length(request.request_json, '$.targets') = 1
          ORDER BY waiter.priority DESC, waiter.created_at, waiter.work_key
          LIMIT ?`,
          )
          .all(bounded),
      FingerprintWakeupRowSchema,
    ).map(({ work_key }) => ({ workKey: work_key }));
  }

  acknowledgeFingerprintWakeups(workKeys: readonly string[]): number {
    const keys = WakeupWorkKeysSchema.parse(workKeys);
    return this.#database.transaction(() => {
      const remove = this.#database.prepare(DELETE_FINGERPRINT_WAKEUP_SQL);
      let acknowledged = 0;
      for (const key of new Set(keys)) {
        const result = remove.run(key);
        if (result.changes === 1) acknowledged += 1;
      }
      return acknowledged;
    })();
  }

  retireFingerprintWaiter(workKey: string): void {
    const key = WaiterWorkKeySchema.parse(workKey);
    this.#database.transaction(() => {
      this.#database.prepare(DELETE_FINGERPRINT_WAKEUP_SQL).run(key);
      this.#database.prepare(DELETE_FINGERPRINT_WAITER_SQL).run(key);
    })();
  }

  retirePublicationWaiter(workKey: string): void {
    const key = WaiterWorkKeySchema.parse(workKey);
    this.#database.transaction(() => {
      this.#database.prepare(DELETE_PUBLICATION_WAKEUP_SQL).run(key);
      this.#database
        .prepare("DELETE FROM publication_waiter WHERE work_key = ?")
        .run(key);
    })();
  }

  resolvePublication(
    poemId: string,
    modelKey: string,
    requiredSourceRevisionId?: string,
    sourceNfcSha256?: string,
  ): null | PublicationResolution {
    const row = queryOptional(
      { operation: "productionResolutionDemand.publication" },
      () =>
        this.#database
          .prepare(
            `SELECT target.current_source_nfc_sha256,
                  target.current_source_revision_id, model.pointer_version,
                  scope.writer_epoch
             FROM resolution_target target
             JOIN resolution_scope scope USING(scope_hash)
             JOIN resolution_model model
               ON model.scope_hash = target.scope_hash
              AND model.source_poem_id = target.source_poem_id
              AND model.source_author_slug = target.source_author_slug
            WHERE target.poem_id = ? AND model.model_key = ?
              AND scope.expires_at > ?
            ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(poemId, modelKey, this.#now()),
      PublicationRowSchema,
    );
    if (row?.current_source_revision_id === undefined) return null;
    if (row.current_source_revision_id === null) return null;
    if (
      requiredSourceRevisionId !== undefined &&
      row.current_source_revision_id !== requiredSourceRevisionId &&
      (sourceNfcSha256 === undefined ||
        row.current_source_nfc_sha256 !== sourceNfcSha256)
    )
      return null;
    return {
      expectedPointerVersion: row.pointer_version,
      sourceRevisionId: row.current_source_revision_id,
      writerEpoch: row.writer_epoch,
    };
  }

  refreshPublicationConflict(input: {
    readonly eventId: string;
    readonly expectedPointerVersion: null | number;
    readonly expectedWriterEpoch: number;
    readonly modelKey: string;
    readonly poemId: string;
    readonly priority: number;
    readonly sourceRevisionId: string;
  }): null | PublicationResolution {
    const current = this.resolvePublication(
      input.poemId,
      input.modelKey,
      input.sourceRevisionId,
    );
    if (
      current &&
      (current.expectedPointerVersion !== input.expectedPointerVersion ||
        current.writerEpoch !== input.expectedWriterEpoch)
    )
      return current;
    const target = queryOptional(
      { operation: "productionResolutionDemand.conflictTarget" },
      () =>
        this.#database
          .prepare(
            `SELECT target.source_poem_id, target.source_author_slug
               FROM resolution_target target
               JOIN resolution_scope scope USING(scope_hash)
               JOIN resolution_model model
                 ON model.scope_hash = target.scope_hash
                AND model.source_poem_id = target.source_poem_id
                AND model.source_author_slug = target.source_author_slug
              WHERE target.poem_id = ?
                AND target.current_source_revision_id = ?
                AND model.model_key = ? AND scope.expires_at > ?
              ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
          )
          .get(
            input.poemId,
            input.sourceRevisionId,
            input.modelKey,
            this.#now(),
          ),
      PublicationSourceTargetRowSchema,
    );
    if (!target) {
      this.registerPublicationDemand(
        input.poemId,
        input.modelKey,
        input.sourceRevisionId,
        input.priority,
      );
      return null;
    }
    const inserted = this.wakeSourceResolution({
      eventId: input.eventId,
      targets: [
        {
          sourceAuthorSlug: target.source_author_slug,
          sourcePoemId: target.source_poem_id,
        },
      ],
    });
    return inserted
      ? null
      : this.resolvePublication(
          input.poemId,
          input.modelKey,
          input.sourceRevisionId,
        );
  }

  refreshCollectionConflict(input: {
    readonly eventId: string;
    readonly expectedWriterEpoch: number;
    readonly targets: readonly SourceResolutionTarget[];
  }): null | number {
    const writerEpoch = this.#writerEpochForTargets(input.targets);
    if (writerEpoch !== null && writerEpoch !== input.expectedWriterEpoch)
      return writerEpoch;
    const inserted = this.wakeSourceResolution({
      eventId: input.eventId,
      targets: input.targets,
    });
    if (inserted) return null;
    const refreshed = this.#writerEpochForTargets(input.targets);
    return refreshed === input.expectedWriterEpoch ? null : refreshed;
  }

  resolveOrRegisterPublication(
    input: PublicationResolutionWaiterInput,
  ): PublicationResolutionRegistration {
    const parsed = PublicationWaiterInputSchema.parse(input);
    return this.#database.transaction(() => {
      const existing = queryOptional(
        { operation: "productionResolutionDemand.publicationWaiter" },
        () =>
          this.#database
            .prepare(
              `SELECT poem_id, model_key, source_revision_id, source_nfc_sha256
               FROM publication_waiter WHERE work_key = ?`,
            )
            .get(parsed.workKey),
        PublicationWaiterIdentityRowSchema,
      );
      if (
        existing &&
        (existing.poem_id !== parsed.poemId ||
          existing.model_key !== parsed.modelKey ||
          existing.source_revision_id !== parsed.sourceRevisionId ||
          (existing.source_nfc_sha256 !== null &&
            existing.source_nfc_sha256 !== parsed.sourceNfcSha256))
      )
        throw new Error("PRODUCTION_RESOLUTION_WAITER_IDENTITY_CONFLICT");
      const resolution = this.resolvePublication(
        parsed.poemId,
        parsed.modelKey,
        parsed.sourceRevisionId,
        parsed.sourceNfcSha256,
      );
      if (resolution !== null) {
        this.#database
          .prepare(DELETE_PUBLICATION_WAKEUP_SQL)
          .run(parsed.workKey);
        return { resolution, status: "resolved" as const };
      }
      const now = this.#now();
      this.#database
        .prepare(
          `INSERT INTO publication_waiter (
             work_key, poem_id, model_key, source_revision_id,
             source_nfc_sha256, priority, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(work_key) DO UPDATE SET
             source_nfc_sha256 = COALESCE(
               publication_waiter.source_nfc_sha256,
               excluded.source_nfc_sha256
             ),
             priority = max(publication_waiter.priority, excluded.priority),
             updated_at = excluded.updated_at`,
        )
        .run(
          parsed.workKey,
          parsed.poemId,
          parsed.modelKey,
          parsed.sourceRevisionId,
          parsed.sourceNfcSha256 ?? null,
          parsed.priority,
          now,
          now,
        );
      this.registerPublicationDemand(
        parsed.poemId,
        parsed.modelKey,
        parsed.sourceRevisionId,
        parsed.priority,
      );
      return { status: "waiting" as const };
    })();
  }

  getPublicationFingerprint(workKey: string): null | string {
    const key = WaiterWorkKeySchema.parse(workKey);
    return (
      queryScalar(
        { operation: "productionResolutionDemand.publicationFingerprint" },
        () =>
          this.#database
            .prepare(
              "SELECT source_nfc_sha256 FROM publication_waiter WHERE work_key = ?",
            )
            .pluck()
            .get(key),
        Sha256Schema.nullable(),
      ) ?? null
    );
  }

  listPublicationWakeups(limit = 100): readonly PublicationWakeup[] {
    const bounded = WakeupLimitSchema.parse(limit);
    return queryMany(
      { operation: "productionResolutionDemand.publicationWakeups" },
      () =>
        this.#database
          .prepare(
            `SELECT wakeup.work_key, wakeup.poem_id, wakeup.model_key,
                wakeup.source_revision_id, wakeup.expected_pointer_version,
                wakeup.writer_epoch
           FROM publication_wakeup wakeup
           JOIN publication_waiter waiter USING(work_key)
          ORDER BY waiter.priority DESC, wakeup.created_at, wakeup.work_key
          LIMIT ?`,
          )
          .all(bounded),
      PublicationWakeupRowSchema,
    ).map((row) => {
      return {
        modelKey: row.model_key,
        poemId: row.poem_id,
        resolution: {
          expectedPointerVersion: row.expected_pointer_version,
          sourceRevisionId: row.source_revision_id,
          writerEpoch: row.writer_epoch,
        },
        workKey: row.work_key,
      };
    });
  }

  acknowledgePublicationWakeups(workKeys: readonly string[]): number {
    const keys = WakeupWorkKeysSchema.parse(workKeys);
    return this.#database.transaction(() => {
      const removeWakeup = this.#database.prepare(
        DELETE_PUBLICATION_WAKEUP_SQL,
      );
      let acknowledged = 0;
      for (const key of new Set(keys)) {
        const result = removeWakeup.run(key);
        if (result.changes === 1) acknowledged += 1;
      }
      return acknowledged;
    })();
  }

  counts(): ProductionResolutionDemandCounts {
    const count = (table: string) =>
      queryRequired(
        { operation: `productionResolutionDemand.count.${table}` },
        () =>
          this.#database.prepare(`SELECT count(*) FROM ${table}`).pluck().get(),
        CountSchema,
      );
    return {
      demands:
        count("resolution_demand") +
        count("resolution_canonical_demand") +
        count("resolution_fingerprint_demand"),
      requests: count("resolution_request"),
      scopes: count("resolution_scope"),
    };
  }

  metrics(): ProductionResolutionDemandMetrics {
    const stateCount = (state: "retry_wait" | "superseded") =>
      queryRequired(
        { operation: `productionResolutionDemand.stateCount.${state}` },
        () =>
          this.#database
            .prepare("SELECT count(*) FROM resolution_request WHERE state = ?")
            .pluck()
            .get(state),
        CountSchema,
      );
    return {
      canonicalBurst: this.#canonicalBurst(),
      canonicalModeBurst: this.#canonicalModeBurst(),
      fingerprintSplitBurst: this.#fingerprintSplitBurst(),
      retryWaitRequests: stateCount("retry_wait"),
      sourceBurst: this.#sourceBurst(),
      supersededRequests: stateCount("superseded"),
    };
  }

  #writerEpochForTargets(
    targets: readonly SourceResolutionTarget[],
  ): null | number {
    if (targets.length === 0) return null;
    const epochs = targets.map((target) =>
      queryOptional(
        { operation: "productionResolutionDemand.writerEpoch" },
        () =>
          this.#database
            .prepare(
              `SELECT scope.writer_epoch
               FROM resolution_target target
               JOIN resolution_scope scope USING(scope_hash)
              WHERE target.source_poem_id = ?
                AND target.source_author_slug = ? AND scope.expires_at > ?
              ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
            )
            .get(target.sourcePoemId, target.sourceAuthorSlug, this.#now()),
        WriterEpochRowSchema,
      ),
    );
    if (epochs.some((epoch) => !epoch)) return null;
    return Math.max(
      ...epochs.flatMap((epoch) => (epoch ? [epoch.writer_epoch] : [])),
    );
  }

  #insertAndClaim(
    request: ProductionResolutionRequest,
    now: number,
  ): null | ProductionResolutionDemandClaim {
    const requestJson = canonicalJson(request);
    const scopeHash = sha256(requestJson);
    const insert = this.#database.transaction(() => {
      this.#database
        .prepare(UPSERT_PENDING_REQUEST_SQL)
        .run(scopeHash, requestJson, now, now, now);
      this.#insertRequestMembers(scopeHash, request);
    });
    insert();
    const row = queryRequired(
      { operation: "productionResolutionDemand.insertedRequest" },
      () =>
        this.#database
          .prepare(
            `SELECT scope_hash, request_json, lease_epoch
             FROM resolution_request WHERE scope_hash = ?`,
          )
          .get(scopeHash),
      RequestRowSchema,
    );
    return this.#claimRow(row, now);
  }

  #canonicalBurst(): number {
    return this.#scheduler().canonical_burst;
  }

  #canonicalModeBurst(): number {
    return this.#scheduler().canonical_mode_burst;
  }

  #fingerprintSplitBurst(): number {
    return this.#scheduler().fingerprint_split_burst;
  }

  #scheduler(): z.infer<typeof SchedulerRowSchema> {
    return queryRequired(
      { operation: "productionResolutionDemand.scheduler" },
      () =>
        this.#database
          .prepare(
            `SELECT canonical_burst, canonical_mode_burst,
                  fingerprint_split_burst, source_burst
             FROM resolution_scheduler WHERE singleton = 1`,
          )
          .get(),
      SchedulerRowSchema,
    );
  }

  #sourceBurst(): number {
    return this.#scheduler().source_burst;
  }

  #oldestDueRequest(now: number): undefined | z.infer<typeof RequestRowSchema> {
    return queryOptional(
      { operation: "productionResolutionDemand.oldestDue" },
      () =>
        this.#database
          .prepare(
            `SELECT scope_hash, request_json, lease_epoch
           FROM resolution_request
          WHERE state IN ('pending', 'retry_wait') AND retry_at <= ?
          ORDER BY created_at, scope_hash LIMIT 1`,
          )
          .get(now),
      RequestRowSchema,
    );
  }

  #oldestDueSourceRequest(
    now: number,
  ): undefined | z.infer<typeof RequestRowSchema> {
    return queryOptional(
      { operation: "productionResolutionDemand.oldestDueSource" },
      () =>
        this.#database
          .prepare(
            `SELECT scope_hash, request_json, lease_epoch
           FROM resolution_request
          WHERE state IN ('pending', 'retry_wait') AND retry_at <= ?
            AND schema_version = 1
          ORDER BY created_at, scope_hash LIMIT 1`,
          )
          .get(now),
      RequestRowSchema,
    );
  }

  #oldestDueBoundRequest(
    now: number,
    mode: "all" | "canonical" | "fingerprint" = "all",
  ): undefined | z.infer<typeof RequestRowSchema> {
    const schemaFilter =
      mode === "all" ? "IN (2, 3)" : mode === "canonical" ? "= 2" : "= 3";
    // Rank compact request identities before fetching the winning JSON. Large
    // request payloads otherwise travel through the grouped candidate scan.
    return queryOptional(
      { operation: "productionResolutionDemand.oldestDueBound" },
      () =>
        this.#database
          .prepare(
            `WITH selected AS MATERIALIZED (
           SELECT request.scope_hash
           FROM resolution_request request
           JOIN (
             SELECT member.scope_hash, demand.priority AS value
               FROM resolution_canonical_request_member member
               JOIN resolution_canonical_demand demand
                 ON demand.poem_id = member.poem_id
                AND demand.source_revision_id = member.source_revision_id
                AND demand.model_key = member.model_key
             UNION ALL
             SELECT member.scope_hash, demand.priority AS value
               FROM resolution_fingerprint_request_member member
               JOIN resolution_fingerprint_demand demand
                 ON demand.algorithm = member.algorithm
                AND demand.line_nfc_hash = member.line_nfc_hash
                AND demand.prompt_material_hash = member.prompt_material_hash
                AND demand.model_key = member.model_key
           ) priority ON priority.scope_hash = request.scope_hash
          WHERE request.state IN ('pending', 'retry_wait')
            AND request.retry_at <= ?
            AND request.schema_version ${schemaFilter}
          GROUP BY request.scope_hash
          ORDER BY MAX(priority.value) DESC, request.created_at,
                   request.scope_hash LIMIT 1
           )
           SELECT request.scope_hash, request.request_json, request.lease_epoch
           FROM selected
           JOIN resolution_request request USING(scope_hash)`,
          )
          .get(now),
      RequestRowSchema,
    );
  }

  #oldestDueBoundSplitRequest(
    now: number,
  ): undefined | z.infer<typeof RequestRowSchema> {
    return queryOptional(
      { operation: "productionResolutionDemand.oldestDueSplit" },
      () =>
        this.#database
          .prepare(
            `SELECT scope_hash, request_json, lease_epoch
           FROM resolution_request
          WHERE state = 'retry_wait' AND retry_at <= ?
            AND attempt_count = 0 AND last_error_code IS NULL
            AND schema_version IN (2, 3)
          ORDER BY created_at, scope_hash LIMIT 1`,
          )
          .get(now),
      RequestRowSchema,
    );
  }

  #preferredUncoveredBoundMode(
    now: number,
  ): "canonical" | "fingerprint" | null {
    // Select the best candidate inside each indexed demand schedule first.
    // Applying LIMIT only after UNION forces SQLite to materialize every
    // uncovered demand merely to choose between these two modes.
    const row = queryOptional(
      { operation: "productionResolutionDemand.preferredBoundMode" },
      () =>
        this.#database
          .prepare(
            `SELECT mode FROM (
               SELECT mode, priority, first_seen_at FROM (
                 SELECT 'canonical' AS mode, demand.priority,
                        demand.first_seen_at
                   FROM resolution_canonical_demand demand
                  WHERE NOT EXISTS (
                    SELECT 1 FROM resolution_canonical_request_member member
                    JOIN resolution_request request USING(scope_hash)
                    WHERE member.poem_id = demand.poem_id
                      AND member.source_revision_id = demand.source_revision_id
                      AND member.model_key = demand.model_key
                      AND request.state IN ('pending', 'running', 'retry_wait')
                  )
                  ORDER BY demand.priority DESC, demand.first_seen_at,
                           demand.poem_id, demand.source_revision_id,
                           demand.model_key
                  LIMIT 1
               )
               UNION ALL
               SELECT mode, priority, first_seen_at FROM (
                 SELECT 'fingerprint' AS mode, demand.priority,
                        demand.first_seen_at
                   FROM resolution_fingerprint_demand demand
                  WHERE NOT EXISTS (
                    SELECT 1 FROM resolution_fingerprint_request_member member
                    JOIN resolution_request request USING(scope_hash)
                    WHERE member.algorithm = demand.algorithm
                      AND member.line_nfc_hash = demand.line_nfc_hash
                      AND member.prompt_material_hash = demand.prompt_material_hash
                      AND member.model_key = demand.model_key
                      AND request.state IN ('pending', 'running', 'retry_wait')
                  )
                    AND NOT EXISTS (
                      SELECT 1 FROM resolution_target target
                      JOIN resolution_scope scope USING(scope_hash)
                      JOIN resolution_model model
                        ON model.scope_hash = target.scope_hash
                       AND model.source_poem_id = target.source_poem_id
                       AND model.source_author_slug = target.source_author_slug
                      WHERE target.fingerprint_algorithm = demand.algorithm
                        AND target.current_source_nfc_sha256 = demand.line_nfc_hash
                        AND target.prompt_material_hash = demand.prompt_material_hash
                        AND model.model_key = demand.model_key
                        AND scope.expires_at > ?
                    )
                  ORDER BY demand.priority DESC, demand.first_seen_at,
                           demand.algorithm, demand.line_nfc_hash,
                           demand.prompt_material_hash, demand.model_key
                  LIMIT 1
               )
             ) ORDER BY priority DESC, first_seen_at, mode LIMIT 1`,
          )
          .get(now),
      BoundModeRowSchema,
    );
    return row?.mode ?? null;
  }

  #buildRequest(
    now: number,
    mode: "all" | "canonical" | "fingerprint" | "source" = "all",
    candidateLimit = MAX_PRODUCTION_RESOLUTION_TARGETS * 3,
  ): null | ProductionResolutionRequest {
    if (mode === "fingerprint") return this.#buildFingerprintRequest(now);
    const canonicalRows =
      mode === "source"
        ? []
        : queryMany(
            { operation: "productionResolutionDemand.canonicalCandidates" },
            () =>
              this.#database
                .prepare(
                  `SELECT demand.poem_id, demand.source_revision_id,
                demand.model_key, demand.priority, demand.first_seen_at
           FROM resolution_canonical_demand demand
          WHERE NOT EXISTS (
            SELECT 1 FROM resolution_canonical_request_member member
            JOIN resolution_request request USING(scope_hash)
            WHERE member.poem_id = demand.poem_id
              AND member.source_revision_id = demand.source_revision_id
              AND member.model_key = demand.model_key
              AND request.state IN ('pending', 'running', 'retry_wait')
          )
            AND NOT EXISTS (
              SELECT 1 FROM resolution_target target
              JOIN resolution_scope scope USING(scope_hash)
              JOIN resolution_model model
                ON model.scope_hash = target.scope_hash
               AND model.source_poem_id = target.source_poem_id
               AND model.source_author_slug = target.source_author_slug
              WHERE target.poem_id = demand.poem_id
                AND target.current_source_revision_id = demand.source_revision_id
                AND model.model_key = demand.model_key
                AND scope.expires_at > ?
            )
          ORDER BY demand.priority DESC, demand.first_seen_at,
                   demand.poem_id, demand.source_revision_id, demand.model_key
          LIMIT ?`,
                )
                .all(now, candidateLimit),
            CanonicalDemandRowSchema,
          );
    const rows =
      mode === "canonical"
        ? []
        : queryMany(
            { operation: "productionResolutionDemand.sourceCandidates" },
            () =>
              this.#database
                .prepare(
                  `SELECT demand.source_poem_id, demand.source_author_slug,
                    demand.model_key, demand.priority, demand.first_seen_at
               FROM resolution_demand demand
              WHERE NOT EXISTS (
                SELECT 1 FROM resolution_request_member member
                JOIN resolution_request request USING(scope_hash)
                WHERE member.source_poem_id = demand.source_poem_id
                  AND member.source_author_slug = demand.source_author_slug
                  AND member.model_key = demand.model_key
                  AND request.state IN ('pending', 'running', 'retry_wait')
              )
                AND NOT EXISTS (
                  SELECT 1 FROM resolution_target target
                  JOIN resolution_scope scope USING(scope_hash)
                  JOIN resolution_model model
                    ON model.scope_hash = target.scope_hash
                   AND model.source_poem_id = target.source_poem_id
                   AND model.source_author_slug = target.source_author_slug
                  WHERE target.source_poem_id = demand.source_poem_id
                    AND target.source_author_slug = demand.source_author_slug
                    AND target.current_source_nfc_sha256 IS NOT NULL
                    AND model.model_key = demand.model_key
                    AND scope.expires_at > ?
                )
              ORDER BY demand.priority DESC, demand.first_seen_at,
                       demand.source_poem_id, demand.source_author_slug,
                       demand.model_key
              LIMIT ?`,
                )
                .all(now, candidateLimit),
            DemandRowSchema,
          );
    const grouped = new Map<
      string,
      {
        firstSeenAt: number;
        modelKeys: string[];
        priority: number;
        sourceAuthorSlug: string;
        sourcePoemId: string;
      }
    >();
    for (const row of rows) {
      const key = `${row.source_poem_id}\u{1f}${row.source_author_slug}`;
      const target = grouped.get(key) ?? {
        firstSeenAt: row.first_seen_at,
        modelKeys: [],
        priority: row.priority,
        sourceAuthorSlug: row.source_author_slug,
        sourcePoemId: row.source_poem_id,
      };
      if (!target.modelKeys.includes(row.model_key))
        target.modelKeys.push(row.model_key);
      grouped.set(key, target);
    }
    const canonicalGrouped = new Map<
      string,
      {
        firstSeenAt: number;
        modelKeys: string[];
        poemId: string;
        priority: number;
        sourceRevisionId: string;
      }
    >();
    for (const row of canonicalRows) {
      const key = `${row.poem_id}\u{1f}${row.source_revision_id}`;
      const target = canonicalGrouped.get(key) ?? {
        firstSeenAt: row.first_seen_at,
        modelKeys: [],
        poemId: row.poem_id,
        priority: row.priority,
        sourceRevisionId: row.source_revision_id,
      };
      if (!target.modelKeys.includes(row.model_key))
        target.modelKeys.push(row.model_key);
      canonicalGrouped.set(key, target);
    }
    const candidates = [
      ...(canonicalGrouped.size > 0
        ? canonicalGrouped.values()
        : grouped.values()),
    ].toSorted(
      (left, right) =>
        right.priority - left.priority || left.firstSeenAt - right.firstSeenAt,
    );
    let targets: ProductionResolutionRequest["targets"] = [];
    for (const candidate of candidates) {
      if (targets.length >= MAX_PRODUCTION_RESOLUTION_TARGETS) break;
      const {
        firstSeenAt: _firstSeenAt,
        priority: _priority,
        ...target
      } = candidate;
      const proposed = [...targets, target];
      const request = normalizeProductionResolutionRequest({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: canonicalGrouped.size > 0 ? 2 : 1,
        targets: proposed,
      });
      if (
        Buffer.byteLength(canonicalJson(request)) >
        MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES
      )
        break;
      targets = request.targets;
    }
    if (targets.length === 0) return null;
    return normalizeProductionResolutionRequest({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: canonicalGrouped.size > 0 ? 2 : 1,
      targets,
    });
  }

  #buildFingerprintRequest(
    now: number,
    candidateLimit = MAX_PRODUCTION_RESOLUTION_TARGETS * 3,
  ): null | ProductionResolutionRequest {
    const rows = queryMany(
      { operation: "productionResolutionDemand.fingerprintCandidates" },
      () =>
        this.#database
          .prepare(
            `SELECT demand.algorithm, demand.line_nfc_hash,
                demand.prompt_material_hash, demand.model_key,
                demand.priority, demand.first_seen_at
           FROM resolution_fingerprint_demand demand
          WHERE NOT EXISTS (
            SELECT 1 FROM resolution_fingerprint_request_member member
            JOIN resolution_request request USING(scope_hash)
            WHERE member.algorithm = demand.algorithm
              AND member.line_nfc_hash = demand.line_nfc_hash
              AND member.prompt_material_hash = demand.prompt_material_hash
              AND member.model_key = demand.model_key
              AND request.state IN ('pending', 'running', 'retry_wait')
          )
            AND NOT EXISTS (
              SELECT 1 FROM resolution_target target
              JOIN resolution_scope scope USING(scope_hash)
              JOIN resolution_model model
                ON model.scope_hash = target.scope_hash
               AND model.source_poem_id = target.source_poem_id
               AND model.source_author_slug = target.source_author_slug
              WHERE target.fingerprint_algorithm = demand.algorithm
                AND target.current_source_nfc_sha256 = demand.line_nfc_hash
                AND target.prompt_material_hash = demand.prompt_material_hash
                AND model.model_key = demand.model_key
                AND scope.expires_at > ?
            )
          ORDER BY demand.priority DESC, demand.first_seen_at,
                   demand.algorithm, demand.line_nfc_hash,
                   demand.prompt_material_hash, demand.model_key
          LIMIT ?`,
          )
          .all(now, candidateLimit),
      FingerprintDemandRowSchema,
    );
    const grouped = new Map<
      string,
      {
        fingerprintAlgorithm: typeof PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM;
        firstSeenAt: number;
        lineNfcHash: string;
        modelKeys: string[];
        priority: number;
        promptMaterialHash: string;
      }
    >();
    for (const row of rows) {
      const key = `${row.algorithm}\u{1f}${row.line_nfc_hash}\u{1f}${row.prompt_material_hash}`;
      const target = grouped.get(key) ?? {
        fingerprintAlgorithm: row.algorithm,
        firstSeenAt: row.first_seen_at,
        lineNfcHash: row.line_nfc_hash,
        modelKeys: [],
        priority: row.priority,
        promptMaterialHash: row.prompt_material_hash,
      };
      if (!target.modelKeys.includes(row.model_key))
        target.modelKeys.push(row.model_key);
      grouped.set(key, target);
    }
    // eslint-disable-next-line unicorn/prefer-iterator-to-array -- The configured TypeScript library does not expose Iterator#toArray yet.
    const candidates = [...grouped.values()].toSorted(
      (left, right) =>
        right.priority - left.priority || left.firstSeenAt - right.firstSeenAt,
    );
    let targets: Extract<
      ProductionResolutionRequest,
      { schemaVersion: 3 }
    >["targets"] = [];
    for (const candidate of candidates) {
      if (targets.length >= MAX_FINGERPRINT_RESOLUTION_TARGETS) break;
      const {
        firstSeenAt: _firstSeenAt,
        priority: _priority,
        ...target
      } = candidate;
      const request = normalizeProductionResolutionRequest({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 3,
        targets: [...targets, target],
      });
      if (request.schemaVersion !== 3)
        throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_REQUEST_INVALID");
      if (
        Buffer.byteLength(canonicalJson(request)) >
        MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES
      )
        break;
      targets = request.targets;
    }
    if (targets.length === 0) return null;
    return normalizeProductionResolutionRequest({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 3,
      targets,
    });
  }

  #insertRequestMembers(
    scopeHash: string,
    request: ProductionResolutionRequest,
  ): void {
    const sourceMember = this.#database.prepare(INSERT_REQUEST_MEMBER_SQL);
    const canonicalMember = this.#database.prepare(
      INSERT_CANONICAL_REQUEST_MEMBER_SQL,
    );
    const fingerprintMember = this.#database.prepare(
      INSERT_FINGERPRINT_REQUEST_MEMBER_SQL,
    );
    for (const target of request.targets)
      for (const modelKey of target.modelKeys)
        if (isProductionResolutionFingerprintTarget(target))
          fingerprintMember.run(
            scopeHash,
            target.fingerprintAlgorithm,
            target.lineNfcHash,
            target.promptMaterialHash,
            modelKey,
          );
        else if (isProductionResolutionSourceTarget(target))
          sourceMember.run(
            scopeHash,
            target.sourcePoemId,
            target.sourceAuthorSlug,
            modelKey,
          );
        else
          canonicalMember.run(
            scopeHash,
            target.poemId,
            target.sourceRevisionId,
            modelKey,
          );
  }

  #claimRow(
    row: z.infer<typeof RequestRowSchema>,
    now: number,
    fingerprintSplit = false,
  ): null | ProductionResolutionDemandClaim {
    const request = ProductionResolutionRequestSchema.parse(
      JSON.parse(row.request_json),
    );
    const source = request.schemaVersion === 1;
    const fingerprint = request.schemaVersion === 3;
    const leaseToken = randomUUID();
    const leaseEpoch = row.lease_epoch + 1;
    const updated = this.#database.transaction(() => {
      const claimed = this.#database
        .prepare(
          `UPDATE resolution_request
              SET state = 'running', lease_owner = ?, lease_token = ?,
                  lease_epoch = ?, lease_expires_at = ?,
                  attempt_count = attempt_count + 1, updated_at = ?
            WHERE scope_hash = ? AND state IN ('pending', 'retry_wait')
              AND retry_at <= ?`,
        )
        .run(
          this.#owner,
          leaseToken,
          leaseEpoch,
          now + this.#leaseMs,
          now,
          row.scope_hash,
          now,
        );
      if (claimed.changes === 1)
        this.#database
          .prepare(
            `UPDATE resolution_scheduler
                SET canonical_burst = ?, canonical_mode_burst = ?,
                    fingerprint_split_burst = ?, source_burst = ?,
                    updated_at = ?
              WHERE singleton = 1`,
          )
          .run(
            fingerprintSplit
              ? this.#canonicalBurst()
              : source
                ? 0
                : Math.min(
                    MAX_CANONICAL_CLAIM_BURST,
                    this.#canonicalBurst() + 1,
                  ),
            fingerprintSplit
              ? this.#canonicalModeBurst()
              : fingerprint
                ? 0
                : Math.min(
                    MAX_CANONICAL_MODE_CLAIM_BURST,
                    this.#canonicalModeBurst() + 1,
                  ),
            fingerprintSplit
              ? Math.min(
                  MAX_FINGERPRINT_SPLIT_CLAIM_BURST,
                  this.#fingerprintSplitBurst() + 1,
                )
              : 0,
            fingerprintSplit
              ? this.#sourceBurst()
              : source
                ? Math.min(MAX_SOURCE_CLAIM_BURST, this.#sourceBurst() + 1)
                : fingerprint
                  ? this.#sourceBurst()
                  : 0,
            now,
          );
      return claimed;
    })();
    if (updated.changes !== 1) return this.claim();
    return {
      leaseEpoch,
      leaseToken,
      request,
      scopeHash: row.scope_hash,
    };
  }

  #retireCompatibleCanonicalDemand(
    poemId: string,
    modelKey: string,
    currentSourceRevisionId: null | string,
    currentSourceNfcSha256: null | string,
    now: number,
  ): void {
    if (currentSourceRevisionId === null) return;
    const compatible = `(
      member.source_revision_id = ? OR EXISTS (
        SELECT 1 FROM publication_waiter waiter
         WHERE waiter.poem_id = member.poem_id
           AND waiter.model_key = member.model_key
           AND waiter.source_revision_id = member.source_revision_id
           AND waiter.source_nfc_sha256 IS NOT NULL
           AND waiter.source_nfc_sha256 = ?
      )
    )`;
    this.#database
      .prepare(
        `UPDATE resolution_request
            SET state = 'superseded', lease_owner = NULL, lease_token = NULL,
                lease_expires_at = NULL, updated_at = ?
          WHERE state IN ('pending', 'retry_wait')
            AND scope_hash IN (
              SELECT member.scope_hash
                FROM resolution_canonical_request_member member
               WHERE member.poem_id = ? AND member.model_key = ?
                 AND ${compatible}
            )`,
      )
      .run(
        now,
        poemId,
        modelKey,
        currentSourceRevisionId,
        currentSourceNfcSha256,
      );
    this.#database
      .prepare(
        `DELETE FROM resolution_canonical_demand AS demand
          WHERE demand.poem_id = ? AND demand.model_key = ?
            AND (demand.source_revision_id = ? OR EXISTS (
              SELECT 1 FROM publication_waiter waiter
               WHERE waiter.poem_id = demand.poem_id
                 AND waiter.model_key = demand.model_key
                 AND waiter.source_revision_id = demand.source_revision_id
                 AND waiter.source_nfc_sha256 IS NOT NULL
                 AND waiter.source_nfc_sha256 = ?
            ))`,
      )
      .run(poemId, modelKey, currentSourceRevisionId, currentSourceNfcSha256);
  }

  #retireFingerprintTargetDemand(
    target: {
      readonly fingerprintAlgorithm: typeof PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM;
      readonly lineNfcHash: string;
      readonly promptMaterialHash: string;
    },
    modelKey: string,
    now: number,
  ): void {
    this.#database
      .prepare(
        `UPDATE resolution_request
            SET state = 'superseded', lease_owner = NULL, lease_token = NULL,
                lease_expires_at = NULL, updated_at = ?
          WHERE state IN ('pending', 'retry_wait') AND scope_hash IN (
            SELECT member.scope_hash
              FROM resolution_fingerprint_request_member member
             WHERE member.algorithm = ? AND member.line_nfc_hash = ?
               AND member.prompt_material_hash = ? AND member.model_key = ?
          )`,
      )
      .run(
        now,
        target.fingerprintAlgorithm,
        target.lineNfcHash,
        target.promptMaterialHash,
        modelKey,
      );
    this.#database
      .prepare(
        `DELETE FROM resolution_fingerprint_demand
          WHERE algorithm = ? AND line_nfc_hash = ?
            AND prompt_material_hash = ? AND model_key = ?`,
      )
      .run(
        target.fingerprintAlgorithm,
        target.lineNfcHash,
        target.promptMaterialHash,
        modelKey,
      );
  }

  #supersedeOrphanedCanonicalRequests(now: number): void {
    // Membership tables are schema-exclusive and indexed by scope_hash. Avoid
    // parsing every due request_json blob merely to rediscover its schema.
    // Select orphan keys through the covering ready index before updating:
    // a direct state-filtered UPDATE fetches every large request row even
    // when its demand is still present and no mutation is necessary.
    this.#database
      .prepare(
        `UPDATE resolution_request
            SET state = 'superseded', lease_owner = NULL, lease_token = NULL,
                lease_expires_at = NULL, updated_at = ?
          WHERE scope_hash IN (
            SELECT scope_hash FROM resolution_request
            WHERE state IN ('pending', 'retry_wait')
            AND EXISTS (
              SELECT 1
                FROM resolution_canonical_request_member member
                LEFT JOIN resolution_canonical_demand demand
                  ON demand.poem_id = member.poem_id
                 AND demand.source_revision_id = member.source_revision_id
                 AND demand.model_key = member.model_key
               WHERE member.scope_hash = resolution_request.scope_hash
                 AND demand.poem_id IS NULL
            )
          )`,
      )
      .run(now);
    this.#database
      .prepare(
        `UPDATE resolution_request
            SET state = 'superseded', lease_owner = NULL,
                lease_token = NULL, lease_expires_at = NULL, updated_at = ?
          WHERE scope_hash IN (
            SELECT scope_hash FROM resolution_request
            WHERE state IN ('pending','retry_wait')
            AND EXISTS (
              SELECT 1
                FROM resolution_fingerprint_request_member member
                LEFT JOIN resolution_fingerprint_demand demand
                  ON demand.algorithm = member.algorithm
                 AND demand.line_nfc_hash = member.line_nfc_hash
                 AND demand.prompt_material_hash = member.prompt_material_hash
                 AND demand.model_key = member.model_key
               WHERE member.scope_hash = resolution_request.scope_hash
                 AND demand.algorithm IS NULL
            )
          )`,
      )
      .run(now);
  }

  #applySourceResolutionRefreshes(now: number): void {
    const pending = queryScalar(
      { operation: "productionResolutionDemand.pendingRefresh" },
      () =>
        this.#database
          .prepare(
            `SELECT 1 FROM source_resolution_refresh_event
          WHERE applied_at IS NULL LIMIT 1`,
          )
          .pluck()
          .get(),
      PendingRefreshFlagSchema,
    );
    if (pending === undefined) return;
    this.#database.transaction(() => {
      this.#database
        .prepare(
          `DELETE FROM resolution_target
            WHERE EXISTS (
              SELECT 1 FROM source_resolution_refresh_event event
              JOIN source_resolution_refresh_target target USING(event_id)
              WHERE event.applied_at IS NULL
                AND target.source_poem_id = resolution_target.source_poem_id
                AND target.source_author_slug = resolution_target.source_author_slug
            )`,
        )
        .run();
      this.#database
        .prepare(
          `UPDATE resolution_request AS request
              SET state = 'pending', retry_at = ?, updated_at = ?
            WHERE request.state IN ('pending', 'retry_wait')
              AND EXISTS (
                SELECT 1 FROM resolution_request_member member
                JOIN source_resolution_refresh_target target
                  ON target.source_poem_id = member.source_poem_id
                 AND target.source_author_slug = member.source_author_slug
                JOIN source_resolution_refresh_event event USING(event_id)
                WHERE member.scope_hash = request.scope_hash
                  AND event.applied_at IS NULL
              )`,
        )
        .run(now, now);
      this.#database
        .prepare(
          `UPDATE source_resolution_refresh_event AS event
              SET applied_at = ?
            WHERE event.applied_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM source_resolution_refresh_target target
                JOIN resolution_request_member member
                  ON member.source_poem_id = target.source_poem_id
                 AND member.source_author_slug = target.source_author_slug
                JOIN resolution_request request USING(scope_hash)
                WHERE target.event_id = event.event_id
                  AND request.state = 'running'
              )`,
        )
        .run(now);
    })();
  }

  #migrate(): void {
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS resolution_cache_migration (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
      ) STRICT;
    `);
    const applied = new Set(
      queryMany(
        { operation: "productionResolutionDemand.migrations" },
        () =>
          this.#database
            .prepare("SELECT version FROM resolution_cache_migration")
            .pluck()
            .all(),
        SqliteSafeIntegerSchema.positive(),
      ),
    );
    if (!applied.has(1)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(cacheSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (?, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(1, this.#now());
      });
      migrate();
    }
    if (!applied.has(2)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(canonicalDemandSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (2, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(3)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(schedulerSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (3, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(4)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(`
          ALTER TABLE resolution_scheduler ADD COLUMN source_burst INTEGER
            NOT NULL DEFAULT 0
            CHECK(source_burst BETWEEN 0 AND ${String(MAX_SOURCE_CLAIM_BURST)});
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (4, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(5)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(`
          ALTER TABLE resolution_target
            ADD COLUMN current_source_nfc_sha256 TEXT
            CHECK(current_source_nfc_sha256 IS NULL OR length(current_source_nfc_sha256) = 64);
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (5, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(6)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(publicationWakeupSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (6, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(7)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(cacheHotPathIndexSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (7, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(8)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(fingerprintDemandSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (8, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(9)) {
      const migrate = this.#database.transaction(() => {
        const now = this.#now();
        // Cache v8 could receive the new fingerprint-specific conflict body,
        // but its client-side enum collapsed those responses to HTTP_409.
        // Retry only v3 singleton requests immediately so v9 records the
        // precise terminal code and wakes the exact waiter.
        this.#database
          .prepare(
            `UPDATE resolution_request
                SET state = 'pending', retry_at = ?, updated_at = ?
              WHERE state = 'retry_wait'
                AND last_error_code = 'PRODUCTION_RESOLUTION_HTTP_409'
                AND json_extract(request_json, '$.schemaVersion') = 3
                AND json_array_length(request_json, '$.targets') = 1`,
          )
          .run(now, now);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (9, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(now);
      });
      migrate();
    }
    if (!applied.has(10)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(`
          ALTER TABLE resolution_scheduler
            ADD COLUMN canonical_mode_burst INTEGER NOT NULL DEFAULT 0
            CHECK(canonical_mode_burst BETWEEN 0 AND ${String(MAX_CANONICAL_MODE_CLAIM_BURST)});
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (10, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(11)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(`
          ALTER TABLE resolution_scheduler
            ADD COLUMN fingerprint_split_burst INTEGER NOT NULL DEFAULT 0
            CHECK(fingerprint_split_burst BETWEEN 0 AND ${String(MAX_FINGERPRINT_SPLIT_CLAIM_BURST)});
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (11, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(12)) {
      const migrate = this.#database.transaction(() => {
        const now = this.#now();
        // Fingerprint requests are now emitted as exact singletons. Older
        // all-or-nothing failures may have left an unattempted bisection tree
        // covering the same durable demands. Retire only those multi-target
        // children so the singleton builder can reissue each demand directly;
        // attempted and terminal rows remain intact as audit history.
        this.#database
          .prepare(
            `UPDATE resolution_request
                SET state = 'superseded', lease_owner = NULL,
                    lease_token = NULL, lease_expires_at = NULL,
                    updated_at = ?
              WHERE state = 'retry_wait' AND attempt_count = 0
                AND json_extract(request_json, '$.schemaVersion') = 3
                AND json_array_length(request_json, '$.targets') > 1`,
          )
          .run(now);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (12, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(now);
      });
      migrate();
    }
    if (!applied.has(13)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(sourceResolutionRefreshSchemaSql());
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (13, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(14)) {
      const migrate = this.#database.transaction(() => {
        this.#database.exec(`
          CREATE INDEX IF NOT EXISTS source_resolution_refresh_event_unapplied
            ON source_resolution_refresh_event(created_at, event_id)
            WHERE applied_at IS NULL;
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (14, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(15)) {
      const migrate = this.#database.transaction(() => {
        const now = this.#now();
        this.#database
          .prepare(
            `INSERT INTO resolution_fingerprint_wakeup (work_key, created_at)
             SELECT fingerprint.work_key, ?
               FROM resolution_fingerprint_waiter fingerprint
              WHERE EXISTS (
                SELECT 1
                  FROM resolution_fingerprint_request_member member
                  JOIN resolution_request request USING(scope_hash)
                 WHERE member.algorithm = fingerprint.algorithm
                   AND member.line_nfc_hash = fingerprint.line_nfc_hash
                   AND member.prompt_material_hash = fingerprint.prompt_material_hash
                   AND member.model_key = fingerprint.model_key
                   AND request.state = 'retry_wait'
                   AND request.last_error_code IN (
                     'PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED',
                     'PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE',
                     'PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS',
                     'PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT'
                   )
                   AND json_array_length(request.request_json, '$.targets') = 1
              ) OR EXISTS (
                SELECT 1
                  FROM publication_waiter publication
                  JOIN resolution_canonical_request_member member
                    ON member.poem_id = publication.poem_id
                   AND member.source_revision_id = publication.source_revision_id
                   AND member.model_key = publication.model_key
                  JOIN resolution_request request USING(scope_hash)
                 WHERE publication.work_key = fingerprint.work_key
                   AND request.state = 'retry_wait'
                   AND request.last_error_code IN (
                     'PRODUCTION_RESOLUTION_TARGET_UNRESOLVED',
                     'PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT',
                     'PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT'
                   )
                   AND json_array_length(request.request_json, '$.targets') = 1
              )
             ON CONFLICT(work_key) DO UPDATE SET created_at = excluded.created_at`,
          )
          .run(now);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (15, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(now);
      });
      migrate();
    }
    if (!applied.has(16)) {
      const migrate = this.#database.transaction(() => {
        const columns = queryMany(
          { operation: "productionResolutionDemand.requestColumns" },
          () =>
            this.#database
              .prepare("PRAGMA table_xinfo(resolution_request)")
              .all(),
          RequestTableInfoRowSchema,
        );
        if (!columns.includes("schema_version"))
          this.#database.exec(`
            ALTER TABLE resolution_request ADD COLUMN schema_version INTEGER
              GENERATED ALWAYS AS (
                CAST(json_extract(request_json, '$.schemaVersion') AS INTEGER)
              ) VIRTUAL;
          `);
        this.#database.exec(`
          CREATE INDEX IF NOT EXISTS resolution_request_ready_schema
            ON resolution_request(
              schema_version, state, retry_at, created_at, scope_hash
            );
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (16, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
    if (!applied.has(17)) {
      const migrate = this.#database.transaction(() => {
        // Candidate membership checks need only state, not large request JSON
        // payloads. Keep those probes on compact covering index pages.
        this.#database.exec(`
          CREATE INDEX IF NOT EXISTS resolution_request_identity_state
            ON resolution_request(scope_hash, state);
        `);
        this.#database
          .prepare(
            `INSERT INTO resolution_cache_migration (version, applied_at)
             VALUES (17, ?) ON CONFLICT(version) DO NOTHING`,
          )
          .run(this.#now());
      });
      migrate();
    }
  }
}

function sourceResolutionRefreshSchemaSql(): string {
  return `
    CREATE TABLE source_resolution_refresh_event (
      event_id TEXT PRIMARY KEY CHECK(
        length(event_id) = 64 AND event_id NOT GLOB '*[^0-9a-f]*'
      ),
      created_at INTEGER NOT NULL,
      applied_at INTEGER
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE source_resolution_refresh_target (
      event_id TEXT NOT NULL REFERENCES source_resolution_refresh_event(event_id)
        ON DELETE CASCADE,
      source_poem_id TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      PRIMARY KEY(event_id, source_poem_id, source_author_slug)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX source_resolution_refresh_target_source
      ON source_resolution_refresh_target(
        source_poem_id, source_author_slug, event_id
      );
    CREATE INDEX source_resolution_refresh_event_unapplied
      ON source_resolution_refresh_event(created_at, event_id)
      WHERE applied_at IS NULL;
  `;
}

function fingerprintDemandSchemaSql(): string {
  return `
    ALTER TABLE resolution_target
      ADD COLUMN fingerprint_algorithm TEXT
      CHECK(fingerprint_algorithm IS NULL
        OR fingerprint_algorithm = '${PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM}');
    ALTER TABLE resolution_target
      ADD COLUMN prompt_material_hash TEXT
      CHECK(prompt_material_hash IS NULL OR (
        length(prompt_material_hash) = 64
        AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
      ));
    CREATE TABLE resolution_fingerprint_demand (
      algorithm TEXT NOT NULL CHECK(
        algorithm = '${PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM}'
      ),
      line_nfc_hash TEXT NOT NULL CHECK(
        length(line_nfc_hash) = 64
        AND line_nfc_hash NOT GLOB '*[^0-9a-f]*'
      ),
      prompt_material_hash TEXT NOT NULL CHECK(
        length(prompt_material_hash) = 64
        AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
      ),
      model_key TEXT NOT NULL,
      priority INTEGER NOT NULL,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL,
      PRIMARY KEY(algorithm, line_nfc_hash, prompt_material_hash, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE resolution_fingerprint_request_member (
      scope_hash TEXT NOT NULL REFERENCES resolution_request(scope_hash),
      algorithm TEXT NOT NULL CHECK(
        algorithm = '${PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM}'
      ),
      line_nfc_hash TEXT NOT NULL CHECK(length(line_nfc_hash) = 64),
      prompt_material_hash TEXT NOT NULL CHECK(length(prompt_material_hash) = 64),
      model_key TEXT NOT NULL,
      PRIMARY KEY(
        scope_hash, algorithm, line_nfc_hash, prompt_material_hash, model_key
      )
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE resolution_fingerprint_waiter (
      work_key TEXT PRIMARY KEY CHECK(
        length(work_key) = 64 AND work_key NOT GLOB '*[^0-9a-f]*'
      ),
      algorithm TEXT NOT NULL CHECK(
        algorithm = '${PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM}'
      ),
      line_nfc_hash TEXT NOT NULL CHECK(
        length(line_nfc_hash) = 64 AND line_nfc_hash NOT GLOB '*[^0-9a-f]*'
      ),
      prompt_material_hash TEXT NOT NULL CHECK(
        length(prompt_material_hash) = 64
        AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
      ),
      model_key TEXT NOT NULL,
      priority INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE resolution_fingerprint_wakeup (
      work_key TEXT PRIMARY KEY
        REFERENCES resolution_fingerprint_waiter(work_key) ON DELETE CASCADE,
      created_at INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX resolution_fingerprint_demand_schedule
      ON resolution_fingerprint_demand(
        priority DESC, first_seen_at, algorithm, line_nfc_hash,
        prompt_material_hash, model_key
      );
    CREATE INDEX resolution_fingerprint_request_member_demand
      ON resolution_fingerprint_request_member(
        algorithm, line_nfc_hash, prompt_material_hash, model_key, scope_hash
      );
    CREATE INDEX resolution_fingerprint_waiter_identity
      ON resolution_fingerprint_waiter(
        algorithm, line_nfc_hash, prompt_material_hash, model_key,
        priority DESC, created_at, work_key
      );
    CREATE INDEX resolution_fingerprint_wakeup_ready
      ON resolution_fingerprint_wakeup(created_at, work_key);
    CREATE INDEX resolution_target_fingerprint_lookup
      ON resolution_target(
        fingerprint_algorithm, current_source_nfc_sha256,
        prompt_material_hash, scope_hash, source_poem_id, source_author_slug
      ) WHERE fingerprint_algorithm IS NOT NULL;
  `;
}

function isTerminalFingerprintFailure(code: string): boolean {
  return [
    "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
    "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
    "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
    "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
  ].includes(code);
}

function isTerminalCanonicalFailure(code: string): boolean {
  return [
    "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
    "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
    "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
  ].includes(code);
}

function cacheHotPathIndexSql(): string {
  return `
    CREATE INDEX IF NOT EXISTS resolution_target_canonical_lookup
      ON resolution_target(
        poem_id, current_source_revision_id, scope_hash,
        source_poem_id, source_author_slug
      );
    CREATE INDEX IF NOT EXISTS resolution_target_resolved_source
      ON resolution_target(
        source_poem_id, source_author_slug, scope_hash
      ) WHERE current_source_nfc_sha256 IS NOT NULL;
    CREATE INDEX IF NOT EXISTS resolution_demand_schedule
      ON resolution_demand(
        priority DESC, first_seen_at,
        source_poem_id, source_author_slug, model_key
      );
    CREATE INDEX IF NOT EXISTS resolution_canonical_demand_schedule
      ON resolution_canonical_demand(
        priority DESC, first_seen_at,
        poem_id, source_revision_id, model_key
      );
  `;
}

function publicationWakeupSchemaSql(): string {
  return `
    CREATE TABLE IF NOT EXISTS publication_waiter (
      work_key TEXT PRIMARY KEY CHECK(length(work_key) = 64),
      poem_id TEXT NOT NULL,
      model_key TEXT NOT NULL,
      source_revision_id TEXT NOT NULL CHECK(length(source_revision_id) = 64),
      source_nfc_sha256 TEXT CHECK(
        source_nfc_sha256 IS NULL OR length(source_nfc_sha256) = 64
      ),
      priority INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS publication_waiter_resolution
      ON publication_waiter(
        poem_id, model_key, source_revision_id, source_nfc_sha256, work_key
      );
    CREATE TABLE IF NOT EXISTS publication_wakeup (
      work_key TEXT PRIMARY KEY REFERENCES publication_waiter(work_key)
        ON DELETE CASCADE,
      poem_id TEXT NOT NULL,
      model_key TEXT NOT NULL,
      source_revision_id TEXT NOT NULL CHECK(length(source_revision_id) = 64),
      expected_pointer_version INTEGER,
      writer_epoch INTEGER NOT NULL CHECK(writer_epoch > 0),
      created_at INTEGER NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS publication_wakeup_ready
      ON publication_wakeup(created_at, work_key);
  `;
}

function schedulerSchemaSql(): string {
  return `
    CREATE TABLE IF NOT EXISTS resolution_scheduler (
      singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
      canonical_burst INTEGER NOT NULL
        CHECK(canonical_burst BETWEEN 0 AND ${String(MAX_CANONICAL_CLAIM_BURST)}),
      updated_at INTEGER NOT NULL
    ) STRICT;
    INSERT INTO resolution_scheduler (singleton, canonical_burst, updated_at)
    VALUES (1, 0, 0) ON CONFLICT(singleton) DO NOTHING;
  `;
}

function canonicalDemandSchemaSql(): string {
  return `
    CREATE TABLE IF NOT EXISTS resolution_canonical_demand (
      poem_id TEXT NOT NULL,
      source_revision_id TEXT NOT NULL CHECK(length(source_revision_id) = 64),
      model_key TEXT NOT NULL,
      priority INTEGER NOT NULL,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL,
      PRIMARY KEY(poem_id, source_revision_id, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS resolution_canonical_request_member (
      scope_hash TEXT NOT NULL REFERENCES resolution_request(scope_hash),
      poem_id TEXT NOT NULL,
      source_revision_id TEXT NOT NULL,
      model_key TEXT NOT NULL,
      PRIMARY KEY(scope_hash, poem_id, source_revision_id, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_canonical_request_member_demand
      ON resolution_canonical_request_member(
        poem_id, source_revision_id, model_key, scope_hash
      );
  `;
}

function secretsEqual(left: string, right: string): boolean {
  const leftDigest = hash("sha256", left, "buffer");
  const rightDigest = hash("sha256", right, "buffer");
  return timingSafeEqual(leftDigest, rightDigest);
}

function cacheSchemaSql(): string {
  return `
    CREATE TABLE IF NOT EXISTS resolution_demand (
      source_poem_id TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      model_key TEXT NOT NULL,
      priority INTEGER NOT NULL,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL,
      PRIMARY KEY(source_poem_id, source_author_slug, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS resolution_request (
      scope_hash TEXT PRIMARY KEY CHECK(length(scope_hash) = 64),
      request_json TEXT NOT NULL CHECK(json_valid(request_json)),
      schema_version INTEGER GENERATED ALWAYS AS (
        CAST(json_extract(request_json, '$.schemaVersion') AS INTEGER)
      ) VIRTUAL,
      state TEXT NOT NULL CHECK(state IN (
        'pending', 'running', 'retry_wait', 'succeeded', 'superseded'
      )),
      retry_at INTEGER NOT NULL,
      attempt_count INTEGER NOT NULL CHECK(attempt_count >= 0),
      lease_owner TEXT,
      lease_token TEXT,
      lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= 0),
      lease_expires_at INTEGER,
      last_error_code TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      CHECK((state = 'running') = (lease_owner IS NOT NULL)),
      CHECK((state = 'running') = (lease_token IS NOT NULL)),
      CHECK((state = 'running') = (lease_expires_at IS NOT NULL))
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_request_ready
      ON resolution_request(state, retry_at, created_at, scope_hash);
    CREATE INDEX IF NOT EXISTS resolution_request_ready_schema
      ON resolution_request(
        schema_version, state, retry_at, created_at, scope_hash
      );
    CREATE TABLE IF NOT EXISTS resolution_request_member (
      scope_hash TEXT NOT NULL REFERENCES resolution_request(scope_hash),
      source_poem_id TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      model_key TEXT NOT NULL,
      PRIMARY KEY(scope_hash, source_poem_id, source_author_slug, model_key)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_request_member_demand
      ON resolution_request_member(
        source_poem_id, source_author_slug, model_key, scope_hash
      );
    CREATE TABLE IF NOT EXISTS resolution_scope (
      scope_hash TEXT PRIMARY KEY REFERENCES resolution_request(scope_hash),
      manifest_hash TEXT NOT NULL CHECK(length(manifest_hash) = 64),
      observed_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      writer_epoch INTEGER NOT NULL CHECK(writer_epoch > 0),
      response_json TEXT NOT NULL CHECK(json_valid(response_json)),
      updated_at INTEGER NOT NULL,
      CHECK(expires_at > observed_at)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_scope_fresh
      ON resolution_scope(expires_at, observed_at DESC, scope_hash);
    CREATE TABLE IF NOT EXISTS resolution_target (
      scope_hash TEXT NOT NULL REFERENCES resolution_scope(scope_hash)
        ON DELETE CASCADE,
      source_poem_id TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      poem_id TEXT NOT NULL,
      author_id TEXT NOT NULL,
      author_name_arabic TEXT NOT NULL,
      current_source_revision_id TEXT,
      source_pointer_version INTEGER,
      PRIMARY KEY(scope_hash, source_poem_id, source_author_slug)
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_target_source
      ON resolution_target(source_poem_id, source_author_slug, scope_hash);
    CREATE INDEX IF NOT EXISTS resolution_target_poem
      ON resolution_target(poem_id, scope_hash);
    CREATE TABLE IF NOT EXISTS resolution_model (
      scope_hash TEXT NOT NULL,
      source_poem_id TEXT NOT NULL,
      source_author_slug TEXT NOT NULL,
      model_key TEXT NOT NULL,
      pointer_version INTEGER,
      PRIMARY KEY(
        scope_hash, source_poem_id, source_author_slug, model_key
      ),
      FOREIGN KEY(scope_hash, source_poem_id, source_author_slug)
        REFERENCES resolution_target(
          scope_hash, source_poem_id, source_author_slug
        ) ON DELETE CASCADE
    ) STRICT, WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS resolution_model_lookup
      ON resolution_model(model_key, scope_hash, source_poem_id);
  `;
}
