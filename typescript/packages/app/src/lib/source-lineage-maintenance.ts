import { randomUUID } from "node:crypto";

import type { D1Database } from "@cloudflare/workers-types";
import { SourceNameSchema } from "@saqi/precedent-iso";
import type {
  CorpusRevisionStore,
  LegacySourceLineageAdoptionResult,
} from "@saqi/precedent-node";
import {
  type SourceAdapterProfileV1,
  SourceAdapterProfileV1Schema,
} from "@saqi/source-adapter";
import { z } from "zod";

const PAGE_SIZE = 10;
const DEFAULT_LEASE_MS = 5 * 60_000;
const MaintenanceStateSchema = z.enum([
  "active",
  "blocked",
  "complete",
  "failed",
  "idle",
]);
const MaximumPagesSchema = z.number().int().min(1).max(2);
const OwnerSchema = z.string().trim().min(1).max(128);

const LeaseRowSchema = z
  .strictObject({
    cursor_poem_id: z.string().nullable(),
    lease_epoch: z.number().int().positive(),
    lease_token: z.uuid(),
    pass: z.number().int().nonnegative(),
  })
  .transform(({ cursor_poem_id, lease_epoch, lease_token, pass }) => ({
    cursorPoemId: cursor_poem_id,
    leaseEpoch: lease_epoch,
    leaseToken: lease_token,
    pass,
  }));
const PoemIdResultSchema = z
  .object({
    results: z.array(z.strictObject({ poem_id: z.uuid() })).max(PAGE_SIZE),
  })
  .transform(({ results }) => results.map(({ poem_id }) => poem_id));
const CountRowSchema = z
  .strictObject({ count: z.number().int().nonnegative() })
  .transform(({ count }) => count);
const StatusRowSchema = z
  .strictObject({
    adopted_total: z.number().int().nonnegative(),
    conflict_total: z.number().int().nonnegative(),
    cursor_poem_id: z.string().nullable(),
    last_error_code: z.string().nullable(),
    pass: z.number().int().nonnegative(),
    scanned_total: z.number().int().nonnegative(),
    state: MaintenanceStateSchema,
    updated_at: z.number().int().nonnegative(),
  })
  .transform((row) => ({
    adoptedTotal: row.adopted_total,
    conflictTotal: row.conflict_total,
    cursorPoemId: row.cursor_poem_id,
    lastErrorCode: row.last_error_code,
    pass: row.pass,
    scannedTotal: row.scanned_total,
    state: row.state,
    updatedAt: row.updated_at,
  }));

export interface SourceLineageMaintenanceStatus {
  readonly adoptedTotal: number;
  readonly conflictTotal: number;
  readonly cursorPoemId: null | string;
  readonly lastErrorCode: null | string;
  readonly pass: number;
  readonly remaining: number;
  readonly scannedTotal: number;
  readonly state: "active" | "blocked" | "complete" | "failed" | "idle";
  readonly updatedAt: number;
}

export interface SourceLineageMaintenanceLease {
  readonly cursorPoemId: null | string;
  readonly leaseEpoch: number;
  readonly leaseToken: string;
  readonly pass: number;
}

export interface SourceLineageMaintenanceRepository {
  acquire(
    owner: string,
    now: number,
    leaseMs: number
  ): Promise<null | SourceLineageMaintenanceLease>;
  commitPage(
    lease: SourceLineageMaintenanceLease,
    cursorPoemId: string,
    result: LegacySourceLineageAdoptionResult,
    now: number
  ): Promise<boolean>;
  fail(
    lease: SourceLineageMaintenanceLease,
    code: string,
    now: number
  ): Promise<void>;
  finishPass(
    lease: SourceLineageMaintenanceLease,
    now: number
  ): Promise<boolean>;
  poemIds(lease: SourceLineageMaintenanceLease): Promise<readonly string[]>;
  quarantine(
    lease: SourceLineageMaintenanceLease,
    conflicts: LegacySourceLineageAdoptionResult["conflicts"],
    now: number
  ): Promise<void>;
  remaining(): Promise<number>;
  status(): Promise<Omit<SourceLineageMaintenanceStatus, "remaining">>;
  yieldLease(
    lease: SourceLineageMaintenanceLease,
    now: number
  ): Promise<boolean>;
}

export class D1SourceLineageMaintenanceRepository implements SourceLineageMaintenanceRepository {
  readonly #database: D1Database;
  readonly #sourceName: string;
  readonly #slugPrefix: string;
  readonly #slugSuffix: string;

  constructor(
    database: D1Database,
    options: {
      readonly sourceName: string;
      readonly sourceProfile: SourceAdapterProfileV1;
    }
  ) {
    this.#database = database;
    this.#sourceName = SourceNameSchema.parse(options.sourceName);
    const profile = SourceAdapterProfileV1Schema.parse(options.sourceProfile);
    const marker = "{id}";
    const markerIndex = profile.routes.poemSlug.indexOf(marker);
    this.#slugPrefix = profile.routes.poemSlug
      .slice(0, markerIndex)
      .replace(/^\//u, "");
    this.#slugSuffix = profile.routes.poemSlug.slice(
      markerIndex + marker.length
    );
  }

  async acquire(
    owner: string,
    now: number,
    leaseMs: number
  ): Promise<null | SourceLineageMaintenanceLease> {
    const token = randomUUID();
    const row = await this.#database
      .prepare(
        `
        UPDATE source_lineage_maintenance_job
        SET state = 'active', lease_owner = ?, lease_token = ?,
          lease_epoch = lease_epoch + 1, lease_expires_at = ?, updated_at = ?
        WHERE singleton = 1
          AND (lease_token IS NULL OR lease_expires_at <= ?)
        RETURNING cursor_poem_id, lease_epoch, lease_token, pass
      `
      )
      .bind(owner, token, now + leaseMs, now, now)
      .first<unknown>();
    return row === null ? null : LeaseRowSchema.parse(row);
  }

  async poemIds(
    lease: SourceLineageMaintenanceLease
  ): Promise<readonly string[]> {
    return PoemIdResultSchema.parse(
      await this.#database
        .prepare(
          `
          WITH eligible AS (
            SELECT poem.id, substr(
              poem.slug, length(?) + 1,
              length(poem.slug) - length(?) - length(?)
            ) AS external_id
            FROM poem
            WHERE poem.id > COALESCE(?, '')
              AND length(poem.slug) > length(?) + length(?)
              AND substr(poem.slug, 1, length(?)) = ?
              AND (? = '' OR substr(poem.slug, -length(?)) = ?)
              AND substr(
                poem.slug, length(?) + 1,
                length(poem.slug) - length(?) - length(?)
              ) NOT GLOB '*[^0-9]*'
              AND substr(poem.slug, length(?) + 1, 1) BETWEEN '1' AND '9'
          )
          SELECT poem.id AS poem_id
          FROM eligible
          JOIN poem ON poem.id = eligible.id
          WHERE NOT EXISTS (
              SELECT 1
              FROM source_poem_identity source_poem
              JOIN source_author_identity source_author
                ON source_author.id = source_poem.source_author_id
              JOIN poem_source_revision revision
                ON revision.source_poem_id = source_poem.id
              JOIN poem_source_pointer pointer
                ON pointer.source_poem_id = source_poem.id
               AND pointer.revision_id = revision.id
              WHERE source_poem.source_name = ?
                AND source_poem.canonical_poem_id = poem.id
                AND source_poem.external_id = eligible.external_id
                AND source_poem.tombstoned_at IS NULL
                AND source_author.source_name = ?
                AND source_author.canonical_author_id = poem.author_id
                AND poem.active_source_revision_id = revision.id
            )
            AND NOT EXISTS (
              SELECT 1 FROM source_lineage_conflict conflict
              WHERE conflict.poem_id = poem.id AND conflict.resolved_at IS NULL
            )
          ORDER BY poem.id
          LIMIT ?
        `
        )
        .bind(
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          lease.cursorPoemId,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugSuffix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#sourceName,
          this.#sourceName,
          PAGE_SIZE
        )
        .all<unknown>()
    );
  }

  async quarantine(
    lease: SourceLineageMaintenanceLease,
    conflicts: LegacySourceLineageAdoptionResult["conflicts"],
    now: number
  ): Promise<void> {
    if (conflicts.length === 0) return;
    const statements = conflicts.map(({ code, poemId }) =>
      this.#database
        .prepare(
          `
          INSERT INTO source_lineage_conflict (
            poem_id, error_code, first_seen_at, last_seen_at, attempt_count,
            lease_epoch, resolved_at
          ) SELECT ?, ?, ?, ?, 1, ?, NULL
          WHERE EXISTS (
            SELECT 1 FROM source_lineage_maintenance_job
            WHERE singleton = 1 AND lease_token = ? AND lease_epoch = ?
              AND lease_expires_at > ?
          )
          ON CONFLICT(poem_id) DO UPDATE SET
            error_code = excluded.error_code,
            last_seen_at = excluded.last_seen_at,
            attempt_count = source_lineage_conflict.attempt_count + 1,
            lease_epoch = excluded.lease_epoch,
            resolved_at = NULL
        `
        )
        .bind(
          poemId,
          code,
          now,
          now,
          lease.leaseEpoch,
          lease.leaseToken,
          lease.leaseEpoch,
          now
        )
    );
    await this.#database.batch(statements);
  }

  async commitPage(
    lease: SourceLineageMaintenanceLease,
    cursorPoemId: string,
    result: LegacySourceLineageAdoptionResult,
    now: number
  ): Promise<boolean> {
    const row = await this.#database
      .prepare(
        `
        UPDATE source_lineage_maintenance_job
        SET cursor_poem_id = ?, scanned_total = scanned_total + ?,
          adopted_total = adopted_total + ?, lease_expires_at = ?, updated_at = ?
        WHERE singleton = 1 AND lease_token = ? AND lease_epoch = ?
          AND lease_expires_at > ?
        RETURNING singleton
      `
      )
      .bind(
        cursorPoemId,
        result.scanned,
        result.adopted,
        now + DEFAULT_LEASE_MS,
        now,
        lease.leaseToken,
        lease.leaseEpoch,
        now
      )
      .first<unknown>();
    return row !== null;
  }

  async remaining(): Promise<number> {
    return CountRowSchema.parse(
      await this.#database
        .prepare(
          `
          WITH eligible AS (
            SELECT poem.id, substr(
              poem.slug, length(?) + 1,
              length(poem.slug) - length(?) - length(?)
            ) AS external_id
            FROM poem
            WHERE length(poem.slug) > length(?) + length(?)
              AND substr(poem.slug, 1, length(?)) = ?
              AND (? = '' OR substr(poem.slug, -length(?)) = ?)
              AND substr(
                poem.slug, length(?) + 1,
                length(poem.slug) - length(?) - length(?)
              ) NOT GLOB '*[^0-9]*'
              AND substr(poem.slug, length(?) + 1, 1) BETWEEN '1' AND '9'
          ), unresolved AS (
            SELECT poem.id
            FROM eligible
            JOIN poem ON poem.id = eligible.id
            WHERE NOT EXISTS (
              SELECT 1
              FROM source_poem_identity source_poem
              JOIN source_author_identity source_author
                ON source_author.id = source_poem.source_author_id
              JOIN poem_source_revision revision
                ON revision.source_poem_id = source_poem.id
              JOIN poem_source_pointer pointer
                ON pointer.source_poem_id = source_poem.id
               AND pointer.revision_id = revision.id
              WHERE source_poem.source_name = ?
                AND source_poem.canonical_poem_id = poem.id
                AND source_poem.external_id = eligible.external_id
                AND source_poem.tombstoned_at IS NULL
                AND source_author.source_name = ?
                AND source_author.canonical_author_id = poem.author_id
                AND poem.active_source_revision_id = revision.id
            )
          )
          SELECT COUNT(*) AS count FROM unresolved
        `
        )
        .bind(
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugSuffix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#slugPrefix,
          this.#slugSuffix,
          this.#slugPrefix,
          this.#sourceName,
          this.#sourceName
        )
        .first<unknown>()
    );
  }

  async finishPass(
    lease: SourceLineageMaintenanceLease,
    now: number
  ): Promise<boolean> {
    const remaining = await this.remaining();
    const row = await this.#database
      .prepare(
        `
        UPDATE source_lineage_maintenance_job
        SET state = ?, cursor_poem_id = NULL, pass = pass + 1,
          lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
          last_error_code = ?, updated_at = ?
        WHERE singleton = 1 AND lease_token = ? AND lease_epoch = ?
          AND lease_expires_at > ?
        RETURNING singleton
      `
      )
      .bind(
        remaining === 0 ? "complete" : "blocked",
        remaining === 0 ? null : "SOURCE_LINEAGE_CONFLICTS_REMAIN",
        now,
        lease.leaseToken,
        lease.leaseEpoch,
        now
      )
      .first<unknown>();
    return row !== null;
  }

  async fail(
    lease: SourceLineageMaintenanceLease,
    code: string,
    now: number
  ): Promise<void> {
    await this.#database
      .prepare(
        `
        UPDATE source_lineage_maintenance_job
        SET state = 'failed', last_error_code = ?, lease_owner = NULL,
          lease_token = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE singleton = 1 AND lease_token = ? AND lease_epoch = ?
      `
      )
      .bind(code, now, lease.leaseToken, lease.leaseEpoch)
      .run();
  }

  async status(): Promise<Omit<SourceLineageMaintenanceStatus, "remaining">> {
    return StatusRowSchema.parse(
      await this.#database
        .prepare(
          `
          SELECT adopted_total,
            (SELECT COUNT(*) FROM source_lineage_conflict
             WHERE resolved_at IS NULL) AS conflict_total,
            cursor_poem_id,
            last_error_code, pass, scanned_total, state, updated_at
          FROM source_lineage_maintenance_job WHERE singleton = 1
        `
        )
        .first<unknown>()
    );
  }

  async yieldLease(
    lease: SourceLineageMaintenanceLease,
    now: number
  ): Promise<boolean> {
    const row = await this.#database
      .prepare(
        `
        UPDATE source_lineage_maintenance_job
        SET state = 'idle', lease_owner = NULL, lease_token = NULL,
          lease_expires_at = NULL, last_error_code = NULL, updated_at = ?
        WHERE singleton = 1 AND lease_token = ? AND lease_epoch = ?
        RETURNING singleton
      `
      )
      .bind(now, lease.leaseToken, lease.leaseEpoch)
      .first<unknown>();
    return row !== null;
  }
}

interface SourceLineageMaintenanceRunner {
  run(options?: {
    maxPages?: number;
    owner?: string;
  }): Promise<SourceLineageMaintenanceStatus>;
  status(): Promise<SourceLineageMaintenanceStatus>;
}

export class SourceLineageMaintenanceJob implements SourceLineageMaintenanceRunner {
  readonly #adopter: Pick<CorpusRevisionStore, "adoptLegacySourceLineage">;
  readonly #clock: () => number;
  readonly #repository: SourceLineageMaintenanceRepository;

  constructor(options: {
    readonly adopter: Pick<CorpusRevisionStore, "adoptLegacySourceLineage">;
    readonly clock?: () => number;
    readonly repository: SourceLineageMaintenanceRepository;
  }) {
    this.#adopter = options.adopter;
    this.#clock = options.clock ?? Date.now;
    this.#repository = options.repository;
  }

  async run(
    options: { maxPages?: number; owner?: string } = {}
  ): Promise<SourceLineageMaintenanceStatus> {
    const maxPages = MaximumPagesSchema.parse(options.maxPages ?? 2);
    const owner = OwnerSchema.parse(options.owner ?? `worker-${randomUUID()}`);
    const lease = await this.#repository.acquire(
      owner,
      this.#clock(),
      DEFAULT_LEASE_MS
    );
    if (!lease) return this.status();
    try {
      for (let page = 0; page < maxPages; page += 1) {
        // eslint-disable-next-line no-await-in-loop -- Each keyset page commits a fenced cursor before the next page is read.
        const poemIds = await this.#repository.poemIds(lease);
        if (poemIds.length === 0) {
          // eslint-disable-next-line no-await-in-loop -- Empty keyset pages atomically close the owned pass.
          const finished = await this.#repository.finishPass(
            lease,
            this.#clock()
          );
          if (!finished) throw new Error("SOURCE_LINEAGE_LEASE_LOST");
          // eslint-disable-next-line no-await-in-loop -- Return-await preserves the owned pass failure boundary.
          return await this.status();
        }
        // eslint-disable-next-line no-await-in-loop -- The existing adoption invariant accepts at most ten IDs and owns writer serialization.
        const result = await this.#adopter.adoptLegacySourceLineage({
          poemIds,
        });
        // eslint-disable-next-line no-await-in-loop -- Conflict evidence is durable before the page cursor advances.
        await this.#repository.quarantine(
          lease,
          result.conflicts,
          this.#clock()
        );
        const cursorPoemId = poemIds.at(-1);
        if (!cursorPoemId) throw new Error("SOURCE_LINEAGE_CURSOR_MISSING");
        // eslint-disable-next-line no-await-in-loop -- CAS cursor advancement makes crash replay safe.
        const committed = await this.#repository.commitPage(
          lease,
          cursorPoemId,
          result,
          this.#clock()
        );
        if (!committed) throw new Error("SOURCE_LINEAGE_LEASE_LOST");
      }
      if (!(await this.#repository.yieldLease(lease, this.#clock())))
        throw new Error("SOURCE_LINEAGE_LEASE_LOST");
      return await this.status();
    } catch (error) {
      const code =
        error instanceof Error && /^[A-Z][A-Z\d_]{2,99}$/u.test(error.message)
          ? error.message
          : "SOURCE_LINEAGE_WRITER_FAILURE";
      await this.#repository.fail(lease, code, this.#clock());
      throw error;
    }
  }

  async status(): Promise<SourceLineageMaintenanceStatus> {
    const [status, remaining] = await Promise.all([
      this.#repository.status(),
      this.#repository.remaining(),
    ]);
    return { ...status, remaining };
  }
}
