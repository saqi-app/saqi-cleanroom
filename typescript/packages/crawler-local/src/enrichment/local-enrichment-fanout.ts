import { randomUUID } from "node:crypto";

import {
  type CanonicalPoemBindingV1,
  PoemEnrichmentInputSchema,
  PoemEnrichmentInputV2Schema,
} from "@saqi/precedent-iso";
import { canonicalAuthorUrl, canonicalPoemUrl } from "@saqi/source-adapter";
import { z } from "zod";

import { collectionWorkKinds } from "../collection/collection-scheduler.js";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector.js";
import type { ArtifactStore } from "../persistence/artifact-store.js";
import type { Ledger } from "../persistence/ledger.js";
import { LostLeaseError } from "../persistence/ledger.js";
import type { WorkDefinition, WorkItem } from "../persistence/schema.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";
import {
  bindCollectedPoem,
  type CatalogPoemMapping,
  prepareCollectedPoem,
} from "../publication/corpus-import-actions.js";
import type { SolEnrichmentSeedPort } from "./sol-coordinator.js";

const LOCAL_ENRICHMENT_FANOUT_VERSION = "local-enrichment-fanout-v2";
const LOCAL_ENRICHMENT_FANOUT_SCHEMA = "local-enrichment-fanout@2";
const MAXIMUM_ATTEMPTS = 100;
const LEASE_DURATION_MS = 5 * 60_000;
const POLL_MS = 30_000;
const RESOLUTION_RETRY_MS = 5 * 60_000;
const BatchSizeSchema = z.number().int().min(1).max(1_000);
const CycleMaximumSchema = z.number().int().min(1).max(10_000);
const ProfileIdentifierSchema = z.string().min(1).max(100);

const SourceSchema = z.strictObject({
  artifactHash: z.string().regex(/^[a-f\d]{64}$/),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
const ControlSchema = z.strictObject({
  collectorImplementationVersion: z.literal(collectorImplementationVersion()),
  collectorSchemaVersion: z.literal(collectorSchemaVersion()),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
});
const PendingSourceJobSchema = z.strictObject({
  jobType: z.literal("pending-resolution"),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
  source: SourceSchema,
});
const ResolvedSourceJobSchema = z.strictObject({
  enrichmentInput: PoemEnrichmentInputSchema,
  jobType: z.literal("resolved-revision"),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
});
const BoundSourceJobSchema = z.strictObject({
  enrichmentInput: PoemEnrichmentInputV2Schema,
  jobType: z.literal("bound-revision"),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
});
const QuarantinedSourceJobSchema = z.strictObject({
  errorCode: z.string().regex(/^[A-Z][A-Z0-9_]{0,99}$/),
  jobType: z.literal("quarantined-source"),
  modelKey: z.string().min(1).max(100),
  pipelineVersion: z.string().min(1).max(100),
  source: z.strictObject({
    artifactHash: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullable(),
    workKey: z.string().regex(/^[a-f\d]{64}$/),
  }),
});
const SourceJobSchema = z.discriminatedUnion("jobType", [
  BoundSourceJobSchema,
  PendingSourceJobSchema,
  QuarantinedSourceJobSchema,
  ResolvedSourceJobSchema,
]);
const CollectedWorkInputSchema = z.strictObject({
  authorHref: z.url(),
  authorNameArabic: z.string().trim().min(1).max(512).optional(),
  poemHref: z.url(),
  refreshGeneration: z
    .string()
    .regex(/^[\w.-]{1,64}$/)
    .optional(),
});
const CollectedArtifactSchema = z.strictObject({
  artifactSchemaVersion: z.literal(1),
  collectedBy: z.literal(collectorImplementationVersion()),
  source: z.looseObject({
    author: z.looseObject({
      canonicalId: z.string(),
      href: z.url(),
      path: z.string(),
      slug: z.string(),
    }),
    canonicalId: z.string(),
    href: z.url(),
    numericId: z.string().regex(/^[1-9]\d*$/),
    slug: z.string(),
  }),
  sourceHash: z.string().regex(/^[a-f\d]{64}$/),
  sourceContext: z
    .strictObject({
      authorNameArabic: z.string().trim().min(1).max(512),
      refreshGeneration: z.string().regex(/^[\w.-]{1,64}$/),
    })
    .optional(),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
const CursorSchema = z.strictObject({
  eventSequence: z.number().int().nonnegative(),
});

export interface LocalEnrichmentFanoutProfile {
  readonly modelKey: string;
  readonly pipelineVersion: string;
  readonly seed: SolEnrichmentSeedPort["seed"];
}

export interface LocalEnrichmentFanoutSummary {
  readonly cursor: number;
  readonly deadLettered: number;
  readonly earliestWakeAt: null | number;
  readonly pendingResolution: number;
  readonly ready: number;
  readonly retried: number;
  readonly scanned: number;
  readonly seeded: number;
}

type LocalEnrichmentFanoutBoundary = "provider_seeded" | "source_jobs_seeded";

type LocalEnrichmentFanoutBoundaryHook = (
  boundary: LocalEnrichmentFanoutBoundary,
) => void;

export interface LocalEnrichmentFanoutOptions {
  /** Test-only crash injection after an idempotent downstream boundary. */
  readonly afterBoundary?: LocalEnrichmentFanoutBoundaryHook | undefined;
  readonly artifacts: ArtifactStore;
  readonly batchSize?: number;
  readonly ledger: Ledger;
  readonly owner?: string;
  readonly profile: LocalEnrichmentFanoutProfile;
  /** Disable only when the runtime owns periodic global lease recovery. */
  readonly recoverExpiredLeases?: boolean;
  readonly resolver: LocalEnrichmentResolver;
}

interface LocalEnrichmentFanoutPort {
  cycle(options?: {
    readonly maximum?: number;
    readonly now?: () => number;
  }): Promise<LocalEnrichmentFanoutSummary>;
  status(now?: number): ReturnType<Ledger["availability"]>;
}

export interface LocalEnrichmentResolver {
  resolve(
    source: WorkItem,
    artifact: unknown,
  ):
    | LocalEnrichmentResolution
    | null
    | Promise<LocalEnrichmentResolution | null>;
}

export interface LocalEnrichmentResolution {
  readonly binding?: CanonicalPoemBindingV1;
  readonly mapping: CatalogPoemMapping;
  readonly observedAt: string;
  readonly writerEpoch: number;
}

interface LocalEnrichmentSourceDefinition {
  readonly definition: WorkDefinition;
  readonly provenance: {
    readonly artifactHash: null | string;
    readonly sourceWorkKey: string;
  };
}

/**
 * Durable collection-to-model fanout. Production identity resolution is
 * required for exact baseline deduplication, but publication execution is not.
 */
export class LocalEnrichmentFanout implements LocalEnrichmentFanoutPort {
  readonly #afterBoundary: LocalEnrichmentFanoutOptions["afterBoundary"];
  readonly #artifacts: ArtifactStore;
  readonly #batchSize: number;
  readonly #controlKind: string;
  readonly #controlWorkKey: string;
  readonly #ledger: Ledger;
  readonly #recoverExpiredLeases: boolean;
  readonly #owner: string;
  readonly #profile: LocalEnrichmentFanoutProfile;
  readonly #resolver: LocalEnrichmentResolver;
  readonly #sourceKind: string;

  constructor(options: LocalEnrichmentFanoutOptions) {
    this.#afterBoundary = options.afterBoundary;
    this.#artifacts = options.artifacts;
    this.#batchSize = BatchSizeSchema.parse(options.batchSize ?? 250);
    this.#ledger = options.ledger;
    this.#recoverExpiredLeases = options.recoverExpiredLeases ?? true;
    this.#profile = {
      modelKey: ProfileIdentifierSchema.parse(options.profile.modelKey),
      pipelineVersion: ProfileIdentifierSchema.parse(
        options.profile.pipelineVersion,
      ),
      seed: options.profile.seed,
    };
    if (this.#profile.modelKey !== "sol-5.6")
      throw new Error("LOCAL_FANOUT_CODEX_PROFILE_REQUIRED");
    this.#resolver = options.resolver;
    const profileId = inputHash({
      modelKey: this.#profile.modelKey,
      pipelineVersion: this.#profile.pipelineVersion,
    }).slice(0, 16);
    this.#controlKind = `local-enrichment-control-${profileId}`;
    this.#sourceKind = `local-enrichment-source-${profileId}`;
    this.#owner =
      options.owner ??
      `local-enrichment-${this.#profile.modelKey}-${String(process.pid)}-${randomUUID()}`;
    const input = ControlSchema.parse({
      collectorImplementationVersion: collectorImplementationVersion(),
      collectorSchemaVersion: collectorSchemaVersion(),
      modelKey: this.#profile.modelKey,
      pipelineVersion: this.#profile.pipelineVersion,
    });
    this.#ledger.retireIncompatible(
      [this.#controlKind, this.#sourceKind],
      this.#requirements(),
    );
    this.#controlWorkKey = this.#ledger.seed({
      implementationVersion: LOCAL_ENRICHMENT_FANOUT_VERSION,
      input,
      inputHash: inputHash(input),
      kind: this.#controlKind,
      priority: 1_000,
      schemaVersion: LOCAL_ENRICHMENT_FANOUT_SCHEMA,
    }).workKey;
  }

  async cycle(
    options: { readonly maximum?: number; readonly now?: () => number } = {},
  ): Promise<LocalEnrichmentFanoutSummary> {
    const now = options.now ?? Date.now;
    if (this.#recoverExpiredLeases) this.#ledger.recoverExpired(now());
    const maximum = CycleMaximumSchema.parse(
      options.maximum ?? this.#batchSize,
    );
    const summary = {
      deadLettered: 0,
      pendingResolution: 0,
      retried: 0,
      scanned: 0,
      seeded: 0,
    };
    const control = this.#ledger.claim(
      this.#owner,
      now(),
      LEASE_DURATION_MS,
      [this.#controlKind],
      this.#requirements(),
    );
    if (control) {
      try {
        this.#ledger.renew(control, now(), LEASE_DURATION_MS);
        await this.#scan(control, summary, now);
      } catch (error) {
        if (error instanceof LostLeaseError) {
          if (this.#recoverExpiredLeases) this.#ledger.recoverExpired(now());
        } else {
          this.#ledger.retry(
            control,
            "LOCAL_ENRICHMENT_SCAN_FAILED",
            now() + retryDelay(control.work.attemptCount),
            now(),
          );
          summary.retried += 1;
        }
      }
    }
    for (let index = 0; index < maximum; index += 1) {
      const claim = this.#ledger.claim(
        this.#owner,
        now(),
        LEASE_DURATION_MS,
        [this.#sourceKind],
        this.#requirements(),
      );
      if (!claim) break;
      try {
        this.#ledger.renew(claim, now(), LEASE_DURATION_MS);
        const job = SourceJobSchema.parse(claim.work.input);
        if (
          job.modelKey !== this.#profile.modelKey ||
          job.pipelineVersion !== this.#profile.pipelineVersion
        )
          throw new Error("LOCAL_ENRICHMENT_PROFILE_MISMATCH");
        if (job.jobType === "quarantined-source") {
          this.#ledger.deadLetter(claim, job.errorCode, now());
          summary.deadLettered += 1;
          continue;
        }
        let input;
        let outputArtifactHash: string;
        if (
          job.jobType === "bound-revision" ||
          job.jobType === "resolved-revision"
        ) {
          input = job.enrichmentInput;
          // eslint-disable-next-line no-await-in-loop -- Source jobs are claimed and durably transitioned one at a time.
          const storedInput = await this.#artifacts.put(
            `${canonicalJson({ enrichmentInput: input })}\n`,
          );
          outputArtifactHash = storedInput.hash;
        } else {
          const source = this.#ledger.get(job.source.workKey);
          // eslint-disable-next-line no-await-in-loop -- Verify each claimed source before resolving or seeding its provider work.
          const sourceVerification = await this.#artifacts.verify(
            job.source.artifactHash,
          );
          if (
            !source ||
            !["succeeded", "imported"].includes(source.state) ||
            source.outputArtifactHash !== job.source.artifactHash ||
            !sourceVerification.ok
          )
            throw new Error("LOCAL_ENRICHMENT_SOURCE_INVALID");
          // eslint-disable-next-line no-await-in-loop -- Read the verified source inside its owning claim before advancing that claim.
          const sourceContents = await this.#artifacts.read(
            job.source.artifactHash,
          );
          const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
          assertCollectedArtifactBinding(source, artifact);
          // eslint-disable-next-line no-await-in-loop -- Resolution is claim-local and must precede the matching durable seed.
          const resolution = await this.#resolver.resolve(source, artifact);
          if (!resolution) {
            this.#ledger.operatorRelease(
              claim,
              "LOCAL_ENRICHMENT_IDENTITY_PENDING",
              now(),
              now() + RESOLUTION_RETRY_MS,
            );
            summary.pendingResolution += 1;
            continue;
          }
          const preparedInput = prepareCollectedPoem(
            artifact,
            resolution.mapping,
            resolution.observedAt,
            resolution.writerEpoch,
          ).enrichmentInput;
          if (!resolution.binding) {
            this.#ledger.operatorRelease(
              claim,
              "LOCAL_ENRICHMENT_BINDING_PENDING",
              now(),
              now() + RESOLUTION_RETRY_MS,
            );
            summary.pendingResolution += 1;
            continue;
          }
          input = bindCollectedPoem(preparedInput, resolution.binding);
          outputArtifactHash = job.source.artifactHash;
        }
        const seeded = this.#profile.seed(input, claim.work.priority);
        this.#afterBoundary?.("provider_seeded");
        this.#ledger.checkpoint(
          claim,
          {
            artifactHash: null,
            kind: "local-enrichment-provider-work",
            payload: {
              modelKey: this.#profile.modelKey,
              workKey: seeded.workKey,
            },
          },
          now(),
        );
        this.#ledger.succeed(claim, outputArtifactHash, now());
        if (seeded.inserted) summary.seeded += 1;
      } catch (error) {
        if (error instanceof LostLeaseError) {
          if (this.#recoverExpiredLeases) this.#ledger.recoverExpired(now());
          continue;
        }
        if (claim.work.attemptCount >= MAXIMUM_ATTEMPTS) {
          this.#ledger.deadLetter(claim, "LOCAL_ENRICHMENT_FAILED", now());
          summary.deadLettered += 1;
        } else {
          this.#ledger.retry(
            claim,
            "LOCAL_ENRICHMENT_FAILED",
            now() + retryDelay(claim.work.attemptCount),
            now(),
          );
          summary.retried += 1;
        }
      }
    }
    const availability = this.status(now());
    await Promise.resolve();
    return {
      ...summary,
      cursor: await this.#cursor(),
      earliestWakeAt: availability.earliestAvailableAt,
      ready: availability.ready,
    };
  }

  status(now = Date.now()) {
    return this.#ledger.availability(
      [this.#controlKind, this.#sourceKind],
      now,
      this.#requirements(),
    );
  }

  async #cursor(): Promise<number> {
    const checkpoint = this.#ledger.latestCheckpoint(
      this.#controlWorkKey,
      "local-enrichment-cursor",
    );
    if (!checkpoint) return 0;
    if (!checkpoint.artifactHash)
      throw new Error("LOCAL_ENRICHMENT_CURSOR_ARTIFACT_MISSING");
    const cursorContents = await this.#artifacts.read(checkpoint.artifactHash);
    return CursorSchema.parse(JSON.parse(cursorContents.toString("utf8")))
      .eventSequence;
  }

  #requirements() {
    return {
      implementationVersion: LOCAL_ENRICHMENT_FANOUT_VERSION,
      schemaVersion: LOCAL_ENRICHMENT_FANOUT_SCHEMA,
    } as const;
  }

  async #scan(
    claim: Parameters<Ledger["checkpoint"]>[0],
    summary: { scanned: number },
    now: () => number,
  ): Promise<void> {
    const page = this.#ledger.listSucceededAfter(
      await this.#cursor(),
      [collectionWorkKinds().poemDetail],
      this.#batchSize,
      {
        implementationVersion: collectorImplementationVersion(),
        schemaVersion: collectorSchemaVersion(),
      },
    );
    const definitions: LocalEnrichmentSourceDefinition[] = [];
    for (const { work } of page.items) {
      this.#ledger.renew(claim, now(), LEASE_DURATION_MS);
      const artifactHash: null | string = work.outputArtifactHash;
      let input: z.infer<typeof SourceJobSchema>;
      try {
        if (!artifactHash)
          throw new Error("LOCAL_ENRICHMENT_SOURCE_ARTIFACT_MISSING");
        // eslint-disable-next-line no-await-in-loop -- Source verification is sequential so the scan lease can be renewed between items.
        const verification = await this.#artifacts.verify(artifactHash);
        if (!verification.ok)
          throw new Error("LOCAL_ENRICHMENT_SOURCE_INVALID");
        // eslint-disable-next-line no-await-in-loop -- Source reads are sequential so the scan lease can be renewed between items.
        const sourceContents = await this.#artifacts.read(artifactHash);
        const artifact: unknown = JSON.parse(sourceContents.toString("utf8"));
        assertCollectedArtifactBinding(work, artifact);
        // eslint-disable-next-line no-await-in-loop -- Each resolution must stay paired with its source while the scan lease is renewed between items.
        const resolution = await this.#resolver.resolve(work, artifact);
        input = SourceJobSchema.parse(
          resolution?.binding
            ? {
                enrichmentInput: bindCollectedPoem(
                  prepareCollectedPoem(
                    artifact,
                    resolution.mapping,
                    resolution.observedAt,
                    resolution.writerEpoch,
                  ).enrichmentInput,
                  resolution.binding,
                ),
                jobType: "bound-revision",
                modelKey: this.#profile.modelKey,
                pipelineVersion: this.#profile.pipelineVersion,
              }
            : {
                jobType: "pending-resolution",
                modelKey: this.#profile.modelKey,
                pipelineVersion: this.#profile.pipelineVersion,
                source: { artifactHash, workKey: work.workKey },
              },
        );
      } catch (error) {
        if (isGlobalResolutionError(error)) throw error;
        input = SourceJobSchema.parse({
          errorCode: quarantineCode(error),
          jobType: "quarantined-source",
          modelKey: this.#profile.modelKey,
          pipelineVersion: this.#profile.pipelineVersion,
          source: { artifactHash, workKey: work.workKey },
        });
      }
      definitions.push({
        provenance: {
          artifactHash,
          sourceWorkKey: work.workKey,
        },
        definition: {
          implementationVersion: LOCAL_ENRICHMENT_FANOUT_VERSION,
          input,
          inputHash: inputHash(input),
          kind: this.#sourceKind,
          priority: work.priority,
          schemaVersion: LOCAL_ENRICHMENT_FANOUT_SCHEMA,
        },
      });
    }
    const seeded = this.#ledger.seedMany(
      definitions.map(({ definition }) => definition),
    );
    for (const [index, result] of seeded.entries()) {
      this.#ledger.renew(claim, now(), LEASE_DURATION_MS);
      const provenance = definitions[index]?.provenance;
      if (!provenance) throw new Error("LOCAL_ENRICHMENT_PROVENANCE_MISSING");
      this.#ledger.checkpoint(
        claim,
        {
          artifactHash: provenance.artifactHash,
          kind: "local-enrichment-source-provenance",
          payload: {
            sourceWorkKey: provenance.sourceWorkKey,
            targetWorkKey: result.workKey,
          },
        },
        now(),
      );
    }
    this.#afterBoundary?.("source_jobs_seeded");
    const cursor = CursorSchema.parse({ eventSequence: page.cursor });
    const artifact = await this.#artifacts.put(`${canonicalJson(cursor)}\n`);
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: artifact.hash,
        kind: "local-enrichment-cursor",
        payload: cursor,
      },
      now(),
    );
    this.#ledger.retry(claim, "LOCAL_ENRICHMENT_POLL", now() + POLL_MS, now());
    summary.scanned += page.items.length;
  }
}

function retryDelay(attempt: number): number {
  return Math.min(
    30 * 60_000,
    30_000 * 2 ** Math.min(Math.max(attempt - 1, 0), 6),
  );
}

export function assertCollectedArtifactBinding(
  work: WorkItem,
  artifact: unknown,
): void {
  if (
    work.kind !== collectionWorkKinds().poemDetail ||
    work.implementationVersion !== collectorImplementationVersion() ||
    work.schemaVersion !== collectorSchemaVersion()
  )
    throw new Error("LOCAL_ENRICHMENT_SOURCE_PROFILE_MISMATCH");
  const input = CollectedWorkInputSchema.parse(work.input);
  const envelope = CollectedArtifactSchema.parse(artifact);
  if (envelope.workKey !== work.workKey)
    throw new Error("LOCAL_ENRICHMENT_SOURCE_WORK_KEY_MISMATCH");
  const poem = canonicalPoemUrl(envelope.source.href);
  const author = canonicalAuthorUrl(envelope.source.author.href);
  if (
    poem.href !== input.poemHref ||
    poem.canonicalId !== envelope.source.canonicalId ||
    poem.numericId !== envelope.source.numericId ||
    poem.slug !== envelope.source.slug
  )
    throw new Error("LOCAL_ENRICHMENT_SOURCE_POEM_MISMATCH");
  if (
    author.href !== input.authorHref ||
    author.canonicalId !== envelope.source.author.canonicalId ||
    author.path !== envelope.source.author.path ||
    author.slug !== envelope.source.author.slug
  )
    throw new Error("LOCAL_ENRICHMENT_SOURCE_AUTHOR_MISMATCH");
  if (sha256(canonicalJson(envelope.source)) !== envelope.sourceHash)
    throw new Error("LOCAL_ENRICHMENT_SOURCE_HASH_MISMATCH");
}

function quarantineCode(error: unknown): string {
  if (error instanceof Error && /^[A-Z][A-Z0-9_]{0,99}$/.test(error.message))
    return error.message;
  return "LOCAL_ENRICHMENT_SOURCE_QUARANTINED";
}

function isGlobalResolutionError(error: unknown): boolean {
  return (
    error instanceof Error &&
    [
      "PRODUCTION_RESOLUTION_OBSERVED_AT_FUTURE",
      "PRODUCTION_RESOLUTION_STALE",
    ].includes(error.message)
  );
}
