import { randomUUID } from "node:crypto";
import { setInterval } from "node:timers";

import {
  acceptedPublicationEnrichmentProfile,
  AnyPoemEnrichmentOutputSchema,
  approvedEnrichmentValidations,
  CORPUS_REVISION_SCHEMA_VERSION,
  type CorpusImportAction,
  CorpusImportActionSchema,
  ENRICHMENT_PUBLICATION_SCHEMA_ID,
  ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
  enrichmentPublicationActionHashBody,
  type EnrichmentPublicationV2Item,
  EnrichmentPublicationV2RequestSchema,
  EnrichmentPublicationV2ResponseSchema,
  LEGACY_CORPUS_REVISION_SCHEMA_VERSION,
  MAX_CORPUS_IMPORT_BYTES,
  MAX_CORPUS_IMPORT_RECORDS,
  PoemEnrichmentInputSchema,
  PoemEnrichmentInputV2Schema,
  PoemEnrichmentOutputV2Schema,
  PoemEnrichmentReviewSchema,
  publicationEnrichmentProfile,
  publicationIntentIdBody,
  READABLE_ENRICHMENT_PROFILES,
  reviewsAcceptEnrichment,
  SOURCE_ADMISSION_SCHEMA_ID,
  SOURCE_ADMISSION_SCHEMA_VERSION,
  type SourceAdmissionV2Item,
  SourceAdmissionV2RequestSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { currentSource } from "@saqi/source-adapter";
import { z } from "zod";

import { SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION } from "../enrichment/sol-coordinator.js";
import type { ArtifactStore } from "../persistence/artifact-store.js";
import {
  DIRECT_ENRICHMENT_PUBLICATION_KIND,
  DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
  type Ledger,
  LostLeaseError,
} from "../persistence/ledger.js";
import type { WorkClaim } from "../persistence/schema.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";
import { HistoricalEnrichmentProviderSchema } from "../ports/provider-contract.js";
import { PublishedPoemStructureSchema } from "./corpus-import-actions.js";
import {
  publicationActionHash,
  type PublicationClient,
  type PublicationClientResult,
} from "./publication-client.js";

export const COLLECTION_PUBLICATION_WORK_KIND = "corpus-publication-collection";
export const ENRICHMENT_PUBLICATION_WORK_KIND = "corpus-publication-enrichment";
export const PUBLICATION_IMPLEMENTATION_VERSION = "publication-lane-v1";
export const PUBLICATION_SCHEMA_VERSION = "publication-work@1";
const PUBLICATION_KINDS = [
  COLLECTION_PUBLICATION_WORK_KIND,
  ENRICHMENT_PUBLICATION_WORK_KIND,
] as const;
const DEFAULT_LEASE_MS = 10 * 60_000;
const PublicationLaneSchema = z.enum(["collection", "enrichment"]);
const PublicationStateSchema = z.enum(["already_current", "published"]);
const PublicationSourceHashSchema = z.string().regex(/^[a-f\d]{64}$/);
const MAX_ATTEMPTS = 12;
export const MAX_DIRECT_PUBLICATION_BATCH_ITEMS = 10;
export const MAX_DIRECT_PUBLICATION_BATCH_REQUESTS = 5;
export const MAX_DIRECT_PUBLICATION_CLAIMS =
  MAX_DIRECT_PUBLICATION_BATCH_ITEMS * MAX_DIRECT_PUBLICATION_BATCH_REQUESTS;

const SourceSchema = z.strictObject({
  artifactHash: z.string().regex(/^[a-f\d]{64}$/),
  workKey: z.string().regex(/^[a-f\d]{64}$/),
});
export type PublicationSource = z.infer<typeof SourceSchema>;

const WorkSchema = z.strictObject({
  actionArtifactHash: z.string().regex(/^[a-f\d]{64}$/),
  actionHash: z.string().regex(/^[a-f\d]{64}$/),
  lane: PublicationLaneSchema,
  sources: z.array(SourceSchema).min(1).max(MAX_CORPUS_IMPORT_RECORDS),
});
const DirectWorkSchema = z.strictObject({
  binding: PoemEnrichmentInputV2Schema.shape.canonicalBinding,
  jobType: z.literal("bound-translation"),
  source: SourceSchema,
});

const PlanSchema = z.strictObject({
  bundleId: z.string().min(1),
  items: z.array(z.unknown()),
  planHash: z.string().regex(/^[a-f\d]{64}$/),
  writerEpoch: z.number().int().positive(),
});
const PromotionSchema = z.strictObject({
  advancedPointers: z.number().int().nonnegative(),
  bundleId: z.string().min(1),
  insertedRevisions: z.number().int().nonnegative(),
  planHash: z.string().regex(/^[a-f\d]{64}$/),
  reusedRevisions: z.number().int().nonnegative(),
  unchangedPointers: z.number().int().nonnegative(),
});
const PublicationSchema = z.strictObject({
  authorSlug: z.string().min(1),
  poemId: z.string().min(1),
  pointerVersion: z.number().int().positive(),
  state: PublicationStateSchema,
});
const ConfirmationSchema = z.strictObject({
  actionHash: z.string().regex(/^[a-f\d]{64}$/),
  responseHash: z.string().regex(/^[a-f\d]{64}$/),
  result: z.unknown(),
});
const PhaseSchema = z.discriminatedUnion("lane", [
  z.strictObject({
    lane: z.literal("collection"),
    promote: ConfirmationSchema.optional(),
    stage: ConfirmationSchema.optional(),
  }),
  z.strictObject({
    lane: z.literal("enrichment"),
    publish: ConfirmationSchema.optional(),
  }),
]);
type PublicationPhase = z.infer<typeof PhaseSchema>;

interface DirectPublicationCandidate {
  readonly claim: WorkClaim;
  readonly input: z.infer<typeof DirectWorkSchema>;
  readonly item: EnrichmentPublicationV2Item;
}

interface DirectPublicationBatch {
  readonly candidates: readonly DirectPublicationCandidate[];
  readonly request: z.infer<typeof EnrichmentPublicationV2RequestSchema>;
}

function directPublicationRequest(
  items: readonly EnrichmentPublicationV2Item[],
) {
  return EnrichmentPublicationV2RequestSchema.parse({
    items,
    schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
    schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
  });
}

const SolArtifactSchema = z.strictObject({
  generationAttemptId: z.string().min(1),
  input: PoemEnrichmentInputSchema,
  model: z.string().min(1).optional(),
  modelKey: z.string().min(1).optional(),
  output: AnyPoemEnrichmentOutputSchema,
  outputHash: z.string().regex(/^[a-f\d]{64}$/),
  pipelineVersion: z.string().min(1),
  provider: HistoricalEnrichmentProviderSchema.optional(),
  reasoningEffort: z.string().min(1).optional(),
  reviewAttemptIds: z.array(z.string().min(1)).length(2),
  reviews: z.array(PoemEnrichmentReviewSchema).length(2),
});
const PublicationSolArtifactSchema = SolArtifactSchema.extend({
  input: z.union([PoemEnrichmentInputSchema, PoemEnrichmentInputV2Schema]),
  output: PoemEnrichmentOutputV2Schema,
});
const CollectedDetailArtifactSchema = z.looseObject({
  source: z.strictObject({
    author: z.strictObject({
      canonicalId: z.string(),
      href: z.url(),
      path: z.string(),
      slug: z.string(),
    }),
    canonicalId: z.string(),
    href: z.url(),
    lines: z.array(z.string()),
    numericId: z.string(),
    slug: z.string(),
    structure: PublishedPoemStructureSchema,
    title: z.string(),
    verses: z.number().int().nonnegative().nullable(),
  }),
});

export interface CollectionPublicationCandidate {
  readonly source: PublicationSource;
  readonly stageAction: CorpusImportAction;
}

export interface EnrichmentPublicationOptions {
  readonly expectedPointerVersion: null | number;
  readonly poemId?: string;
  readonly source: PublicationSource;
  readonly sourceRevisionId?: string;
  readonly taskKey: string;
  readonly variant?: number;
  readonly writerEpoch: number;
}

export interface PublicationLaneOptions {
  readonly artifacts: ArtifactStore;
  /** Test-only crash boundary after a durable receipt and before markImported. */
  readonly beforeMarkImported?: (() => void) | undefined;
  readonly client: PublicationClient;
  readonly leaseDurationMs?: number;
  readonly leaseHeartbeatMs?: number;
  readonly ledger: Ledger;
  readonly onCollectionPromoted?:
    | ((input: {
        readonly eventId: string;
        readonly targets: readonly {
          readonly sourceAuthorSlug: string;
          readonly sourcePoemId: string;
        }[];
      }) => Promise<void> | void)
    | undefined;
  readonly owner?: string;
  /** Disable only when the runtime owns periodic global lease recovery. */
  readonly recoverExpiredLeases?: boolean;
  readonly refreshEnrichment?:
    | ((
        action: Extract<CorpusImportAction, { action: "publish-enrichment" }>,
      ) =>
        | null
        | Pick<
            EnrichmentPublicationOptions,
            "expectedPointerVersion" | "writerEpoch"
          >
        | Promise<null | Pick<
            EnrichmentPublicationOptions,
            "expectedPointerVersion" | "writerEpoch"
          >>)
    | undefined;
  readonly refreshWriterEpoch?:
    | ((
        action: Extract<CorpusImportAction, { action: "stage-and-plan" }>,
      ) => null | number | Promise<null | number>)
    | undefined;
}

export interface PublicationRunSummary {
  readonly authWait: number;
  readonly claimed: number;
  readonly confirmed: number;
  readonly deadLettered: number;
  readonly earliestWakeAt: null | number;
  readonly networkWait: number;
  readonly quotaWait: number;
  readonly ready: number;
  readonly retried: number;
  readonly retryAt?: number;
  readonly serviceWait: number;
  readonly stopped:
    | "aborted"
    | "auth_wait"
    | "idle"
    | "maximum"
    | "network_wait"
    | "paused"
    | "service_wait";
}

export interface PublicationLanePort {
  admitSource(
    item: SourceAdmissionV2Item,
    signal?: AbortSignal,
  ): Promise<PublicationClientResult>;
  run(
    signal?: AbortSignal,
    options?: {
      maximum?: number;
      now?: () => number;
      paused?: () => boolean | Promise<boolean>;
    },
  ): Promise<PublicationRunSummary>;
  seedCollection(
    chunks: readonly {
      readonly action: CorpusImportAction;
      readonly sources: readonly PublicationSource[];
    }[],
    priority?: number,
  ): Promise<ReturnType<Ledger["seedMany"]>>;
  seedEnrichment(
    action: CorpusImportAction,
    source: PublicationSource,
    priority?: number,
  ): Promise<ReturnType<Ledger["seedMany"]>[number]>;
  status(now?: number): ReturnType<Ledger["availability"]>;
}

export function planCollectionPublicationChunks(
  candidates: readonly CollectionPublicationCandidate[],
): readonly {
  readonly action: CorpusImportAction;
  readonly sources: readonly PublicationSource[];
}[] {
  const normalized = candidates
    .map((candidate) => {
      const action = CorpusImportActionSchema.parse(candidate.stageAction);
      if (
        action.action !== "stage-and-plan" ||
        action.input.records.length !== 1
      ) {
        throw new Error(
          "COLLECTION_PUBLICATION_REQUIRES_SINGLE_RECORD_ACTIONS",
        );
      }
      const [record] = action.input.records;
      if (!record) throw new Error("COLLECTION_PUBLICATION_RECORD_MISSING");
      return {
        record,
        schemaVersion: action.input.bundle.schemaVersion,
        source: SourceSchema.parse(candidate.source),
        writerEpoch: action.input.bundle.writerEpoch,
      };
    })
    .toSorted((left, right) => {
      const a = [
        left.record.sourceName,
        left.record.sourceAuthorId,
        left.record.sourcePoemId,
      ].join("\u{1F}");
      const b = [
        right.record.sourceName,
        right.record.sourceAuthorId,
        right.record.sourcePoemId,
      ].join("\u{1F}");
      return a.localeCompare(b);
    });
  const identities = normalized.map(
    ({ record }) => `${record.sourceName}\u{1F}${record.sourcePoemId}`,
  );
  if (new Set(identities).size !== identities.length)
    throw new Error("COLLECTION_PUBLICATION_DUPLICATE_SOURCE_ID");
  const chunks: (typeof normalized)[] = [];
  let current: typeof normalized = [];
  for (const candidate of normalized) {
    const firstCurrent = current.at(0);
    if (
      firstCurrent &&
      (candidate.writerEpoch !== firstCurrent.writerEpoch ||
        candidate.schemaVersion !== firstCurrent.schemaVersion)
    ) {
      chunks.push(current);
      current = [];
    }
    const proposed = [...current, candidate];
    if (
      proposed.length > MAX_CORPUS_IMPORT_RECORDS ||
      actionBytes(buildChunk(proposed)) > MAX_CORPUS_IMPORT_BYTES
    ) {
      if (current.length === 0)
        throw new Error("COLLECTION_PUBLICATION_RECORD_TOO_LARGE");
      chunks.push(current);
      current = [candidate];
      if (actionBytes(buildChunk(current)) > MAX_CORPUS_IMPORT_BYTES)
        throw new Error("COLLECTION_PUBLICATION_RECORD_TOO_LARGE");
    } else current = proposed;
  }
  if (current.length > 0) chunks.push(current);
  return chunks.map((chunk) => ({
    action: buildChunk(chunk),
    sources: chunk.map(({ source }) => source),
  }));
}

function buildChunk(
  chunk: readonly {
    record: Extract<
      CorpusImportAction,
      { action: "stage-and-plan" }
    >["input"]["records"][number];
    source: PublicationSource;
    schemaVersion: number;
    writerEpoch: number;
  }[],
): CorpusImportAction {
  const first = chunk.at(0);
  if (!first) throw new Error("COLLECTION_PUBLICATION_CHUNK_EMPTY");
  const seed = sha256(
    canonicalJson(
      chunk.map(({ record }, ordinal) => {
        const {
          bundleId: _oldBundle,
          ordinal: _oldOrdinal,
          recordHash: _oldHash,
          ...body
        } = record;
        return { ...body, ordinal };
      }),
    ),
  );
  const bundleId = `${currentSource().name}-chunk:${seed}`;
  const records = chunk.map(({ record }, ordinal) => {
    const {
      bundleId: _oldBundle,
      ordinal: _oldOrdinal,
      recordHash: _oldHash,
      ...body
    } = record;
    const hashedBody = { ...body, ordinal };
    return {
      ...hashedBody,
      bundleId,
      recordHash: sha256(canonicalJson(hashedBody)),
    };
  });
  const recordHashes = records.map(({ recordHash }) => recordHash);
  return CorpusImportActionSchema.parse({
    action: "stage-and-plan",
    input: {
      bundle: {
        expectedRecordCount: records.length,
        id: bundleId,
        manifestHash: sha256(canonicalJson({ recordHashes })),
        schemaVersion: first.schemaVersion,
        writerEpoch: first.writerEpoch,
      },
      records,
      rootHash: sha256(canonicalJson(recordHashes)),
    },
  });
}

function actionBytes(action: CorpusImportAction): number {
  return Buffer.byteLength(canonicalJson(action));
}

export function prepareEnrichmentPublication(
  rawArtifact: unknown,
  options: EnrichmentPublicationOptions,
): CorpusImportAction {
  const artifact = SolArtifactSchema.parse(rawArtifact);
  const versionProfile = READABLE_ENRICHMENT_PROFILES.find(
    ({ promptVersion }) => promptVersion === artifact.pipelineVersion,
  );
  const profile = publicationEnrichmentProfile({
    model: artifact.model ?? versionProfile?.model ?? "",
    promptVersion: artifact.pipelineVersion,
    reasoningEffort:
      artifact.reasoningEffort ?? versionProfile?.reasoningEffort ?? "",
  });
  const sourceContentHashes = new Set([
    sha256(canonicalJson(artifact.input.linesArabic)),
    sha256(canonicalJson({ content: artifact.input.linesArabic })),
  ]);
  if (
    !profile ||
    (artifact.modelKey !== undefined &&
      artifact.modelKey !== profile.modelKey) ||
    (artifact.provider !== undefined &&
      artifact.provider !== profile.provider) ||
    new Set([artifact.generationAttemptId, ...artifact.reviewAttemptIds])
      .size !== 3 ||
    options.taskKey !== options.source.workKey ||
    !sourceContentHashes.has(artifact.input.sourceContentSha256) ||
    sha256(canonicalJson(artifact.output)) !== artifact.outputHash ||
    !reviewsAcceptEnrichment(artifact.reviews)
  )
    throw new Error("ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
  const sourceRevisionId = PublicationSourceHashSchema.parse(
    options.sourceRevisionId ?? artifact.input.sourceRevisionId,
  );
  const artifactId = `${profile.modelKey}:${sourceRevisionId}:${artifact.outputHash}`;
  const validationPolicies = approvedEnrichmentValidations(profile).all;
  const validations = artifact.reviews.map((report, index) => {
    const policy = validationPolicies[index];
    if (!policy) throw new Error("ENRICHMENT_VALIDATION_POLICY_MISSING");
    const { attempt, validatorKey, validatorVersion } = policy;
    const highestSeverity: "critical" | "major" | "minor" | "none" =
      (["critical", "major", "minor"] as const).find((severity) =>
        report.findings.some((finding) => finding.severity === severity),
      ) ?? "none";
    const reportHash = sha256(canonicalJson(report));
    return {
      artifactId,
      attempt,
      highestSeverity,
      id: `validation:${artifactId}:${validatorKey}:${String(attempt)}:${reportHash}`,
      outcome: report.verdict,
      report,
      reportHash,
      validatorKey,
      validatorVersion,
    };
  });
  return CorpusImportActionSchema.parse({
    action: "publish-enrichment",
    input: {
      artifact: {
        id: artifactId,
        model: profile.model,
        modelKey: profile.modelKey,
        payload: artifact.output,
        payloadHash: artifact.outputHash,
        promptVersion: profile.promptVersion,
        reasoningEffort: profile.reasoningEffort,
        schemaVersion: "schemaVersion" in artifact.output ? 2 : 1,
        sourceRevisionId,
        taskKey: options.taskKey,
        variant: options.variant ?? 0,
      },
      publication: {
        artifactId,
        expectedPointerVersion: options.expectedPointerVersion,
        poemId: options.poemId ?? artifact.input.poemId,
        requiredValidations: validations.map(
          ({ attempt, validatorKey, validatorVersion }) => ({
            attempt,
            validatorKey,
            validatorVersion,
          }),
        ),
        writerEpoch: options.writerEpoch,
      },
      validations,
    },
  });
}

export function prepareBoundEnrichmentPublication(
  rawArtifact: unknown,
  rawBinding: unknown,
  translationWorkKey: string,
): EnrichmentPublicationV2Item {
  const artifact = PublicationSolArtifactSchema.parse(rawArtifact);
  const binding = DirectWorkSchema.shape.binding.parse(rawBinding);
  if (
    artifact.input.schemaVersion === 2 &&
    canonicalJson(artifact.input.canonicalBinding) !== canonicalJson(binding)
  ) {
    throw new Error("BOUND_ENRICHMENT_ARTIFACT_BINDING_MISMATCH");
  }
  const profile = acceptedPublicationEnrichmentProfile({
    model: artifact.model ?? "",
    promptVersion: artifact.pipelineVersion,
    reasoningEffort: artifact.reasoningEffort ?? "",
  });
  if (
    profile?.provider !== "sol" ||
    artifact.modelKey !== profile.modelKey ||
    artifact.provider !== profile.provider ||
    new Set([artifact.generationAttemptId, ...artifact.reviewAttemptIds])
      .size !== 3 ||
    !new Set([
      sha256(canonicalJson(artifact.input.linesArabic)),
      sha256(canonicalJson({ content: artifact.input.linesArabic })),
    ]).has(artifact.input.sourceContentSha256) ||
    sha256(sourceLineNfcHashBody(artifact.input.linesArabic)) !==
      binding.lineNfcHash ||
    sha256(
      sourcePromptMaterialHashBody({
        authorArabic: artifact.input.authorArabic,
        linesArabic: artifact.input.linesArabic,
        titleArabic: artifact.input.titleArabic,
      }),
    ) !== binding.promptMaterialHash ||
    sha256(canonicalJson(artifact.output)) !== artifact.outputHash ||
    !reviewsAcceptEnrichment(artifact.reviews)
  ) {
    throw new Error("BOUND_ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
  }
  const artifactId = `${profile.modelKey}:${binding.sourceRevisionId}:${artifact.outputHash}`;
  const validationPolicies = approvedEnrichmentValidations(profile).all;
  const validations = artifact.reviews.map((report, index) => {
    const policy = validationPolicies[index];
    if (!policy) throw new Error("ENRICHMENT_VALIDATION_POLICY_MISSING");
    const reportHash = sha256(canonicalJson(report));
    const highestSeverity: "critical" | "major" | "minor" | "none" =
      (["critical", "major", "minor"] as const).find((severity) =>
        report.findings.some((finding) => finding.severity === severity),
      ) ?? "none";
    return {
      artifactId,
      attempt: policy.attempt,
      highestSeverity,
      id: `validation:${artifactId}:${policy.validatorKey}:${String(policy.attempt)}:${reportHash}`,
      outcome: "pass" as const,
      report,
      reportHash,
      validatorKey: policy.validatorKey,
      validatorVersion: policy.validatorVersion,
    };
  });
  const publicationIntentId = sha256(
    publicationIntentIdBody({
      artifactId,
      bindingId: binding.bindingId,
      modelKey: profile.modelKey,
      promptVersion: profile.promptVersion,
    }),
  );
  const withoutActionHash = {
    artifact: {
      id: artifactId,
      model: profile.model,
      modelKey: profile.modelKey,
      payload: artifact.output,
      payloadHash: artifact.outputHash,
      promptVersion: profile.promptVersion,
      reasoningEffort: profile.reasoningEffort,
      schemaVersion: 2,
      sourceRevisionId: binding.sourceRevisionId,
      taskKey: PublicationSourceHashSchema.parse(translationWorkKey),
      variant: 0,
    },
    binding,
    publicationIntentId,
    validations,
  };
  return {
    actionHash: sha256(enrichmentPublicationActionHashBody(withoutActionHash)),
    ...withoutActionHash,
  };
}

export class PublicationLane implements PublicationLanePort {
  readonly #artifacts: ArtifactStore;
  readonly #client: PublicationClient;
  readonly #ledger: Ledger;
  readonly #recoverExpiredLeases: boolean;
  readonly #leaseDurationMs: number;
  readonly #leaseHeartbeatMs: number;
  readonly #owner: string;
  readonly #onCollectionPromoted: PublicationLaneOptions["onCollectionPromoted"];
  readonly #beforeMarkImported: (() => void) | undefined;
  readonly #refreshEnrichment: PublicationLaneOptions["refreshEnrichment"];
  readonly #refreshWriterEpoch: PublicationLaneOptions["refreshWriterEpoch"];
  #endpointWait:
    | {
        readonly retryAt: number;
        readonly state: "network_wait" | "service_wait";
      }
    | undefined;
  constructor(options: PublicationLaneOptions) {
    this.#artifacts = options.artifacts;
    this.#client = options.client;
    this.#ledger = options.ledger;
    this.#recoverExpiredLeases = options.recoverExpiredLeases ?? true;
    this.#leaseDurationMs = options.leaseDurationMs ?? DEFAULT_LEASE_MS;
    this.#leaseHeartbeatMs =
      options.leaseHeartbeatMs ?? Math.floor(this.#leaseDurationMs / 3);
    if (
      this.#leaseHeartbeatMs <= 0 ||
      this.#leaseHeartbeatMs >= this.#leaseDurationMs
    )
      throw new Error("Invalid publication lease heartbeat");
    this.#owner =
      options.owner ?? `publication-${String(process.pid)}-${randomUUID()}`;
    this.#onCollectionPromoted = options.onCollectionPromoted;
    this.#beforeMarkImported = options.beforeMarkImported;
    this.#refreshEnrichment = options.refreshEnrichment;
    this.#refreshWriterEpoch = options.refreshWriterEpoch;
  }

  async admitSource(
    item: SourceAdmissionV2Item,
    signal?: AbortSignal,
  ): Promise<PublicationClientResult> {
    return this.#client.sendSourceAdmissions(
      SourceAdmissionV2RequestSchema.parse({
        items: [item],
        schemaId: SOURCE_ADMISSION_SCHEMA_ID,
        schemaVersion: SOURCE_ADMISSION_SCHEMA_VERSION,
      }),
      signal,
    );
  }

  async seedCollection(
    chunks: readonly {
      readonly action: CorpusImportAction;
      readonly sources: readonly PublicationSource[];
    }[],
    priority = 0,
  ) {
    for (const { action, sources } of chunks) {
      if (
        action.action !== "stage-and-plan" ||
        action.input.records.length !== sources.length
      ) {
        throw new Error("COLLECTION_PUBLICATION_SOURCE_COUNT_MISMATCH");
      }
      for (const [index, record] of action.input.records.entries()) {
        const source = sources[index];
        if (!source) throw new Error("COLLECTION_PUBLICATION_SOURCE_MISSING");
        // eslint-disable-next-line no-await-in-loop -- Every record must be bound to its corresponding source artifact before the collection batch is seeded.
        const sourceContents = await this.#artifacts.read(source.artifactHash);
        const raw: unknown = JSON.parse(sourceContents.toString("utf8"));
        const detail = CollectedDetailArtifactSchema.parse(raw).source;
        if (
          detail.numericId !== record.sourcePoemId ||
          detail.author.slug !== record.sourceAuthorId ||
          detail.author.href !== record.sourceAuthorUrl ||
          detail.href !== record.sourcePoemUrl ||
          detail.title !== record.titleArabic ||
          record.sourceName !== currentSource().name ||
          collectionDocumentHash(
            action.input.bundle.schemaVersion,
            detail.title,
            detail.lines,
          ) !== record.contentHash
        ) {
          throw new Error("COLLECTION_PUBLICATION_SOURCE_ARTIFACT_MISMATCH");
        }
      }
    }
    return this.#seed(
      chunks.map(({ action, sources }) => ({
        action,
        lane: "collection" as const,
        sources,
      })),
      priority,
    );
  }
  async seedEnrichment(
    action: CorpusImportAction,
    source: PublicationSource,
    priority = 0,
  ) {
    if (action.action !== "publish-enrichment")
      throw new Error("PUBLICATION_LANE_ACTION_MISMATCH");
    const sourceContents = await this.#artifacts.read(source.artifactHash);
    const rawArtifact: unknown = JSON.parse(sourceContents.toString("utf8"));
    const reconstructed = prepareEnrichmentPublication(rawArtifact, {
      expectedPointerVersion: action.input.publication.expectedPointerVersion,
      poemId: action.input.publication.poemId,
      source,
      sourceRevisionId: action.input.artifact.sourceRevisionId,
      taskKey: action.input.artifact.taskKey,
      variant: action.input.artifact.variant,
      writerEpoch: action.input.publication.writerEpoch,
    });
    if (canonicalJson(reconstructed) !== canonicalJson(action))
      throw new Error("ENRICHMENT_PUBLICATION_SOURCE_ARTIFACT_MISMATCH");
    const [result] = await this.#seed(
      [{ action, lane: "enrichment" as const, sources: [source] }],
      priority,
    );
    if (!result) throw new Error("ENRICHMENT_PUBLICATION_SEED_MISSING");
    return result;
  }
  status(now = Date.now()) {
    const legacy = this.#ledger.availability(PUBLICATION_KINDS, now, {
      implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
      schemaVersion: PUBLICATION_SCHEMA_VERSION,
    });
    const direct = this.#ledger.availability(
      [DIRECT_ENRICHMENT_PUBLICATION_KIND],
      now,
      {
        implementationVersion: SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
      },
    );
    return {
      earliestAvailableAt:
        legacy.earliestAvailableAt === null
          ? direct.earliestAvailableAt
          : direct.earliestAvailableAt === null
            ? legacy.earliestAvailableAt
            : Math.min(legacy.earliestAvailableAt, direct.earliestAvailableAt),
      ready: legacy.ready + direct.ready,
    };
  }

  async run(
    signal?: AbortSignal,
    options: {
      maximum?: number;
      now?: () => number;
      paused?: () => boolean | Promise<boolean>;
    } = {},
  ): Promise<PublicationRunSummary> {
    const maximum = options.maximum ?? Infinity;
    const now = options.now ?? Date.now;
    if (!(
      maximum === Infinity ||
      (Number.isSafeInteger(maximum) && maximum > 0)
    ))
      throw new Error("maximum must be a positive integer");
    const counts = {
      authWait: 0,
      claimed: 0,
      confirmed: 0,
      deadLettered: 0,
      networkWait: 0,
      quotaWait: 0,
      retried: 0,
      serviceWait: 0,
    };
    // Other ready items must not bypass an endpoint outage's probe deadline.
    // This transient gate resets with the process; durable work remains queued.
    if (this.#endpointWait && this.#endpointWait.retryAt > now()) {
      const availability = this.status(now());
      return {
        ...counts,
        earliestWakeAt: availability.earliestAvailableAt,
        ready: availability.ready,
        retryAt: this.#endpointWait.retryAt,
        stopped: signal?.aborted
          ? "aborted"
          : (await options.paused?.())
            ? "paused"
            : this.#endpointWait.state,
      };
    }
    if (this.#recoverExpiredLeases) this.#ledger.recoverExpired(now());
    const compacted = signal?.aborted ? 0 : await this.#compactCollection(now);
    // A compacted successor contains up to 50 source records. Publish that
    // successor immediately, then yield so the next cycle can compact the next
    // bounded group instead of falling back to one request per source.
    const claimMaximum = compacted > 0 ? 1 : maximum;
    while (
      counts.claimed < claimMaximum &&
      counts.authWait === 0 &&
      counts.networkWait === 0 &&
      counts.quotaWait === 0 &&
      counts.serviceWait === 0 &&
      !signal?.aborted &&
      // eslint-disable-next-line no-await-in-loop -- Pause is an admission gate that must be polled before each remote publication claim.
      !(await options.paused?.())
    ) {
      const directClaims: WorkClaim[] = [];
      const firstDirectClaim = this.#ledger.claim(
        this.#owner,
        now(),
        this.#leaseDurationMs,
        [DIRECT_ENRICHMENT_PUBLICATION_KIND],
        {
          implementationVersion:
            SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
          schemaVersion: DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
        },
      );
      if (firstDirectClaim) {
        directClaims.push(firstDirectClaim);
        counts.claimed += 1;
        while (
          directClaims.length < MAX_DIRECT_PUBLICATION_CLAIMS &&
          counts.claimed < claimMaximum &&
          !signal?.aborted &&
          // eslint-disable-next-line no-await-in-loop -- Pause must stop batch admission before another durable lease is acquired.
          !(await options.paused?.())
        ) {
          const next = this.#ledger.claim(
            this.#owner,
            now(),
            this.#leaseDurationMs,
            [DIRECT_ENRICHMENT_PUBLICATION_KIND],
            {
              implementationVersion:
                SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
              schemaVersion: DIRECT_ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            },
          );
          if (!next) break;
          directClaims.push(next);
          counts.claimed += 1;
        }
        try {
          // eslint-disable-next-line no-await-in-loop -- A bounded request window must settle before another remote publication window is admitted.
          await this.#runDirectClaims(
            directClaims,
            counts,
            now,
            signal,
            options.paused,
          );
        } catch (error) {
          for (const claim of directClaims) {
            if (this.#ledger.get(claim.work.workKey)?.state !== "running")
              continue;
            try {
              this.#retryUnexpected(claim, counts, now());
            } catch (transition) {
              if (!(transition instanceof LostLeaseError)) throw transition;
            }
          }
          if (error instanceof LostLeaseError) continue;
        }
        continue;
      }
      const claim = this.#ledger.claim(
        this.#owner,
        now(),
        this.#leaseDurationMs,
        PUBLICATION_KINDS,
        {
          implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
          schemaVersion: PUBLICATION_SCHEMA_VERSION,
        },
      );
      if (!claim) break;
      counts.claimed += 1;
      try {
        // Publication claims must commit in ledger order so a quota/lease
        // transition stops admission before another remote request starts.
        // eslint-disable-next-line no-await-in-loop -- Preserve ordered quota and lease transitions.
        await this.#withHeartbeat(claim, counts, now, signal);
      } catch (error) {
        if (error instanceof LostLeaseError) continue;
        const at = now();
        try {
          if (claim.work.attemptCount >= MAX_ATTEMPTS) {
            this.#ledger.deadLetter(claim, "PUBLICATION_EXCEPTION", at);
            counts.deadLettered += 1;
          } else {
            this.#ledger.retry(
              claim,
              "PUBLICATION_EXCEPTION",
              at + backoff(claim.work.attemptCount),
              at,
            );
            counts.retried += 1;
          }
        } catch (transition) {
          if (!(transition instanceof LostLeaseError)) throw transition;
        }
      }
    }
    const availability = this.status(now());
    return {
      ...counts,
      ...(this.#endpointWait && this.#endpointWait.retryAt > now()
        ? { retryAt: this.#endpointWait.retryAt }
        : {}),
      earliestWakeAt: availability.earliestAvailableAt,
      ready: availability.ready,
      stopped: signal?.aborted
        ? "aborted"
        : (await options.paused?.())
          ? "paused"
          : counts.authWait > 0
            ? "auth_wait"
            : counts.networkWait > 0
              ? "network_wait"
              : counts.serviceWait > 0
                ? "service_wait"
                : counts.claimed >= claimMaximum
                  ? "maximum"
                  : "idle",
    };
  }

  async #compactCollection(now: () => number): Promise<number> {
    const ready = this.#ledger.availability(
      [COLLECTION_PUBLICATION_WORK_KIND],
      now(),
      {
        implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: PUBLICATION_SCHEMA_VERSION,
      },
    ).ready;
    if (ready < 2) return 0;
    const claims: WorkClaim[] = [];
    const candidates: {
      claim: WorkClaim;
      source: PublicationSource;
      stageAction: CorpusImportAction;
    }[] = [];
    const release = () => {
      const at = now();
      for (const claim of claims) {
        if (this.#ledger.get(claim.work.workKey)?.state !== "running") continue;
        this.#ledger.operatorRelease(claim, "PUBLICATION_BATCH_DEFERRED", at);
      }
    };
    try {
      for (
        let index = 0;
        index < Math.min(ready, MAX_CORPUS_IMPORT_RECORDS);
        index += 1
      ) {
        const claim = this.#ledger.claim(
          this.#owner,
          now(),
          this.#leaseDurationMs,
          [COLLECTION_PUBLICATION_WORK_KIND],
          {
            implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
            schemaVersion: PUBLICATION_SCHEMA_VERSION,
          },
        );
        if (!claim) break;
        claims.push(claim);
        const input = WorkSchema.parse(claim.work.input);
        // eslint-disable-next-line no-await-in-loop -- Eligibility must be proven for each leased original before any successor is seeded.
        const phase = await this.#loadPhase(claim.work.workKey);
        if (
          phase !== null ||
          input.lane !== "collection" ||
          input.sources.length !== 1
        ) {
          release();
          return 0;
        }
        // eslint-disable-next-line no-await-in-loop -- Each content-addressed action is authenticated while its exact original claim remains leased.
        const contents = await this.#artifacts.read(input.actionArtifactHash);
        const action = CorpusImportActionSchema.parse(
          JSON.parse(contents.toString("utf8")),
        );
        if (
          action.action !== "stage-and-plan" ||
          action.input.records.length !== 1 ||
          publicationActionHash(action) !== input.actionHash
        ) {
          release();
          return 0;
        }
        const source = input.sources[0];
        if (!source) throw new Error("PUBLICATION_BATCH_SOURCE_MISSING");
        candidates.push({ claim, source, stageAction: action });
      }
      if (candidates.length < 2) {
        release();
        return 0;
      }
      const groups = Map.groupBy(
        candidates,
        ({ claim }) => claim.work.priority,
      );
      for (const [priority, group] of groups) {
        const chunks = planCollectionPublicationChunks(group);
        // eslint-disable-next-line no-await-in-loop -- Each priority group must be durably seeded before its original claims are superseded.
        const successors = await this.seedCollection(
          chunks,
          Math.min(1_000_000, priority + 1),
        );
        const successorBySource = new Map<string, string>();
        for (const [index, chunk] of chunks.entries()) {
          const successor = successors[index];
          if (!successor)
            throw new Error("PUBLICATION_BATCH_SUCCESSOR_MISSING");
          for (const source of chunk.sources)
            successorBySource.set(source.workKey, successor.workKey);
        }
        for (const candidate of group) {
          const successorWorkKey = successorBySource.get(
            candidate.source.workKey,
          );
          if (!successorWorkKey)
            throw new Error("PUBLICATION_BATCH_SOURCE_SUCCESSOR_MISSING");
          this.#ledger.checkpoint(
            candidate.claim,
            {
              artifactHash: null,
              kind: "publication-successor",
              payload: { successorWorkKey },
            },
            now(),
          );
          this.#ledger.deadLetter(
            candidate.claim,
            "PUBLICATION_BATCHED",
            now(),
          );
        }
      }
      return candidates.length;
    } catch (error) {
      release();
      throw error;
    }
  }

  async #seed(
    items: readonly {
      action: CorpusImportAction;
      lane: "collection" | "enrichment";
      sources: readonly PublicationSource[];
    }[],
    priority: number,
  ): Promise<ReturnType<Ledger["seedMany"]>> {
    return this.#ledger.seedMany(
      await Promise.all(
        items.map(async ({ action: raw, lane, sources }) => {
          const action = CorpusImportActionSchema.parse(raw);
          if ((lane === "collection") !== (action.action === "stage-and-plan"))
            throw new Error("PUBLICATION_LANE_ACTION_MISMATCH");
          const serialized = `${canonicalJson(action)}\n`;
          const stored = await this.#artifacts.put(serialized);
          const input = WorkSchema.parse({
            actionArtifactHash: stored.hash,
            actionHash: publicationActionHash(action),
            lane,
            sources,
          });
          return {
            implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
            input,
            inputHash: inputHash(input),
            kind:
              lane === "collection"
                ? COLLECTION_PUBLICATION_WORK_KIND
                : ENRICHMENT_PUBLICATION_WORK_KIND,
            priority,
            schemaVersion: PUBLICATION_SCHEMA_VERSION,
          };
        }),
      ),
    );
  }

  async #withHeartbeat(
    claim: WorkClaim,
    counts: {
      authWait: number;
      confirmed: number;
      deadLettered: number;
      networkWait: number;
      quotaWait: number;
      retried: number;
      serviceWait: number;
    },
    now: () => number,
    signal?: AbortSignal,
  ) {
    const controller = new AbortController();
    const operationSignal = signal
      ? AbortSignal.any([signal, controller.signal])
      : controller.signal;
    let leaseFailure: unknown = null;
    const heartbeat = setInterval(() => {
      try {
        this.#ledger.renew(claim, now(), this.#leaseDurationMs);
      } catch (error) {
        leaseFailure = error;
        controller.abort(error);
      }
    }, this.#leaseHeartbeatMs);
    heartbeat.unref();
    try {
      await this.#runClaim(claim, counts, now, operationSignal);
      if (leaseFailure)
        throw leaseFailure instanceof Error
          ? leaseFailure
          : new Error("PUBLICATION_LEASE_HEARTBEAT_FAILED", {
              cause: leaseFailure,
            });
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #runClaim(
    claim: WorkClaim,
    counts: {
      authWait: number;
      confirmed: number;
      deadLettered: number;
      networkWait: number;
      quotaWait: number;
      retried: number;
      serviceWait: number;
    },
    now: () => number,
    signal?: AbortSignal,
  ) {
    if (claim.work.kind === DIRECT_ENRICHMENT_PUBLICATION_KIND) {
      throw new Error("DIRECT_PUBLICATION_REQUIRES_BATCH_RUNNER");
    }
    let input: z.infer<typeof WorkSchema>;
    let action: CorpusImportAction;
    let phase: PublicationPhase;
    try {
      input = WorkSchema.parse(claim.work.input);
      const actionContents = await this.#artifacts.read(
        input.actionArtifactHash,
      );
      action = CorpusImportActionSchema.parse(
        JSON.parse(actionContents.toString("utf8")),
      );
      if (publicationActionHash(action) !== input.actionHash)
        throw new Error("action hash mismatch");
      phase =
        (await this.#loadPhase(claim.work.workKey)) ??
        (input.lane === "collection"
          ? { lane: "collection" }
          : { lane: "enrichment" });
      if (phase.lane !== input.lane) throw new Error("phase lane mismatch");
    } catch {
      this.#ledger.deadLetter(claim, "PUBLICATION_LOCAL_STATE_CORRUPT", now());
      counts.deadLettered += 1;
      return;
    }
    if (this.#releaseIfAborted(claim, signal, now())) return;

    if (input.lane === "collection") {
      if (action.action !== "stage-and-plan" || phase.lane !== "collection")
        throw new Error("PUBLICATION_ACTION_MISMATCH");
      if (!phase.stage) {
        const result = await this.#client.send(action, signal);
        // A confirmed response is valuable recovery state even when an
        // operator stop races the request. Persist it before releasing the
        // claim so restart never repeats a remote call whose response arrived.
        if (
          result.state !== "confirmed" &&
          this.#releaseIfAborted(claim, signal, now())
        )
          return;
        if (result.state === "conflict") {
          await this.#supersedeConflict(
            claim,
            input,
            action,
            result,
            counts,
            now(),
          );
          return;
        }
        if (!this.#acceptOrTransition(claim, result, counts, now())) return;
        const plan = PlanSchema.parse(result.result);
        phase = { ...phase, stage: confirmation(result, plan) };
        await this.#savePhase(claim, phase, now());
        if (this.#releaseIfAborted(claim, signal, now())) return;
      }
      const stage = phase.stage;
      if (!stage) throw new Error("PUBLICATION_STAGE_RECEIPT_MISSING");
      const plan = PlanSchema.parse(stage.result);
      if (
        plan.bundleId !== action.input.bundle.id ||
        plan.writerEpoch !== action.input.bundle.writerEpoch ||
        plan.items.length !== action.input.records.length
      )
        throw new Error("PUBLICATION_PLAN_MISMATCH");
      if (!phase.promote) {
        const promote = CorpusImportActionSchema.parse({
          action: "promote",
          bundleId: plan.bundleId,
          expectedPlanHash: plan.planHash,
          writerEpoch: plan.writerEpoch,
        });
        const result = await this.#client.send(promote, signal);
        if (
          result.state !== "confirmed" &&
          this.#releaseIfAborted(claim, signal, now())
        )
          return;
        if (result.state === "conflict") {
          await this.#supersedeConflict(
            claim,
            input,
            action,
            result,
            counts,
            now(),
          );
          return;
        }
        if (!this.#acceptOrTransition(claim, result, counts, now())) return;
        const promoted = PromotionSchema.parse(result.result);
        if (
          promoted.planHash !== plan.planHash ||
          promoted.bundleId !== plan.bundleId ||
          promoted.insertedRevisions + promoted.reusedRevisions !==
            action.input.records.length ||
          promoted.advancedPointers + promoted.unchangedPointers !==
            action.input.records.length
        )
          throw new Error("PUBLICATION_PROMOTION_MISMATCH");
        phase = { ...phase, promote: confirmation(result, promoted) };
        await this.#savePhase(claim, phase, now());
        if (this.#releaseIfAborted(claim, signal, now())) return;
      }
      await this.#onCollectionPromoted?.({
        eventId: claim.work.workKey,
        targets: action.input.records.map((record) => ({
          sourceAuthorSlug: record.sourceAuthorId,
          sourcePoemId: record.sourcePoemId,
        })),
      });
    } else {
      if (action.action !== "publish-enrichment" || phase.lane !== "enrichment")
        throw new Error("PUBLICATION_ACTION_MISMATCH");
      if (!phase.publish) {
        const result = await this.#client.send(action, signal);
        if (
          result.state !== "confirmed" &&
          this.#releaseIfAborted(claim, signal, now())
        )
          return;
        if (result.state === "conflict") {
          await this.#supersedeConflict(
            claim,
            input,
            action,
            result,
            counts,
            now(),
          );
          return;
        }
        if (!this.#acceptOrTransition(claim, result, counts, now())) return;
        phase = {
          ...phase,
          publish: confirmation(result, PublicationSchema.parse(result.result)),
        };
        await this.#savePhase(claim, phase, now());
        if (this.#releaseIfAborted(claim, signal, now())) return;
      }
    }
    this.#beforeMarkImported?.();
    for (const source of input.sources)
      this.#ledger.markImported(source.workKey, source.artifactHash, now());
    const receipt = await this.#artifacts.put(`${canonicalJson(phase)}\n`);
    this.#ledger.succeed(claim, receipt.hash, now());
    counts.confirmed += 1;
  }

  async #runDirectClaims(
    claims: readonly WorkClaim[],
    counts: {
      authWait: number;
      claimed: number;
      confirmed: number;
      deadLettered: number;
      networkWait: number;
      quotaWait: number;
      retried: number;
      serviceWait: number;
    },
    now: () => number,
    signal?: AbortSignal,
    paused?: () => boolean | Promise<boolean>,
  ): Promise<void> {
    const candidates: DirectPublicationCandidate[] = [];
    for (const claim of claims) {
      try {
        const input = DirectWorkSchema.parse(claim.work.input);
        // eslint-disable-next-line no-await-in-loop -- Every leased claim must authenticate its own immutable artifact before it enters a request.
        const contents = await this.#artifacts.read(input.source.artifactHash);
        candidates.push({
          claim,
          input,
          item: prepareBoundEnrichmentPublication(
            JSON.parse(contents.toString("utf8")),
            input.binding,
            input.source.workKey,
          ),
        });
      } catch (error) {
        if (
          error instanceof Error &&
          "code" in error &&
          typeof error.code === "string" &&
          ["EAGAIN", "EBUSY", "EINTR", "EIO", "EMFILE", "ENFILE"].includes(
            error.code,
          )
        ) {
          this.#retryDirect(
            claim,
            "BOUND_PUBLICATION_ARTIFACT_IO_UNAVAILABLE",
            counts,
            now(),
          );
          continue;
        }
        this.#ledger.deadLetter(
          claim,
          "BOUND_PUBLICATION_LOCAL_STATE_CORRUPT",
          now(),
        );
        counts.deadLettered += 1;
      }
    }
    const batches = this.#directPublicationBatches(candidates, counts, now);
    const admitted = batches.slice(0, MAX_DIRECT_PUBLICATION_BATCH_REQUESTS);
    if (admitted.length === 0) return;
    // Do not hold claims beyond the bounded concurrency window. Releasing
    // overflow restores its attempt budget and lets the outer loop reclaim it
    // after these requests settle.
    for (const deferred of batches
      .slice(MAX_DIRECT_PUBLICATION_BATCH_REQUESTS)
      .flatMap(({ candidates: deferredCandidates }) => deferredCandidates)) {
      this.#ledger.operatorRelease(
        deferred.claim,
        "PUBLICATION_BATCH_DEFERRED",
        now(),
      );
      counts.claimed -= 1;
    }
    const settled = await Promise.all(
      admitted.map(async (batch) => {
        try {
          await this.#runDirectBatch(batch, counts, now, signal, paused);
          return { state: "fulfilled" as const };
        } catch (error: unknown) {
          return { error, state: "rejected" as const };
        }
      }),
    );
    const failures: unknown[] = [];
    for (const result of settled)
      if (result.state === "rejected") failures.push(result.error);
    if (failures.length > 0)
      throw new AggregateError(failures, "DIRECT_PUBLICATION_BATCH_FAILED");
  }

  async #runDirectBatch(
    batch: DirectPublicationBatch,
    counts: {
      authWait: number;
      confirmed: number;
      deadLettered: number;
      networkWait: number;
      quotaWait: number;
      retried: number;
      serviceWait: number;
    },
    now: () => number,
    signal?: AbortSignal,
    paused?: () => boolean | Promise<boolean>,
  ): Promise<void> {
    if (signal?.aborted || (await paused?.())) {
      for (const { claim } of batch.candidates)
        this.#releaseIfStopped(claim, signal, now());
      return;
    }
    const { result, staleWorkKeys } = await this.#sendDirectBatch(
      batch,
      now,
      signal,
    );
    const active = batch.candidates.filter(
      ({ claim }) => !staleWorkKeys.has(claim.work.workKey),
    );
    if (result.state !== "confirmed" && signal?.aborted) {
      for (const { claim } of active)
        this.#releaseIfAborted(claim, signal, now());
      return;
    }
    if (result.state !== "confirmed") {
      for (const { claim } of active)
        this.#acceptOrTransition(claim, result, counts, now());
      return;
    }
    const response = EnrichmentPublicationV2ResponseSchema.parse(result.result);
    const outcomes = new Map<
      string,
      (typeof response.results)[number] | null
    >();
    for (const outcome of response.results) {
      const publicationIntentId =
        outcome.status === "published"
          ? outcome.receipt.publicationIntentId
          : outcome.publicationIntentId;
      outcomes.set(
        publicationIntentId,
        outcomes.has(publicationIntentId) ? null : outcome,
      );
    }
    for (const candidate of active) {
      const outcome = outcomes.get(candidate.item.publicationIntentId);
      if (!outcome) {
        this.#retryDirect(
          candidate.claim,
          "BOUND_PUBLICATION_RECEIPT_MISMATCH",
          counts,
          now(),
        );
        continue;
      }
      // eslint-disable-next-line no-await-in-loop -- Each exact outcome is durably applied to its own claim before the batch advances.
      await this.#applyDirectOutcome(candidate, outcome, counts, now);
    }
  }

  #directPublicationBatches(
    candidates: readonly DirectPublicationCandidate[],
    counts: { deadLettered: number },
    now: () => number,
  ): DirectPublicationBatch[] {
    const batches: DirectPublicationBatch[] = [];
    let pending: DirectPublicationCandidate[] = [];
    const flush = () => {
      if (pending.length === 0) return;
      batches.push({
        candidates: pending,
        request: directPublicationRequest(pending.map(({ item }) => item)),
      });
      pending = [];
    };
    for (const candidate of candidates) {
      if (pending.length === MAX_DIRECT_PUBLICATION_BATCH_ITEMS) flush();
      const proposed = [...pending, candidate];
      const request = directPublicationRequest(
        proposed.map(({ item }) => item),
      );
      if (
        Buffer.byteLength(canonicalJson(request)) <= MAX_CORPUS_IMPORT_BYTES
      ) {
        pending = proposed;
        continue;
      }
      flush();
      const singleton = directPublicationRequest([candidate.item]);
      if (
        Buffer.byteLength(canonicalJson(singleton)) > MAX_CORPUS_IMPORT_BYTES
      ) {
        this.#ledger.deadLetter(
          candidate.claim,
          "BOUND_PUBLICATION_ACTION_TOO_LARGE",
          now(),
        );
        counts.deadLettered += 1;
      } else {
        pending = [candidate];
      }
    }
    flush();
    return batches;
  }

  async #sendDirectBatch(
    batch: DirectPublicationBatch,
    now: () => number,
    signal?: AbortSignal,
  ): Promise<{
    result: PublicationClientResult;
    staleWorkKeys: ReadonlySet<string>;
  }> {
    const controller = new AbortController();
    const operationSignal = signal
      ? AbortSignal.any([signal, controller.signal])
      : controller.signal;
    const staleWorkKeys = new Set<string>();
    let heartbeatFailure: unknown = null;
    const heartbeat = setInterval(() => {
      try {
        const activeClaims = batch.candidates
          .map(({ claim }) => claim)
          .filter(({ work }) => !staleWorkKeys.has(work.workKey));
        const renewed = this.#ledger.renewMany(
          activeClaims,
          now(),
          this.#leaseDurationMs,
        );
        for (const workKey of renewed.stale) staleWorkKeys.add(workKey);
        if (renewed.stale.length > 0) controller.abort(new LostLeaseError());
      } catch (error) {
        heartbeatFailure = error;
        controller.abort(error);
      }
    }, this.#leaseHeartbeatMs);
    heartbeat.unref();
    try {
      const result = await this.#client.sendBoundEnrichment(
        batch.request,
        operationSignal,
      );
      if (heartbeatFailure)
        throw heartbeatFailure instanceof Error
          ? heartbeatFailure
          : new Error("PUBLICATION_BATCH_HEARTBEAT_FAILED", {
              cause: heartbeatFailure,
            });
      return {
        result,
        staleWorkKeys,
      };
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #applyDirectOutcome(
    candidate: DirectPublicationCandidate,
    outcome: z.infer<
      typeof EnrichmentPublicationV2ResponseSchema
    >["results"][number],
    counts: {
      confirmed: number;
      deadLettered: number;
      retried: number;
    },
    now: () => number,
  ): Promise<void> {
    const { claim, input, item } = candidate;
    if (outcome.status === "rejected") {
      if (outcome.retryable) {
        this.#retryDirect(
          claim,
          `BOUND_PUBLICATION_${outcome.code}`,
          counts,
          now(),
        );
      } else {
        this.#ledger.deadLetter(
          claim,
          `BOUND_PUBLICATION_${outcome.code}`,
          now(),
        );
        counts.deadLettered += 1;
      }
      return;
    }
    const receipt = outcome.receipt;
    if (
      receipt.actionHash !== item.actionHash ||
      receipt.artifactId !== item.artifact.id ||
      receipt.poemId !== item.binding.poemId ||
      receipt.publicationIntentId !== item.publicationIntentId ||
      receipt.sourceRevisionId !== item.binding.sourceRevisionId
    ) {
      this.#retryDirect(
        claim,
        "BOUND_PUBLICATION_RECEIPT_MISMATCH",
        counts,
        now(),
      );
      return;
    }
    const receiptArtifact = await this.#artifacts.put(
      `${canonicalJson(receipt)}\n`,
    );
    this.#beforeMarkImported?.();
    this.#ledger.confirmDirectPublication(
      claim,
      input.source.workKey,
      input.source.artifactHash,
      receiptArtifact.hash,
      now(),
    );
    counts.confirmed += 1;
  }

  #retryDirect(
    claim: WorkClaim,
    errorCode: string,
    counts: { deadLettered: number; retried: number },
    now: number,
  ): void {
    if (claim.work.attemptCount >= MAX_ATTEMPTS) {
      this.#ledger.deadLetter(claim, errorCode, now);
      counts.deadLettered += 1;
    } else {
      this.#ledger.retry(
        claim,
        errorCode,
        now + backoff(claim.work.attemptCount),
        now,
      );
      counts.retried += 1;
    }
  }

  #retryUnexpected(
    claim: WorkClaim,
    counts: { deadLettered: number; retried: number },
    now: number,
  ): void {
    this.#retryDirect(claim, "PUBLICATION_EXCEPTION", counts, now);
  }

  #releaseIfStopped(
    claim: WorkClaim,
    signal: AbortSignal | undefined,
    now: number,
  ): void {
    this.#ledger.operatorRelease(
      claim,
      signal?.aborted
        ? "PUBLICATION_OPERATOR_STOP"
        : "PUBLICATION_OPERATOR_PAUSED",
      now,
    );
  }

  #releaseIfAborted(
    claim: WorkClaim,
    signal: AbortSignal | undefined,
    now: number,
  ): boolean {
    if (!signal?.aborted) return false;
    this.#ledger.operatorRelease(claim, "PUBLICATION_OPERATOR_STOP", now);
    return true;
  }

  #acceptOrTransition(
    claim: WorkClaim,
    result: PublicationClientResult,
    counts: {
      authWait: number;
      deadLettered: number;
      networkWait: number;
      quotaWait: number;
      retried: number;
      serviceWait: number;
    },
    now: number,
  ): result is Extract<PublicationClientResult, { state: "confirmed" }> {
    if (result.state === "confirmed") {
      this.#endpointWait = undefined;
      return true;
    }
    if (result.state === "conflict") return false;
    if (result.state === "auth_wait") {
      // Expired or rejected credentials are an operator/environmental gate,
      // not evidence that this immutable action is bad. Keep the exact item
      // pending indefinitely without spending its finite attempt budget. The
      // runtime preflight probes credentials again on this bounded schedule.
      this.#ledger.operatorRelease(
        claim,
        result.errorCode,
        now,
        Math.max(result.retryAt, now + 1),
      );
      counts.authWait += 1;
      return false;
    }
    if (result.state === "network_wait") {
      this.#endpointWait = {
        retryAt: Math.max(result.retryAt, now + 1),
        state: result.state,
      };
      // Connectivity is an environmental condition, not failed work. Release
      // the lease without consuming the finite attempt budget and let the
      // client's exponential probe schedule wake this exact work item later.
      this.#ledger.operatorRelease(
        claim,
        result.errorCode,
        now,
        Math.max(result.retryAt, now + 1),
      );
      counts.networkWait += 1;
      return false;
    }
    if (result.state === "service_wait") {
      this.#endpointWait = {
        retryAt: Math.max(result.retryAt, now + 1),
        state: result.state,
      };
      // Cloudflare, D1, and R2 can have extended transient incidents. Preserve
      // the exact content-addressed action indefinitely and probe on a bounded
      // schedule instead of converting infrastructure downtime into data loss.
      this.#ledger.operatorRelease(
        claim,
        result.errorCode,
        now,
        Math.max(result.retryAt, now + 1),
      );
      counts.serviceWait += 1;
      return false;
    }
    if (result.state === "retry_wait") {
      if (result.errorCode === "PUBLICATION_RATE_LIMITED") {
        this.#ledger.quotaWait(claim, result.errorCode, result.retryAt, now);
        counts.quotaWait += 1;
      } else if (claim.work.attemptCount >= MAX_ATTEMPTS) {
        this.#ledger.deadLetter(claim, result.errorCode, now);
        counts.deadLettered += 1;
      } else {
        this.#ledger.retry(
          claim,
          result.errorCode,
          Math.max(result.retryAt, now + backoff(claim.work.attemptCount)),
          now,
        );
        counts.retried += 1;
      }
    } else {
      this.#ledger.deadLetter(claim, result.errorCode, now);
      counts.deadLettered += 1;
    }
    return false;
  }

  async #supersedeConflict(
    claim: WorkClaim,
    input: z.infer<typeof WorkSchema>,
    action: CorpusImportAction,
    result: Extract<PublicationClientResult, { state: "conflict" }>,
    counts: { deadLettered: number; retried: number },
    now: number,
  ): Promise<void> {
    let successor: ReturnType<Ledger["seed"]> | undefined;
    if (action.action === "publish-enrichment") {
      const refreshed = (await this.#refreshEnrichment?.(action)) ?? null;
      const source = input.sources[0];
      if (refreshed && source) {
        const sourceContents = await this.#artifacts.read(source.artifactHash);
        const successorAction = prepareEnrichmentPublication(
          JSON.parse(sourceContents.toString("utf8")),
          {
            ...refreshed,
            poemId: action.input.publication.poemId,
            source,
            taskKey: action.input.artifact.taskKey,
            variant: action.input.artifact.variant,
          },
        );
        successor = await this.seedEnrichment(
          successorAction,
          source,
          claim.work.priority,
        );
      }
    } else if (action.action === "stage-and-plan") {
      const writerEpoch = (await this.#refreshWriterEpoch?.(action)) ?? null;
      if (writerEpoch !== null) {
        const successorAction = CorpusImportActionSchema.parse({
          ...action,
          input: {
            ...action.input,
            bundle: { ...action.input.bundle, writerEpoch },
          },
        });
        const seeded = await this.seedCollection(
          [{ action: successorAction, sources: input.sources }],
          claim.work.priority,
        );
        successor = seeded[0];
      }
    }
    if (!successor || successor.workKey === claim.work.workKey) {
      this.#ledger.operatorRelease(claim, result.errorCode, now, now + 30_000);
      counts.retried += 1;
      return;
    }
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: null,
        kind: "publication-successor",
        payload: { successorWorkKey: successor.workKey },
      },
      now,
    );
    this.#ledger.deadLetter(claim, "PUBLICATION_SUPERSEDED", now);
    counts.deadLettered += 1;
  }

  async #loadPhase(workKey: string): Promise<null | PublicationPhase> {
    const checkpoint = this.#ledger.latestCheckpoint(
      workKey,
      "publication-phase",
    );
    if (!checkpoint) return null;
    if (!checkpoint.artifactHash)
      throw new Error("PUBLICATION_PHASE_ARTIFACT_MISSING");
    const phaseContents = await this.#artifacts.read(checkpoint.artifactHash);
    return PhaseSchema.parse(JSON.parse(phaseContents.toString("utf8")));
  }
  async #savePhase(
    claim: WorkClaim,
    phase: PublicationPhase,
    now: number,
  ): Promise<void> {
    const parsed = PhaseSchema.parse(phase);
    const artifact = await this.#artifacts.put(`${canonicalJson(parsed)}\n`);
    this.#ledger.checkpoint(
      claim,
      {
        artifactHash: artifact.hash,
        kind: "publication-phase",
        payload: {
          lane: parsed.lane,
          remoteConfirmed:
            parsed.lane === "collection"
              ? parsed.promote
                ? "promote"
                : "stage"
              : "publish",
        },
      },
      now,
    );
  }
}

function collectionDocumentHash(
  schemaVersion: number,
  titleArabic: string,
  linesArabic: readonly string[],
): string {
  if (schemaVersion === LEGACY_CORPUS_REVISION_SCHEMA_VERSION) {
    return sha256(canonicalJson({ content: linesArabic }));
  }
  if (schemaVersion === CORPUS_REVISION_SCHEMA_VERSION) {
    return sha256(canonicalJson({ content: linesArabic, titleArabic }));
  }
  throw new Error("COLLECTION_PUBLICATION_SCHEMA_UNSUPPORTED");
}

function confirmation(
  result: Extract<PublicationClientResult, { state: "confirmed" }>,
  parsed: unknown,
) {
  return {
    actionHash: result.actionHash,
    responseHash: result.responseHash,
    result: parsed,
  };
}
function backoff(attempt: number) {
  return Math.min(
    30 * 60_000,
    30_000 * 2 ** Math.min(Math.max(attempt - 1, 0), 6),
  );
}
