import {
  acceptedPublicationEnrichmentProfile,
  type EnrichmentPublicationReceiptV1,
  type EnrichmentPublicationV2Request,
  isApprovedEnrichmentValidationSet,
  PoemEnrichmentReviewSchema,
  publicationEnrichmentProfile,
  reviewsAcceptEnrichment,
  type SourceAdmissionV2Request,
} from "@saqi/precedent-iso";

import type {
  CorpusRevisionStore,
  EnrichmentArtifactInput,
  EnrichmentValidationInput,
  ImportCounts,
  PromotionPlan,
  PublicationResult,
  PublishEnrichmentInput,
  SourceAdmissionResult,
  StageBundleInput,
  StageRecordInput,
} from "./corpus-revision-store";
import { CorpusRevisionConflictError } from "./corpus-revision-store";

export interface SourceImportBundle {
  bundle: StageBundleInput;
  records: readonly StageRecordInput[];
  rootHash: string;
}

export interface EnrichmentPublication {
  artifact: EnrichmentArtifactInput;
  publication: PublishEnrichmentInput;
  validations: readonly EnrichmentValidationInput[];
}

export interface PromotionSummary extends ImportCounts {
  planHash: string;
}

export interface CorpusImportPipeline {
  admitSource(
    input: SourceAdmissionV2Request["items"][number],
  ): Promise<SourceAdmissionResult>;
  promote(
    bundleId: string,
    writerEpoch: number,
    expectedPlanHash: string,
  ): Promise<PromotionSummary>;
  publishBoundEnrichment(
    input: EnrichmentPublicationV2Request["items"][number],
  ): Promise<EnrichmentPublicationReceiptV1>;
  publishEnrichment(input: EnrichmentPublication): Promise<PublicationResult>;
  stageAndPlan(input: SourceImportBundle): Promise<PromotionPlan>;
}

/**
 * Schema-neutral orchestration around CorpusRevisionStore. A dry run persists
 * only immutable staging rows and a frozen promotion plan; publication is a
 * separate, hash-confirmed operation that can be replayed after any crash.
 */
export class CorpusImportCoordinator implements CorpusImportPipeline {
  readonly #store: CorpusRevisionStore;

  constructor(store: CorpusRevisionStore) {
    this.#store = store;
  }

  admitSource(
    input: SourceAdmissionV2Request["items"][number],
  ): Promise<SourceAdmissionResult> {
    return this.#store.admitSource(input);
  }

  async stageAndPlan(input: SourceImportBundle): Promise<PromotionPlan> {
    if (input.records.length !== input.bundle.expectedRecordCount) {
      throw new CorpusRevisionConflictError("IMPORT_INPUT_COUNT_MISMATCH");
    }
    const ordinals = input.records.map(({ ordinal }) => ordinal);
    if (
      new Set(ordinals).size !== ordinals.length ||
      ordinals.some((ordinal, index) => ordinal !== index)
    ) {
      throw new CorpusRevisionConflictError("IMPORT_ORDINALS_NOT_CONTIGUOUS");
    }
    if (input.records.some(({ bundleId }) => bundleId !== input.bundle.id)) {
      throw new CorpusRevisionConflictError("IMPORT_RECORD_BUNDLE_MISMATCH");
    }

    await this.#store.createOrReuseBundle(input.bundle);
    for (const record of input.records) {
      // eslint-disable-next-line no-await-in-loop -- D1 writes are intentionally ordered for deterministic resumability.
      await this.#store.stageRecord(record);
    }
    await this.#store.sealBundle(input.bundle.id, input.rootHash);
    return this.#store.planPromotion(input.bundle.id, input.bundle.writerEpoch);
  }

  async promote(
    bundleId: string,
    writerEpoch: number,
    expectedPlanHash: string,
  ): Promise<PromotionSummary> {
    const plan = await this.#store.planPromotion(bundleId, writerEpoch);
    if (plan.planHash !== expectedPlanHash) {
      throw new CorpusRevisionConflictError("PROMOTION_PLAN_NOT_CONFIRMED");
    }
    for (const item of plan.items) {
      // eslint-disable-next-line no-await-in-loop -- Each idempotent item must preserve frozen plan order.
      await this.#store.promoteRecord(plan, item);
    }

    const counts = countsFromFrozenPlan(plan);
    await this.#store.recordImportReceipt(plan, counts);
    return { bundleId, planHash: plan.planHash, ...counts };
  }

  async publishEnrichment(
    input: EnrichmentPublication,
  ): Promise<PublicationResult> {
    if (input.artifact.id !== input.publication.artifactId) {
      throw new CorpusRevisionConflictError("PUBLICATION_ARTIFACT_MISMATCH");
    }
    if (
      input.validations.some(
        ({ artifactId }) => artifactId !== input.artifact.id,
      )
    ) {
      throw new CorpusRevisionConflictError("VALIDATION_ARTIFACT_MISMATCH");
    }
    const profile = publicationEnrichmentProfile(input.artifact);
    if (
      !profile ||
      (input.artifact.modelKey !== undefined &&
        input.artifact.modelKey !== profile.modelKey)
    ) {
      throw new CorpusRevisionConflictError("ENRICHMENT_PROVENANCE_REJECTED");
    }
    if (
      input.validations.length !== 2 ||
      input.publication.requiredValidations.length !== 2
    ) {
      throw new CorpusRevisionConflictError("VALIDATION_SET_MUST_HAVE_TWO");
    }
    if (
      !isApprovedEnrichmentValidationSet(input.validations, profile) ||
      !isApprovedEnrichmentValidationSet(
        input.publication.requiredValidations,
        profile,
      )
    ) {
      throw new CorpusRevisionConflictError("VALIDATION_POLICY_REJECTED");
    }
    if (
      new Set(input.validations.map(({ validatorKey }) => validatorKey))
        .size !== 2
    ) {
      throw new CorpusRevisionConflictError("VALIDATORS_MUST_BE_DISTINCT");
    }
    const reviews = input.validations.map((validation) => {
      const review = PoemEnrichmentReviewSchema.parse(validation.report);
      const highestSeverity = highestReviewSeverity(review);
      if (
        validation.outcome !== review.verdict ||
        validation.highestSeverity !== highestSeverity
      ) {
        throw new CorpusRevisionConflictError(
          "VALIDATION_DECLARATION_MISMATCH",
        );
      }
      return review;
    });
    if (!reviewsAcceptEnrichment(reviews)) {
      throw new CorpusRevisionConflictError("ENRICHMENT_REVIEWS_REJECTED");
    }
    const available = new Set(
      input.validations.map(({ validatorKey, validatorVersion, attempt }) =>
        validationIdentity(validatorKey, validatorVersion, attempt),
      ),
    );
    if (
      available.size !== input.publication.requiredValidations.length ||
      input.publication.requiredValidations.some(
        ({ validatorKey, validatorVersion, attempt }) =>
          !available.has(
            validationIdentity(validatorKey, validatorVersion, attempt),
          ),
      )
    ) {
      throw new CorpusRevisionConflictError("REQUIRED_VALIDATION_NOT_STAGED");
    }

    await this.#store.putEnrichmentArtifact(input.artifact);
    for (const validation of input.validations) {
      // eslint-disable-next-line no-await-in-loop -- Immutable validations retain caller-declared order.
      await this.#store.putEnrichmentValidation(validation);
    }
    return this.#store.publishEnrichment(input.publication);
  }

  async publishBoundEnrichment(
    input: EnrichmentPublicationV2Request["items"][number],
  ): Promise<EnrichmentPublicationReceiptV1> {
    const staged: EnrichmentPublication = {
      artifact: input.artifact,
      publication: {
        artifactId: input.artifact.id,
        expectedPointerVersion: null,
        poemId: input.binding.poemId,
        requiredValidations: input.validations.map(
          ({ attempt, validatorKey, validatorVersion }) => ({
            attempt,
            validatorKey,
            validatorVersion,
          }),
        ),
        writerEpoch: 1,
      },
      validations: input.validations,
    };
    validateEnrichmentPublication(staged);
    await this.#store.putEnrichmentArtifact(staged.artifact);
    for (const validation of staged.validations) {
      // eslint-disable-next-line no-await-in-loop -- Immutable validations retain caller-declared order.
      await this.#store.putEnrichmentValidation(validation);
    }
    return this.#store.publishBoundEnrichment(input);
  }
}

function validateEnrichmentPublication(input: EnrichmentPublication): void {
  if (input.artifact.id !== input.publication.artifactId) {
    throw new CorpusRevisionConflictError("PUBLICATION_ARTIFACT_MISMATCH");
  }
  if (
    input.validations.some(({ artifactId }) => artifactId !== input.artifact.id)
  ) {
    throw new CorpusRevisionConflictError("VALIDATION_ARTIFACT_MISMATCH");
  }
  const profile = acceptedPublicationEnrichmentProfile(input.artifact);
  if (
    !profile ||
    (input.artifact.modelKey !== undefined &&
      input.artifact.modelKey !== profile.modelKey)
  ) {
    throw new CorpusRevisionConflictError("ENRICHMENT_PROVENANCE_REJECTED");
  }
  if (
    input.validations.length !== 2 ||
    input.publication.requiredValidations.length !== 2 ||
    !isApprovedEnrichmentValidationSet(input.validations, profile) ||
    !isApprovedEnrichmentValidationSet(
      input.publication.requiredValidations,
      profile,
    ) ||
    new Set(input.validations.map(({ validatorKey }) => validatorKey)).size !==
      2
  ) {
    throw new CorpusRevisionConflictError("VALIDATION_POLICY_REJECTED");
  }
  const reviews = input.validations.map((validation) => {
    const review = PoemEnrichmentReviewSchema.parse(validation.report);
    if (
      validation.outcome !== review.verdict ||
      validation.highestSeverity !== highestReviewSeverity(review)
    ) {
      throw new CorpusRevisionConflictError("VALIDATION_DECLARATION_MISMATCH");
    }
    return review;
  });
  if (!reviewsAcceptEnrichment(reviews)) {
    throw new CorpusRevisionConflictError("ENRICHMENT_REVIEWS_REJECTED");
  }
  const available = new Set(
    input.validations.map(({ validatorKey, validatorVersion, attempt }) =>
      validationIdentity(validatorKey, validatorVersion, attempt),
    ),
  );
  if (
    available.size !== input.publication.requiredValidations.length ||
    input.publication.requiredValidations.some(
      ({ validatorKey, validatorVersion, attempt }) =>
        !available.has(
          validationIdentity(validatorKey, validatorVersion, attempt),
        ),
    )
  ) {
    throw new CorpusRevisionConflictError("REQUIRED_VALIDATION_NOT_STAGED");
  }
}

function validationIdentity(
  validatorKey: string,
  validatorVersion: string,
  attempt: number,
): string {
  return JSON.stringify([validatorKey, validatorVersion, attempt]);
}

function highestReviewSeverity(
  review: ReturnType<typeof PoemEnrichmentReviewSchema.parse>,
): "critical" | "major" | "minor" | "none" {
  if (review.findings.some(({ severity }) => severity === "critical"))
    return "critical";
  if (review.findings.some(({ severity }) => severity === "major"))
    return "major";
  if (review.findings.some(({ severity }) => severity === "minor"))
    return "minor";
  return "none";
}

function countsFromFrozenPlan(
  plan: PromotionPlan,
): Omit<ImportCounts, "bundleId"> {
  return {
    advancedPointers: plan.items.filter(
      ({ pointerAction }) => pointerAction !== "unchanged",
    ).length,
    insertedRevisions: plan.items.filter(
      ({ revisionAction }) => revisionAction === "insert",
    ).length,
    reusedRevisions: plan.items.filter(
      ({ revisionAction }) => revisionAction === "reuse",
    ).length,
    unchangedPointers: plan.items.filter(
      ({ pointerAction }) => pointerAction === "unchanged",
    ).length,
  };
}
