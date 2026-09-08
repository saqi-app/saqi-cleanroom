import { z } from "zod";

import {
  ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES,
  acceptedPublicationEnrichmentProfile,
  approvedEnrichmentValidations,
  SAQI_PRODUCTION_DATABASE_ID,
} from "./corpus-import.js";
import {
  PoemEnrichmentInputSchema,
  PoemEnrichmentOutputV2Schema,
  PoemEnrichmentReviewSchema,
  ReviewHighestSeveritySchema,
} from "./enrichment.js";

const HashSchema = z.string().regex(/^[\da-f]{64}$/);
const IdentifierSchema = z
  .string()
  .trim()
  .min(1)
  .max(1_024)
  .refine((value) => !/[\u{0}-\u{1f}\u{7f}]/u.test(value), {
    message: "Identifiers cannot contain control characters",
  });
const VersionSchema = z.number().int().positive();
const SourceAdmissionStatusSchema = z.enum(["admitted", "unchanged"]);
const SourceAdmissionRejectionCodeSchema = z.enum([
  "AUTHOR_NOT_FOUND",
  "IDENTITY_CONFLICT",
  "SOURCE_TOMBSTONED",
  "SOURCE_VALIDATION_FAILED",
  "WRITER_EPOCH_MISMATCH",
]);
const PublicationOutcomeSchema = z.enum(["published", "already-published"]);
const PublicationRejectionCodeSchema = z.enum([
  "ACTION_HASH_CONFLICT",
  "ARTIFACT_INVALID",
  "BINDING_INVALID",
  "POINTER_CONFLICT",
  "SOURCE_CHANGED",
  "SOURCE_TOMBSTONED",
  "VALIDATION_INVALID",
]);
const WholeSecondTimestampSchema = z.iso
  .datetime({ offset: true })
  .refine((value) => Date.parse(value) % 1_000 === 0, {
    message: "Timestamp must resolve to a whole second",
  });
const ModelKeySchema = z.enum(
  ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey),
);
const ModelSchema = z.enum(
  ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.map(({ model }) => model),
);
const PromptVersionSchema = z.enum(
  ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.map(
    ({ promptVersion }) => promptVersion,
  ),
);
const ReasoningEffortSchema = z.enum(
  ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.map(
    ({ reasoningEffort }) => reasoningEffort,
  ),
);
const ExternalPoemIdSchema = z
  .string()
  .max(128)
  .regex(/^[1-9]\d*$/);
export const SourceNameSchema = z
  .string()
  .trim()
  .regex(/^[a-z][a-z0-9_-]{1,63}$/u);
export const SourceOriginSchema = z
  .url({ protocol: /^https$/u })
  .regex(
    /^https:\/\/(?:\[[\d.:A-Fa-f]+\]|[^\s/?#:@]+)\/?$/u,
    "Source origin must be a clean HTTPS origin",
  )
  .transform((value) => (value.endsWith("/") ? value.slice(0, -1) : value));
export const SourceConfigurationSchema = z.strictObject({
  name: SourceNameSchema,
  origin: SourceOriginSchema,
});

export const CANONICAL_POEM_BINDING_SCHEMA_ID = "saqi.canonical-poem-binding";
export const CANONICAL_POEM_BINDING_SCHEMA_VERSION = 1;
export const POEM_ENRICHMENT_INPUT_V2_SCHEMA_VERSION = 2;
export const SOURCE_ADMISSION_SCHEMA_ID = "saqi.source-admission";
export const SOURCE_ADMISSION_SCHEMA_VERSION = 2;
export const ENRICHMENT_PUBLICATION_SCHEMA_ID = "saqi.enrichment-publication";
export const ENRICHMENT_PUBLICATION_SCHEMA_VERSION = 2;
export const ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_ID =
  "saqi.enrichment-publication-receipt";
export const ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_VERSION = 1;
export const MAX_SOURCE_ADMISSION_ITEMS = 50;
export const MAX_ENRICHMENT_PUBLICATION_ITEMS = 50;

/** A production-issued, immutable authorization to enrich one source revision. */
export const CanonicalPoemAdmissionEvidenceV1Schema = z.strictObject({
  databaseId: z.literal(SAQI_PRODUCTION_DATABASE_ID),
  issuedAt: WholeSecondTimestampSchema,
  sourcePointerVersion: VersionSchema,
});

export const CanonicalPoemBindingV1Schema = z.strictObject({
  admissionEvidence: CanonicalPoemAdmissionEvidenceV1Schema,
  authorId: IdentifierSchema,
  authorNameArabic: z.string().trim().min(1).max(512),
  bindingId: HashSchema,
  externalPoemId: ExternalPoemIdSchema,
  lineNfcHash: HashSchema,
  poemId: IdentifierSchema,
  promptMaterialHash: HashSchema,
  schemaId: z.literal(CANONICAL_POEM_BINDING_SCHEMA_ID),
  schemaVersion: z.literal(CANONICAL_POEM_BINDING_SCHEMA_VERSION),
  sourceName: SourceNameSchema,
  sourceRevisionId: HashSchema,
});
export type CanonicalPoemBindingV1 = z.infer<
  typeof CanonicalPoemBindingV1Schema
>;

/** The v1 enrichment input plus the production identity bound before inference. */
export const PoemEnrichmentInputV2Schema = PoemEnrichmentInputSchema.extend({
  canonicalBinding: CanonicalPoemBindingV1Schema,
  schemaVersion: z.literal(POEM_ENRICHMENT_INPUT_V2_SCHEMA_VERSION),
}).superRefine(({ canonicalBinding, poemId, sourceRevisionId }, context) => {
  if (canonicalBinding.poemId !== poemId) {
    context.addIssue({
      code: "custom",
      message: "Enrichment poem ID must match its canonical binding",
      path: ["poemId"],
    });
  }
  if (canonicalBinding.sourceRevisionId !== sourceRevisionId) {
    context.addIssue({
      code: "custom",
      message: "Enrichment source revision must match its canonical binding",
      path: ["sourceRevisionId"],
    });
  }
});
export type PoemEnrichmentInputV2 = z.infer<typeof PoemEnrichmentInputV2Schema>;

export const SourceAdmissionV2ItemSchema = z.strictObject({
  admissionId: HashSchema,
  lineNfcHash: HashSchema,
  linesArabic: z.array(z.string().max(5_000)).min(1).max(2_000),
  sourceAuthorId: IdentifierSchema,
  sourceAuthorUrl: z.url().max(4_096),
  sourceContentSha256: HashSchema,
  sourceName: SourceNameSchema,
  externalPoemId: ExternalPoemIdSchema,
  sourcePoemUrl: z.url().max(4_096),
  sourceRevisionId: HashSchema,
  titleArabic: z.string().trim().min(1).max(512),
});

export const SourceAdmissionV2RequestSchema = z
  .strictObject({
    items: z
      .array(SourceAdmissionV2ItemSchema)
      .min(1)
      .max(MAX_SOURCE_ADMISSION_ITEMS),
    schemaId: z.literal(SOURCE_ADMISSION_SCHEMA_ID),
    schemaVersion: z.literal(SOURCE_ADMISSION_SCHEMA_VERSION),
  })
  .superRefine(({ items }, context) => {
    if (
      new Set(items.map(({ admissionId }) => admissionId)).size !== items.length
    ) {
      context.addIssue({
        code: "custom",
        message: "Admission IDs must be unique",
      });
    }
  });

export const SourceAdmissionV2ResultSchema = z.discriminatedUnion("status", [
  z.strictObject({
    admissionId: HashSchema,
    binding: CanonicalPoemBindingV1Schema,
    status: SourceAdmissionStatusSchema,
  }),
  z.strictObject({
    admissionId: HashSchema,
    code: SourceAdmissionRejectionCodeSchema,
    message: z.string().trim().min(1).max(2_000),
    retryable: z.boolean(),
    status: z.literal("rejected"),
  }),
]);

export const SourceAdmissionV2ResponseSchema = z.strictObject({
  observedAt: WholeSecondTimestampSchema,
  results: z
    .array(SourceAdmissionV2ResultSchema)
    .min(1)
    .max(MAX_SOURCE_ADMISSION_ITEMS),
  schemaId: z.literal(SOURCE_ADMISSION_SCHEMA_ID),
  schemaVersion: z.literal(SOURCE_ADMISSION_SCHEMA_VERSION),
  writerEpoch: VersionSchema,
});

const EnrichmentValidationV2Schema = z.strictObject({
  artifactId: IdentifierSchema,
  attempt: z.number().int().positive(),
  highestSeverity: ReviewHighestSeveritySchema,
  id: IdentifierSchema,
  outcome: z.literal("pass"),
  report: PoemEnrichmentReviewSchema,
  reportHash: HashSchema,
  validatorKey: IdentifierSchema,
  validatorVersion: IdentifierSchema,
});

export const EnrichmentPublicationV2ItemSchema = z
  .strictObject({
    actionHash: HashSchema,
    artifact: z.strictObject({
      id: IdentifierSchema,
      model: ModelSchema,
      modelKey: ModelKeySchema,
      payload: PoemEnrichmentOutputV2Schema,
      payloadHash: HashSchema,
      promptVersion: PromptVersionSchema,
      reasoningEffort: ReasoningEffortSchema,
      schemaVersion: VersionSchema,
      sourceRevisionId: HashSchema,
      taskKey: IdentifierSchema,
      variant: z.number().int().nonnegative(),
    }),
    binding: CanonicalPoemBindingV1Schema,
    publicationIntentId: HashSchema,
    validations: z.array(EnrichmentValidationV2Schema).length(2),
  })
  .superRefine(({ artifact, binding, validations }, context) => {
    if (artifact.sourceRevisionId !== binding.sourceRevisionId) {
      context.addIssue({
        code: "custom",
        message: "Artifact source revision must match its canonical binding",
        path: ["artifact", "sourceRevisionId"],
      });
    }
    if (validations.some(({ artifactId }) => artifactId !== artifact.id)) {
      context.addIssue({
        code: "custom",
        message: "Every validation must refer to the published artifact",
        path: ["validations"],
      });
    }
    const profile = acceptedPublicationEnrichmentProfile(artifact);
    if (!profile) {
      context.addIssue({
        code: "custom",
        message: "Artifact must match an exact accepted publication profile",
        path: ["artifact"],
      });
      return;
    }
    const approvedValidations = approvedEnrichmentValidations(profile).all;
    const validationIdentities = new Set(
      validations.map(({ attempt, validatorKey, validatorVersion }) =>
        JSON.stringify([attempt, validatorKey, validatorVersion]),
      ),
    );
    if (
      !approvedValidations.every(
        ({ attempt, validatorKey, validatorVersion }) =>
          validationIdentities.has(
            JSON.stringify([attempt, validatorKey, validatorVersion]),
          ),
      ) ||
      validations.some(
        ({ highestSeverity, report }) =>
          highestSeverity === "critical" ||
          highestSeverity === "major" ||
          report.verdict !== "pass",
      )
    ) {
      context.addIssue({
        code: "custom",
        message: "Artifact must carry its two approved passing validations",
        path: ["validations"],
      });
    }
  });

export const EnrichmentPublicationV2RequestSchema = z
  .strictObject({
    items: z
      .array(EnrichmentPublicationV2ItemSchema)
      .min(1)
      .max(MAX_ENRICHMENT_PUBLICATION_ITEMS),
    schemaId: z.literal(ENRICHMENT_PUBLICATION_SCHEMA_ID),
    schemaVersion: z.literal(ENRICHMENT_PUBLICATION_SCHEMA_VERSION),
  })
  .superRefine(({ items }, context) => {
    if (
      new Set(items.map(({ publicationIntentId }) => publicationIntentId))
        .size !== items.length
    ) {
      context.addIssue({
        code: "custom",
        message: "Publication intent IDs must be unique",
      });
    }
  });

export const EnrichmentPublicationReceiptV1Schema = z.strictObject({
  actionHash: HashSchema,
  artifactId: IdentifierSchema,
  committedAt: WholeSecondTimestampSchema,
  modelKey: ModelKeySchema,
  outcome: PublicationOutcomeSchema,
  poemId: IdentifierSchema,
  pointerVersion: VersionSchema,
  promptVersion: PromptVersionSchema,
  publicationIntentId: HashSchema,
  schemaId: z.literal(ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_ID),
  schemaVersion: z.literal(ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_VERSION),
  sourceRevisionId: HashSchema,
  writerEpoch: VersionSchema,
});

export const EnrichmentPublicationV2ResultSchema = z.discriminatedUnion(
  "status",
  [
    z.strictObject({
      receipt: EnrichmentPublicationReceiptV1Schema,
      status: z.literal("published"),
    }),
    z.strictObject({
      code: PublicationRejectionCodeSchema,
      message: z.string().trim().min(1).max(2_000),
      publicationIntentId: HashSchema,
      retryable: z.boolean(),
      status: z.literal("rejected"),
    }),
  ],
);

export const EnrichmentPublicationV2ResponseSchema = z.strictObject({
  results: z
    .array(EnrichmentPublicationV2ResultSchema)
    .min(1)
    .max(MAX_ENRICHMENT_PUBLICATION_ITEMS),
  schemaId: z.literal(ENRICHMENT_PUBLICATION_SCHEMA_ID),
  schemaVersion: z.literal(ENRICHMENT_PUBLICATION_SCHEMA_VERSION),
});

export type SourceAdmissionV2Request = z.infer<
  typeof SourceAdmissionV2RequestSchema
>;
export type SourceAdmissionV2Item = z.infer<typeof SourceAdmissionV2ItemSchema>;
export type SourceAdmissionV2Result = z.infer<
  typeof SourceAdmissionV2ResultSchema
>;
export type SourceAdmissionV2Response = z.infer<
  typeof SourceAdmissionV2ResponseSchema
>;
export type EnrichmentPublicationV2Request = z.infer<
  typeof EnrichmentPublicationV2RequestSchema
>;
export type EnrichmentPublicationV2Item = z.infer<
  typeof EnrichmentPublicationV2ItemSchema
>;
export type EnrichmentPublicationV2Result = z.infer<
  typeof EnrichmentPublicationV2ResultSchema
>;
export type EnrichmentPublicationV2Response = z.infer<
  typeof EnrichmentPublicationV2ResponseSchema
>;
export type EnrichmentPublicationReceiptV1 = z.infer<
  typeof EnrichmentPublicationReceiptV1Schema
>;

/** UTF-8 bytes of this return value are hashed with SHA-256. */
export function sourceLineNfcHashBody(linesArabic: readonly string[]): string {
  return stableCanonicalJson(linesArabic.map((line) => line.normalize("NFC")));
}

/** UTF-8 bytes of this return value are hashed with SHA-256. */
export function sourcePromptMaterialHashBody(input: {
  readonly authorArabic: string;
  readonly linesArabic: readonly string[];
  readonly titleArabic: string;
}): string {
  return stableCanonicalJson({
    authorArabic: input.authorArabic.normalize("NFC"),
    linesArabic: input.linesArabic.map((line) => line.normalize("NFC")),
    titleArabic: input.titleArabic.normalize("NFC"),
  });
}

/** Admission evidence is informational and deliberately excluded from identity. */
export function canonicalPoemBindingIdBody(
  binding: Omit<CanonicalPoemBindingV1, "admissionEvidence" | "bindingId">,
): string {
  return stableCanonicalJson(binding);
}

export function sourceAdmissionIdBody(
  item: Omit<z.infer<typeof SourceAdmissionV2ItemSchema>, "admissionId">,
): string {
  return stableCanonicalJson(item);
}

export function publicationIntentIdBody(input: {
  readonly artifactId: string;
  readonly bindingId: string;
  readonly modelKey: string;
  readonly promptVersion: string;
}): string {
  return stableCanonicalJson(input);
}

export function enrichmentPublicationActionHashBody(
  item: Omit<z.input<typeof EnrichmentPublicationV2ItemSchema>, "actionHash">,
): string {
  return stableCanonicalJson(item);
}

export function stableCanonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string")
    return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Non-finite JSON number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value))
    return `[${value.map(stableCanonicalJson).join(",")}]`;
  if (typeof value !== "object") throw new TypeError("Unsupported JSON value");
  return `{${Object.entries(value)
    .filter(([, nested]) => nested !== undefined)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(
      ([key, nested]) =>
        `${JSON.stringify(key)}:${stableCanonicalJson(nested)}`,
    )
    .join(",")}}`;
}
