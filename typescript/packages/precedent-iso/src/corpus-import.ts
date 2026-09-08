import { z } from "zod";

import {
  AnyPoemEnrichmentOutputSchema,
  PoemEnrichmentReviewSchema,
  ReviewHighestSeveritySchema,
} from "./enrichment.js";

const HashSchema = z.string().regex(/^[\da-f]{64}$/);
const IdSchema = z
  .string()
  .trim()
  .min(1)
  .max(1_024)
  .refine((value) => !/[\u{0}-\u{1F}\u{7F}]/u.test(value), {
    message: "Identifiers cannot contain control characters",
  });
const VersionSchema = z.number().int().positive();
const ValidationOutcomeSchema = z.enum(["fail", "pass"]);
export const MAX_CORPUS_IMPORT_RECORDS = 50;
// D1 caps each string, BLOB, and complete row at 2,000,000 decimal bytes.
// Keeping the whole request below that boundary also bounds every JSON value
// persisted from it and avoids local-success/remote-rejection drift.
export const MAX_CORPUS_IMPORT_BYTES = 2_000_000;
export const SAQI_PRODUCTION_DATABASE_ID =
  "ffaae610-4dae-4d7e-bf86-8232f46ca2b5";
export const PublicationIdentitySchema = z.strictObject({
  databaseId: z.literal(SAQI_PRODUCTION_DATABASE_ID),
  schemaId: z.literal("saqi.publication-identity"),
  schemaVersion: z.literal(1),
  service: z.literal("saqi-production"),
});
export const LEGACY_CORPUS_REVISION_SCHEMA_VERSION = 1;
export const CORPUS_REVISION_SCHEMA_VERSION = 2;
export const APPROVED_ENRICHMENT_MODEL = "gpt-5.6-sol";
export const APPROVED_ENRICHMENT_REASONING_EFFORT = "medium";
export const APPROVED_ENRICHMENT_PROMPT_VERSION = "sol-word-gloss-v3";
export const APPROVED_ENRICHMENT_PROFILES = [
  {
    backendKey: "openai-codex-cli",
    displayName: "Sol 5.6",
    displayOrder: 100,
    iconKey: "openai",
    model: APPROVED_ENRICHMENT_MODEL,
    modelKey: "sol-5.6",
    modelVendorKey: "openai",
    provider: "sol",
    promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
    reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
    validatorPrefix: "sol",
  },
] as const;
const PREVIOUS_SOL_ENRICHMENT_PROFILE = {
  ...APPROVED_ENRICHMENT_PROFILES[0],
  promptVersion: "sol-word-gloss-v2",
  reasoningEffort: "high",
} as const;
export const ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES = [
  ...APPROVED_ENRICHMENT_PROFILES,
  PREVIOUS_SOL_ENRICHMENT_PROFILE,
] as const;
export type AcceptedPublicationEnrichmentProfile =
  (typeof ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES)[number];
export const LEGACY_ENRICHMENT_PROFILES = [
  {
    ...APPROVED_ENRICHMENT_PROFILES[0],
    promptVersion: "sol-enrichment-v1",
    reasoningEffort: "high",
  },
  {
    backendKey: "anthropic-claude-code-cli",
    displayName: "Claude Opus 5",
    displayOrder: 200,
    iconKey: "anthropic",
    model: "claude-opus-5",
    modelKey: "claude-opus-5",
    modelVendorKey: "anthropic",
    provider: "claude",
    promptVersion: "claude-opus-5-word-gloss-v2",
    reasoningEffort: "max",
    validatorPrefix: "claude-opus-5",
  },
  {
    backendKey: "anthropic-claude-code-cli",
    displayName: "Claude Opus 5",
    displayOrder: 201,
    iconKey: "anthropic",
    model: "claude-opus-5",
    modelKey: "claude-opus-5",
    modelVendorKey: "anthropic",
    provider: "claude",
    promptVersion: "claude-opus-5-enrichment-v1",
    reasoningEffort: "max",
    validatorPrefix: "claude-opus-5",
  },
  {
    backendKey: "agy-cli",
    displayName: "Gemini 3.1 Pro High",
    displayOrder: 300,
    iconKey: "google",
    model: "gemini-3.1-pro-high",
    modelKey: "agy-gemini-3.1-pro-high",
    modelVendorKey: "google",
    provider: "agy",
    promptVersion: "agy-gemini-3-1-pro-high-word-gloss-v2",
    reasoningEffort: "high",
    validatorPrefix: "agy-gemini-3-1-pro-high",
  },
  {
    backendKey: "agy-cli",
    displayName: "Claude Opus 4.6",
    displayOrder: 310,
    iconKey: "anthropic",
    model: "claude-opus-4-6-thinking",
    modelKey: "agy-claude-opus-4.6-thinking", // gitleaks:allow -- Public legacy model identifier, not a credential.
    modelVendorKey: "anthropic",
    provider: "agy",
    promptVersion: "agy-claude-opus-4-6-word-gloss-v2",
    reasoningEffort: "high",
    validatorPrefix: "agy-claude-opus-4-6-thinking",
  },
  {
    backendKey: "agy-cli",
    displayName: "Claude Opus 4.6",
    displayOrder: 311,
    iconKey: "anthropic",
    model: "claude-opus-4-6-thinking",
    modelKey: "agy-claude-opus-4.6-thinking", // gitleaks:allow -- Public legacy model identifier, not a credential.
    modelVendorKey: "anthropic",
    provider: "agy",
    promptVersion: "agy-claude-opus-4-6-enrichment-v1",
    reasoningEffort: "high",
    validatorPrefix: "agy-claude-opus-4-6-thinking",
  },
  PREVIOUS_SOL_ENRICHMENT_PROFILE,
] as const;
export const READABLE_ENRICHMENT_PROFILES = [
  ...APPROVED_ENRICHMENT_PROFILES,
  ...LEGACY_ENRICHMENT_PROFILES,
] as const;
export type ApprovedEnrichmentProfile =
  (typeof APPROVED_ENRICHMENT_PROFILES)[number];
export type ReadableEnrichmentProfile =
  (typeof READABLE_ENRICHMENT_PROFILES)[number];

export type EnrichmentModelIconKey =
  (typeof READABLE_ENRICHMENT_PROFILES)[number]["iconKey"];

export function approvedEnrichmentProfileByModelKey(
  modelKey: string,
): ApprovedEnrichmentProfile | undefined {
  return APPROVED_ENRICHMENT_PROFILES.find(
    (profile) => profile.modelKey === modelKey,
  );
}

function requiredApprovedEnrichmentProfile(
  modelKey: string,
): ApprovedEnrichmentProfile {
  const profile = approvedEnrichmentProfileByModelKey(modelKey);
  if (!profile)
    throw new Error(`Enrichment profile is not registered: ${modelKey}`);
  return profile;
}
const DEFAULT_APPROVED_ENRICHMENT_PROFILE =
  requiredApprovedEnrichmentProfile("sol-5.6");

export function approvedEnrichmentProfile(input: {
  readonly model: string;
  readonly promptVersion: string;
  readonly reasoningEffort: string;
}): ApprovedEnrichmentProfile | undefined {
  return APPROVED_ENRICHMENT_PROFILES.find(
    (profile) =>
      profile.model === input.model &&
      profile.promptVersion === input.promptVersion &&
      profile.reasoningEffort === input.reasoningEffort,
  );
}

export function readableEnrichmentProfile(input: {
  readonly model: string;
  readonly promptVersion: string;
  readonly reasoningEffort: string;
}): ReadableEnrichmentProfile | undefined {
  return READABLE_ENRICHMENT_PROFILES.find(
    (profile) =>
      profile.model === input.model &&
      profile.promptVersion === input.promptVersion &&
      profile.reasoningEffort === input.reasoningEffort,
  );
}

/** Accept completed work from the previous recipe without scheduling it anew. */
export function acceptedPublicationEnrichmentProfile(input: {
  readonly model: string;
  readonly promptVersion: string;
  readonly reasoningEffort: string;
}): AcceptedPublicationEnrichmentProfile | undefined {
  return ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.find(
    (profile) =>
      profile.model === input.model &&
      profile.promptVersion === input.promptVersion &&
      profile.reasoningEffort === input.reasoningEffort,
  );
}

/**
 * Profiles accepted by the generic publication endpoint. The endpoint retains
 * one exact legacy Sol recipe for create-only backfill; all other legacy
 * recipes remain readable but cannot create or move public pointers.
 */
export function publicationEnrichmentProfile(input: {
  readonly model: string;
  readonly promptVersion: string;
  readonly reasoningEffort: string;
}): ReadableEnrichmentProfile | undefined {
  return (
    acceptedPublicationEnrichmentProfile(input) ??
    (input.model === LEGACY_ENRICHMENT_PROFILES[0].model &&
    input.promptVersion === LEGACY_ENRICHMENT_PROFILES[0].promptVersion &&
    input.reasoningEffort === LEGACY_ENRICHMENT_PROFILES[0].reasoningEffort
      ? LEGACY_ENRICHMENT_PROFILES[0]
      : undefined)
  );
}

export function approvedEnrichmentValidations(
  profile: ReadableEnrichmentProfile,
) {
  const fidelity = {
    attempt: 1,
    validatorKey: `${profile.validatorPrefix}-fidelity-review`,
    validatorVersion: profile.promptVersion,
  } as const;
  const grounding = {
    attempt: 2,
    validatorKey: `${profile.validatorPrefix}-grounding-review`,
    validatorVersion: profile.promptVersion,
  } as const;
  return { all: [fidelity, grounding] as const, fidelity, grounding };
}
export const APPROVED_ENRICHMENT_VALIDATIONS = [
  {
    attempt: 1,
    validatorKey: "sol-fidelity-review",
    validatorVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
  },
  {
    attempt: 2,
    validatorKey: "sol-grounding-review",
    validatorVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
  },
] as const;

export interface EnrichmentValidationIdentity {
  readonly attempt: number;
  readonly validatorKey: string;
  readonly validatorVersion: string;
}

export function isApprovedEnrichmentValidationSet(
  validations: readonly EnrichmentValidationIdentity[],
  profile: ReadableEnrichmentProfile = DEFAULT_APPROVED_ENRICHMENT_PROFILE,
): boolean {
  const approved = approvedEnrichmentValidations(profile).all;
  if (validations.length !== approved.length) return false;
  const identities = new Set(
    validations.map(({ attempt, validatorKey, validatorVersion }) =>
      JSON.stringify([validatorKey, validatorVersion, attempt]),
    ),
  );
  return approved.every(({ attempt, validatorKey, validatorVersion }) =>
    identities.has(JSON.stringify([validatorKey, validatorVersion, attempt])),
  );
}

const WholeSecondTimestampSchema = z.iso
  .datetime({ offset: true })
  .refine((value) => Date.parse(value) % 1_000 === 0, {
    message: "Timestamp must resolve to a whole second",
  });

const CorpusContentArabicV1Schema = z.strictObject({
  content: z.array(z.string().max(5_000)).min(1).max(2_000),
});

const CorpusContentArabicV2Schema = z.strictObject({
  content: z.array(z.string().max(5_000)).min(1).max(2_000),
  titleArabic: z.string().trim().min(1).max(512),
});

export const CorpusContentArabicSchema = z.union([
  CorpusContentArabicV1Schema,
  CorpusContentArabicV2Schema,
]);
export type CorpusContentArabic = z.infer<typeof CorpusContentArabicSchema>;

export const CorpusStageRecordSchema = z.strictObject({
  authorNameArabic: z.string().trim().min(1).max(512),
  bundleId: IdSchema,
  canonicalAuthorId: IdSchema,
  canonicalPoemId: IdSchema.nullable(),
  contentArabic: CorpusContentArabicSchema,
  contentHash: HashSchema,
  observedAt: WholeSecondTimestampSchema,
  ordinal: z.number().int().nonnegative(),
  recordHash: HashSchema,
  sourceAuthorId: IdSchema,
  sourceAuthorUrl: z.url().max(4_096),
  sourceName: IdSchema,
  sourcePoemId: IdSchema,
  sourcePoemUrl: z.url().max(4_096),
  titleArabic: z.string().trim().min(1).max(512),
});

export const CorpusImportActionSchema = z.discriminatedUnion("action", [
  z.strictObject({
    action: z.literal("stage-and-plan"),
    input: z.strictObject({
      bundle: z.strictObject({
        expectedRecordCount: z
          .number()
          .int()
          .nonnegative()
          .max(MAX_CORPUS_IMPORT_RECORDS),
        id: IdSchema,
        manifestHash: HashSchema,
        schemaVersion: VersionSchema,
        writerEpoch: VersionSchema,
      }),
      records: z.array(CorpusStageRecordSchema).max(MAX_CORPUS_IMPORT_RECORDS),
      rootHash: HashSchema,
    }),
  }),
  z.strictObject({
    action: z.literal("promote"),
    bundleId: IdSchema,
    expectedPlanHash: HashSchema,
    writerEpoch: VersionSchema,
  }),
  z.strictObject({
    action: z.literal("publish-enrichment"),
    input: z.strictObject({
      artifact: z.strictObject({
        id: IdSchema,
        model: z.enum(APPROVED_ENRICHMENT_PROFILES.map(({ model }) => model)),
        modelKey: z
          .enum(APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey))
          .default("sol-5.6"),
        payload: AnyPoemEnrichmentOutputSchema,
        payloadHash: HashSchema,
        promptVersion: z.enum(
          READABLE_ENRICHMENT_PROFILES.map(
            ({ promptVersion }) => promptVersion,
          ),
        ),
        reasoningEffort: z.enum(
          ACCEPTED_PUBLICATION_ENRICHMENT_PROFILES.map(
            ({ reasoningEffort }) => reasoningEffort,
          ),
        ),
        schemaVersion: VersionSchema,
        sourceRevisionId: IdSchema,
        taskKey: IdSchema,
        variant: z.number().int().nonnegative(),
      }),
      publication: z.strictObject({
        artifactId: IdSchema,
        expectedPointerVersion: VersionSchema.nullable(),
        poemId: IdSchema,
        requiredValidations: z
          .array(
            z.strictObject({
              attempt: z.number().int().nonnegative(),
              validatorKey: IdSchema,
              validatorVersion: IdSchema,
            }),
          )
          .length(2),
        writerEpoch: VersionSchema,
      }),
      validations: z
        .array(
          z.strictObject({
            artifactId: IdSchema,
            attempt: z.number().int().nonnegative(),
            highestSeverity: ReviewHighestSeveritySchema,
            id: IdSchema,
            outcome: ValidationOutcomeSchema,
            report: PoemEnrichmentReviewSchema,
            reportHash: HashSchema,
            validatorKey: IdSchema,
            validatorVersion: IdSchema,
          }),
        )
        .length(2),
    }),
  }),
]);

export type CorpusImportAction = z.infer<typeof CorpusImportActionSchema>;
