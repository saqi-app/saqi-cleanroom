import { z } from "zod";

import { APPROVED_ENRICHMENT_PROFILES } from "./corpus-import.js";

export const PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID =
  "saqi.production-resolution-request";
export const PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID =
  "saqi.production-resolution-response";
export const PRODUCTION_RESOLUTION_SCHEMA_VERSION = 1;
export const PRODUCTION_RESOLUTION_REQUEST_V2_SCHEMA_VERSION = 2;
export const PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION = 3;
export const PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION = 2;
export const PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM =
  "sha256-canonical-nfc-v1";
export const MAX_PRODUCTION_RESOLUTION_TARGETS = 50;
export const MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES = 131_072;

const HashSchema = z.string().regex(/^[a-f\d]{64}$/);
export const CanonicalResourceIdSchema = z.union([z.uuid(), HashSchema]);
const SourcePoemIdSchema = z
  .string()
  .max(128)
  .regex(/^[1-9]\d*$/);
const SourceAuthorSlugSchema = z
  .string()
  .trim()
  .min(1)
  .max(1_000)
  .refine((value) => !/[\u{0}-\u{1f}\u{7f}]/u.test(value), {
    message: "Source author slugs cannot contain control characters",
  });
const ModelKeySchema = z.enum(
  APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey),
);
const WholeSecondTimestampSchema = z.iso
  .datetime({ offset: true })
  .refine((value) => Date.parse(value) % 1_000 === 0, {
    message: "Timestamp must resolve to a whole second",
  });

const ProductionResolutionModelKeysSchema = z
  .array(ModelKeySchema)
  .min(1)
  .max(APPROVED_ENRICHMENT_PROFILES.length)
  .refine((modelKeys) => new Set(modelKeys).size === modelKeys.length, {
    message: "Model keys must be unique",
  });

export const ProductionResolutionSourceTargetSchema = z.strictObject({
  modelKeys: ProductionResolutionModelKeysSchema,
  sourceAuthorSlug: SourceAuthorSlugSchema,
  sourcePoemId: SourcePoemIdSchema,
});

export const ProductionResolutionCanonicalTargetSchema = z.strictObject({
  modelKeys: ProductionResolutionModelKeysSchema,
  poemId: CanonicalResourceIdSchema,
  sourceRevisionId: HashSchema,
});

export const ProductionResolutionFingerprintTargetSchema = z.strictObject({
  fingerprintAlgorithm: z.literal(PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM),
  lineNfcHash: HashSchema,
  modelKeys: ProductionResolutionModelKeysSchema,
  promptMaterialHash: HashSchema,
});

export const ProductionResolutionTargetSchema = z.union([
  ProductionResolutionSourceTargetSchema,
  ProductionResolutionCanonicalTargetSchema,
]);

export const ProductionResolutionRequestV1Schema = z
  .strictObject({
    schemaId: z.literal(PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID),
    schemaVersion: z.literal(PRODUCTION_RESOLUTION_SCHEMA_VERSION),
    targets: z
      .array(ProductionResolutionSourceTargetSchema)
      .min(1)
      .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
  })
  .superRefine(({ targets }, context) => {
    const identities = targets.map(productionResolutionTargetIdentity);
    if (new Set(identities).size !== identities.length) {
      context.addIssue({
        code: "custom",
        message: "Production resolution targets must be unique",
      });
    }
  });

export const ProductionResolutionRequestV2Schema = z
  .strictObject({
    schemaId: z.literal(PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID),
    schemaVersion: z.literal(PRODUCTION_RESOLUTION_REQUEST_V2_SCHEMA_VERSION),
    targets: z
      .array(ProductionResolutionTargetSchema)
      .min(1)
      .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
  })
  .superRefine(({ targets }, context) => {
    const identities = targets.map(productionResolutionTargetIdentity);
    if (new Set(identities).size !== identities.length) {
      context.addIssue({
        code: "custom",
        message: "Production resolution targets must be unique",
      });
    }
  });

export const ProductionResolutionRequestV3Schema = z
  .strictObject({
    schemaId: z.literal(PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID),
    schemaVersion: z.literal(PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION),
    targets: z
      .array(ProductionResolutionFingerprintTargetSchema)
      .min(1)
      .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
  })
  .superRefine(({ targets }, context) => {
    const identities = targets.map(productionResolutionTargetIdentity);
    if (new Set(identities).size !== identities.length) {
      context.addIssue({
        code: "custom",
        message: "Production resolution targets must be unique",
      });
    }
  });

export const ProductionResolutionRequestSchema = z.union([
  ProductionResolutionRequestV1Schema,
  ProductionResolutionRequestV2Schema,
  ProductionResolutionRequestV3Schema,
]);

export const ProductionResolutionModelPointerSchema = z.strictObject({
  modelKey: ModelKeySchema,
  pointerVersion: z.number().int().positive(),
});

export const ProductionResolutionResultTargetSchema = z
  .strictObject({
    authorId: CanonicalResourceIdSchema,
    authorNameArabic: z.string().trim().min(1).max(10_000),
    currentSourceNfcSha256: HashSchema.nullable().optional(),
    currentSourceRevisionId: HashSchema.nullable(),
    modelPointers: z
      .array(ProductionResolutionModelPointerSchema)
      .max(APPROVED_ENRICHMENT_PROFILES.length),
    poemId: CanonicalResourceIdSchema,
    sourceAuthorSlug: SourceAuthorSlugSchema,
    sourcePoemId: SourcePoemIdSchema,
    sourcePointerVersion: z.number().int().positive().nullable(),
  })
  .superRefine(
    (
      {
        currentSourceNfcSha256,
        currentSourceRevisionId,
        modelPointers,
        sourcePointerVersion,
      },
      context,
    ) => {
      if (
        (currentSourceRevisionId === null) !==
        (sourcePointerVersion === null)
      ) {
        context.addIssue({
          code: "custom",
          message:
            "Source revision and pointer version must be present together",
        });
      }
      if (
        currentSourceRevisionId === null &&
        currentSourceNfcSha256 !== null &&
        currentSourceNfcSha256 !== undefined
      ) {
        context.addIssue({
          code: "custom",
          message: "Source fingerprint requires an active source revision",
        });
      }
      if (
        new Set(modelPointers.map(({ modelKey }) => modelKey)).size !==
        modelPointers.length
      ) {
        context.addIssue({
          code: "custom",
          message: "Model pointers must be unique",
        });
      }
    },
  );

export const ProductionResolutionActiveFingerprintSchema = z.strictObject({
  algorithm: z.literal(PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM),
  lineNfcHash: HashSchema,
  promptMaterialHash: HashSchema,
});

export const ProductionResolutionFingerprintResultTargetSchema =
  ProductionResolutionResultTargetSchema.safeExtend({
    activeSourceFingerprint: ProductionResolutionActiveFingerprintSchema,
    currentSourceNfcSha256: HashSchema,
    currentSourceRevisionId: HashSchema,
    sourcePointerVersion: z.number().int().positive(),
  });

export const ProductionResolutionResponseBodyV1Schema = z.strictObject({
  expiresAt: WholeSecondTimestampSchema,
  observedAt: WholeSecondTimestampSchema,
  schemaId: z.literal(PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID),
  schemaVersion: z.literal(PRODUCTION_RESOLUTION_SCHEMA_VERSION),
  scopeHash: HashSchema,
  targets: z
    .array(ProductionResolutionResultTargetSchema)
    .min(1)
    .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
  writerEpoch: z.number().int().positive(),
});

export const ProductionResolutionResponseBodyV2Schema = z.strictObject({
  expiresAt: WholeSecondTimestampSchema,
  observedAt: WholeSecondTimestampSchema,
  schemaId: z.literal(PRODUCTION_RESOLUTION_RESPONSE_SCHEMA_ID),
  schemaVersion: z.literal(PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION),
  scopeHash: HashSchema,
  targets: z
    .array(ProductionResolutionFingerprintResultTargetSchema)
    .min(1)
    .max(MAX_PRODUCTION_RESOLUTION_TARGETS),
  writerEpoch: z.number().int().positive(),
});

export const ProductionResolutionResponseBodySchema = z.union([
  ProductionResolutionResponseBodyV1Schema,
  ProductionResolutionResponseBodyV2Schema,
]);

export const ProductionResolutionResponseV1Schema =
  ProductionResolutionResponseBodyV1Schema.extend({ manifestHash: HashSchema });
export const ProductionResolutionResponseV2Schema =
  ProductionResolutionResponseBodyV2Schema.extend({ manifestHash: HashSchema });
export const ProductionResolutionResponseSchema = z.union([
  ProductionResolutionResponseV1Schema,
  ProductionResolutionResponseV2Schema,
]);

export const ProductionResolutionApiResponseSchema = z.strictObject({
  ok: z.literal(true),
  result: ProductionResolutionResponseSchema,
});

export type ProductionResolutionRequest = z.infer<
  typeof ProductionResolutionRequestSchema
>;
export type ProductionResolutionResponse = z.infer<
  typeof ProductionResolutionResponseSchema
>;
export type ProductionResolutionResponseBody = z.infer<
  typeof ProductionResolutionResponseBodySchema
>;
export type ProductionResolutionResultTarget = z.infer<
  typeof ProductionResolutionResultTargetSchema
>;
export type ProductionResolutionTarget = z.infer<
  typeof ProductionResolutionTargetSchema
>;
export type ProductionResolutionFingerprintTarget = z.infer<
  typeof ProductionResolutionFingerprintTargetSchema
>;

export function normalizeProductionResolutionRequest(
  input: unknown,
): ProductionResolutionRequest {
  const request = ProductionResolutionRequestSchema.parse(input);
  if (request.schemaVersion === PRODUCTION_RESOLUTION_SCHEMA_VERSION) {
    return {
      ...request,
      targets: request.targets
        .map((target) => ({
          ...target,
          modelKeys: target.modelKeys.toSorted(),
        }))
        .toSorted(
          (left, right) =>
            left.sourcePoemId.localeCompare(right.sourcePoemId) ||
            left.sourceAuthorSlug.localeCompare(right.sourceAuthorSlug),
        ),
    };
  }
  if (
    request.schemaVersion === PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION
  ) {
    return {
      ...request,
      targets: request.targets
        .map((target) => ({
          ...target,
          modelKeys: target.modelKeys.toSorted(),
        }))
        .toSorted(
          (left, right) =>
            left.fingerprintAlgorithm.localeCompare(
              right.fingerprintAlgorithm,
            ) ||
            left.lineNfcHash.localeCompare(right.lineNfcHash) ||
            left.promptMaterialHash.localeCompare(right.promptMaterialHash),
        ),
    };
  }
  return {
    ...request,
    targets: request.targets
      .map((target) => ({ ...target, modelKeys: target.modelKeys.toSorted() }))
      .toSorted((left, right) => {
        if (isProductionResolutionSourceTarget(left)) {
          if (isProductionResolutionSourceTarget(right))
            return (
              left.sourcePoemId.localeCompare(right.sourcePoemId) ||
              left.sourceAuthorSlug.localeCompare(right.sourceAuthorSlug)
            );
          return -1;
        }
        if (isProductionResolutionSourceTarget(right)) return 1;
        return (
          left.poemId.localeCompare(right.poemId) ||
          left.sourceRevisionId.localeCompare(right.sourceRevisionId)
        );
      }),
  };
}

export function isProductionResolutionSourceTarget(
  target: ProductionResolutionTarget,
): target is z.infer<typeof ProductionResolutionSourceTargetSchema> {
  return "sourcePoemId" in target;
}

export function productionResolutionTargetIdentity(
  target: ProductionResolutionFingerprintTarget | ProductionResolutionTarget,
): string {
  if (isProductionResolutionFingerprintTarget(target)) {
    return `fingerprint\u{1f}${target.fingerprintAlgorithm}\u{1f}${target.lineNfcHash}\u{1f}${target.promptMaterialHash}`;
  }
  return isProductionResolutionSourceTarget(target)
    ? `source\u{1f}${target.sourcePoemId}`
    : `canonical\u{1f}${target.poemId}\u{1f}${target.sourceRevisionId}`;
}

export function isProductionResolutionFingerprintTarget(
  target: ProductionResolutionFingerprintTarget | ProductionResolutionTarget,
): target is ProductionResolutionFingerprintTarget {
  return "fingerprintAlgorithm" in target;
}
