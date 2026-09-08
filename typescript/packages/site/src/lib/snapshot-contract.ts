import { PoemWordGlossesSchema } from "@saqi/precedent-iso";
import { z } from "zod";

const UnsafeControl =
  /[\u{0000}-\u{0008}\u{000b}\u{000c}\u{000e}-\u{001f}\u{007f}\u{202a}-\u{202e}\u{2066}-\u{2069}]/u;
const SafeStringSchema = z
  .string()
  .refine(
    (value) => !UnsafeControl.test(value),
    "Text contains unsafe control characters",
  );
const NonBlankSchema = SafeStringSchema.trim().min(1).max(500);
const InsightTextSchema = SafeStringSchema.trim().min(1).max(20_000);
const ContentLineSchema = SafeStringSchema.max(5_000).transform((value) =>
  value.normalize("NFKC"),
);
const RouteSegmentSchema = NonBlankSchema.refine(
  (value) => !value.includes("/") && value !== "." && value !== "..",
  "Route segments cannot contain slashes or dot-paths",
);
const ProviderVendorSchema = z.enum(["anthropic", "google", "openai", "other"]);

const InsightsTrackSchema = z.enum(["legacy", "sol", "model"]);

export const SnapshotAuthorSchema = z.object({
  id: NonBlankSchema,
  slug: RouteSegmentSchema,
  nameArabic: NonBlankSchema,
  nameEnglish: NonBlankSchema.optional(),
});

export const SnapshotPoemInsightsSchema = z.strictObject({
  summary: InsightTextSchema,
  themes: z.array(InsightTextSchema).min(1).max(100),
  historicalContext: InsightTextSchema,
  literaryDevices: z.array(InsightTextSchema).min(1).max(100),
  culturalSignificance: InsightTextSchema,
  notableLines: z
    .array(
      z.strictObject({
        line: InsightTextSchema,
        explanation: InsightTextSchema,
      }),
    )
    .min(1)
    .max(100),
});

export const SnapshotPoemSchema = z.object({
  id: RouteSegmentSchema,
  slug: RouteSegmentSchema,
  authorId: NonBlankSchema,
  verses: z.number().int().positive().max(1_000),
  nameArabic: NonBlankSchema,
  nameEnglish: NonBlankSchema.optional(),
  linesArabic: z
    .array(ContentLineSchema)
    .min(1)
    .max(2_000)
    .refine((lines) => lines.some((line) => line.trim().length > 0), {
      message: "Arabic content must contain at least one nonblank line",
    }),
  linesEnglish: z.array(ContentLineSchema).min(1).max(2_000).optional(),
  linesEnglishAttributionCertainty: SafeStringSchema.trim()
    .min(1)
    .max(100)
    .optional(),
  linesEnglishModel: SafeStringSchema.trim().min(1).max(100).optional(),
  linesEnglishModelVendor: ProviderVendorSchema.optional(),
  linesEnglishGemini: z.array(ContentLineSchema).min(1).max(2_000).optional(),
  linesEnglishGeminiModel: SafeStringSchema.trim().min(1).max(100).optional(),
  linesEnglishSol: z.array(ContentLineSchema).min(1).max(2_000).optional(),
  linesEnglishSolModel: SafeStringSchema.trim().min(1).max(100).optional(),
  linesEnglishSolReasoningEffort: SafeStringSchema.trim()
    .min(1)
    .max(100)
    .optional(),
  modelEnrichments: z
    .array(
      z.strictObject({
        backendKey: SafeStringSchema.trim().min(1).max(100).optional(),
        backendName: SafeStringSchema.trim().min(1).max(100).optional(),
        displayName: SafeStringSchema.trim().min(1).max(100).optional(),
        insights: SnapshotPoemInsightsSchema.optional(),
        lines: z.array(ContentLineSchema).min(1).max(2_000),
        model: SafeStringSchema.trim().min(1).max(100),
        modelKey: SafeStringSchema.trim().min(1).max(100),
        profileKey: SafeStringSchema.trim().min(1).max(100).optional(),
        reasoningEffort: SafeStringSchema.trim().min(1).max(100),
        vendorKey: ProviderVendorSchema.optional(),
        wordGlosses: PoemWordGlossesSchema.optional(),
      }),
    )
    .max(20)
    .optional(),
  insights: SnapshotPoemInsightsSchema.optional(),
  insightsModel: SafeStringSchema.trim().min(1).max(100).optional(),
  insightsReasoningEffort: SafeStringSchema.trim().min(1).max(100).optional(),
  insightsTrack: InsightsTrackSchema.optional(),
});

export type Author = z.infer<typeof SnapshotAuthorSchema>;
export type Poem = z.infer<typeof SnapshotPoemSchema>;
