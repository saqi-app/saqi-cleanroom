import { z } from "zod";

import { PoemInsightsSchema, PoemWordGlossesSchema } from "../enrichment.js";
import {
  ContentLineSchema,
  IdentityTextSchema,
  RouteSegmentSchema,
} from "./text.js";

export const PoemContentSchema = z.object({
  content: z
    .array(ContentLineSchema)
    .min(1)
    .max(2_000)
    .refine(
      (lines) =>
        lines.reduce((total, line) => total + line.length, 0) <= 1_000_000,
      "Poem exceeds the character budget",
    ),
});

export const PoemSchema = z.object({
  id: RouteSegmentSchema,
  slug: RouteSegmentSchema,
  authorId: IdentityTextSchema,
  verses: z.number().int().positive().max(1_000),
  nameArabic: IdentityTextSchema.optional(),
  nameEnglish: IdentityTextSchema.optional(),
});
export type Poem = z.infer<typeof PoemSchema>;

export const PoemCopySchema = z.object({
  id: RouteSegmentSchema,
  title: IdentityTextSchema,
  lines: z.array(ContentLineSchema).max(2_000),
});
export type PoemCopy = z.infer<typeof PoemCopySchema>;

export const PoemModelEnrichmentSchema = z.strictObject({
  insights: PoemInsightsSchema.optional(),
  lines: z.array(ContentLineSchema).min(1).max(2_000),
  model: IdentityTextSchema,
  modelKey: IdentityTextSchema,
  reasoningEffort: IdentityTextSchema,
  wordGlosses: PoemWordGlossesSchema.optional(),
});
export type PoemModelEnrichment = z.infer<typeof PoemModelEnrichmentSchema>;

export const LoadedPoemSchema = z.object({
  id: RouteSegmentSchema,
  slug: RouteSegmentSchema,
  authorId: IdentityTextSchema,
  verses: z.number().int().positive().max(1_000),
  nameArabic: IdentityTextSchema.optional(),
  nameEnglish: IdentityTextSchema.optional(),
  linesArabic: z.array(ContentLineSchema).max(2_000),
  linesEnglish: z.array(ContentLineSchema).max(2_000).optional(),
  linesEnglishGemini: z.array(ContentLineSchema).max(2_000).optional(),
  linesEnglishSol: z.array(ContentLineSchema).max(2_000).optional(),
  modelEnrichments: z.array(PoemModelEnrichmentSchema).max(20).optional(),
});
export type LoadedPoem = z.infer<typeof LoadedPoemSchema>;
