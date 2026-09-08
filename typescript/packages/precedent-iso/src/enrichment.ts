import { z } from "zod";

const MAX_LINE_CHARACTERS = 5_000;
const MAX_LINES = 2_000;
const MAX_PROSE_CHARACTERS = 20_000;
const MAX_GLOSS_CHARACTERS = 2_000;
const MAX_LIST_ITEMS = 100;
const SHA256_HEX = /^[\da-f]{64}$/;
const UNSAFE_CONTROL =
  /[\u{0000}-\u{0008}\u{000b}\u{000c}\u{000e}-\u{001f}\u{007f}\u{202a}-\u{202e}\u{2066}-\u{2069}]/u;
const ARABIC_SCRIPT = /\p{Script=Arabic}/u;
// Require explicit assistant-wrapper language. Bare first-person negatives are
// common and legitimate in poetry ("I cannot bear...", "I will not listen").
const REFUSAL =
  /\b(?:as an ai|(?:i (?:cannot|can't|will not|won't)|(?:i am|i'm) unable to) (?:assist|comply|complete|fulfill|help|perform|provide|translate)(?:\b|\s+with))\b/i;

const SafeTextSchema = z
  .string()
  .refine(
    (value) => !UNSAFE_CONTROL.test(value),
    "Text contains unsafe control characters",
  );
const IdentifierSchema = z.string().trim().min(1).max(512);
const HashSchema = z.string().regex(SHA256_HEX);
const SourceLineSchema = SafeTextSchema.max(MAX_LINE_CHARACTERS);
const TranslatedLineSchema = SafeTextSchema.max(MAX_LINE_CHARACTERS).refine(
  (line) => !line.includes("\n") && !line.includes("\r"),
  "A translated line cannot contain embedded line breaks",
);
const InsightTextSchema = SafeTextSchema.trim()
  .min(1)
  .max(MAX_PROSE_CHARACTERS);

export const ENRICHMENT_INPUT_SCHEMA_ID = "saqi.poem-enrichment-input";
export const ENRICHMENT_INPUT_SCHEMA_VERSION = 1;
export const ENRICHMENT_OUTPUT_SCHEMA_ID = "saqi.poem-enrichment-output";
export const ENRICHMENT_OUTPUT_SCHEMA_VERSION = 1;
export const ENRICHMENT_OUTPUT_V2_SCHEMA_VERSION = 2;
export const WORD_GLOSS_TOKENIZER_VERSION = "saqi-orthographic-v1";
export const ENRICHMENT_REVIEW_SCHEMA_ID = "saqi.poem-enrichment-review";
export const ENRICHMENT_REVIEW_SCHEMA_VERSION = 1;

export const PoemEnrichmentInputSchema = z.strictObject({
  schemaId: z.literal(ENRICHMENT_INPUT_SCHEMA_ID),
  schemaVersion: z.literal(ENRICHMENT_INPUT_SCHEMA_VERSION),
  poemId: IdentifierSchema,
  sourceRevisionId: HashSchema,
  sourceContentSha256: HashSchema,
  titleArabic: SafeTextSchema.trim().min(1).max(512),
  authorArabic: SafeTextSchema.trim().min(1).max(512),
  linesArabic: z
    .array(SourceLineSchema)
    .min(1)
    .max(MAX_LINES)
    .refine(
      (lines) => lines.some((line) => line.trim().length > 0),
      "Arabic content must contain at least one nonblank line",
    ),
});
export type PoemEnrichmentInput = z.infer<typeof PoemEnrichmentInputSchema>;

export const PoemInsightsSchema = z.strictObject({
  summary: InsightTextSchema,
  themes: z.array(InsightTextSchema).min(1).max(MAX_LIST_ITEMS),
  historicalContext: InsightTextSchema,
  literaryDevices: z.array(InsightTextSchema).min(1).max(MAX_LIST_ITEMS),
  culturalSignificance: InsightTextSchema,
  notableLines: z
    .array(
      z.strictObject({
        line: InsightTextSchema,
        explanation: InsightTextSchema,
      }),
    )
    .min(1)
    .max(MAX_LIST_ITEMS),
});
export type PoemInsights = z.infer<typeof PoemInsightsSchema>;

export const PoemEnrichmentOutputSchema = z.strictObject({
  translation: z.strictObject({
    lines: z.array(TranslatedLineSchema).min(1).max(MAX_LINES),
  }),
  insights: PoemInsightsSchema,
});
export type PoemEnrichmentOutput = z.infer<typeof PoemEnrichmentOutputSchema>;

const GlossMeaningSchema = SafeTextSchema.trim()
  .min(1)
  .max(MAX_GLOSS_CHARACTERS);
const WordGlossPartSchema = z.strictObject({
  meaning: GlossMeaningSchema,
  surface: SafeTextSchema.min(1).max(MAX_LINE_CHARACTERS),
});
const WordGlossTextSegmentSchema = z.strictObject({
  kind: z.literal("text"),
  surface: SafeTextSchema.max(MAX_LINE_CHARACTERS),
});
const WordGlossWordSegmentSchema = z.strictObject({
  kind: z.literal("word"),
  meaning: GlossMeaningSchema,
  parts: z.array(WordGlossPartSchema).min(1).max(20).optional(),
  surface: SafeTextSchema.min(1).max(MAX_LINE_CHARACTERS),
  tokenIndex: z
    .number()
    .int()
    .nonnegative()
    .max(MAX_LINE_CHARACTERS - 1),
});
export const WordGlossSegmentSchema = z.discriminatedUnion("kind", [
  WordGlossTextSegmentSchema,
  WordGlossWordSegmentSchema,
]);
export const PoemWordGlossesSchema = z.strictObject({
  lines: z
    .array(
      z.strictObject({
        lineIndex: z
          .number()
          .int()
          .nonnegative()
          .max(MAX_LINES - 1),
        segments: z.array(WordGlossSegmentSchema).max(MAX_LINE_CHARACTERS),
      }),
    )
    .min(1)
    .max(MAX_LINES),
  tokenizerVersion: z.literal(WORD_GLOSS_TOKENIZER_VERSION),
});

export const PoemEnrichmentOutputV2Schema = z.strictObject({
  schemaId: z.literal(ENRICHMENT_OUTPUT_SCHEMA_ID),
  schemaVersion: z.literal(ENRICHMENT_OUTPUT_V2_SCHEMA_VERSION),
  translation: z.strictObject({
    lines: z.array(TranslatedLineSchema).min(1).max(MAX_LINES),
  }),
  wordGlosses: PoemWordGlossesSchema,
});
export type PoemEnrichmentOutputV2 = z.infer<
  typeof PoemEnrichmentOutputV2Schema
>;

export const AnyPoemEnrichmentOutputSchema = z.union([
  PoemEnrichmentOutputV2Schema,
  PoemEnrichmentOutputSchema,
]);
export type AnyPoemEnrichmentOutput = z.infer<
  typeof AnyPoemEnrichmentOutputSchema
>;

export const PoemEnrichmentWireV2Schema = z.strictObject({
  translation: z.strictObject({
    lines: z.array(TranslatedLineSchema).min(1).max(MAX_LINES),
  }),
  wordGlosses: z.strictObject({
    lines: z
      .array(
        z.strictObject({
          lineIndex: z
            .number()
            .int()
            .nonnegative()
            .max(MAX_LINES - 1),
          tokens: z
            .array(
              z.strictObject({
                meaning: GlossMeaningSchema,
                parts: z.preprocess(
                  (value) => (value === null ? undefined : value),
                  z.array(WordGlossPartSchema).min(1).max(20).optional(),
                ),
                tokenIndex: z
                  .number()
                  .int()
                  .nonnegative()
                  .max(MAX_LINE_CHARACTERS - 1),
              }),
            )
            .max(MAX_LINE_CHARACTERS),
        }),
      )
      .min(1)
      .max(MAX_LINES),
  }),
});
export type PoemEnrichmentWireV2 = z.infer<typeof PoemEnrichmentWireV2Schema>;

const ReviewVerdictSchema = z.enum(["pass", "fail"]);
const ReviewFindingSeveritySchema = z.enum(["critical", "major", "minor"]);
export const ReviewHighestSeveritySchema = z.enum([
  "critical",
  "major",
  "minor",
  "none",
]);

export const PoemEnrichmentReviewSchema = z.strictObject({
  verdict: ReviewVerdictSchema,
  fidelityScore: z.number().int().min(0).max(100),
  insightScore: z.number().int().min(0).max(100),
  findings: z
    .array(
      z.strictObject({
        severity: ReviewFindingSeveritySchema,
        code: z.string().regex(/^[A-Z][A-Z0-9_]{1,63}$/),
        lineIndex: z
          .number()
          .int()
          .nonnegative()
          .max(MAX_LINES - 1)
          .nullable(),
        explanation: InsightTextSchema,
      }),
    )
    .max(200),
});
export type PoemEnrichmentReview = z.infer<typeof PoemEnrichmentReviewSchema>;

export interface EnrichmentValidationFinding {
  code: string;
  lineIndex: null | number;
  severity: "critical" | "major";
}

export interface EnrichmentValidationResult {
  findings: EnrichmentValidationFinding[];
  passed: boolean;
}

export function validatePoemEnrichment(
  input: PoemEnrichmentInput,
  output: PoemEnrichmentOutput,
): EnrichmentValidationResult {
  const findings: EnrichmentValidationFinding[] = [];
  const translatedLines = output.translation.lines;

  if (translatedLines.length !== input.linesArabic.length) {
    findings.push({
      code: "TRANSLATION_LINE_COUNT_MISMATCH",
      lineIndex: null,
      severity: "critical",
    });
  }

  const comparableLineCount = Math.min(
    input.linesArabic.length,
    translatedLines.length,
  );
  for (let index = 0; index < comparableLineCount; index += 1) {
    const source = input.linesArabic[index] ?? "";
    const translation = translatedLines[index] ?? "";
    if (source.trim().length === 0 && translation !== "") {
      findings.push({
        code: "BLANK_SOURCE_SLOT_CHANGED",
        lineIndex: index,
        severity: "critical",
      });
    }
    if (source.trim().length > 0 && translation.trim().length === 0) {
      findings.push({
        code: "NONBLANK_SOURCE_LINE_UNTRANSLATED",
        lineIndex: index,
        severity: "critical",
      });
    }
    if (translation.trim().length > 0 && REFUSAL.test(translation)) {
      findings.push({
        code: "TRANSLATION_REFUSAL_OR_WRAPPER",
        lineIndex: index,
        severity: "critical",
      });
    }
    if (
      translation.trim().length > 0 &&
      source.trim() === translation.trim() &&
      ARABIC_SCRIPT.test(source)
    ) {
      findings.push({
        code: "SOURCE_LINE_COPIED_AS_TRANSLATION",
        lineIndex: index,
        severity: "major",
      });
    }
  }

  const sourceLines = new Set(
    input.linesArabic.filter((line) => line.trim().length > 0),
  );
  const notableLines = new Set<string>();
  for (const notable of output.insights.notableLines) {
    if (!sourceLines.has(notable.line)) {
      findings.push({
        code: "NOTABLE_LINE_NOT_IN_SOURCE",
        lineIndex: null,
        severity: "critical",
      });
    }
    if (notableLines.has(notable.line)) {
      findings.push({
        code: "DUPLICATE_NOTABLE_LINE",
        lineIndex: null,
        severity: "major",
      });
    }
    notableLines.add(notable.line);
  }

  return { findings, passed: findings.length === 0 };
}

export type DeterministicWordGlossSegment =
  | { kind: "text"; surface: string }
  | { kind: "word"; surface: string; tokenIndex: number };

const ARABIC_ORTHOGRAPHIC_RUN =
  /[[\p{Script_Extensions=Arabic}&&[\p{L}\p{N}]]\p{M}]+/gv;

/** Split an Arabic line without normalizing or discarding a single source byte. */
export function tokenizeArabicForGlosses(
  line: string,
): DeterministicWordGlossSegment[] {
  const segments: DeterministicWordGlossSegment[] = [];
  let cursor = 0;
  let tokenIndex = 0;
  const appendText = (surface: string) => {
    if (surface.length === 0) return;
    const previous = segments.at(-1);
    if (previous?.kind === "text") previous.surface += surface;
    else segments.push({ kind: "text", surface });
  };
  for (const match of line.matchAll(ARABIC_ORTHOGRAPHIC_RUN)) {
    const index = match.index;
    const surface = match[0];
    appendText(line.slice(cursor, index));
    segments.push({ kind: "word", surface, tokenIndex });
    tokenIndex += 1;
    cursor = index + surface.length;
  }
  appendText(line.slice(cursor));
  return segments;
}

export function materializePoemEnrichmentV2(
  input: PoemEnrichmentInput,
  rawWire: PoemEnrichmentWireV2,
): PoemEnrichmentOutputV2 {
  const wire = PoemEnrichmentWireV2Schema.parse(rawWire);
  if (wire.translation.lines.length !== input.linesArabic.length)
    throw new Error("TRANSLATION_LINE_COUNT_MISMATCH");
  if (wire.wordGlosses.lines.length !== input.linesArabic.length)
    throw new Error("WORD_GLOSS_LINE_COUNT_MISMATCH");

  const glossLines = new Map(
    wire.wordGlosses.lines.map((line) => [line.lineIndex, line.tokens]),
  );
  if (glossLines.size !== wire.wordGlosses.lines.length)
    throw new Error("WORD_GLOSS_LINE_INDEX_DUPLICATE");

  const lines = input.linesArabic.map((source, lineIndex) => {
    const deterministic = tokenizeArabicForGlosses(source);
    const expectedWords = deterministic.filter(
      (
        segment,
      ): segment is Extract<DeterministicWordGlossSegment, { kind: "word" }> =>
        segment.kind === "word",
    );
    const tokens = glossLines.get(lineIndex);
    if (!tokens) throw new Error("WORD_GLOSS_LINE_INDEX_MISSING");
    const byIndex = new Map(tokens.map((token) => [token.tokenIndex, token]));
    if (byIndex.size !== tokens.length)
      throw new Error("WORD_GLOSS_TOKEN_INDEX_DUPLICATE");
    if (
      byIndex.size !== expectedWords.length ||
      expectedWords.some(({ tokenIndex }) => !byIndex.has(tokenIndex))
    )
      throw new Error("WORD_GLOSS_TOKEN_COVERAGE_MISMATCH");

    return {
      lineIndex,
      segments: deterministic.map((segment) => {
        if (segment.kind === "text") return segment;
        const gloss = byIndex.get(segment.tokenIndex);
        if (!gloss) throw new Error("WORD_GLOSS_TOKEN_INDEX_MISSING");
        const partsReconstructSource =
          gloss.parts?.map(({ surface }) => surface).join("") ===
          segment.surface;
        return {
          ...segment,
          meaning: gloss.meaning,
          ...(partsReconstructSource ? { parts: gloss.parts } : {}),
        };
      }),
    };
  });

  return PoemEnrichmentOutputV2Schema.parse({
    schemaId: ENRICHMENT_OUTPUT_SCHEMA_ID,
    schemaVersion: ENRICHMENT_OUTPUT_V2_SCHEMA_VERSION,
    translation: wire.translation,
    wordGlosses: { lines, tokenizerVersion: WORD_GLOSS_TOKENIZER_VERSION },
  });
}

export function validatePoemEnrichmentV2(
  input: PoemEnrichmentInput,
  output: PoemEnrichmentOutputV2,
): EnrichmentValidationResult {
  const findings: EnrichmentValidationFinding[] = [];
  const translationValidation = validateTranslationLines(
    input,
    output.translation.lines,
  );
  findings.push(...translationValidation);
  if (output.wordGlosses.lines.length !== input.linesArabic.length) {
    findings.push({
      code: "WORD_GLOSS_LINE_COUNT_MISMATCH",
      lineIndex: null,
      severity: "critical",
    });
  }
  const linesByIndex = new Map(
    output.wordGlosses.lines.map((line) => [line.lineIndex, line]),
  );
  if (linesByIndex.size !== output.wordGlosses.lines.length) {
    findings.push({
      code: "WORD_GLOSS_LINE_INDEX_DUPLICATE",
      lineIndex: null,
      severity: "critical",
    });
  }
  for (const [lineIndex, source] of input.linesArabic.entries()) {
    const line = linesByIndex.get(lineIndex);
    if (!line) {
      findings.push({
        code: "WORD_GLOSS_LINE_INDEX_MISSING",
        lineIndex,
        severity: "critical",
      });
      continue;
    }
    if (line.segments.map(({ surface }) => surface).join("") !== source) {
      findings.push({
        code: "WORD_GLOSS_SOURCE_RECONSTRUCTION_MISMATCH",
        lineIndex,
        severity: "critical",
      });
    }
    const expected = tokenizeArabicForGlosses(source).filter(
      (segment) => segment.kind === "word",
    );
    const actual = line.segments.filter((segment) => segment.kind === "word");
    if (
      actual.length !== expected.length ||
      actual.some(
        (segment, index) =>
          !Object.is(segment.tokenIndex, index) ||
          segment.surface !== expected[index]?.surface ||
          (segment.parts !== undefined &&
            segment.parts.map(({ surface }) => surface).join("") !==
              segment.surface),
      )
    ) {
      findings.push({
        code: "WORD_GLOSS_TOKEN_COVERAGE_MISMATCH",
        lineIndex,
        severity: "critical",
      });
    }
  }
  return { findings, passed: findings.length === 0 };
}

function validateTranslationLines(
  input: PoemEnrichmentInput,
  translatedLines: readonly string[],
): EnrichmentValidationFinding[] {
  const findings: EnrichmentValidationFinding[] = [];
  if (translatedLines.length !== input.linesArabic.length) {
    findings.push({
      code: "TRANSLATION_LINE_COUNT_MISMATCH",
      lineIndex: null,
      severity: "critical",
    });
  }
  const count = Math.min(input.linesArabic.length, translatedLines.length);
  for (let index = 0; index < count; index += 1) {
    const source = input.linesArabic[index] ?? "";
    const translation = translatedLines[index] ?? "";
    if (source.trim().length === 0 && translation !== "")
      findings.push({
        code: "BLANK_SOURCE_SLOT_CHANGED",
        lineIndex: index,
        severity: "critical",
      });
    if (source.trim().length > 0 && translation.trim().length === 0)
      findings.push({
        code: "NONBLANK_SOURCE_LINE_UNTRANSLATED",
        lineIndex: index,
        severity: "critical",
      });
    if (translation.trim().length > 0 && REFUSAL.test(translation))
      findings.push({
        code: "TRANSLATION_REFUSAL_OR_WRAPPER",
        lineIndex: index,
        severity: "critical",
      });
    if (
      translation.trim().length > 0 &&
      source.trim() === translation.trim() &&
      ARABIC_SCRIPT.test(source)
    )
      findings.push({
        code: "SOURCE_LINE_COPIED_AS_TRANSLATION",
        lineIndex: index,
        severity: "major",
      });
  }
  return findings;
}

export function reviewsAcceptEnrichment(
  reviews: readonly PoemEnrichmentReview[],
): boolean {
  return reviews.length === 2 && reviews.every(reviewAcceptsEnrichment);
}

export function reviewAcceptsEnrichment(review: PoemEnrichmentReview): boolean {
  return (
    review.verdict === "pass" &&
    review.fidelityScore >= 92 &&
    review.insightScore >= 88 &&
    review.findings.every(
      (finding) =>
        finding.severity !== "critical" && finding.severity !== "major",
    )
  );
}
