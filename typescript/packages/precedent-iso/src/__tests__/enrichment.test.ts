import { describe, expect, it } from "vitest";

import {
  ENRICHMENT_INPUT_SCHEMA_ID,
  ENRICHMENT_INPUT_SCHEMA_VERSION,
  materializePoemEnrichmentV2,
  PoemEnrichmentInputSchema,
  PoemEnrichmentOutputSchema,
  PoemEnrichmentReviewSchema,
  reviewsAcceptEnrichment,
  tokenizeArabicForGlosses,
  validatePoemEnrichment,
  validatePoemEnrichmentV2,
} from "../enrichment.js";

const HASH = "a".repeat(64);
const INPUT = PoemEnrichmentInputSchema.parse({
  schemaId: ENRICHMENT_INPUT_SCHEMA_ID,
  schemaVersion: ENRICHMENT_INPUT_SCHEMA_VERSION,
  poemId: "poem-1",
  sourceRevisionId: HASH,
  sourceContentSha256: HASH,
  titleArabic: "عنوان",
  authorArabic: "شاعر",
  linesArabic: ["بيت أول", "", "بيت ثان"],
});
const OUTPUT = PoemEnrichmentOutputSchema.parse({
  translation: { lines: ["First verse", "", "Second verse"] },
  insights: {
    summary: "A short summary.",
    themes: ["Memory"],
    historicalContext: "Historical context.",
    literaryDevices: ["Metaphor"],
    culturalSignificance: "Cultural significance.",
    notableLines: [{ line: "بيت أول", explanation: "A notable opening." }],
  },
});

describe("poem enrichment contracts", () => {
  it("accepts an exact line-aligned output", () => {
    expect(validatePoemEnrichment(INPUT, OUTPUT)).toEqual({
      findings: [],
      passed: true,
    });
  });

  it("rejects extra generated fields", () => {
    expect(() =>
      PoemEnrichmentOutputSchema.parse({ ...OUTPUT, provenance: "model" }),
    ).toThrow();
  });

  it.each(["themes", "literaryDevices", "notableLines"] as const)(
    "requires a substantive %s list",
    (field) => {
      expect(() =>
        PoemEnrichmentOutputSchema.parse({
          ...OUTPUT,
          insights: { ...OUTPUT.insights, [field]: [] },
        }),
      ).toThrow();
    },
  );

  it("rejects changed blank slots and fabricated notable lines", () => {
    const invalid = PoemEnrichmentOutputSchema.parse({
      ...OUTPUT,
      translation: { lines: ["First verse", "invented", "Second verse"] },
      insights: {
        ...OUTPUT.insights,
        notableLines: [
          { line: "ليس في القصيدة", explanation: "Fabricated quotation." },
        ],
      },
    });

    expect(validatePoemEnrichment(INPUT, invalid)).toEqual({
      findings: [
        {
          code: "BLANK_SOURCE_SLOT_CHANGED",
          lineIndex: 1,
          severity: "critical",
        },
        {
          code: "NOTABLE_LINE_NOT_IN_SOURCE",
          lineIndex: null,
          severity: "critical",
        },
      ],
      passed: false,
    });
  });

  it.each(["I cannot bear this longing", "I will not listen"])(
    "allows the poetic first-person negative: %s",
    (line) => {
      expect(
        validatePoemEnrichment(INPUT, {
          ...OUTPUT,
          translation: { lines: [line, "", "Second verse"] },
        }).passed,
      ).toBe(true);
    },
  );

  it.each([
    "As an AI, I cannot do that",
    "I cannot translate this poem",
    "I'm unable to comply with this request",
  ])("rejects the explicit model wrapper: %s", (line) => {
    expect(
      validatePoemEnrichment(INPUT, {
        ...OUTPUT,
        translation: { lines: [line, "", "Second verse"] },
      }).findings,
    ).toContainEqual({
      code: "TRANSLATION_REFUSAL_OR_WRAPPER",
      lineIndex: 0,
      severity: "critical",
    });
  });

  it("requires exactly two high-quality passing reviews", () => {
    const passingReview = PoemEnrichmentReviewSchema.parse({
      verdict: "pass",
      fidelityScore: 95,
      insightScore: 93,
      findings: [],
    });
    expect(reviewsAcceptEnrichment([passingReview, passingReview])).toBe(true);
    expect(reviewsAcceptEnrichment([passingReview])).toBe(false);
    expect(
      reviewsAcceptEnrichment([
        passingReview,
        { ...passingReview, insightScore: 87 },
      ]),
    ).toBe(false);
  });

  it("tokenizes Arabic while preserving the exact source surface", () => {
    const source = "  وَبِالشَّوقِ، نكتبُ!  ";
    const segments = tokenizeArabicForGlosses(source);
    expect(segments.map(({ surface }) => surface).join("")).toBe(source);
    expect(
      segments.flatMap((segment) =>
        segment.kind === "word"
          ? [{ surface: segment.surface, tokenIndex: segment.tokenIndex }]
          : [],
      ),
    ).toEqual([
      { surface: "وَبِالشَّوقِ", tokenIndex: 0 },
      { surface: "نكتبُ", tokenIndex: 1 },
    ]);
  });

  it("materializes complete indexed glosses into a strict v2 artifact", () => {
    const output = materializePoemEnrichmentV2(INPUT, {
      translation: { lines: ["First verse", "", "Second verse"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            tokens: [
              {
                meaning: "verse",
                parts: [
                  { meaning: "first letter", surface: "ب" },
                  { meaning: "remaining stem", surface: "يت" },
                ],
                tokenIndex: 0,
              },
              { meaning: "first", tokenIndex: 1 },
            ],
          },
          { lineIndex: 1, tokens: [] },
          {
            lineIndex: 2,
            tokens: [
              { meaning: "verse", tokenIndex: 0 },
              { meaning: "second", tokenIndex: 1 },
            ],
          },
        ],
      },
    });
    expect(output.schemaVersion).toBe(2);
    expect(validatePoemEnrichmentV2(INPUT, output)).toEqual({
      findings: [],
      passed: true,
    });
    expect(
      output.wordGlosses.lines[0]?.segments
        .map(({ surface }) => surface)
        .join(""),
    ).toBe(INPUT.linesArabic[0]);
    expect(
      output.wordGlosses.lines[0]?.segments.find(
        (segment) => segment.kind === "word" && segment.tokenIndex === 0,
      ),
    ).toHaveProperty("parts");
  });

  it("drops optional parts that do not reconstruct the source token", () => {
    const output = materializePoemEnrichmentV2(INPUT, {
      translation: { lines: ["First verse", "", "Second verse"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            tokens: [
              {
                meaning: "verse",
                parts: [{ meaning: "a respelled stem", surface: "بنو" }],
                tokenIndex: 0,
              },
              { meaning: "first", tokenIndex: 1 },
            ],
          },
          { lineIndex: 1, tokens: [] },
          {
            lineIndex: 2,
            tokens: [
              { meaning: "verse", tokenIndex: 0 },
              { meaning: "second", tokenIndex: 1 },
            ],
          },
        ],
      },
    });

    expect(
      output.wordGlosses.lines[0]?.segments.find(
        (segment) => segment.kind === "word" && segment.tokenIndex === 0,
      ),
    ).not.toHaveProperty("parts");
    expect(validatePoemEnrichmentV2(INPUT, output)).toEqual({
      findings: [],
      passed: true,
    });
  });

  it("rejects missing and duplicate word-gloss identities", () => {
    expect(() =>
      materializePoemEnrichmentV2(INPUT, {
        translation: { lines: ["First verse", "", "Second verse"] },
        wordGlosses: {
          lines: [
            {
              lineIndex: 0,
              tokens: [{ meaning: "verse", tokenIndex: 0 }],
            },
            { lineIndex: 1, tokens: [] },
            { lineIndex: 2, tokens: [] },
          ],
        },
      }),
    ).toThrow("WORD_GLOSS_TOKEN_COVERAGE_MISMATCH");
  });
});
