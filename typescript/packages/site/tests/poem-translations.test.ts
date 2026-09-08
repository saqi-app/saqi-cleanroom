import assert from "node:assert/strict";
import test from "node:test";

import {
  LEGACY_GEMINI_ATTRIBUTION_NOTE,
  LEGACY_TRANSLATION_ATTRIBUTION_NOTE,
  LEGACY_TRANSLATION_MODEL_ESTIMATE,
  poemTranslationTracks,
  poemWordGlossTracks,
  translatedLineCount,
  translationModelName,
  translationModelProvider,
} from "../src/lib/poem-translations";

const ModelInsights = {
  culturalSignificance: "Culture",
  historicalContext: "History",
  literaryDevices: ["Metaphor"],
  notableLines: [{ explanation: "Why", line: "بيت" }],
  summary: "Summary",
  themes: ["Theme"],
};

void test("translation tracks remain poem-wide and deterministic", () => {
  assert.deepEqual(
    poemTranslationTracks({
      linesEnglish: ["Primary one", ""],
      linesEnglishGemini: ["Alternate one", "Alternate two"],
      linesEnglishGeminiModel: "gemini-3.7-flash",
    }),
    [
      {
        attributionCertainty: "inferred_range",
        attributionNote: LEGACY_TRANSLATION_ATTRIBUTION_NOTE,
        key: "legacy",
        lines: ["Primary one", ""],
        model: "Claude 2",
        provider: "anthropic",
      },
      {
        key: "gemini",
        lines: ["Alternate one", "Alternate two"],
        model: "gemini-3.7-flash",
        provider: "google",
      },
    ],
  );
});

void test("Gemini becomes primary only when the primary track is absent", () => {
  assert.deepEqual(
    poemTranslationTracks({ linesEnglishGemini: ["Only translation"] }),
    [
      {
        attributionNote: LEGACY_GEMINI_ATTRIBUTION_NOTE,
        key: "gemini",
        lines: ["Only translation"],
        model: "Gemini 3.5 Flash",
        provider: "google",
      },
    ],
  );
});

void test("legacy translations expose the approved estimated attribution", () => {
  assert.deepEqual(
    poemTranslationTracks({
      linesEnglish: ["Legacy"],
      linesEnglishGemini: ["Alternate"],
      linesEnglishSol: ["Sol"],
    }),
    [
      {
        key: "sol",
        lines: ["Sol"],
        model: "Sol · provenance unavailable",
        provider: "openai",
      },
      {
        attributionCertainty: "inferred_range",
        attributionNote: LEGACY_TRANSLATION_ATTRIBUTION_NOTE,
        key: "legacy",
        lines: ["Legacy"],
        model: "Claude 2",
        provider: "anthropic",
      },
      {
        attributionNote: LEGACY_GEMINI_ATTRIBUTION_NOTE,
        key: "gemini",
        lines: ["Alternate"],
        model: "Gemini 3.5 Flash",
        provider: "google",
      },
    ],
  );
});

void test("legacy and normalized translations remain independently selectable", () => {
  assert.deepEqual(
    poemTranslationTracks({
      linesEnglish: ["Legacy"],
      modelEnrichments: [
        {
          insights: ModelInsights,
          lines: ["Sol"],
          model: "gpt-5.6-sol",
          modelKey: "sol-5.6",
          reasoningEffort: "high",
        },
        {
          insights: ModelInsights,
          lines: ["Claude"],
          model: "claude-opus-5",
          modelKey: "claude-opus-5",
          reasoningEffort: "max",
        },
      ],
    }).map(({ key, lines, model }) => ({ key, lines, model })),
    [
      {
        key: "sol-5.6",
        lines: ["Sol"],
        model: "Sol 5.6",
      },
      {
        key: "claude-opus-5",
        lines: ["Claude"],
        model: "Claude Opus 5",
      },
      {
        key: "legacy",
        lines: ["Legacy"],
        model: "Claude 2",
      },
    ],
  );
});

void test("legacy display estimates preserve recorded provenance and explain uncertainty", () => {
  const tracks = poemTranslationTracks({
    linesEnglish: ["Legacy"],
    linesEnglishModel: "Claude 1 or 2",
    linesEnglishGemini: ["Alternate"],
    linesEnglishGeminiModel: "Gemini (legacy model unknown)",
  });
  assert.deepEqual(tracks.map(({ model }) => model), [
    "Claude 1 or 2",
    "Gemini (legacy model unknown)",
  ]);
  assert.deepEqual(tracks.map(({ model }) => translationModelName(model ?? "")), [
    "Claude 2",
    "Gemini 3.5 Flash",
  ]);
  assert.deepEqual(tracks.map(({ attributionNote }) => attributionNote), [
    LEGACY_TRANSLATION_ATTRIBUTION_NOTE,
    LEGACY_GEMINI_ATTRIBUTION_NOTE,
  ]);
});

void test("explicit legacy model identifiers are never replaced by estimates", () => {
  const tracks = poemTranslationTracks({
    linesEnglish: ["Legacy"],
    linesEnglishModel: "claude-sonnet-4-5-20250929",
    linesEnglishAttributionCertainty: "recorded",
    linesEnglishGemini: ["Alternate"],
    linesEnglishGeminiModel: "gemini-3.7-flash",
  });
  assert.deepEqual(tracks.map(({ model }) => model), [
    "claude-sonnet-4-5-20250929",
    "gemini-3.7-flash",
  ]);
  assert.ok(tracks.every(({ attributionNote }) => attributionNote === undefined));
  assert.equal(tracks[0]?.attributionCertainty, "recorded");
});

void test("Sol is an explicit preferred track without replacing legacy tracks", () => {
  assert.deepEqual(
    poemTranslationTracks({
      linesEnglish: ["Legacy"],
      linesEnglishGemini: ["Gemini"],
      linesEnglishSol: ["Sol"],
      linesEnglishSolModel: "gpt-5.6-sol",
      linesEnglishSolReasoningEffort: "high",
    }).map(({ key }) => key),
    ["sol", "legacy", "gemini"],
  );
});

for (const [model, provider] of [
    ["claude-2", "anthropic"],
    ["claude-sonnet-4-5-20250929", "anthropic"],
    ["gemini-3-pro-preview", "google"],
    ["unregistered-model", "other"],
]) {
  void test(`exact legacy model ${model} retains its provider without inferred certainty`, () => {
    const [track] = poemTranslationTracks({
      linesEnglish: ["Translation"],
      linesEnglishModel: model,
    });
    assert.equal(track.model, model);
    assert.equal(track.provider, provider);
    assert.equal(track.attributionCertainty, undefined);
    assert.equal(track.attributionNote, undefined);
  });
}

void test("Sol renders a concise manifest-backed model name", () => {
  assert.equal(
    poemTranslationTracks({
      linesEnglishSol: ["Sol"],
      linesEnglishSolModel: "gpt-5.6-sol",
      linesEnglishSolReasoningEffort: "high",
    })[0]?.model,
    "Sol 5.6",
  );
});

void test("translation coverage counts aligned nonblank source lines", () => {
  assert.equal(translatedLineCount(["One", "", "Three"], ["أ", "ب", "ج"]), 2);
});

void test("translation model badges use concise labels and provider marks", () => {
  assert.equal(
    translationModelProvider(LEGACY_TRANSLATION_MODEL_ESTIMATE),
    "anthropic",
  );
  assert.equal(translationModelProvider("gemini-3.7-flash"), "google");
  assert.equal(
    translationModelProvider("Sol 5.6 (gpt-5.6-sol) · high reasoning"),
    "openai",
  );
  assert.equal(
    translationModelName("Sol 5.6 (gpt-5.6-sol) · high reasoning"),
    "Sol 5.6",
  );
  assert.equal(
    translationModelName("claude-opus-5 · max reasoning"),
    "Claude Opus 5",
  );
  assert.equal(
    translationModelName("claude-opus-4-6-thinking · high reasoning"),
    "Claude Opus 4.6",
  );
});

void test("word glosses retain their translation model provenance", () => {
  assert.deepEqual(
    poemWordGlossTracks({
      modelEnrichments: [
        {
          lines: ["Translation"],
          model: "gpt-5.6-sol",
          modelKey: "sol-5.6",
          reasoningEffort: "high",
          wordGlosses: {
            lines: [
              {
                lineIndex: 0,
                segments: [
                  {
                    kind: "word",
                    meaning: "house",
                    surface: "بيت",
                    tokenIndex: 0,
                  },
                ],
              },
            ],
            tokenizerVersion: "saqi-orthographic-v1",
          },
        },
      ],
    }).map(({ key, model, provider }) => ({ key, model, provider })),
    [{ key: "sol-5.6", model: "Sol 5.6", provider: "openai" }],
  );
});
