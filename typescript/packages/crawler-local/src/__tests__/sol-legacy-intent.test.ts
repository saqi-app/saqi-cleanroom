import { describe, expect, it } from "vitest";

import { parseLegacySolIntent } from "../persistence/sol-legacy-intent.js";
import { LegacySolIntentProvenanceSchema } from "../persistence/sol-legacy-intent-provenance.js";
import { canonicalJson, sha256 } from "../persistence/work-key.js";

const LEGACY = {
  attemptId: "018f5f5e-6f73-7c30-a607-7730b1f77f27",
  inputHash: "a".repeat(64),
  kind: "generation",
  model: "gpt-5.6-sol",
  pipelineVersion: "sol-enrichment-v1",
  reasoningEffort: "high",
};
const key = (input: typeof LEGACY) =>
  sha256(canonicalJson({ inputHash: input.inputHash, kind: input.kind }));

describe("historical Sol-only intent adapter", () => {
  it.each(["generation", "review-1", "review-2"])(
    "adapts exact old %s identity with historical provenance",
    (kind) => {
      const original = Object.freeze({ ...LEGACY, kind });
      const before = canonicalJson(original);
      const parsed = parseLegacySolIntent(original, key(original));
      expect(parsed.intent).toEqual({
        ...original,
        provider: "sol",
        modelKey: "sol-5.6",
      });
      expect(parsed.provenance).toMatchObject({
        strategy: "sol_only_intent_v1",
        sourceRevision: "1a2f1c34d0f8df55db13df638cc03e0b5e2c0750",
        mappingRevision: "58d65d3d89dec6d8d2d1cb457bdb1d702a99e7d3",
        provider: "sol",
        modelKey: "sol-5.6",
      });
      expect(() =>
        LegacySolIntentProvenanceSchema.parse(parsed.provenance),
      ).not.toThrow();
      expect(canonicalJson(original)).toBe(before);
    },
  );

  it("preserves complete modern identities without applying legacy key rules or provenance", () => {
    const modern = {
      ...LEGACY,
      provider: "sol",
      modelKey: "sol-5.6",
      pipelineVersion: "sol-word-gloss-v2",
    };
    expect(parseLegacySolIntent(modern, "b".repeat(64))).toEqual({
      intent: modern,
    });
  });

  it.each([
    { model: "other-model" },
    { pipelineVersion: "sol-word-gloss-v2" },
    { reasoningEffort: "low" },
    { kind: "other-kind" },
    { provider: "sol" },
    { modelKey: "sol-5.6" },
    { provider: undefined },
    { provider: "claude", modelKey: "sol-5.6" },
    { extra: "unrecognized" },
    { attemptId: "../escape" },
    { inputHash: "not-a-hash" },
  ])(
    "refuses to infer defaults for malformed or nonhistorical shape $0",
    (change) => {
      expect(() =>
        parseLegacySolIntent({ ...LEGACY, ...change }, key(LEGACY)),
      ).toThrow();
    },
  );

  it("requires the original historical operation key instead of rekeying", () => {
    expect(() => parseLegacySolIntent(LEGACY, "b".repeat(64))).toThrow(
      "SOL_IMPORT_LEGACY_INTENT_KEY_MISMATCH",
    );
    expect(() => parseLegacySolIntent(LEGACY, "not-a-key")).toThrow();
  });

  it("returns independent provenance values without changing later parse results", () => {
    const first = parseLegacySolIntent(LEGACY, key(LEGACY));
    const second = parseLegacySolIntent(LEGACY, key(LEGACY));
    expect(first.provenance).not.toBe(second.provenance);
    expect(first.provenance).toEqual(second.provenance);
  });
});
