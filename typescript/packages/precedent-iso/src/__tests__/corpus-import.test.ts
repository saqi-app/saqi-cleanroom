import { describe, expect, it } from "vitest";

import {
  acceptedPublicationEnrichmentProfile,
  approvedEnrichmentProfile,
  approvedEnrichmentProfileByModelKey,
  CorpusImportActionSchema,
  LEGACY_ENRICHMENT_PROFILES,
  MAX_CORPUS_IMPORT_RECORDS,
  publicationEnrichmentProfile,
  readableEnrichmentProfile,
} from "../corpus-import.js";

const HASH = "a".repeat(64);

describe("corpus import contracts", () => {
  it.each([
    ["sol-word-gloss-v3", "medium", true],
    ["sol-word-gloss-v2", "high", false],
  ] as const)(
    "accepts publication of %s without confusing the current recipe",
    (promptVersion, reasoningEffort, current) => {
      const input = { model: "gpt-5.6-sol", promptVersion, reasoningEffort };
      expect(acceptedPublicationEnrichmentProfile(input)).toMatchObject(input);
      expect(publicationEnrichmentProfile(input)).toMatchObject(input);
      expect(readableEnrichmentProfile(input)).toMatchObject(input);
      expect(Boolean(approvedEnrichmentProfile(input))).toBe(current);
    },
  );

  it.each([
    ["sol-word-gloss-v3", "high"],
    ["sol-word-gloss-v2", "medium"],
    ["sol-enrichment-v1", "medium"],
  ])("rejects a mixed %s/%s recipe", (promptVersion, reasoningEffort) => {
    const input = { model: "gpt-5.6-sol", promptVersion, reasoningEffort };
    expect(acceptedPublicationEnrichmentProfile(input)).toBeUndefined();
    expect(publicationEnrichmentProfile(input)).toBeUndefined();
    expect(readableEnrichmentProfile(input)).toBeUndefined();
  });

  it("looks up approved model profiles by stable key instead of array order", () => {
    expect(approvedEnrichmentProfileByModelKey("sol-5.6")).toMatchObject({
      model: "gpt-5.6-sol",
      provider: "sol",
    });
    expect(
      approvedEnrichmentProfileByModelKey("retired-model"),
    ).toBeUndefined();
    expect(approvedEnrichmentProfileByModelKey("unknown")).toBeUndefined();
  });

  it("publishes only approved profiles plus the exact legacy Sol fallback", () => {
    expect(
      publicationEnrichmentProfile({
        model: "gpt-5.6-sol",
        promptVersion: "sol-enrichment-v1",
        reasoningEffort: "high",
      }),
    ).toMatchObject({
      modelKey: "sol-5.6",
      promptVersion: "sol-enrichment-v1",
    });
    expect(
      publicationEnrichmentProfile({
        model: "claude-opus-5",
        promptVersion: "claude-opus-5-enrichment-v1",
        reasoningEffort: "max",
      }),
    ).toBeUndefined();
  });

  it("keeps retired non-Sol profiles readable but never writable", () => {
    for (const profile of LEGACY_ENRICHMENT_PROFILES) {
      if (profile.provider === "sol") continue;
      expect(readableEnrichmentProfile(profile)).toEqual(profile);
      expect(acceptedPublicationEnrichmentProfile(profile)).toBeUndefined();
      expect(publicationEnrichmentProfile(profile)).toBeUndefined();
    }
  });

  it("requires an existing canonical author for every staged record", () => {
    expect(
      CorpusImportActionSchema.safeParse(
        stageAction({ canonicalAuthorId: null }),
      ).success,
    ).toBe(false);
  });

  it("caps each deterministic D1 staging chunk", () => {
    const record = stageAction().input.records[0]!;
    const oversized = stageAction();
    oversized.input.bundle.expectedRecordCount = MAX_CORPUS_IMPORT_RECORDS + 1;
    oversized.input.records = Array.from(
      { length: MAX_CORPUS_IMPORT_RECORDS + 1 },
      () => record,
    );
    expect(CorpusImportActionSchema.safeParse(oversized).success).toBe(false);
  });

  it("rejects fractional observation timestamps and identity delimiters", () => {
    expect(
      CorpusImportActionSchema.safeParse(
        stageAction({ observedAt: "2026-08-25T12:00:00.001Z" }),
      ).success,
    ).toBe(false);
    expect(
      CorpusImportActionSchema.safeParse(
        stageAction({ sourcePoemId: "one\u{1F}two" }),
      ).success,
    ).toBe(false);
  });
});

function stageAction(recordOverrides: Record<string, unknown> = {}) {
  return {
    action: "stage-and-plan" as const,
    input: {
      bundle: {
        expectedRecordCount: 1,
        id: "bundle-1",
        manifestHash: HASH,
        schemaVersion: 1,
        writerEpoch: 1,
      },
      records: [
        {
          authorNameArabic: "شاعر",
          bundleId: "bundle-1",
          canonicalAuthorId: "author-1",
          canonicalPoemId: null,
          contentArabic: { content: ["بيت"] },
          contentHash: HASH,
          observedAt: "2026-08-25T12:00:00Z",
          ordinal: 0,
          recordHash: HASH,
          sourceAuthorId: "495",
          sourceAuthorUrl: "https://source.invalid/writers/495",
          sourceName: "primary-source",
          sourcePoemId: "101680",
          sourcePoemUrl: "https://source.invalid/works/101680",
          titleArabic: "عنوان",
          ...recordOverrides,
        },
      ],
      rootHash: HASH,
    },
  };
}
