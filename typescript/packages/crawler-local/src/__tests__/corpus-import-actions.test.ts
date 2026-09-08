import {
  canonicalPoemBindingIdBody,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { describe, expect, it } from "vitest";

import { sha256 } from "../persistence/work-key";
import {
  bindCollectedPoem,
  prepareCollectedPoem,
  prepareSourceAdmission,
} from "../publication/corpus-import-actions";

const ARTIFACT = {
  artifactSchemaVersion: 1,
  collectedBy: "source-chrome-v3",
  source: {
    author: {
      canonicalId: "source:author:test",
      href: "https://source.invalid/writers/test",
      path: "/writers/test",
      slug: "test",
    },
    canonicalId: "source:poem:42",
    href: "https://source.invalid/works/42",
    lines: ["صدر", "عجز"],
    numericId: "42",
    slug: "work-42",
    structure: "classical",
    title: "قصيدة",
    verses: 1,
  },
  sourceHash: "a".repeat(64),
  workKey: "work",
};

describe("prepareCollectedPoem", () => {
  it("creates a source-only admission without claiming production identity", () => {
    const admission = prepareSourceAdmission({
      ...ARTIFACT,
      sourceContext: {
        authorNameArabic: "شاعر موثق",
        refreshGeneration: "legacy-sol-fixture",
      },
    });
    expect(admission).toMatchObject({
      externalPoemId: "42",
      linesArabic: ["صدر", "عجز"],
      sourceAuthorId: "test",
      sourceName: "source",
      titleArabic: "قصيدة",
    });
    expect(admission.admissionId).toMatch(/^[\da-f]{64}$/);
    expect(admission.sourceRevisionId).toMatch(/^[\da-f]{64}$/);
    expect(admission).not.toHaveProperty("canonicalAuthorId");
    expect(admission).not.toHaveProperty("authorArabic");
    expect(admission).not.toHaveProperty("promptMaterialHash");
  });

  it("binds an exact production admission before provider seeding", () => {
    const prepared = prepareCollectedPoem(
      ARTIFACT,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const binding = bindingFor(prepared.enrichmentInput);
    expect(bindCollectedPoem(prepared.enrichmentInput, binding)).toMatchObject({
      canonicalBinding: binding,
      schemaVersion: 2,
    });
    expect(() =>
      bindCollectedPoem(prepared.enrichmentInput, {
        ...binding,
        promptMaterialHash: "f".repeat(64),
      }),
    ).toThrow("CANONICAL_BINDING_ID_MISMATCH");
  });

  it("maps a verified local artifact to stable staging and enrichment inputs", () => {
    const mapping = {
      authorId: "author-1",
      authorNameArabic: "شاعر",
      poemId: "poem-1",
      sourceAuthorSlug: "test",
      sourcePoemId: "42",
    };
    const first = prepareCollectedPoem(
      ARTIFACT,
      mapping,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    expect(
      prepareCollectedPoem(ARTIFACT, mapping, "2026-08-25T12:00:00.000Z", 1),
    ).toEqual(first);
    expect(first.enrichmentInput).toMatchObject({
      poemId: "poem-1",
      linesArabic: ["صدر", "عجز"],
    });
    expect(first.enrichmentInput.sourceRevisionId).toMatch(/^[\da-f]{64}$/);
    expect(first.stageAction.action).toBe("stage-and-plan");
  });

  it("rejects a mismatched catalog identity", () => {
    expect(() =>
      prepareCollectedPoem(
        ARTIFACT,
        {
          authorId: "author-1",
          authorNameArabic: "شاعر",
          poemId: "poem-1",
          sourceAuthorSlug: "other",
          sourcePoemId: "42",
        },
        "2026-08-25T12:00:00.000Z",
        1,
      ),
    ).toThrow("COLLECTED_POEM_MAPPING_MISMATCH");
  });

  it("creates a new revision for a title-only correction", () => {
    const mapping = {
      authorId: "author-1",
      authorNameArabic: "شاعر",
      poemId: "poem-1",
      sourceAuthorSlug: "test",
      sourcePoemId: "42",
    };
    const original = prepareCollectedPoem(
      ARTIFACT,
      mapping,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const corrected = prepareCollectedPoem(
      {
        ...ARTIFACT,
        source: { ...ARTIFACT.source, title: "قصيدة مصححة" },
      },
      mapping,
      "2026-08-25T12:00:00.000Z",
      1,
    );

    expect(corrected.enrichmentInput.sourceContentSha256).toBe(
      original.enrichmentInput.sourceContentSha256,
    );
    expect(corrected.enrichmentInput.sourceRevisionId).not.toBe(
      original.enrichmentInput.sourceRevisionId,
    );
    if (
      original.stageAction.action !== "stage-and-plan" ||
      corrected.stageAction.action !== "stage-and-plan"
    ) {
      throw new Error("unexpected action");
    }
    expect(corrected.stageAction.input.bundle.schemaVersion).toBe(2);
    expect(corrected.stageAction.input.records[0]?.contentHash).not.toBe(
      original.stageAction.input.records[0]?.contentHash,
    );
  });
});

const MAPPING = {
  authorId: "author-1",
  authorNameArabic: "شاعر",
  poemId: "poem-1",
  sourceAuthorSlug: "test",
  sourcePoemId: "42",
};

function bindingFor(
  input: ReturnType<typeof prepareCollectedPoem>["enrichmentInput"],
) {
  const identity = {
    authorId: MAPPING.authorId,
    authorNameArabic: input.authorArabic,
    externalPoemId: MAPPING.sourcePoemId,
    lineNfcHash: sha256(sourceLineNfcHashBody(input.linesArabic)),
    poemId: MAPPING.poemId,
    promptMaterialHash: sha256(
      sourcePromptMaterialHashBody({
        authorArabic: input.authorArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    ),
    schemaId: "saqi.canonical-poem-binding" as const,
    schemaVersion: 1 as const,
    sourceName: "source" as const,
    sourceRevisionId: input.sourceRevisionId,
  };
  return {
    ...identity,
    admissionEvidence: {
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5" as const,
      issuedAt: "2026-08-25T12:00:00Z",
      sourcePointerVersion: 1,
    },
    bindingId: sha256(canonicalPoemBindingIdBody(identity)),
  };
}
