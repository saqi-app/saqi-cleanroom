import { describe, expect, it } from "vitest";

import {
  CANONICAL_POEM_BINDING_SCHEMA_ID,
  CanonicalPoemBindingV1Schema,
  ENRICHMENT_PUBLICATION_SCHEMA_ID,
  EnrichmentPublicationV2RequestSchema,
  EnrichmentPublicationV2ResponseSchema,
  POEM_ENRICHMENT_INPUT_V2_SCHEMA_VERSION,
  PoemEnrichmentInputV2Schema,
  SOURCE_ADMISSION_SCHEMA_ID,
  SourceAdmissionV2RequestSchema,
  SourceConfigurationSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "../source-bound-publication.js";

const HASH_A = "a".repeat(64);
const HASH_B = "b".repeat(64);
const HASH_C = "c".repeat(64);
const HASH_D = "d".repeat(64);
const HASH_E = "e".repeat(64);

describe("source-bound publication contracts", () => {
  it("normalizes a strict source configuration", () => {
    expect(
      SourceConfigurationSchema.parse({
        name: "primary-source",
        origin: "https://source.invalid",
      }),
    ).toEqual({ name: "primary-source", origin: "https://source.invalid" });
  });

  it.each(["a", "source.name", "Source", `s${"x".repeat(64)}`])(
    "rejects invalid source name %s",
    (name) => {
      expect(
        SourceConfigurationSchema.safeParse({
          name,
          origin: "https://source.invalid",
        }).success,
      ).toBe(false);
    },
  );

  it.each([
    // eslint-disable-next-line unicorn/prefer-https -- Intentional insecure-origin rejection fixture.
    "http://source.invalid",
    "https://user@source.invalid",
    "https://source.invalid/path",
    "https://source.invalid?query=1",
  ])("rejects invalid source origin %s", (origin) => {
    expect(
      SourceConfigurationSchema.safeParse({
        name: "primary-source",
        origin,
      }).success,
    ).toBe(false);
  });

  it("accepts a production-issued canonical binding", () => {
    expect(CanonicalPoemBindingV1Schema.parse(binding())).toMatchObject({
      externalPoemId: "101680",
      lineNfcHash: HASH_B,
      poemId: "poem-1",
    });
  });

  it("requires enrichment identity to equal the canonical binding", () => {
    const input = enrichmentInput();
    expect(PoemEnrichmentInputV2Schema.safeParse(input).success).toBe(true);
    expect(
      PoemEnrichmentInputV2Schema.safeParse({ ...input, poemId: "poem-2" })
        .success,
    ).toBe(false);
    expect(
      PoemEnrichmentInputV2Schema.safeParse({
        ...input,
        sourceRevisionId: HASH_E,
      }).success,
    ).toBe(false);
  });

  it("requires unique source admission IDs", () => {
    const item = admissionItem();
    expect(
      SourceAdmissionV2RequestSchema.safeParse({
        items: [item],
        schemaId: SOURCE_ADMISSION_SCHEMA_ID,
        schemaVersion: 2,
      }).success,
    ).toBe(true);
    expect(
      SourceAdmissionV2RequestSchema.safeParse({
        items: [item, item],
        schemaId: SOURCE_ADMISSION_SCHEMA_ID,
        schemaVersion: 2,
      }).success,
    ).toBe(false);
  });

  it("validates generic source names and numeric external poem identities", () => {
    const item = admissionItem();
    const request = {
      items: [item],
      schemaId: SOURCE_ADMISSION_SCHEMA_ID,
      schemaVersion: 2,
    };
    expect(
      SourceAdmissionV2RequestSchema.safeParse({
        ...request,
        items: [{ ...item, sourceName: "other-source" }],
      }).success,
    ).toBe(true);
    expect(
      SourceAdmissionV2RequestSchema.safeParse({
        ...request,
        items: [{ ...item, sourceName: "Invalid Source" }],
      }).success,
    ).toBe(false);
    expect(
      SourceAdmissionV2RequestSchema.safeParse({
        ...request,
        items: [{ ...item, externalPoemId: "poem-1" }],
      }).success,
    ).toBe(false);
  });

  it("defines canonical NFC hash bodies independent of Unicode composition", () => {
    expect(sourceLineNfcHashBody(["ا\u{654}"])).toBe(
      sourceLineNfcHashBody(["\u{623}"]),
    );
    expect(
      sourcePromptMaterialHashBody({
        authorArabic: "ا\u{654}",
        linesArabic: ["بيت"],
        titleArabic: "عنوان",
      }),
    ).toBe(
      sourcePromptMaterialHashBody({
        authorArabic: "\u{623}",
        linesArabic: ["بيت"],
        titleArabic: "عنوان",
      }),
    );
  });

  it("rejects mismatched artifact revisions, validations, and model profiles", () => {
    const item = publicationItem();
    const request = publicationRequest(item);
    expect(
      EnrichmentPublicationV2RequestSchema.safeParse(request).success,
    ).toBe(true);
    expect(
      EnrichmentPublicationV2RequestSchema.safeParse(
        publicationRequest({
          ...item,
          artifact: { ...item.artifact, sourceRevisionId: HASH_E },
        }),
      ).success,
    ).toBe(false);
    expect(
      EnrichmentPublicationV2RequestSchema.safeParse(
        publicationRequest({
          ...item,
          validations: [
            { ...item.validations[0]!, artifactId: "other-artifact" },
            item.validations[1]!,
          ],
        }),
      ).success,
    ).toBe(false);
    expect(
      EnrichmentPublicationV2RequestSchema.safeParse(
        publicationRequest({
          ...item,
          artifact: { ...item.artifact, model: "retired-model" },
        }),
      ).success,
    ).toBe(false);
  });

  it.each([
    ["sol-word-gloss-v3", "medium", "sol-word-gloss-v3", true],
    ["sol-word-gloss-v2", "high", "sol-word-gloss-v2", true],
    ["sol-word-gloss-v3", "high", "sol-word-gloss-v3", false],
    ["sol-word-gloss-v2", "medium", "sol-word-gloss-v2", false],
    ["sol-word-gloss-v3", "medium", "sol-word-gloss-v2", false],
    ["sol-word-gloss-v2", "high", "sol-word-gloss-v3", false],
  ] as const)(
    "checks exact recipe %s/%s and review version %s",
    (promptVersion, reasoningEffort, validatorVersion, accepted) => {
      const item = publicationItem();
      item.artifact.promptVersion = promptVersion;
      item.artifact.reasoningEffort = reasoningEffort;
      item.validations = item.validations.map((validation) => ({
        ...validation,
        validatorVersion,
      }));
      expect(
        EnrichmentPublicationV2RequestSchema.safeParse(publicationRequest(item))
          .success,
      ).toBe(accepted);
    },
  );

  it.each(["published", "already-published"] as const)(
    "accepts a %s publication receipt",
    (outcome) => {
      const base = {
        actionHash: HASH_E,
        artifactId: "artifact-1",
        committedAt: "2026-09-01T12:00:00Z",
        modelKey: "sol-5.6",
        poemId: "poem-1",
        pointerVersion: 2,
        promptVersion: "sol-word-gloss-v2",
        publicationIntentId: HASH_D,
        schemaId: "saqi.enrichment-publication-receipt",
        schemaVersion: 1,
        sourceRevisionId: HASH_A,
        writerEpoch: 3,
      };
      expect(
        EnrichmentPublicationV2ResponseSchema.safeParse({
          results: [{ receipt: { ...base, outcome }, status: "published" }],
          schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
          schemaVersion: 2,
        }).success,
      ).toBe(true);
    },
  );
});

function binding() {
  return {
    admissionEvidence: {
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
      issuedAt: "2026-09-01T12:00:00Z",
      sourcePointerVersion: 2,
    },
    authorId: "author-1",
    authorNameArabic: "شاعر",
    bindingId: HASH_C,
    externalPoemId: "101680",
    lineNfcHash: HASH_B,
    poemId: "poem-1",
    promptMaterialHash: HASH_C,
    schemaId: CANONICAL_POEM_BINDING_SCHEMA_ID,
    schemaVersion: 1,
    sourceName: "primary-source",
    sourceRevisionId: HASH_A,
  };
}

function enrichmentInput() {
  return {
    authorArabic: "شاعر",
    canonicalBinding: binding(),
    linesArabic: ["بيت"],
    poemId: "poem-1",
    schemaId: "saqi.poem-enrichment-input",
    schemaVersion: POEM_ENRICHMENT_INPUT_V2_SCHEMA_VERSION,
    sourceContentSha256: HASH_B,
    sourceRevisionId: HASH_A,
    titleArabic: "عنوان",
  };
}

function admissionItem() {
  return {
    admissionId: HASH_D,
    externalPoemId: "101680",
    lineNfcHash: HASH_B,
    linesArabic: ["بيت"],
    sourceAuthorId: "495",
    sourceAuthorUrl: "https://source.invalid/writers/495",
    sourceContentSha256: HASH_B,
    sourceName: "primary-source",
    sourcePoemUrl: "https://source.invalid/works/101680",
    sourceRevisionId: HASH_A,
    titleArabic: "عنوان",
  };
}

function publicationItem() {
  const artifact = {
    id: "artifact-1",
    model: "gpt-5.6-sol",
    modelKey: "sol-5.6",
    payload: {
      schemaId: "saqi.poem-enrichment-output",
      schemaVersion: 2,
      translation: { lines: ["verse"] },
      wordGlosses: {
        lines: [{ lineIndex: 0, segments: [] }],
        tokenizerVersion: "saqi-orthographic-v1",
      },
    },
    payloadHash: HASH_D,
    promptVersion: "sol-word-gloss-v2",
    reasoningEffort: "high",
    schemaVersion: 2,
    sourceRevisionId: HASH_A,
    taskKey: "task-1",
    variant: 0,
  };
  const validation = {
    artifactId: artifact.id,
    attempt: 1,
    highestSeverity: "none",
    id: "validation-1",
    outcome: "pass",
    report: {
      fidelityScore: 100,
      findings: [],
      insightScore: 100,
      verdict: "pass",
    },
    reportHash: HASH_E,
    validatorKey: "sol-fidelity-review",
    validatorVersion: "sol-word-gloss-v2",
  };
  return {
    actionHash: HASH_E,
    artifact,
    binding: binding(),
    publicationIntentId: HASH_D,
    validations: [
      validation,
      {
        ...validation,
        attempt: 2,
        id: "validation-2",
        validatorKey: "sol-grounding-review",
      },
    ],
  };
}

function publicationRequest(item: ReturnType<typeof publicationItem>) {
  return {
    items: [item],
    schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
    schemaVersion: 2,
  };
}
