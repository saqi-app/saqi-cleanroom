import {
  APPROVED_ENRICHMENT_MODEL,
  APPROVED_ENRICHMENT_PROMPT_VERSION,
  APPROVED_ENRICHMENT_REASONING_EFFORT,
  APPROVED_ENRICHMENT_VALIDATIONS,
} from "@saqi/precedent-iso";
import { describe, expect, it, vi } from "vitest";

import {
  CorpusImportCoordinator,
  type EnrichmentPublication,
} from "../corpus-import-coordinator";
import type { CorpusRevisionStore } from "../corpus-revision-store";

const HASH = "a".repeat(64);
const PASS_REVIEW = {
  fidelityScore: 96,
  findings: [],
  insightScore: 94,
  verdict: "pass" as const,
};

describe("CorpusImportCoordinator enrichment security", () => {
  it.each(["sol-enrichment-v1", "sol-word-gloss-v2"])(
    "stages exact %s high provenance through the generic endpoint",
    async (promptVersion) => {
      const putArtifact = vi.fn().mockResolvedValue(undefined);
      const putValidation = vi.fn().mockResolvedValue(undefined);
      const publish = vi.fn().mockResolvedValue({
        authorSlug: "author-1",
        poemId: "poem-1",
        pointerVersion: 1,
        state: "published",
      });
      const coordinator = new CorpusImportCoordinator(
        fakeStore({
          publishEnrichment: publish,
          putEnrichmentArtifact: putArtifact,
          putEnrichmentValidation: putValidation,
        }),
      );
      const input = legacySolPublication(promptVersion);

      await expect(coordinator.publishEnrichment(input)).resolves.toMatchObject(
        {
          pointerVersion: 1,
          state: "published",
        },
      );
      expect(putArtifact).toHaveBeenCalledExactlyOnceWith(input.artifact);
      expect(putValidation).toHaveBeenCalledTimes(2);
      expect(publish).toHaveBeenCalledExactlyOnceWith(input.publication);
    },
  );

  it.each([
    [
      "unapproved provenance",
      (input: EnrichmentPublication) => ({
        ...input,
        artifact: { ...input.artifact, model: "weaker-model" },
      }),
      "ENRICHMENT_PROVENANCE_REJECTED",
    ],
    [
      "readable but non-fallback legacy provenance",
      (input: EnrichmentPublication) => ({
        ...input,
        artifact: {
          ...input.artifact,
          model: "claude-opus-5",
          promptVersion: "claude-opus-5-enrichment-v1",
          reasoningEffort: "max",
        },
      }),
      "ENRICHMENT_PROVENANCE_REJECTED",
    ],
    [
      "one reviewer",
      (input: EnrichmentPublication) => ({
        ...input,
        publication: {
          ...input.publication,
          requiredValidations: input.publication.requiredValidations.slice(
            0,
            1,
          ),
        },
        validations: input.validations.slice(0, 1),
      }),
      "VALIDATION_SET_MUST_HAVE_TWO",
    ],
    [
      "duplicate reviewer",
      (input: EnrichmentPublication) => ({
        ...input,
        validations: [
          input.validations[0],
          {
            ...input.validations[1],
            validatorKey: input.validations[0]?.validatorKey ?? "missing",
          },
        ],
      }),
      "VALIDATION_POLICY_REJECTED",
    ],
    [
      "unapproved reviewer version",
      (input: EnrichmentPublication) => ({
        ...input,
        validations: [
          { ...input.validations[0], validatorVersion: "unapproved" },
          input.validations[1],
        ],
      }),
      "VALIDATION_POLICY_REJECTED",
    ],
    [
      "caller-declared verdict mismatch",
      (input: EnrichmentPublication) => ({
        ...input,
        validations: [
          { ...input.validations[0], outcome: "fail" as const },
          input.validations[1],
        ],
      }),
      "VALIDATION_DECLARATION_MISMATCH",
    ],
    [
      "low-quality review",
      (input: EnrichmentPublication) => ({
        ...input,
        validations: [
          {
            ...input.validations[0],
            report: { ...PASS_REVIEW, fidelityScore: 50 },
          },
          input.validations[1],
        ],
      }),
      "ENRICHMENT_REVIEWS_REJECTED",
    ],
  ])("rejects %s before staging", async (_name, mutate, code) => {
    const putArtifact = vi.fn();
    const coordinator = new CorpusImportCoordinator(
      fakeStore({ putEnrichmentArtifact: putArtifact }),
    );
    await expect(
      coordinator.publishEnrichment(mutate(publication())),
    ).rejects.toThrow(code);
    expect(putArtifact).not.toHaveBeenCalled();
  });
});

function publication(): EnrichmentPublication {
  const validations = APPROVED_ENRICHMENT_VALIDATIONS.map(
    ({ attempt, validatorKey, validatorVersion }, index) => ({
      artifactId: "artifact-1",
      attempt,
      highestSeverity: "none" as const,
      id: `validation-${String(index + 1)}`,
      outcome: "pass" as const,
      report: PASS_REVIEW,
      reportHash: HASH,
      validatorKey,
      validatorVersion,
    }),
  );
  return {
    artifact: {
      id: "artifact-1",
      model: APPROVED_ENRICHMENT_MODEL,
      payload: {
        insights: {
          culturalSignificance: "Significance",
          historicalContext: "Context",
          literaryDevices: ["Metaphor"],
          notableLines: [{ explanation: "Explanation", line: "بيت" }],
          summary: "Summary",
          themes: ["Theme"],
        },
        translation: { lines: ["Verse"] },
      },
      payloadHash: HASH,
      promptVersion: APPROVED_ENRICHMENT_PROMPT_VERSION,
      reasoningEffort: APPROVED_ENRICHMENT_REASONING_EFFORT,
      schemaVersion: 1,
      sourceRevisionId: HASH,
      taskKey: "task-1",
      variant: 0,
    },
    publication: {
      artifactId: "artifact-1",
      expectedPointerVersion: null,
      poemId: "poem-1",
      requiredValidations: validations.map(
        ({ attempt, validatorKey, validatorVersion }) => ({
          attempt,
          validatorKey,
          validatorVersion,
        }),
      ),
      writerEpoch: 1,
    },
    validations,
  };
}

function legacySolPublication(
  promptVersion = "sol-enrichment-v1",
): EnrichmentPublication {
  const input = publication();
  const validations = input.validations.map((validation) => ({
    ...validation,
    validatorVersion: promptVersion,
  }));
  return {
    artifact: {
      ...input.artifact,
      promptVersion,
      reasoningEffort: "high",
    },
    publication: {
      ...input.publication,
      requiredValidations: validations.map(
        ({ attempt, validatorKey, validatorVersion }) => ({
          attempt,
          validatorKey,
          validatorVersion,
        }),
      ),
    },
    validations,
  };
}

function fakeStore(
  overrides: Partial<CorpusRevisionStore> = {},
): CorpusRevisionStore {
  const nothing = () => Promise.resolve();
  return {
    advanceWriterEpoch: nothing,
    createOrReuseBundle: nothing,
    planPromotion: () => Promise.reject(new Error("unused")),
    promoteRecord: () => Promise.reject(new Error("unused")),
    publishEnrichment: () => Promise.reject(new Error("unused")),
    putEnrichmentArtifact: nothing,
    putEnrichmentValidation: nothing,
    recordImportReceipt: nothing,
    sealBundle: nothing,
    stageRecord: nothing,
    ...overrides,
  };
}
