import { tmpdir } from "node:os";
import { join } from "node:path";

import { configureSource, parseAuthorPoemManifest } from "@saqi/source-adapter";
import { beforeEach, describe, expect, it } from "vitest";

import { collectionWorkKinds } from "../collection/collection-scheduler";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import { SOL_ENRICHMENT_WORK_KIND } from "../enrichment/sol-coordinator";
import { SOL_PIPELINE_VERSION } from "../enrichment/sol-runner";
import { ArtifactStore } from "../persistence/artifact-store";
import { Ledger } from "../persistence/ledger";
import {
  BASELINE_MISSING_INSIGHTS_PRIORITY,
  BASELINE_NO_TRANSLATION_PRIORITY,
  BASELINE_SHORTEST_POEM_BONUS,
  planCertifiedOrphanRepairs,
  planProductionBaseline,
  seedCertifiedDetailDelta,
  seedProductionDetailRecovery,
} from "../persistence/production-baseline-planner";
import { inputHash } from "../persistence/work-key";
import { prepareCollectedPoem } from "../publication/corpus-import-actions";
import { CAPTURED_SOURCE_PROFILE } from "./support/source-profile";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

beforeEach(() => {
  configureSource({
    name: "source",
    origin: "https://source.invalid",
    profile: CAPTURED_SOURCE_PROFILE,
  });
});

const AUTHOR_ID = "00000000-0000-4000-8000-000000000001";

async function* values<T>(input: Iterable<T>): AsyncGenerator<T> {
  yield* input;
}

function productionPoem(index: number): Record<string, unknown> {
  const suffix = String(index + 1).padStart(12, "0");
  const noTranslation = index < 27_184;
  const missingInsights = index >= 27_184 && index < 37_184;
  return {
    author_id: AUTHOR_ID,
    content_arabic: { content: [`بيت ${String(index + 1)}`] },
    id: `00000000-0000-4000-8000-${suffix}`,
    insights: missingInsights ? null : { summary: "exists" },
    name_arabic: `قصيدة ${String(index + 1)}`,
    slug: `work-${String(index + 1)}`,
    translation: noTranslation ? null : { lines: ["exists"] },
    translation_gemini: null,
  };
}

describe("production baseline planner", () => {
  it.each(["running", "CODEX_OPERATION_UNRESOLVED_QUARANTINED"])(
    "does not duplicate unpublished v2 work in %s state",
    async (state) => {
      const ledger = Ledger.open(":memory:");
      const templateLedger = Ledger.open(":memory:");
      const plan = (target: Ledger) =>
        planProductionBaseline({
          authors: values([
            { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
          ]),
          ledger: target,
          poems: values([productionPoem(0)]),
        });
      try {
        await plan(templateLedger);
        const template = templateLedger.claim("template", Date.now(), 60_000, [
          SOL_ENRICHMENT_WORK_KIND,
        ]);
        if (!template) throw new Error("Missing baseline template");
        const {
          input,
          inputHash: hash,
          kind,
          priority,
          schemaVersion,
        } = template.work;
        const legacy = ledger.seed({
          implementationVersion: "sol-word-gloss-v2",
          input,
          inputHash: hash,
          kind,
          priority,
          schemaVersion,
        });
        const claim = ledger.claim("legacy", Date.now(), 60_000, [kind]);
        if (!claim) throw new Error("Missing legacy claim");
        if (state !== "running") ledger.deadLetter(claim, state);

        const result = await plan(ledger);
        expect(result.report).toMatchObject({
          duplicateEnrichmentWork: 1,
          seededEnrichmentWork: 0,
        });
        expect(ledger.status().total).toBe(1);
        expect(ledger.get(legacy.workKey)?.implementationVersion).toBe(
          "sol-word-gloss-v2",
        );
        expect(
          ledger.claim("new-profile", Date.now(), 60_000, [kind], {
            implementationVersion: SOL_PIPELINE_VERSION,
          }),
        ).toBeNull();
      } finally {
        ledger.close();
        templateLedger.close();
      }
    },
  );

  it.each([
    {
      version: "sol-enrichment-v1",
      model: "sol-5.6",
      current: true,
      seeded: 0,
    },
    {
      version: "sol-word-gloss-v2",
      model: "sol-5.6",
      current: true,
      seeded: 0,
    },
    {
      version: SOL_PIPELINE_VERSION,
      model: "sol-5.6",
      current: true,
      seeded: 0,
    },
    {
      version: "sol-enrichment-v1",
      model: "sol-5.6",
      current: false,
      seeded: 1,
    },
    {
      version: "sol-word-gloss-v2",
      model: "sol-5.6",
      current: false,
      seeded: 1,
    },
    {
      version: SOL_PIPELINE_VERSION,
      model: "sol-5.6",
      current: false,
      seeded: 1,
    },
    { version: "unknown-version", model: "sol-5.6", current: true, seeded: 1 },
    {
      version: "claude-opus-5-enrichment-v1",
      model: "sol-5.6",
      current: true,
      seeded: 1,
    },
    {
      version: SOL_PIPELINE_VERSION,
      model: "other-model",
      current: true,
      seeded: 1,
    },
  ])(
    "seeds $seeded translations for $model/$version with current source=$current",
    async ({ version, model, current, seeded }) => {
      const ledger = Ledger.open(":memory:");
      try {
        const result = await planProductionBaseline({
          authors: values([
            { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
          ]),
          ledger,
          poems: values([
            {
              ...productionPoem(0),
              active_source_revision_id: "a".repeat(64),
              model_publications: [
                {
                  model_key: model,
                  prompt_version: version,
                  source_revision_id: (current ? "a" : "b").repeat(64),
                },
              ],
            },
          ]),
        });
        expect(result.report).toMatchObject({
          currentProfileWork: 1 - seeded,
          missingProfileWork: seeded,
          seededEnrichmentWork: seeded,
        });
        const claim = ledger.claim(
          "profile-test",
          Date.now(),
          60_000,
          [SOL_ENRICHMENT_WORK_KIND],
          { implementationVersion: SOL_PIPELINE_VERSION },
        );
        if (seeded === 0) expect(claim).toBeNull();
        else
          expect(claim?.work.input).toMatchObject({
            sourceRevisionId: "a".repeat(64),
          });
      } finally {
        ledger.close();
      }
    },
  );

  it(
    "streams and replays 100,064 production rows with deterministic priorities",
    { timeout: 120_000 },
    async () => {
      const ledger = Ledger.open(":memory:");
      const authors = [
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ];
      async function* poems(): AsyncGenerator<Record<string, unknown>> {
        for (let index = 0; index < 100_064; index += 1) {
          yield productionPoem(index);
        }
      }
      const first = await planProductionBaseline({
        authors: values(authors),
        batchSize: 500,
        ledger,
        poems: poems(),
      });
      expect(first.report).toMatchObject({
        authors: 1,
        duplicateSolWork: 0,
        duplicateEnrichmentWork: 0,
        currentProfileWork: 0,
        missingInsights: 10_000,
        missingProfileWork: 100_064,
        noTranslation: 27_184,
        poems: 100_064,
        seededEnrichmentWork: 100_064,
        seededSolWork: 100_064,
        skippedComplete: 62_880,
      });
      expect(first.canonicalPoemIds.size).toBe(100_064);
      const firstClaim = ledger.claim(
        "priority-test",
        Date.now(),
        60_000,
        [SOL_ENRICHMENT_WORK_KIND],
        { implementationVersion: SOL_PIPELINE_VERSION },
      );
      expect(firstClaim?.work.priority).toBe(
        BASELINE_NO_TRANSLATION_PRIORITY + BASELINE_SHORTEST_POEM_BONUS,
      );

      const replay = await planProductionBaseline({
        authors: values(authors),
        batchSize: 777,
        ledger,
        poems: poems(),
      });
      expect(replay.report).toMatchObject({
        duplicateEnrichmentWork: 100_064,
        duplicateSolWork: 100_064,
        seededEnrichmentWork: 0,
        seededSolWork: 0,
      });
      expect(replay.report.planHash).toBe(first.report.planHash);
      expect(
        ledger
          .status()
          .kindProgress.find(({ kind }) => kind === SOL_ENRICHMENT_WORK_KIND)
          ?.total,
      ).toBe(100_064);
      expect(BASELINE_MISSING_INSIGHTS_PRIORITY).toBeLessThan(
        BASELINE_NO_TRANSLATION_PRIORITY,
      );
      ledger.close();
    },
  );

  it("uses the canonical v2 corpus revision for an unmanaged legacy poem", async () => {
    const ledger = Ledger.open(":memory:");
    await planProductionBaseline({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poems: values([productionPoem(0)]),
    });
    const claim = ledger.claim(
      "revision-test",
      Date.now(),
      60_000,
      [SOL_ENRICHMENT_WORK_KIND],
      { implementationVersion: SOL_PIPELINE_VERSION },
    );
    const collected = prepareCollectedPoem(
      {
        artifactSchemaVersion: 1,
        collectedBy: "contract-test",
        source: {
          author: {
            canonicalId: "source:author:mutanabi",
            href: "https://source.invalid/writers/mutanabi",
            path: "/writers/mutanabi",
            slug: "mutanabi",
          },
          canonicalId: "source:poem:1",
          href: "https://source.invalid/works/1",
          lines: ["بيت 1"],
          numericId: "1",
          slug: "work-1",
          structure: "free_verse",
          title: "قصيدة 1",
          verses: null,
        },
        sourceHash: "a".repeat(64),
        workKey: "contract-test",
      },
      {
        authorId: AUTHOR_ID,
        authorNameArabic: "المتنبي",
        poemId: "00000000-0000-4000-8000-000000000001",
        sourceAuthorSlug: "mutanabi",
        sourcePoemId: "1",
      },
      "2026-08-28T00:00:00.000Z",
      1,
    );
    expect(claim?.work.input).toMatchObject({
      sourceRevisionId: collected.enrichmentInput.sourceRevisionId,
    });
    ledger.close();
  });

  it("accepts source-bound content objects only when their title is consistent", async () => {
    const ledger = Ledger.open(":memory:");
    const poem = productionPoem(0);
    poem["content_arabic"] = {
      content: ["بيت 1"],
      titleArabic: "قصيدة 1",
    };
    await expect(
      planProductionBaseline({
        authors: values([
          { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
        ]),
        ledger,
        poems: values([poem]),
      }),
    ).resolves.toMatchObject({ report: { poems: 1 } });
    poem["content_arabic"] = {
      content: ["بيت 1"],
      titleArabic: "عنوان مختلف",
    };
    await expect(
      planProductionBaseline({
        authors: values([
          { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
        ]),
        ledger,
        poems: values([poem]),
      }),
    ).rejects.toThrow("source-bound content title must match poem title");
    ledger.close();
  });

  it("rejects retired model profiles before they can seed work", async () => {
    const ledger = Ledger.open(":memory:");
    await expect(
      planProductionBaseline({
        authors: values([]),
        enrichmentProfiles: [
          {
            implementationVersion: "retired-enrichment-v1",
            kind: "poem-enrichment-retired",
            modelKey: "retired-model",
          },
        ],
        ledger,
        poems: values([]),
      }),
    ).rejects.toThrow("BASELINE_CODEX_PROFILE_REQUIRED");
    expect(ledger.status().kindProgress).toEqual([]);
    ledger.close();
  });

  it("rejects multiple enrichment profiles", async () => {
    const ledger = Ledger.open(":memory:");
    await expect(
      planProductionBaseline({
        authors: values([]),
        enrichmentProfiles: [
          {
            implementationVersion: "enrichment-a",
            kind: "poem-enrichment-a",
            modelKey: "duplicate-model",
          },
          {
            implementationVersion: "enrichment-b",
            kind: "poem-enrichment-b",
            modelKey: "duplicate-model",
          },
        ],
        ledger,
        poems: values([]),
      }),
    ).rejects.toThrow("BASELINE_CODEX_PROFILE_REQUIRED");
    ledger.close();
  });

  it("reports orphan identities without guessing or seeding", async () => {
    const ledger = Ledger.open(":memory:");
    const result = await planProductionBaseline({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poems: values([
        {
          ...productionPoem(0),
          author_id: null,
          ineligible_reason: "missing_arabic_author",
        },
        {
          ...productionPoem(1),
          author_id: "00000000-0000-4000-8000-999999999999",
          ineligible_reason: "missing_arabic_author",
        },
      ]),
    });
    expect(result.report.orphans).toEqual([
      {
        canonicalSourceId: "source:poem:1",
        poemId: "00000000-0000-4000-8000-000000000001",
        reason: "author_id_null",
      },
      {
        canonicalSourceId: "source:poem:2",
        poemId: "00000000-0000-4000-8000-000000000002",
        reason: "author_missing_from_export",
      },
    ]);
    expect(result.report.ineligible).toEqual([
      expect.objectContaining({
        canonicalSourceId: "source:poem:1",
        reason: "missing_arabic_author",
      }),
      expect.objectContaining({
        canonicalSourceId: "source:poem:2",
        reason: "missing_arabic_author",
      }),
    ]);
    expect(ledger.status().total).toBe(0);
    ledger.close();
  });

  it("accepts lean D1 availability flags without exporting old output bodies", async () => {
    const ledger = Ledger.open(":memory:");
    const poem = productionPoem(0);
    delete poem["translation"];
    delete poem["translation_gemini"];
    delete poem["insights"];
    poem["translation_missing"] = 1;
    poem["insights_missing"] = 0;
    const result = await planProductionBaseline({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poems: values([poem]),
    });
    expect(result.report).toMatchObject({
      noTranslation: 1,
      seededSolWork: 1,
    });
    ledger.close();
  });

  it("falls back to Arabic content titles and retains ineligible canonical IDs", async () => {
    const ledger = Ledger.open(":memory:");
    const eligible = {
      ...productionPoem(80_292),
      content_arabic: { content: ["أَنا الَّذي نَظَرَ الأَعمى إِلى أَدَبي"] },
      name_arabic: "أَنا الَّذي نَظَرَ الأَعمى إِلى أَدَبي",
      translation: null,
      translation_gemini: null,
    };
    const ineligible = {
      ...productionPoem(78_926),
      content_arabic: { content: [""] },
      ineligible_reason: "missing_arabic_title_and_content",
      name_arabic: "",
    };
    const result = await planProductionBaseline({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poems: values([eligible, ineligible]),
    });
    expect(result.canonicalPoemIds).toEqual(
      new Set(["source:poem:80293", "source:poem:78927"]),
    );
    expect(result.report).toMatchObject({
      eligiblePoems: 1,
      ineligible: [
        {
          canonicalSourceId: "source:poem:78927",
          poemId: "00000000-0000-4000-8000-000000078927",
          reason: "missing_arabic_title_and_content",
          slug: "work-78927",
        },
      ],
      ineligiblePoems: 1,
      poems: 2,
      seededSolWork: 1,
    });
    expect(ledger.status().total).toBe(1);
    ledger.close();
  });

  it("rejects exact-schema unsafe and oversized source text before seeding", async () => {
    const ledger = Ledger.open(":memory:");
    const unsafe = {
      ...productionPoem(40),
      content_arabic: { content: ["بيت\u{0007} غير آمن"] },
      ineligible_reason: "unsafe_source_text",
    };
    const oversized = {
      ...productionPoem(41),
      content_arabic: { content: ["ا".repeat(5_001)] },
      ineligible_reason: "source_text_constraints",
    };
    const result = await planProductionBaseline({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poems: values([unsafe, oversized]),
    });
    expect(result.canonicalPoemIds.size).toBe(2);
    expect(result.report.ineligible.map(({ reason }) => reason)).toEqual([
      "unsafe_source_text",
      "source_text_constraints",
    ]);
    expect(result.report.seededEnrichmentWork).toBe(0);
    expect(ledger.status().total).toBe(0);
    ledger.close();
  });

  it("seeds only manifest details absent from prod and verified CAS", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-detail-delta-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const authorHref = "https://source.invalid/writers/test";
    const priorInput = {
      authorHref,
      poemHref: "https://source.invalid/works/2",
    };
    ledger.seed({
      implementationVersion: collectorImplementationVersion(),
      input: priorInput,
      inputHash: inputHash(priorInput),
      kind: collectionWorkKinds().poemDetail,
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    });
    const claim = ledger.claim("fixture", Date.now(), 60_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!claim) throw new Error("Expected detail fixture claim");
    const verifiedArtifact = await artifacts.put("verified");
    ledger.succeed(claim, verifiedArtifact.hash);
    const manifest = parseAuthorPoemManifest({
      authorHref,
      challengeDetected: false,
      declaredPoemCountText: "3",
      kind: "author_poem_manifest",
      poems: [1, 2, 3].map((id) => ({
        href: `/works/${String(id)}`,
        title: `قصيدة ${String(id)}`,
        verseCountText: "1",
      })),
      schemaVersion: 1,
      sourceUrl: authorHref,
      terminal: true,
    });
    await expect(
      seedCertifiedDetailDelta({
        artifacts,
        ledger,
        manifests: [manifest],
        productionCanonicalIds: new Set(["source:poem:1"]),
      }),
    ).resolves.toEqual({
      alreadyInProduction: 1,
      duplicateManifestIds: 0,
      seeded: 1,
      verifiedLedgerArtifacts: 1,
    });
    const replay = await seedCertifiedDetailDelta({
      artifacts,
      ledger,
      manifests: [manifest],
      productionCanonicalIds: new Set(["source:poem:1"]),
    });
    expect(replay.seeded).toBe(0);
    ledger.close();
  });

  it("repairs orphans only from certified manifest and identity evidence", () => {
    const manifest = parseAuthorPoemManifest({
      authorHref: "https://source.invalid/writers/test",
      challengeDetected: false,
      declaredPoemCountText: "1",
      kind: "author_poem_manifest",
      poems: [{ href: "/works/1", title: "قصيدة", verseCountText: "1" }],
      schemaVersion: 1,
      sourceUrl: "https://source.invalid/writers/test",
      terminal: true,
    });
    const orphan = {
      canonicalSourceId: "source:poem:1",
      poemId: "00000000-0000-4000-8000-000000000001",
      reason: "author_id_null" as const,
    };
    expect(
      planCertifiedOrphanRepairs({
        manifests: [manifest],
        orphans: [orphan],
        sourceAuthorToProductionAuthorId: new Map([
          ["source:author:test", AUTHOR_ID],
        ]),
      }),
    ).toEqual({
      repairs: [
        {
          authorId: AUTHOR_ID,
          canonicalAuthorId: "source:author:test",
          canonicalSourceId: "source:poem:1",
          poemId: orphan.poemId,
        },
      ],
      unresolved: [],
    });
    expect(
      planCertifiedOrphanRepairs({
        manifests: [manifest],
        orphans: [orphan],
        sourceAuthorToProductionAuthorId: new Map(),
      }).unresolved,
    ).toEqual([
      {
        canonicalSourceId: "source:poem:1",
        poemId: orphan.poemId,
        reason: "source_author_identity_missing",
      },
    ]);
  });

  it("seeds bounded source recrawls without treating legacy text as evidence", async () => {
    const ledger = Ledger.open(":memory:");
    const firstPoem = productionPoem(0);
    const secondPoem = productionPoem(1);
    const poemIds = new Set([
      String(firstPoem["id"]),
      String(secondPoem["id"]),
      "00000000-0000-4000-8000-999999999999",
    ]);
    const options = () => ({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poemIds,
      poems: values([firstPoem, secondPoem]),
    });
    await expect(seedProductionDetailRecovery(options())).resolves.toEqual({
      activeExisting: 0,
      applied: true,
      candidateWork: 2,
      completedNeedsRefresh: 0,
      duplicateWork: 0,
      identityConflicts: 0,
      newWork: 2,
      priorityRaiseCandidates: 0,
      refreshedExisting: 0,
      refreshedSeeded: 0,
      requested: 3,
      seeded: 2,
      terminalExisting: 0,
      unresolvedPoemIds: ["00000000-0000-4000-8000-999999999999"],
    });
    await expect(seedProductionDetailRecovery(options())).resolves.toEqual({
      activeExisting: 2,
      applied: true,
      candidateWork: 2,
      completedNeedsRefresh: 0,
      duplicateWork: 2,
      identityConflicts: 0,
      newWork: 0,
      priorityRaiseCandidates: 2,
      refreshedExisting: 0,
      refreshedSeeded: 0,
      requested: 3,
      seeded: 0,
      terminalExisting: 0,
      unresolvedPoemIds: ["00000000-0000-4000-8000-999999999999"],
    });
    const detail = ledger.claim("fixture", Date.now(), 60_000, [
      collectionWorkKinds().poemDetail,
    ]);
    expect(detail?.work.input).toEqual(
      expect.objectContaining({
        authorHref: "https://source.invalid/writers/mutanabi",
        authorNameArabic: "المتنبي",
        refreshGeneration:
          "legacy-sol-0000000000000000000000000000000000000000",
      }),
    );
    expect(String(detail?.work.input["poemHref"])).toMatch(
      /^https:\/\/source\.invalid\/works\/[12]$/,
    );
    expect(
      ledger.sourceAuthorMetadata("https://source.invalid/writers/mutanabi"),
    ).toEqual({
      authorNameArabic: "المتنبي",
      refreshGeneration: "legacy-sol-0000000000000000000000000000000000000000",
    });
    expect(detail?.work.priority).toBe(1_000);
    ledger.close();
  });

  it("creates one manifest-scoped refresh after completed detail work", async () => {
    const ledger = Ledger.open(":memory:");
    const input = {
      authorHref: "https://source.invalid/writers/mutanabi",
      poemHref: "https://source.invalid/works/1",
    };
    ledger.seed({
      implementationVersion: collectorImplementationVersion(),
      input,
      inputHash: inputHash(input),
      kind: collectionWorkKinds().poemDetail,
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    });
    const claim = ledger.claim("fixture", Date.now(), 60_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!claim) throw new Error("Expected detail claim");
    ledger.succeed(claim, "a".repeat(64));
    const report = await seedProductionDetailRecovery({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      manifestHash: "b".repeat(64),
      poemIds: new Set([String(productionPoem(0)["id"])]),
      poems: values([productionPoem(0)]),
    });
    expect(report).toMatchObject({
      candidateWork: 1,
      completedNeedsRefresh: 1,
      refreshedSeeded: 1,
      seeded: 1,
    });
    const replay = await seedProductionDetailRecovery({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      manifestHash: "b".repeat(64),
      poemIds: new Set([String(productionPoem(0)["id"])]),
      poems: values([productionPoem(0)]),
    });
    expect(replay).toMatchObject({
      activeExisting: 1,
      candidateWork: 1,
      duplicateWork: 1,
      priorityRaiseCandidates: 1,
    });
    ledger.close();
  });

  it("does not persist recovery author metadata during a dry run", async () => {
    const ledger = Ledger.open(":memory:");
    await seedProductionDetailRecovery({
      apply: false,
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      poemIds: new Set([String(productionPoem(0)["id"])]),
      poems: values([productionPoem(0)]),
    });
    expect(
      ledger.sourceAuthorMetadata("https://source.invalid/writers/mutanabi"),
    ).toBeNull();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === collectionWorkKinds().poemDetail,
        ),
    ).toBeUndefined();
    ledger.close();
  });

  it("raises active recovery work to explicit priority 1000", async () => {
    const ledger = Ledger.open(":memory:");
    const input = {
      authorHref: "https://source.invalid/writers/mutanabi",
      poemHref: "https://source.invalid/works/1",
    };
    ledger.seed({
      implementationVersion: collectorImplementationVersion(),
      input,
      inputHash: inputHash(input),
      kind: collectionWorkKinds().poemDetail,
      priority: 1,
      schemaVersion: collectorSchemaVersion(),
    });
    const report = await seedProductionDetailRecovery({
      authors: values([
        { id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" },
      ]),
      ledger,
      manifestHash: "c".repeat(64),
      poemIds: new Set([String(productionPoem(0)["id"])]),
      poems: values([productionPoem(0)]),
    });
    expect(report).toMatchObject({
      activeExisting: 1,
      duplicateWork: 1,
      priorityRaiseCandidates: 1,
      seeded: 0,
    });
    expect(
      ledger.listReadyWork(
        collectionWorkKinds().poemDetail,
        1,
        {
          implementationVersion: collectorImplementationVersion(),
          schemaVersion: collectorSchemaVersion(),
        },
        Date.now(),
      )[0]?.priority,
    ).toBe(1_000);
    ledger.close();
  });
});
