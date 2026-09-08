import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  canonicalPoemBindingIdBody,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { describe, expect, it } from "vitest";

import { collectionWorkKinds } from "../collection/collection-scheduler";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import {
  assertCollectedArtifactBinding,
  LocalEnrichmentFanout,
} from "../enrichment/local-enrichment-fanout";
import { ENRICHMENT_PROVIDER_SPECS } from "../enrichment/sol-runner";
import { ArtifactStore } from "../persistence/artifact-store";
import { Ledger } from "../persistence/ledger";
import type { WorkItem } from "../persistence/schema";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key";
import {
  bindCollectedPoem,
  prepareCollectedPoem,
} from "../publication/corpus-import-actions";
import { prepareEnrichmentPublication } from "../publication/publication-lane";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DETAIL = {
  artifactSchemaVersion: 1,
  collectedBy: "test",
  source: {
    author: {
      canonicalId: "source:author:poet",
      href: "https://source.invalid/writers/poet",
      path: "/writers/poet",
      slug: "poet",
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
  workKey: "collector",
};
const MAPPING = {
  authorId: "e511352b-1cbc-4f50-89dd-b7e62a3d1895",
  authorNameArabic: "شاعر",
  poemId: "3293b365-7803-4805-9e42-b7ed8cbe2fc8",
  sourceAuthorSlug: "poet",
  sourcePoemId: "42",
};

describe("local enrichment fanout", () => {
  it("replays both durable boundaries without duplicating provider work", async () => {
    const fixture = createFixture();
    await seedDetail(fixture);
    const crashed = new Set<string>();
    const fanout = createFanout(fixture, "sol-5.6", (boundary) => {
      if (crashed.has(boundary)) return;
      crashed.add(boundary);
      throw new Error(`crash:${boundary}`);
    });
    let now = Date.now() + 1_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    now += 31_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    now += 31_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    expect(crashed).toEqual(new Set(["source_jobs_seeded", "provider_seeded"]));
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(1);
    const productionBaseInput = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-26T00:00:00.000Z",
      7,
    ).enrichmentInput;
    const productionInput = bindCollectedPoem(
      productionBaseInput,
      bindingForInput(productionBaseInput, MAPPING),
    );
    expect(
      fixture.ledger.seed(providerDefinition("sol-5.6", productionInput)),
    ).toMatchObject({ inserted: false });
    const replay = await fanout.cycle({ maximum: 10, now: () => now + 31_000 });
    expect(replay.cursor).toBeGreaterThan(0);
    fixture.ledger.close();
  });

  it("rejects a retired provider before creating fanout work", async () => {
    const fixture = createFixture();
    await seedDetail(fixture);
    expect(() => createFanout(fixture, "retired-model")).toThrow(
      "LOCAL_FANOUT_CODEX_PROFILE_REQUIRED",
    );
    expect(providerTotal(fixture.ledger, "provider-retired-model")).toBe(0);
    fixture.ledger.close();
  });

  it("resolves deferred work after publication imports its source", async () => {
    const fixture = createFixture();
    const detail = await seedDetail(fixture);
    let resolved = false;
    const fanout = createFanout(fixture, "sol-5.6", undefined, () =>
      resolved
        ? {
            mapping: MAPPING,
            observedAt: "2026-08-26T00:00:00.000Z",
            writerEpoch: 7,
          }
        : null,
    );
    let now = Date.now() + 1_000;
    await expect(
      fanout.cycle({ maximum: 10, now: () => now }),
    ).resolves.toMatchObject({ pendingResolution: 1, scanned: 1 });
    if (!detail.outputArtifactHash) throw new Error("detail artifact missing");
    fixture.ledger.markImported(
      detail.workKey,
      detail.outputArtifactHash,
      now + 1,
    );

    resolved = true;
    now += 5 * 60_000 + 1;
    await expect(
      fanout.cycle({ maximum: 10, now: () => now }),
    ).resolves.toMatchObject({ deadLettered: 0, seeded: 1 });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(1);
    fixture.ledger.close();
  });

  it("keeps newly resolved work pending until production issues a binding", async () => {
    const fixture = createFixture();
    await seedDetail(fixture);
    const fanout = createFanout(
      fixture,
      "sol-5.6",
      undefined,
      () => ({
        mapping: MAPPING,
        observedAt: "2026-08-26T00:00:00.000Z",
        writerEpoch: 7,
      }),
      false,
    );
    await expect(
      fanout.cycle({ maximum: 10, now: () => Date.now() + 1_000 }),
    ).resolves.toMatchObject({ pendingResolution: 1, seeded: 0 });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(0);
    fixture.ledger.close();
  });

  it("waits attempt-neutrally beyond thirty days and resumes without rescanning", async () => {
    const fixture = createFixture();
    await seedDetail(fixture);
    let resolved = false;
    const fanout = createFanout(fixture, "sol-5.6", undefined, () =>
      resolved
        ? {
            mapping: MAPPING,
            observedAt: "2026-08-26T00:00:00.000Z",
            writerEpoch: 7,
          }
        : null,
    );
    let now = Date.now() + 1_000;
    await fanout.cycle({ maximum: 10, now: () => now });

    now += 31 * 24 * 60 * 60_000;
    await expect(
      fanout.cycle({ maximum: 10, now: () => now }),
    ).resolves.toMatchObject({
      deadLettered: 0,
      pendingResolution: 1,
      scanned: 0,
    });
    const sourceKind = fixture.ledger
      .status()
      .kindProgress.find(({ kind }) =>
        kind.startsWith("local-enrichment-source-"),
      )?.kind;
    if (!sourceKind) throw new Error("local fanout source kind missing");
    now += 5 * 60_000 + 1;
    const inspection = fixture.ledger.claim("inspection", now, 1_000, [
      sourceKind,
    ]);
    expect(inspection?.work.attemptCount).toBe(1);
    if (!inspection) throw new Error("local fanout source claim missing");
    fixture.ledger.operatorRelease(inspection, "TEST_INSPECTION", now);

    resolved = true;
    now += 1;
    await expect(
      fanout.cycle({ maximum: 10, now: () => now }),
    ).resolves.toMatchObject({ deadLettered: 0, seeded: 1 });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(1);
    fixture.ledger.close();
  });

  it("recovers an expired in-process claim before scanning", async () => {
    const fixture = createFixture();
    await seedDetail(fixture);
    const fanout = createFanout(fixture, "sol-5.6");
    const now = Date.now() + 1_000;
    const controlKind = fixture.ledger
      .status()
      .kindProgress.find(({ kind }) =>
        kind.startsWith("local-enrichment-control-"),
      )?.kind;
    if (!controlKind) throw new Error("Expected local fanout control kind");
    expect(
      fixture.ledger.claim("crashed-owner", now, 10, [controlKind], {
        implementationVersion: "local-enrichment-fanout-v2",
        schemaVersion: "local-enrichment-fanout@2",
      }),
    ).not.toBeNull();
    await expect(
      fanout.cycle({ maximum: 10, now: () => now + 20 }),
    ).resolves.toMatchObject({ scanned: 1, seeded: 1 });
    fixture.ledger.close();
  });

  it("renews the scan lease between deterministic slow items", async () => {
    const fixture = createFixture();
    await seedDetail(fixture, DETAIL, "refresh-1", 1);
    await seedDetail(
      fixture,
      { ...DETAIL, source: { ...DETAIL.source, title: "عنوان مصحح" } },
      "refresh-2",
      2,
    );
    let now = Date.now() + 1_000;
    const fanout = createFanout(fixture, "sol-5.6", undefined, () => {
      now += 4 * 60_000;
      return {
        mapping: MAPPING,
        observedAt: "2026-08-26T00:00:00.000Z",
        writerEpoch: 7,
      };
    });
    await expect(
      fanout.cycle({ maximum: 10, now: () => now }),
    ).resolves.toMatchObject({ scanned: 2, seeded: 2 });
    fixture.ledger.close();
  });

  it("uses the title-aware v2 source revision while identical input stays idempotent", () => {
    const original = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-26T00:00:00.000Z",
      7,
    ).enrichmentInput;
    const corrected = prepareCollectedPoem(
      { ...DETAIL, source: { ...DETAIL.source, title: "عنوان مصحح" } },
      MAPPING,
      "2026-08-26T00:00:00.000Z",
      7,
    ).enrichmentInput;
    expect(corrected.sourceRevisionId).not.toBe(original.sourceRevisionId);
    expect(corrected.sourceContentSha256).toBe(original.sourceContentSha256);
    const fixture = createFixture();
    const first = fixture.ledger.seed(providerDefinition("sol-5.6", original));
    const duplicate = fixture.ledger.seed(
      providerDefinition("sol-5.6", original),
    );
    const revision = fixture.ledger.seed(
      providerDefinition("sol-5.6", corrected),
    );
    expect(first.inserted).toBe(true);
    expect(duplicate).toMatchObject({
      inserted: false,
      workKey: first.workKey,
    });
    expect(revision).toMatchObject({ inserted: true });
    expect(revision.workKey).not.toBe(first.workKey);
    fixture.ledger.close();
  });

  it("collapses unchanged refresh generations but emits one title-only revision", async () => {
    const fixture = createFixture();
    await seedDetail(fixture, DETAIL, "refresh-1", 1);
    const fanout = createFanout(fixture, "sol-5.6");
    let now = Date.now() + 1_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(1);
    expect(localSourceTotal(fixture.ledger)).toBe(1);

    await seedDetail(fixture, DETAIL, "refresh-2", now + 1);
    now += 31_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(1);
    expect(localSourceTotal(fixture.ledger)).toBe(1);

    const corrected = {
      ...DETAIL,
      source: { ...DETAIL.source, title: "عنوان مصحح" },
      sourceHash: "b".repeat(64),
    };
    await seedDetail(fixture, corrected, "refresh-3", now + 1);
    now += 31_000;
    await fanout.cycle({ maximum: 10, now: () => now });
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(2);
    expect(localSourceTotal(fixture.ledger)).toBe(2);
    fixture.ledger.close();
  });

  it("quarantines one poisoned completion without blocking good siblings or the cursor", async () => {
    const fixture = createFixture();
    await seedDetail(fixture, DETAIL, "refresh-1", 1);
    seedMissingDetail(fixture, "refresh-2", 3);
    await seedDetail(
      fixture,
      { ...DETAIL, source: { ...DETAIL.source, title: "عنوان مصحح" } },
      "refresh-3",
      5,
    );
    const fanout = createFanout(fixture, "sol-5.6");
    const result = await fanout.cycle({
      maximum: 10,
      now: () => Date.now() + 1_000,
    });
    expect(result).toMatchObject({
      deadLettered: 1,
      scanned: 3,
      seeded: 2,
    });
    expect(result.cursor).toBeGreaterThan(0);
    expect(providerTotal(fixture.ledger, "provider-sol-5.6")).toBe(2);
    expect(localSourceTotal(fixture.ledger)).toBe(3);
    fixture.ledger.close();
  });

  it("binds every collected identity and hash field to its owning work item", async () => {
    const fixture = createFixture();
    const work = await seedDetail(fixture);
    const artifactBuffer = await fixture.artifacts.read(
      work.outputArtifactHash ?? "",
    );
    const artifact = JSON.parse(
      artifactBuffer.toString("utf8"),
    ) as typeof DETAIL;
    expect(() => assertCollectedArtifactBinding(work, artifact)).not.toThrow();

    const rebound = (source: typeof DETAIL.source) => ({
      ...artifact,
      source,
      sourceHash: sha256(canonicalJson(source)),
    });
    const poemMismatch = [
      { ...artifact.source, canonicalId: "source:poem:43" },
      { ...artifact.source, numericId: "43" },
      { ...artifact.source, href: "https://source.invalid/works/43" },
    ];
    for (const source of poemMismatch) {
      expect(() =>
        assertCollectedArtifactBinding(work, rebound(source)),
      ).toThrow("LOCAL_ENRICHMENT_SOURCE_POEM_MISMATCH");
    }
    const wrongAuthor = {
      ...artifact.source,
      author: { ...artifact.source.author, slug: "other" },
    };
    expect(() =>
      assertCollectedArtifactBinding(work, rebound(wrongAuthor)),
    ).toThrow("LOCAL_ENRICHMENT_SOURCE_AUTHOR_MISMATCH");
    expect(() =>
      assertCollectedArtifactBinding(work, {
        ...artifact,
        workKey: "f".repeat(64),
      }),
    ).toThrow("LOCAL_ENRICHMENT_SOURCE_WORK_KEY_MISMATCH");
    expect(() =>
      assertCollectedArtifactBinding(work, {
        ...artifact,
        sourceHash: "f".repeat(64),
      }),
    ).toThrow("LOCAL_ENRICHMENT_SOURCE_HASH_MISMATCH");
    expect(() =>
      assertCollectedArtifactBinding(
        {
          ...work,
          input: {
            ...work.input,
            poemHref: "https://source.invalid/works/43",
          },
        },
        artifact,
      ),
    ).toThrow("LOCAL_ENRICHMENT_SOURCE_POEM_MISMATCH");
    fixture.ledger.close();
  });

  it("produces an artifact that publication binds to the same mapped poem", () => {
    const input = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-26T00:00:00.000Z",
      7,
    ).enrichmentInput;
    const output = {
      insights: {
        culturalSignificance: "دلالة",
        historicalContext: "سياق",
        literaryDevices: ["صورة"],
        notableLines: [{ explanation: "شرح", line: "صدر" }],
        summary: "ملخص",
        themes: ["موضوع"],
      },
      translation: { lines: ["First", "Second"] },
    };
    const profile = ENRICHMENT_PROVIDER_SPECS.sol;
    const action = prepareEnrichmentPublication(
      {
        generationAttemptId: "generation",
        input,
        model: profile.model,
        modelKey: profile.modelKey,
        output,
        outputHash: sha256(canonicalJson(output)),
        pipelineVersion: profile.pipelineVersion,
        provider: "sol",
        reasoningEffort: profile.reasoningEffort,
        reviewAttemptIds: ["review-1", "review-2"],
        reviews: [
          {
            fidelityScore: 100,
            findings: [],
            insightScore: 100,
            verdict: "pass",
          },
          {
            fidelityScore: 99,
            findings: [],
            insightScore: 99,
            verdict: "pass",
          },
        ],
      },
      {
        expectedPointerVersion: null,
        source: { artifactHash: "a".repeat(64), workKey: "b".repeat(64) },
        taskKey: "b".repeat(64),
        writerEpoch: 7,
      },
    );
    if (action.action !== "publish-enrichment")
      throw new Error("expected enrichment publication");
    expect(action.input.publication.poemId).toBe(MAPPING.poemId);
    expect(action.input.artifact.sourceRevisionId).toBe(input.sourceRevisionId);
  });
});

function createFanout(
  fixture: ReturnType<typeof createFixture>,
  modelKey: string,
  afterBoundary?: (boundary: "provider_seeded" | "source_jobs_seeded") => void,
  resolve: () => {
    mapping: typeof MAPPING;
    observedAt: string;
    writerEpoch: number;
  } | null = () => ({
    mapping: MAPPING,
    observedAt: "2026-08-26T00:00:00.000Z",
    writerEpoch: 7,
  }),
  withBinding = true,
) {
  return new LocalEnrichmentFanout({
    ...(afterBoundary ? { afterBoundary } : {}),
    artifacts: fixture.artifacts,
    batchSize: 10,
    ledger: fixture.ledger,
    owner: `fanout-${modelKey}`,
    profile: {
      modelKey,
      pipelineVersion: `pipeline-${modelKey}`,
      seed: (input, priority = 0) =>
        fixture.ledger.seed(providerDefinition(modelKey, input, priority)),
    },
    resolver: {
      resolve: (_source, artifact) => {
        const resolution = resolve();
        if (!resolution || !withBinding) return resolution;
        const input = prepareCollectedPoem(
          artifact,
          resolution.mapping,
          resolution.observedAt,
          resolution.writerEpoch,
        ).enrichmentInput;
        return {
          ...resolution,
          binding: bindingForInput(input, resolution.mapping),
        };
      },
    },
  });
}

function bindingForInput(
  input: ReturnType<typeof prepareCollectedPoem>["enrichmentInput"],
  mapping: typeof MAPPING,
) {
  const identity = {
    authorId: mapping.authorId,
    authorNameArabic: input.authorArabic,
    externalPoemId: mapping.sourcePoemId,
    lineNfcHash: sha256(sourceLineNfcHashBody(input.linesArabic)),
    poemId: mapping.poemId,
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
      issuedAt: "2026-08-26T00:00:00Z",
      sourcePointerVersion: 1,
    },
    bindingId: sha256(canonicalPoemBindingIdBody(identity)),
  };
}

function createFixture() {
  const root = mkdtempSync(join(tmpdir(), "local-enrichment-fanout-"));
  return {
    artifacts: new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    }),
    ledger: Ledger.initialize(join(root, "ledger.sqlite")),
  };
}

function providerDefinition(
  modelKey: string,
  input: Record<string, unknown>,
  priority = 0,
) {
  return {
    implementationVersion: `pipeline-${modelKey}`,
    input,
    inputHash: inputHash(input),
    kind: `provider-${modelKey}`,
    priority,
    schemaVersion: "saqi.poem-enrichment-input@1",
  };
}

function providerTotal(ledger: Ledger, kind: string): number {
  return (
    ledger.status().kindProgress.find((progress) => progress.kind === kind)
      ?.total ?? 0
  );
}

function localSourceTotal(ledger: Ledger): number {
  return ledger
    .status()
    .kindProgress.filter(({ kind }) =>
      kind.startsWith("local-enrichment-source-"),
    )
    .reduce((total, { total: count }) => total + count, 0);
}

async function seedDetail(
  fixture: ReturnType<typeof createFixture>,
  detail = DETAIL,
  refreshGeneration?: string,
  seededAt = 1,
): Promise<WorkItem> {
  const input = {
    authorHref: "https://source.invalid/writers/poet",
    poemHref: "https://source.invalid/works/42",
    ...(refreshGeneration === undefined ? {} : { refreshGeneration }),
  };
  const seeded = fixture.ledger.seed(
    {
      implementationVersion: collectorImplementationVersion(),
      input,
      inputHash: inputHash(input),
      kind: collectionWorkKinds().poemDetail,
      priority: 200,
      schemaVersion: collectorSchemaVersion(),
    },
    seededAt,
  );
  const boundDetail = {
    ...detail,
    collectedBy: collectorImplementationVersion(),
    sourceHash: sha256(canonicalJson(detail.source)),
    workKey: seeded.workKey,
  };
  const stored = await fixture.artifacts.put(`${canonicalJson(boundDetail)}\n`);
  const claim = fixture.ledger.claim("collector", seededAt, 1_000, [
    collectionWorkKinds().poemDetail,
  ]);
  if (!claim) throw new Error("detail claim missing");
  fixture.ledger.succeed(claim, stored.hash, seededAt + 1);
  const work = fixture.ledger.get(seeded.workKey);
  if (!work) throw new Error("detail work missing");
  return work;
}

function seedMissingDetail(
  fixture: ReturnType<typeof createFixture>,
  refreshGeneration: string,
  seededAt: number,
): void {
  const input = {
    authorHref: "https://source.invalid/writers/poet",
    poemHref: "https://source.invalid/works/42",
    refreshGeneration,
  };
  fixture.ledger.seed(
    {
      implementationVersion: collectorImplementationVersion(),
      input,
      inputHash: inputHash(input),
      kind: collectionWorkKinds().poemDetail,
      priority: 200,
      schemaVersion: collectorSchemaVersion(),
    },
    seededAt,
  );
  const claim = fixture.ledger.claim("collector", seededAt, 1_000, [
    collectionWorkKinds().poemDetail,
  ]);
  if (!claim) throw new Error("missing detail claim missing");
  fixture.ledger.succeed(claim, "f".repeat(64), seededAt + 1);
}
