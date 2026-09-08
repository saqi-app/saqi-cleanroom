import { tmpdir } from "node:os";
import { join } from "node:path";

import type {
  CorpusImportAction,
  PoemEnrichmentInput,
} from "@saqi/precedent-iso";
import {
  canonicalPoemBindingIdBody,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { describe, expect, it, vi } from "vitest";

import { collectionWorkKinds } from "../collection/collection-scheduler";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import {
  SOL_ENRICHMENT_WORK_KIND,
  SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
} from "../enrichment/sol-coordinator";
import {
  ENRICHMENT_PROVIDER_SPECS,
  SOL_PIPELINE_VERSION,
} from "../enrichment/sol-runner";
import { ArtifactStore } from "../persistence/artifact-store";
import {
  DIRECT_ENRICHMENT_PUBLICATION_KIND,
  Ledger,
} from "../persistence/ledger";
import {
  canonicalJson,
  inputHash,
  sha256,
  workKey as calculateWorkKey,
} from "../persistence/work-key";
import {
  bindCollectedPoem,
  prepareCollectedPoem,
} from "../publication/corpus-import-actions";
import {
  FANOUT_CONTROL_KIND,
  FANOUT_DETAIL_KIND,
  FANOUT_ENRICHMENT_KIND,
  FANOUT_IMPLEMENTATION_VERSION,
  FANOUT_SCHEMA_VERSION,
  FANOUT_SOL_KIND,
  FanoutReconciler,
  type FanoutReconcilerOptions,
} from "../publication/fanout-reconciler";
import { PublicationClient } from "../publication/publication-client";
import {
  COLLECTION_PUBLICATION_WORK_KIND,
  ENRICHMENT_PUBLICATION_WORK_KIND,
  PUBLICATION_IMPLEMENTATION_VERSION,
  PUBLICATION_SCHEMA_VERSION,
  PublicationLane,
} from "../publication/publication-lane";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DETAIL_INPUT = {
  authorHref: "https://source.invalid/writers/a",
  poemHref: "https://source.invalid/works/42",
};
const DETAIL_SOURCE = {
  author: {
    canonicalId: "source:author:a",
    href: "https://source.invalid/writers/a",
    path: "/writers/a",
    slug: "a",
  },
  canonicalId: "source:poem:42",
  href: "https://source.invalid/works/42",
  lines: ["صدر", "عجز"],
  numericId: "42",
  slug: "work-42",
  structure: "classical",
  title: "قصيدة",
  verses: 1,
};
const DETAIL = {
  artifactSchemaVersion: 1,
  collectedBy: collectorImplementationVersion(),
  source: DETAIL_SOURCE,
  sourceHash: sha256(canonicalJson(DETAIL_SOURCE)),
  workKey: calculateWorkKey({
    implementationVersion: collectorImplementationVersion(),
    input: DETAIL_INPUT,
    inputHash: inputHash(DETAIL_INPUT),
    kind: collectionWorkKinds().poemDetail,
    priority: 0,
    schemaVersion: collectorSchemaVersion(),
  }),
};
const MAPPING = {
  authorId: "author-1",
  authorNameArabic: "شاعر",
  poemId: "poem-1",
  sourceAuthorSlug: "a",
  sourcePoemId: "42",
};

function createScanOnlyFanout(
  artifacts: ArtifactStore,
  ledger: Ledger,
  afterBoundary?: FanoutReconcilerOptions["afterBoundary"],
  batchSize?: number,
): FanoutReconciler {
  const publication = new PublicationLane({
    artifacts,
    client: new PublicationClient({
      endpoint: "https://example.test/api/corpus-import",
      transport: async () => {
        throw new Error("not called");
      },
    }),
    ledger,
  });
  return new FanoutReconciler({
    afterBoundary,
    artifacts,
    ...(batchSize ? { batchSize } : {}),
    enrichment: [
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        seed: () => {
          throw new Error("not called without a resolution");
        },
        workKind: SOL_ENRICHMENT_WORK_KIND,
      },
    ],
    ledger,
    publication,
    resolvers: { collected: () => null, enrichment: () => null },
  });
}

async function succeedDetail(
  artifacts: ArtifactStore,
  ledger: Ledger,
  now: number,
): Promise<string> {
  const artifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
  const seeded = ledger.seed(
    {
      implementationVersion: collectorImplementationVersion(),
      input: DETAIL_INPUT,
      inputHash: inputHash(DETAIL_INPUT),
      kind: collectionWorkKinds().poemDetail,
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    },
    now,
  );
  const claim = ledger.claim("collector", now, 1_000, [
    collectionWorkKinds().poemDetail,
  ]);
  if (!claim) throw new Error("detail claim missing");
  ledger.succeed(claim, artifact.hash, now);
  return seeded.workKey;
}

async function succeedLegacySol(
  artifacts: ArtifactStore,
  ledger: Ledger,
  poemId: string,
  now: number,
  pipelineVersion = "sol-enrichment-v1",
): Promise<void> {
  const input: PoemEnrichmentInput = {
    authorArabic: "شاعر",
    linesArabic: ["صدر", "عجز"],
    poemId,
    schemaId: "saqi.poem-enrichment-input",
    schemaVersion: 1,
    sourceContentSha256: sha256(canonicalJson(["صدر", "عجز"])),
    sourceRevisionId: sha256(`revision:${poemId}`),
    titleArabic: "قصيدة",
  };
  const output = {
    insights: {
      culturalSignificance: "c",
      historicalContext: "h",
      literaryDevices: ["d"],
      notableLines: [{ explanation: "e", line: "صدر" }],
      summary: "s",
      themes: ["t"],
    },
    translation: { lines: ["first", "second"] },
  };
  const artifact = await artifacts.put(
    `${canonicalJson({
      generationAttemptId: `legacy-generation-${poemId}`,
      input,
      model: "gpt-5.6-sol",
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      output,
      outputHash: sha256(canonicalJson(output)),
      pipelineVersion,
      provider: "sol",
      reasoningEffort: "high",
      reviewAttemptIds: [`legacy-r1-${poemId}`, `legacy-r2-${poemId}`],
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
    })}\n`,
  );
  ledger.seed(
    {
      implementationVersion: pipelineVersion,
      input,
      inputHash: inputHash(input),
      kind: SOL_ENRICHMENT_WORK_KIND,
      priority: 200,
      schemaVersion: "saqi.poem-enrichment-input@1",
    },
    now,
  );
  const claim = ledger.claim(
    "legacy-sol",
    now + 1,
    1_000,
    [SOL_ENRICHMENT_WORK_KIND],
    { implementationVersion: pipelineVersion },
  );
  if (!claim) throw new Error("legacy Sol claim missing");
  ledger.succeed(claim, artifact.hash, now + 2);
}

function legacySolFanoutDefinitions(ledger: Ledger) {
  return ledger.listWorkDefinitionsAfter(null, FANOUT_ENRICHMENT_KIND, 100, {
    implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
    schemaVersion: FANOUT_SCHEMA_VERSION,
  }).items;
}

describe("fanout reconciliation", () => {
  it("preserves both active-cycle and close-settlement errors", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-close-errors-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const cycleFailure = new Error("cycle settlement failed");
      const closeFailure = new Error("close settlement failed");
      let closing: Promise<void> | undefined;
      const reconciler = createScanOnlyFanout(artifacts, ledger, (boundary) => {
        if (boundary !== "scan") return;
        closing = reconciler.close();
      });
      const retry = vi.spyOn(ledger, "retry").mockImplementation(() => {
        throw cycleFailure;
      });
      const release = vi
        .spyOn(ledger, "operatorRelease")
        .mockImplementation(() => {
          throw closeFailure;
        });
      await expect(reconciler.cycle()).rejects.toBe(cycleFailure);
      if (!closing) throw new Error("Close did not start during scan");
      await expect(closing).rejects.toMatchObject({
        errors: [cycleFailure, closeFailure],
      });
      expect(ledger.status().byState.running).toBe(1);
      retry.mockRestore();
      release.mockRestore();
      await reconciler.close();
    } finally {
      ledger.close();
    }
  });

  it("releases only its retained control lease after settlement fails", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-close-failure-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const reconciler = createScanOnlyFanout(artifacts, ledger);
      const failure = new Error("SQLITE_BUSY");
      const retry = vi.spyOn(ledger, "retry").mockImplementation(() => {
        throw failure;
      });
      await expect(reconciler.cycle()).rejects.toBe(failure);
      expect(ledger.status().byState.running).toBe(1);
      retry.mockRestore();
      await reconciler.close();
      expect(ledger.status().byState.running).toBe(0);
      expect(ledger.status().affectedByErrorCode).toContainEqual({
        code: "FANOUT_CYCLE_SETTLEMENT",
        count: 1,
      });
      await expect(reconciler.cycle()).rejects.toThrow("FANOUT_CLOSED");
      await reconciler.close();
    } finally {
      ledger.close();
    }
  });

  it("rejects close when owned lease release cannot persist and permits retry", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-close-retry-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const reconciler = createScanOnlyFanout(artifacts, ledger);
      const retry = vi.spyOn(ledger, "retry").mockImplementation(() => {
        throw new Error("scan settlement failed");
      });
      await expect(reconciler.cycle()).rejects.toThrow(
        "scan settlement failed",
      );
      retry.mockRestore();
      const failure = new Error("release write failed");
      const release = vi
        .spyOn(ledger, "operatorRelease")
        .mockImplementation(() => {
          throw failure;
        });
      await expect(reconciler.close()).rejects.toBe(failure);
      expect(ledger.status().byState.running).toBe(1);
      release.mockRestore();
      await reconciler.close();
      expect(ledger.status().byState.running).toBe(0);
    } finally {
      ledger.close();
    }
  });

  it("waits for an active scan before close and refuses overlapping cycles", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-close-drain-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const entered = Promise.withResolvers<undefined>();
      const finish = Promise.withResolvers<undefined>();
      const put = artifacts.put.bind(artifacts);
      vi.spyOn(artifacts, "put").mockImplementation(async (...args) => {
        entered.resolve(undefined);
        await finish.promise;
        return put(...args);
      });
      const reconciler = createScanOnlyFanout(artifacts, ledger);
      const cycle = reconciler.cycle();
      await entered.promise;
      await expect(reconciler.cycle()).rejects.toThrow("FANOUT_CYCLE_ACTIVE");
      let closed = false;
      const closing = reconciler.close().then(() => {
        closed = true;
      });
      await Promise.resolve();
      expect(closed).toBe(false);
      expect(ledger.status().byState.running).toBe(1);
      finish.resolve(undefined);
      await cycle;
      await closing;
      expect(ledger.status().byState.running).toBe(0);
    } finally {
      ledger.close();
    }
  });

  it("does not release a replacement owner's control lease", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-close-fenced-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const reconciler = createScanOnlyFanout(artifacts, ledger);
      const retry = vi.spyOn(ledger, "retry").mockImplementation(() => {
        throw new Error("scan settlement failed");
      });
      await expect(reconciler.cycle()).rejects.toThrow(
        "scan settlement failed",
      );
      retry.mockRestore();
      const later = Date.now() + 120_000;
      ledger.recoverExpired(later);
      const replacement = ledger.claim("replacement", later, 60_000, [
        FANOUT_CONTROL_KIND,
      ]);
      if (!replacement) throw new Error("Replacement claim missing");
      await reconciler.close();
      expect(ledger.get(replacement.work.workKey)?.leaseToken).toBe(
        replacement.leaseToken,
      );
    } finally {
      ledger.close();
    }
  });

  it("accelerates an idle durable scan and coalesces duplicate source hints", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-source-wake-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const reconciler = createScanOnlyFanout(artifacts, ledger);
    const start = Date.now() + 1_000;
    await reconciler.cycle({ maximum: 10, now: () => start });
    await succeedDetail(artifacts, ledger, start + 1_000);

    reconciler.requestSourceScan(start + 1_000);
    reconciler.requestSourceScan(start + 1_000);
    await expect(
      reconciler.cycle({ maximum: 10, now: () => start + 1_000 }),
    ).resolves.toMatchObject({ scannedDetails: 1 });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === FANOUT_DETAIL_KIND)?.total,
    ).toBe(1);

    reconciler.requestSourceScan(start + 1_001);
    await expect(
      reconciler.cycle({ maximum: 10, now: () => start + 1_001 }),
    ).resolves.toMatchObject({ scannedDetails: 0 });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === FANOUT_DETAIL_KIND)?.total,
    ).toBe(1);
    ledger.close();
  });

  it("retains a source hint that arrives while the control scan is leased", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-running-source-wake-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    let injected = false;
    const holder: { current: FanoutReconciler | null } = { current: null };
    const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    const reconciler = createScanOnlyFanout(artifacts, ledger, (boundary) => {
      if (boundary !== "scan" || injected) return;
      injected = true;
      const seeded = ledger.seed(
        {
          implementationVersion: collectorImplementationVersion(),
          input: DETAIL_INPUT,
          inputHash: inputHash(DETAIL_INPUT),
          kind: collectionWorkKinds().poemDetail,
          priority: 0,
          schemaVersion: collectorSchemaVersion(),
        },
        start,
      );
      const claim = ledger.claim("collector", start, 1_000, [
        collectionWorkKinds().poemDetail,
      ]);
      if (claim?.work.workKey !== seeded.workKey)
        throw new Error("detail claim missing");
      ledger.succeed(claim, detailArtifact.hash, start);
      holder.current?.requestSourceScan(start);
    });
    holder.current = reconciler;

    await expect(
      reconciler.cycle({ maximum: 10, now: () => start }),
    ).resolves.toMatchObject({ scannedDetails: 0 });
    await expect(
      reconciler.cycle({ maximum: 10, now: () => start }),
    ).resolves.toMatchObject({ scannedDetails: 1 });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === FANOUT_DETAIL_KIND)?.total,
    ).toBe(1);
    ledger.close();
  });

  it("recovers a missed in-process source hint through the 30-second cursor poll", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-source-wake-recovery-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const reconciler = createScanOnlyFanout(artifacts, ledger);
    const start = Date.now() + 1_000;
    await reconciler.cycle({ maximum: 10, now: () => start });
    await succeedDetail(artifacts, ledger, start + 1_000);

    await expect(
      reconciler.cycle({ maximum: 10, now: () => start + 1_000 }),
    ).resolves.toMatchObject({ scannedDetails: 0 });
    await expect(
      reconciler.cycle({ maximum: 10, now: () => start + 30_000 }),
    ).resolves.toMatchObject({ scannedDetails: 1 });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === FANOUT_DETAIL_KIND)?.total,
    ).toBe(1);
    ledger.close();
  });

  it("keeps empty reconciliation polls free of cursor artifacts", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-empty-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: { collected: () => null, enrichment: () => null },
    });

    await reconciler.cycle({ maximum: 1, now: () => 1_000 });
    await reconciler.cycle({ maximum: 1, now: () => 31_001 });
    expect(ledger.referencedArtifactHashes()).toEqual([]);
    ledger.close();
  });

  it("round-robins source kinds and lets the only active kind fill the limit", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-fair-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    const seedJob = (
      kind: string,
      lane: "detail" | "enrichment" | "sol",
      suffix: string,
    ) => {
      const input = {
        lane,
        ...(lane === "detail" ? {} : { modelKey: "sol-5.6" }),
        source: {
          artifactHash: sha256(`missing-artifact:${suffix}`),
          workKey: sha256(`source:${suffix}`),
        },
      };
      return ledger.seed(
        {
          implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
          input,
          inputHash: inputHash(input),
          kind,
          priority: 200,
          schemaVersion: FANOUT_SCHEMA_VERSION,
        },
        1,
      ).workKey;
    };
    const detailKeys = Array.from({ length: 5 }, (_, index) =>
      seedJob(FANOUT_DETAIL_KIND, "detail", `detail-${String(index)}`),
    );
    const enrichmentKey = seedJob(
      FANOUT_ENRICHMENT_KIND,
      "enrichment",
      "enrichment",
    );
    const solKey = seedJob(FANOUT_SOL_KIND, "sol", "sol");
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        collected: () => {
          throw new Error("not called");
        },
        enrichment: () => {
          throw new Error("not called");
        },
      },
    });
    let now = 1_000;
    const clock = () => {
      now += 1;
      return now;
    };

    await expect(
      reconciler.cycle({ maximum: 3, now: clock }),
    ).resolves.toMatchObject({
      retried: 3,
    });
    expect(ledger.get(solKey)?.state).toBe("retry_wait");
    expect(ledger.get(enrichmentKey)?.state).toBe("retry_wait");
    expect(
      detailKeys.filter(
        (workKey) => ledger.get(workKey)?.state === "retry_wait",
      ),
    ).toHaveLength(1);

    await expect(
      reconciler.cycle({ maximum: 3, now: clock }),
    ).resolves.toMatchObject({
      retried: 3,
    });
    expect(
      detailKeys.filter(
        (workKey) => ledger.get(workKey)?.state === "retry_wait",
      ),
    ).toHaveLength(4);
    ledger.close();
  });

  it("batches historical Sol completions within the cycle budget", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-sol-budget-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    const workKeys = Array.from({ length: 3 }, (_, index) => {
      const input = {
        lane: "sol" as const,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        source: {
          artifactHash: sha256(`missing-artifact:${String(index)}`),
          workKey: sha256(`source:${String(index)}`),
        },
      };
      return ledger.seed(
        {
          implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
          input,
          inputHash: inputHash(input),
          kind: FANOUT_SOL_KIND,
          priority: 200,
          schemaVersion: FANOUT_SCHEMA_VERSION,
        },
        1,
      ).workKey;
    });
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        collected: () => {
          throw new Error("not called");
        },
        enrichment: () => {
          throw new Error("not called");
        },
      },
    });

    await expect(
      reconciler.cycle({ maximum: 10, now: () => 1_000 }),
    ).resolves.toMatchObject({ retried: 3 });
    expect(
      workKeys.filter((workKey) => ledger.get(workKey)?.state !== "pending"),
    ).toHaveLength(3);
    ledger.close();
  });

  it("decouples bounded legacy Sol discovery from network fanout claims", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-cursor-"));
    const path = join(root, "ledger.sqlite");
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    let ledger = Ledger.initialize(path);
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-first", start);
    await succeedLegacySol(artifacts, ledger, "legacy-second", start + 3);
    let reconciler = createScanOnlyFanout(artifacts, ledger, undefined, 1);

    await reconciler.cycle({ maximum: 1, now: () => start + 10 });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(2);
    expect(legacySolFanoutDefinitions(ledger)[0]?.input).toMatchObject({
      lane: "enrichment",
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
    });
    ledger.close();

    ledger = Ledger.open(path);
    reconciler = createScanOnlyFanout(artifacts, ledger, undefined, 1);
    await reconciler.cycle({ maximum: 1, now: () => start + 40_001 });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(2);
    ledger.close();
  });

  it("reconstructs a retired terminal Sol waiter and wakes exact fanout immediately", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-retired-terminal-waiter-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    const input = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
      },
      start - 10,
    );
    const sourceClaim = ledger.claim("source", start - 9, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!sourceClaim) throw new Error("source claim missing");
    const artifactHash = sha256("retired-terminal-source");
    ledger.succeed(sourceClaim, artifactHash, start - 8);
    const fanoutInput = {
      lane: "sol" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash, workKey: source.workKey },
    };
    const fanout = ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: fanoutInput,
        inputHash: inputHash(fanoutInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      start - 7,
    );
    const fanoutClaim = ledger.claim("legacy-fanout", start - 6, 1_000, [
      FANOUT_SOL_KIND,
    ]);
    if (!fanoutClaim) throw new Error("fanout claim missing");
    ledger.operatorRelease(
      fanoutClaim,
      "FANOUT_RESOLUTION_PENDING",
      start - 5,
      start + 6 * 60 * 60_000,
    );
    const approvedSol = vi.fn(() => ({
      errorCode: "SOURCE_FINGERPRINT_TERMINAL",
      status: "conflict" as const,
    }));
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol,
        collected: () => null,
        enrichment: () => null,
      },
    });

    await expect(
      reconciler.cycle({ maximum: 1, now: () => start }),
    ).resolves.toMatchObject({ deadLettered: 1 });
    expect(ledger.get(fanout.workKey)).toMatchObject({
      lastErrorCode: "SOURCE_FINGERPRINT_TERMINAL",
      state: "dead_letter",
    });
    expect(approvedSol).toHaveBeenCalledTimes(2);
    ledger.close();
  });

  it("reconstructs a missing waiter once while preserving unresolved backoff", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-retired-waiter-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    const input = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
      },
      start - 10,
    );
    const sourceClaim = ledger.claim("source", start - 9, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!sourceClaim) throw new Error("source claim missing");
    const artifactHash = sha256("retired-waiting-source");
    ledger.succeed(sourceClaim, artifactHash, start - 8);
    const fanoutInput = {
      lane: "sol" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash, workKey: source.workKey },
    };
    const fanout = ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: fanoutInput,
        inputHash: inputHash(fanoutInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      start - 7,
    );
    const fanoutClaim = ledger.claim("legacy-fanout", start - 6, 1_000, [
      FANOUT_SOL_KIND,
    ]);
    if (!fanoutClaim) throw new Error("fanout claim missing");
    const retryAt = start + 6 * 60 * 60_000;
    ledger.operatorRelease(
      fanoutClaim,
      "FANOUT_RESOLUTION_PENDING",
      start - 5,
      retryAt,
    );
    const approvedSol = vi.fn(() => ({ status: "waiting" as const }));
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol,
        collected: () => null,
        enrichment: () => null,
      },
    });

    await reconciler.cycle({ maximum: 1, now: () => start });
    expect(ledger.get(fanout.workKey)).toMatchObject({
      availableAt: retryAt,
      lastErrorCode: "FANOUT_RESOLUTION_PENDING",
    });
    await reconciler.cycle({ maximum: 1, now: () => start + 30_001 });
    expect(approvedSol).toHaveBeenCalledOnce();
    ledger.close();
  });

  it("replays a legacy Sol seed idempotently after a pre-cursor crash", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-crash-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-crash", start);
    let crash = true;
    const reconciler = createScanOnlyFanout(artifacts, ledger, (boundary) => {
      if (boundary === "legacy-sol-backfill" && crash) {
        crash = false;
        throw new Error("synthetic pre-cursor crash");
      }
    });

    await reconciler.cycle({ maximum: 1, now: () => start + 10 });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(1);
    await reconciler.cycle({ maximum: 1, now: () => start + 40_001 });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(1);
    ledger.close();
  });

  it.each(["sol-enrichment-v1", "sol-word-gloss-v2"])(
    "backfills %s independently of a high main fanout cursor",
    async (pipelineVersion) => {
      const root = mkdtempSync(join(tmpdir(), "fanout-old-cursor-"));
      const artifacts = new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      });
      const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
      const start = Date.now() + 1_000;
      await succeedLegacySol(
        artifacts,
        ledger,
        "legacy-high-cursor",
        start,
        pipelineVersion,
      );
      createScanOnlyFanout(artifacts, ledger);
      const control = ledger.claim("cursor-fixture", start + 10, 1_000, [
        FANOUT_CONTROL_KIND,
      ]);
      if (!control) throw new Error("fanout control claim missing");
      const cursor = {
        detail: 1_000_000,
        enrichment: { [ENRICHMENT_PROVIDER_SPECS.sol.modelKey]: 1_000_000 },
        materialBackfill: {
          complete: true,
          detailCursor: null,
          metadataRevision: 0,
          solCursor: null,
        },
        sol: 1_000_000,
      };
      const artifact = await artifacts.put(`${canonicalJson(cursor)}\n`);
      ledger.checkpoint(
        control,
        { artifactHash: artifact.hash, kind: "fanout-cursor", payload: cursor },
        start + 11,
      );
      ledger.operatorRelease(control, "FANOUT_POLL", start + 12, start + 12);

      await expect(
        createScanOnlyFanout(artifacts, ledger).cycle({
          maximum: 1,
          now: () => start + 12,
        }),
      ).resolves.toMatchObject({ scannedEnrichments: 1, scannedSol: 0 });
      expect(legacySolFanoutDefinitions(ledger)).toHaveLength(1);
      ledger.close();
    },
  );

  it("discovers v2 completions arriving after its first empty compatibility scan", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-late-v2-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    const reconciler = createScanOnlyFanout(artifacts, ledger);
    await reconciler.cycle({ maximum: 1, now: () => start });
    await succeedLegacySol(
      artifacts,
      ledger,
      "late-v2",
      start + 1,
      "sol-word-gloss-v2",
    );
    await reconciler.cycle({ maximum: 1, now: () => start + 30_001 });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(1);
    ledger.close();
  });

  it("ignores the obsolete completed v1 backfill cursor", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-v2-cursor-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-v1-missed", start);
    createScanOnlyFanout(artifacts, ledger);
    const control = ledger.claim("legacy-v1-cursor", start + 10, 1_000, [
      FANOUT_CONTROL_KIND,
    ]);
    if (!control) throw new Error("fanout control claim missing");
    const obsolete = { complete: true, eventCursor: 1_000_000 };
    const cursorArtifact = await artifacts.put(`${canonicalJson(obsolete)}\n`);
    ledger.checkpoint(
      control,
      {
        artifactHash: cursorArtifact.hash,
        kind: "fanout-legacy-sol-backfill-v1",
        payload: obsolete,
      },
      start + 11,
    );
    ledger.operatorRelease(control, "FANOUT_POLL", start + 12, start + 12);

    await createScanOnlyFanout(artifacts, ledger).cycle({
      maximum: 1,
      now: () => start + 12,
    });
    expect(legacySolFanoutDefinitions(ledger)).toHaveLength(1);
    ledger.close();
  });

  it("publishes an eligible legacy Sol fallback through its rebound canonical poem", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-publish-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-publish", start);
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        collected: () => null,
        enrichment: (_source, input) => ({
          expectedPointerVersion: 1,
          poemId: bindingFor(input).poemId,
          sourceRevisionId: input.sourceRevisionId,
          writerEpoch: 1,
        }),
      },
    });

    await expect(
      reconciler.cycle({ maximum: 10, now: () => start + 10 }),
    ).resolves.toMatchObject({ enrichmentPublicationsSeeded: 1 });
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === ENRICHMENT_PUBLICATION_WORK_KIND,
        )?.total,
    ).toBe(1);
    const publicationWork = ledger.listWorkDefinitionsAfter(
      null,
      ENRICHMENT_PUBLICATION_WORK_KIND,
      1,
      {
        implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: PUBLICATION_SCHEMA_VERSION,
      },
    ).items[0];
    if (!publicationWork) throw new Error("legacy publication work missing");
    expect(publicationWork.input).toMatchObject({
      lane: "enrichment",
    });
    const actionHash = (
      publicationWork.input as { actionArtifactHash?: string }
    ).actionArtifactHash;
    if (!actionHash) throw new Error("legacy publication action missing");
    const actionContents = await artifacts.read(actionHash);
    const action: unknown = JSON.parse(actionContents.toString("utf8"));
    expect(action).toMatchObject({
      input: { publication: { poemId: MAPPING.poemId } },
    });
    expect(
      ledger.status().kindProgress.find(({ kind }) => kind === FANOUT_SOL_KIND),
    ).toBeUndefined();
    ledger.close();
  });

  it("terminalizes unsupported historical Sol work without generic publication", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-approved-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-approved", start);
    const approvedSol = vi.fn((_source, input: PoemEnrichmentInput) => ({
      binding: bindingFor(input),
      status: "resolved" as const,
    }));
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: new PublicationLane({
        artifacts,
        client: new PublicationClient({
          endpoint: "https://example.test/api/corpus-import",
          transport: async () => {
            throw new Error("not called");
          },
        }),
        ledger,
      }),
      resolvers: {
        approvedSol,
        collected: () => null,
        enrichment: () => {
          throw new Error("generic publication must not be resolved");
        },
      },
    });
    const result = await reconciler.cycle({
      maximum: 10,
      now: () => start + 10,
    });
    expect(
      legacySolFanoutDefinitions(ledger).map((work) => ({
        state: work.state,
        error: ledger.get(work.workKey)?.lastErrorCode,
      })),
    ).toEqual([
      {
        state: "dead_letter",
        error: "LEGACY_GENERIC_PUBLICATION_RETIRED",
      },
    ]);
    expect(result).toMatchObject({
      deadLettered: 1,
      enrichmentPublicationsSeeded: 0,
    });
    expect(approvedSol).toHaveBeenCalledOnce();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === DIRECT_ENRICHMENT_PUBLICATION_KIND,
        ),
    ).toBeUndefined();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === ENRICHMENT_PUBLICATION_WORK_KIND,
        ),
    ).toBeUndefined();
    ledger.close();
  });

  it("supersedes a rejected legacy publication checkpoint after canonical rebinding", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-legacy-sol-rebind-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const start = Date.now() + 1_000;
    await succeedLegacySol(artifacts, ledger, "legacy-rebind", start);
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    let canonical = false;
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        collected: () => null,
        enrichment: (_source, input) => ({
          expectedPointerVersion: 1,
          poemId: canonical ? bindingFor(input).poemId : input.poemId,
          sourceRevisionId: input.sourceRevisionId,
          writerEpoch: 1,
        }),
      },
    });

    await reconciler.cycle({ maximum: 10, now: () => start + 10 });
    const first = ledger.listWorkDefinitionsAfter(
      null,
      ENRICHMENT_PUBLICATION_WORK_KIND,
      10,
      {
        implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: PUBLICATION_SCHEMA_VERSION,
      },
    ).items[0];
    if (!first) throw new Error("legacy publication work missing");
    const firstClaim = ledger.claim("publication", start + 20, 1_000, [
      ENRICHMENT_PUBLICATION_WORK_KIND,
    ]);
    if (firstClaim?.work.workKey !== first.workKey)
      throw new Error("legacy publication claim missing");
    ledger.deadLetter(firstClaim, "CORPUS_IMPORT_REJECTED", start + 21);

    const fanout = legacySolFanoutDefinitions(ledger)[0];
    if (!fanout) throw new Error("legacy fanout missing");
    expect(
      ledger.reopenRejectedLegacySolFanout([fanout.workKey], start + 22),
    ).toBe(true);
    canonical = true;
    await reconciler.cycle({ maximum: 10, now: () => start + 30 });

    const publications = ledger.listWorkDefinitionsAfter(
      null,
      ENRICHMENT_PUBLICATION_WORK_KIND,
      10,
      {
        implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: PUBLICATION_SCHEMA_VERSION,
      },
    ).items;
    expect(publications).toHaveLength(2);
    const replacement = publications.find(
      ({ workKey }) => workKey !== first.workKey,
    );
    if (!replacement) throw new Error("replacement publication missing");
    expect(replacement.state).toBe("pending");
    const actionArtifactHash = (
      replacement.input as { actionArtifactHash?: string }
    ).actionArtifactHash;
    if (!actionArtifactHash) throw new Error("replacement action missing");
    const actionContents = await artifacts.read(actionArtifactHash);
    const action: unknown = JSON.parse(actionContents.toString("utf8"));
    expect(action).toMatchObject({
      input: { publication: { poemId: MAPPING.poemId } },
    });
    ledger.close();
  });

  it("renews queued batch leases across slow mixed resolution outcomes", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-batch-leases-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const baseInput = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const fanoutKeys = Array.from({ length: 3 }, (_, index) => {
      const input = { ...baseInput, poemId: `poem-${String(index)}` };
      const source = ledger.seed(
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          input,
          inputHash: inputHash(input),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: 200,
          schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
        },
        1,
      );
      const sourceClaim = ledger.claim(`source-${String(index)}`, 1, 1_000, [
        SOL_ENRICHMENT_WORK_KIND,
      ]);
      if (!sourceClaim) throw new Error("source claim missing");
      const artifactHash = sha256(`approved-sol:${String(index)}`);
      ledger.succeed(sourceClaim, artifactHash, 2);
      const fanoutInput = {
        lane: "sol" as const,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        source: { artifactHash, workKey: source.workKey },
      };
      return ledger.seed(
        {
          implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
          input: fanoutInput,
          inputHash: inputHash(fanoutInput),
          kind: FANOUT_SOL_KIND,
          priority: 200,
          schemaVersion: FANOUT_SCHEMA_VERSION,
        },
        3,
      ).workKey;
    });
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    let now = 1_000;
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        approvedSol: (_source, input) => {
          now += 45_000;
          return input.poemId === "poem-1"
            ? {
                errorCode: "SOURCE_BINDING_CONTENT_MISMATCH",
                status: "conflict" as const,
              }
            : { status: "waiting" as const };
        },
        collected: () => null,
        enrichment: () => null,
      },
    });

    await expect(
      reconciler.cycle({ maximum: 3, now: () => now }),
    ).resolves.toMatchObject({ deadLettered: 1, pendingResolution: 2 });
    const states = fanoutKeys.map((workKey) => ledger.get(workKey)?.state);
    expect(states.filter((state) => state === "dead_letter")).toHaveLength(1);
    expect(states.filter((state) => state === "pending")).toHaveLength(2);
    expect(
      fanoutKeys.some((workKey) => ledger.get(workKey)?.state === "running"),
    ).toBe(false);
    ledger.close();
  });

  it("heartbeats every queued claim during one operation longer than a lease", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const root = mkdtempSync(join(tmpdir(), "fanout-batch-heartbeat-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    try {
      const baseInput = prepareCollectedPoem(
        DETAIL,
        MAPPING,
        "2026-08-25T12:00:00.000Z",
        1,
      ).enrichmentInput;
      const fanoutKeys = Array.from({ length: 2 }, (_, index) => {
        const input = { ...baseInput, poemId: `slow-poem-${String(index)}` };
        const source = ledger.seed(
          {
            implementationVersion: SOL_PIPELINE_VERSION,
            input,
            inputHash: inputHash(input),
            kind: SOL_ENRICHMENT_WORK_KIND,
            priority: 200,
            schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
          },
          1,
        );
        const sourceClaim = ledger.claim(
          `slow-source-${String(index)}`,
          1,
          1_000,
          [SOL_ENRICHMENT_WORK_KIND],
        );
        if (!sourceClaim) throw new Error("source claim missing");
        const artifactHash = sha256(`slow-sol:${String(index)}`);
        ledger.succeed(sourceClaim, artifactHash, 2);
        const fanoutInput = {
          lane: "sol" as const,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          source: { artifactHash, workKey: source.workKey },
        };
        return ledger.seed(
          {
            implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
            input: fanoutInput,
            inputHash: inputHash(fanoutInput),
            kind: FANOUT_SOL_KIND,
            priority: 200,
            schemaVersion: FANOUT_SCHEMA_VERSION,
          },
          3,
        ).workKey;
      });
      const publication = new PublicationLane({
        artifacts,
        client: new PublicationClient({
          endpoint: "https://example.test/api/corpus-import",
          transport: async () => {
            throw new Error("not called");
          },
        }),
        ledger,
      });
      let resolution = 0;
      const reconciler = new FanoutReconciler({
        artifacts,
        enrichment: [
          {
            implementationVersion: SOL_PIPELINE_VERSION,
            modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            seed: () => {
              throw new Error("not called");
            },
            workKind: SOL_ENRICHMENT_WORK_KIND,
          },
        ],
        ledger,
        publication,
        resolvers: {
          approvedSol: async () => {
            resolution += 1;
            if (resolution === 1)
              await new Promise((resolve) => setTimeout(resolve, 65_000));
            return { status: "waiting" as const };
          },
          collected: () => null,
          enrichment: () => null,
        },
      });

      const cycle = reconciler.cycle({ maximum: 2, now: Date.now });
      await vi.advanceTimersByTimeAsync(65_001);
      await vi.advanceTimersByTimeAsync(20_000);
      await expect(cycle).resolves.toMatchObject({ pendingResolution: 2 });
      expect(
        fanoutKeys.every((workKey) => ledger.get(workKey)?.state === "pending"),
      ).toBe(true);
    } finally {
      ledger.close();
      vi.useRealTimers();
    }
  });

  it("recovers the same fenced claim when host sleep outlives the heartbeat lease", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-host-suspend-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const input = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
      },
      1,
    );
    const sourceClaim = ledger.claim("source", 1, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!sourceClaim) throw new Error("source claim missing");
    const artifactHash = sha256("suspended-sol");
    ledger.succeed(sourceClaim, artifactHash, 2);
    const fanoutInput = {
      lane: "sol" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash, workKey: source.workKey },
    };
    const fanout = ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: fanoutInput,
        inputHash: inputHash(fanoutInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      3,
    );
    let now = 1_000;
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: new PublicationLane({
        artifacts,
        client: new PublicationClient({
          endpoint: "https://example.test/api/corpus-import",
          transport: async () => {
            throw new Error("not called");
          },
        }),
        ledger,
      }),
      resolvers: {
        approvedSol: async () => {
          // Wall time advances while the JS heartbeat does not run.
          now += 991_000;
          return { status: "waiting" as const };
        },
        collected: () => null,
        enrichment: () => null,
      },
    });

    await expect(
      reconciler.cycle({ maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ pendingResolution: 1 });
    expect(ledger.get(fanout.workKey)).toMatchObject({
      lastErrorCode: "FANOUT_RESOLUTION_PENDING",
      state: "pending",
    });
    ledger.close();
  });

  it("claims an already-ready Sol completion before scanning history", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-sol-before-scan-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const input = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
      },
      1,
    );
    const sourceClaim = ledger.claim("sol", 1, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!sourceClaim) throw new Error("sol source claim missing");
    const artifactHash = sha256("approved-sol-artifact");
    ledger.succeed(sourceClaim, artifactHash, 2);
    const fanoutInput = {
      lane: "sol" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash, workKey: source.workKey },
    };
    ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: fanoutInput,
        inputHash: inputHash(fanoutInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      3,
    );
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger,
    });
    const boundaries: string[] = [];
    const reconciler = new FanoutReconciler({
      afterBoundary: (boundary) => {
        if (boundary === "scan") boundaries.push(boundary);
      },
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication,
      resolvers: {
        approvedSol: () => {
          boundaries.push("sol");
          return { status: "waiting" };
        },
        collected: () => null,
        enrichment: () => null,
      },
    });

    const now = Date.now() + 1_000;
    await expect(
      reconciler.cycle({ maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ pendingResolution: 1 });
    expect(boundaries).toEqual(["sol", "scan"]);
    ledger.close();
  });

  it("claims exact resolution wakeups ahead of older ready fanout without starving ordinary work", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-exact-resolution-wake-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const databasePath = join(root, "ledger.sqlite");
    const ledger = Ledger.initialize(databasePath);
    const fanoutKeys: string[] = [];
    const poemIds: string[] = [];
    for (let index = 0; index < 6; index += 1) {
      const input = {
        ...prepareCollectedPoem(
          DETAIL,
          { ...MAPPING, poemId: `poem-${String(index)}` },
          "2026-08-25T12:00:00.000Z",
          1,
        ).enrichmentInput,
        poemId: `legacy-poem-${String(index)}`,
      };
      poemIds.push(input.poemId);
      const source = ledger.seed(
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          input,
          inputHash: inputHash(input),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: 200,
          schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
        },
        index + 1,
      );
      const sourceClaim = ledger.claim(
        `sol-${String(index)}`,
        index + 1,
        1_000,
        [SOL_ENRICHMENT_WORK_KIND],
      );
      if (sourceClaim?.work.workKey !== source.workKey)
        throw new Error("sol source claim missing");
      const artifactHash = sha256(`approved-sol-artifact-${String(index)}`);
      ledger.succeed(sourceClaim, artifactHash, index + 1);
      const fanoutInput = {
        lane: "sol" as const,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        source: { artifactHash, workKey: source.workKey },
      };
      fanoutKeys.push(
        ledger.seed(
          {
            implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
            input: fanoutInput,
            inputHash: inputHash(fanoutInput),
            kind: FANOUT_SOL_KIND,
            priority: 200,
            schemaVersion: FANOUT_SCHEMA_VERSION,
          },
          index + 1,
        ).workKey,
      );
    }
    expect(
      ledger.wakeResolutionPending(
        [fanoutKeys.at(-1)!],
        [FANOUT_SOL_KIND],
        100,
      ),
    ).toEqual({ acknowledge: [fanoutKeys.at(-1)!], retry: [] });
    ledger.close();

    // The cache outbox may be acknowledged before the fanout process exits.
    // The exact scheduling hint must therefore survive a fresh connection.
    const restartedLedger = Ledger.open(databasePath);
    expect(
      restartedLedger.listFanoutPriorityWorkKeys([FANOUT_SOL_KIND], 10, 10_000),
    ).toEqual([fanoutKeys.at(-1)]);
    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called");
        },
      }),
      ledger: restartedLedger,
    });
    const observed: string[] = [];
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger: restartedLedger,
      publication,
      resolvers: {
        approvedSol: (_source, input) => {
          observed.push(input.poemId);
          return { status: "waiting" };
        },
        collected: () => null,
        enrichment: () => null,
      },
    });
    await expect(
      reconciler.cycle({ maximum: 2, now: () => 10_000 }),
    ).resolves.toMatchObject({ pendingResolution: 2 });
    expect(observed).toEqual([poemIds.at(-1), poemIds[0]]);
    // A still-unresolved retry is deferred, not hot-looped, but retains its
    // durable exact priority for the next eligible attempt.
    expect(
      restartedLedger.acknowledgeFanoutPriorityWork([fanoutKeys.at(-1)!]),
    ).toBe(1);
    restartedLedger.close();
  });

  it("bounds priority detail admissions so local enrichment replay keeps cycle capacity", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-detail-budget-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const detailFanoutKeys: string[] = [];
    for (let index = 0; index < 9; index += 1) {
      const numericId = String(index + 1);
      const detailInput = {
        authorHref: DETAIL_INPUT.authorHref,
        poemHref: `https://source.invalid/works/${numericId}`,
      };
      const definition = {
        implementationVersion: collectorImplementationVersion(),
        input: detailInput,
        inputHash: inputHash(detailInput),
        kind: collectionWorkKinds().poemDetail,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      };
      const detailSource = {
        ...DETAIL_SOURCE,
        canonicalId: `source:poem:${numericId}`,
        href: detailInput.poemHref,
        numericId,
        slug: `work-${numericId}`,
      };
      const artifact = await artifacts.put(
        `${canonicalJson({
          ...DETAIL,
          source: detailSource,
          sourceHash: sha256(canonicalJson(detailSource)),
          workKey: calculateWorkKey(definition),
        })}\n`,
      );
      const source = ledger.seed(definition, index + 1);
      const sourceClaim = ledger.claim(
        `detail-${String(index)}`,
        index + 1,
        1_000,
        [collectionWorkKinds().poemDetail],
      );
      if (sourceClaim?.work.workKey !== source.workKey)
        throw new Error("detail source claim missing");
      ledger.succeed(sourceClaim, artifact.hash, index + 1);
      const fanoutInput = {
        lane: "detail" as const,
        source: { artifactHash: artifact.hash, workKey: source.workKey },
      };
      detailFanoutKeys.push(
        ledger.seed(
          {
            implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
            input: fanoutInput,
            inputHash: inputHash(fanoutInput),
            kind: FANOUT_DETAIL_KIND,
            priority: 1_000,
            schemaVersion: FANOUT_SCHEMA_VERSION,
          },
          100 + index,
        ).workKey,
      );
    }
    ledger.prioritizeFanoutWork(detailFanoutKeys, [FANOUT_DETAIL_KIND], 200);

    const enrichmentInput: PoemEnrichmentInput = {
      authorArabic: "شاعر",
      linesArabic: ["صدر", "عجز"],
      poemId: "legacy-replay",
      schemaId: "saqi.poem-enrichment-input",
      schemaVersion: 1,
      sourceContentSha256: sha256(canonicalJson(["صدر", "عجز"])),
      sourceRevisionId: sha256("legacy-replay-revision"),
      titleArabic: "قصيدة",
    };
    const source = ledger.seed(
      {
        implementationVersion: "sol-enrichment-v1",
        input: enrichmentInput,
        inputHash: inputHash(enrichmentInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${enrichmentInput.schemaId}@${String(enrichmentInput.schemaVersion)}`,
      },
      300,
    );
    const sourceClaim = ledger.claim("legacy", 300, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (sourceClaim?.work.workKey !== source.workKey)
      throw new Error("legacy source claim missing");
    const sourceArtifactHash = sha256("legacy-replay-artifact");
    ledger.succeed(sourceClaim, sourceArtifactHash, 300);
    const enrichmentFanoutInput = {
      lane: "enrichment" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash: sourceArtifactHash, workKey: source.workKey },
    };
    ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: enrichmentFanoutInput,
        inputHash: inputHash(enrichmentFanoutInput),
        kind: FANOUT_ENRICHMENT_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      301,
    );

    const admitSource = vi.fn(async () => ({
      errorCode: "SOURCE_ADMISSION_WAIT",
      retryAt: 10_000,
      state: "retry_wait" as const,
    }));
    const enrichment = vi.fn(() => null);
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource,
        seedCollection: () => Promise.resolve([]),
        seedEnrichment: () => {
          throw new Error("not called");
        },
      },
      resolvers: { collected: () => null, enrichment },
    });

    await reconciler.cycle({ maximum: 10, now: () => 1_000 });

    expect(admitSource).toHaveBeenCalledTimes(5);
    expect(enrichment).toHaveBeenCalledOnce();
    ledger.close();
  });

  it("releases the rest of a claimed local batch when shutdown is requested", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-shutdown-release-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const fanoutWorkKeys: string[] = [];
    for (let index = 0; index < 2; index += 1) {
      const enrichmentInput: PoemEnrichmentInput = {
        authorArabic: "شاعر",
        linesArabic: ["صدر", "عجز"],
        poemId: `shutdown-${String(index)}`,
        schemaId: "saqi.poem-enrichment-input",
        schemaVersion: 1,
        sourceContentSha256: sha256(canonicalJson(["صدر", "عجز"])),
        sourceRevisionId: sha256(`shutdown-revision-${String(index)}`),
        titleArabic: "قصيدة",
      };
      const source = ledger.seed(
        {
          implementationVersion: "sol-enrichment-v1",
          input: enrichmentInput,
          inputHash: inputHash(enrichmentInput),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: 200,
          schemaVersion: `${enrichmentInput.schemaId}@${String(enrichmentInput.schemaVersion)}`,
        },
        index + 1,
      );
      const sourceClaim = ledger.claim(
        `shutdown-source-${String(index)}`,
        index + 1,
        1_000,
        [SOL_ENRICHMENT_WORK_KIND],
      );
      if (sourceClaim?.work.workKey !== source.workKey)
        throw new Error("shutdown source claim missing");
      const artifactHash = sha256(`shutdown-artifact-${String(index)}`);
      ledger.succeed(sourceClaim, artifactHash, index + 1);
      const fanoutInput = {
        lane: "enrichment" as const,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        source: { artifactHash, workKey: source.workKey },
      };
      fanoutWorkKeys.push(
        ledger.seed(
          {
            implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
            input: fanoutInput,
            inputHash: inputHash(fanoutInput),
            kind: FANOUT_ENRICHMENT_KIND,
            priority: 200,
            schemaVersion: FANOUT_SCHEMA_VERSION,
          },
          10 + index,
        ).workKey,
      );
    }
    const controller = new AbortController();
    const enrichment = vi.fn(() => {
      controller.abort();
      return null;
    });
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: () => {
          throw new Error("not called");
        },
        seedCollection: () => Promise.resolve([]),
        seedEnrichment: () => {
          throw new Error("not called");
        },
      },
      resolvers: { collected: () => null, enrichment },
    });

    await reconciler.cycle({
      maximum: 2,
      now: () => 1_000,
      signal: controller.signal,
    });

    expect(enrichment).toHaveBeenCalledOnce();
    expect(
      new Set(
        fanoutWorkKeys.map((workKey) => ledger.get(workKey)?.lastErrorCode),
      ),
    ).toEqual(new Set(["FANOUT_RESOLUTION_PENDING", "FANOUT_SHUTDOWN"]));
    expect(fanoutWorkKeys.map((workKey) => ledger.get(workKey)?.state)).toEqual(
      ["pending", "pending"],
    );
    ledger.close();
  });

  it(
    "does not exhaust attempts while source admission is transiently unavailable",
    { timeout: 20_000 },
    async () => {
      const root = mkdtempSync(join(tmpdir(), "fanout-pending-"));
      const artifacts = new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      });
      const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
      const stored = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
      const input = {
        authorHref: "https://source.invalid/writers/a",
        poemHref: "https://source.invalid/works/42",
      };
      const seeded = ledger.seed(
        {
          implementationVersion: collectorImplementationVersion(),
          input,
          inputHash: inputHash(input),
          kind: collectionWorkKinds().poemDetail,
          priority: 0,
          schemaVersion: collectorSchemaVersion(),
        },
        1,
      );
      const sourceClaim = ledger.claim("collector", 1, 1_000, [
        collectionWorkKinds().poemDetail,
      ]);
      if (!sourceClaim) throw new Error("detail claim missing");
      ledger.succeed(sourceClaim, stored.hash, 2);
      const publication = new PublicationLane({
        artifacts,
        client: new PublicationClient({
          endpoint: "https://example.test/api/corpus-import",
          transport: async () => {
            throw new Error("not called");
          },
        }),
        ledger,
      });
      const reconciler = new FanoutReconciler({
        artifacts,
        batchSize: 1,
        ledger,
        publication,
        resolvers: { collected: () => null, enrichment: () => null },
        enrichment: [
          {
            implementationVersion: SOL_PIPELINE_VERSION,
            modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            seed: () => {
              throw new Error("not called");
            },
            workKind: SOL_ENRICHMENT_WORK_KIND,
          },
        ],
      });
      let now = Date.now() + 1_000;
      for (let attempt = 0; attempt < 101; attempt += 1) {
        await reconciler.cycle({ maximum: 1, now: () => now });
        now += 5 * 60_000 + 1;
      }
      const source = ledger.get(seeded.workKey);
      expect(source?.state).toBe("succeeded");
      const pending = ledger
        .status()
        .kindProgress.find(({ kind }) => kind === "fanout-collected-detail");
      expect(pending?.byState).toMatchObject({ dead_letter: 0, retry_wait: 1 });
      const job = ledger.claim("inspection", now, 1_000, [
        "fanout-collected-detail",
      ]);
      expect(job?.work.attemptCount).toBe(102);
      expect(job?.work.lastErrorCode).toBe(
        "PUBLICATION_TRANSPORT_OUTCOME_UNKNOWN",
      );
      ledger.close();
    },
  );

  it("preserves a priority-1000 detail source through fanout seeding", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-detail-priority-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    const input = {
      authorHref: DETAIL.source.author.href,
      poemHref: DETAIL.source.href,
    };
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input,
        inputHash: inputHash(input),
        kind: collectionWorkKinds().poemDetail,
        priority: 1_000,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const claim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!claim) throw new Error("detail claim missing");
    ledger.succeed(claim, artifact.hash, 2);
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol: () => ({ status: "waiting" }),
        collected: () => null,
        enrichment: () => null,
      },
    });
    await reconciler.cycle({ maximum: 1, now: () => Date.now() + 1_000 });
    expect(
      ledger.listWorkDefinitionsAfter(null, FANOUT_DETAIL_KIND, 10, {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      }).items[0]?.priority,
    ).toBe(1_000);
    ledger.close();
  });

  it("wakes exact translated detail admissions and preserves concrete failures", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-translated-detail-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const detailWithContext = {
      ...DETAIL,
      sourceContext: {
        authorNameArabic: MAPPING.authorNameArabic,
        refreshGeneration: "fixture-generation",
      },
    };
    const detailArtifact = await artifacts.put(
      `${canonicalJson(detailWithContext)}\n`,
    );
    const detailInput = {
      authorHref: DETAIL.source.author.href,
      poemHref: DETAIL.source.href,
    };
    ledger.recordSourceAuthorMetadata(
      DETAIL.source.author.href,
      "اسم قديم لا يطابق",
      "fixture-generation",
      1,
    );
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: detailInput,
        inputHash: inputHash(detailInput),
        kind: collectionWorkKinds().poemDetail,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const detailClaim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!detailClaim) throw new Error("detail claim missing");
    ledger.succeed(detailClaim, detailArtifact.hash, 2);

    const enrichmentInput = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    ).enrichmentInput;
    const solArtifact = await artifacts.put("{}\n");
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: enrichmentInput,
        inputHash: inputHash(enrichmentInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${enrichmentInput.schemaId}@${String(enrichmentInput.schemaVersion)}`,
      },
      3,
    );
    const solClaim = ledger.claim("sol", 3, 1_000, [SOL_ENRICHMENT_WORK_KIND]);
    if (!solClaim) throw new Error("Sol claim missing");
    ledger.succeed(solClaim, solArtifact.hash, 4);

    const start = Date.now() + 1_000;
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => ({
          errorCode: "PUBLICATION_RATE_LIMITED",
          retryAt: start + 4_000_000,
          state: "retry_wait" as const,
        }),
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol: () => ({ status: "waiting" }),
        collected: () => null,
        enrichment: () => null,
      },
    });
    const seededCycle = await reconciler.cycle({
      maximum: 1,
      now: () => start,
    });
    expect(seededCycle.translatedDetailsPrioritized).toBe(1);
    const detailFanout = ledger.listWorkDefinitionsAfter(
      null,
      FANOUT_DETAIL_KIND,
      10,
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
    ).items[0];
    if (!detailFanout) throw new Error("detail fanout missing");
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: MAPPING.authorNameArabic,
        linesArabic: DETAIL.source.lines,
        titleArabic: DETAIL.source.title,
      }),
    );
    const lineNfcHash = sha256(sourceLineNfcHashBody(DETAIL.source.lines));
    expect(
      ledger.indexFanoutDetailMaterial({
        detailFanoutWorkKey: detailFanout.workKey,
        lineNfcHash,
        now: start,
        promptMaterialHash,
        sourceWorkKey: detailClaim.work.workKey,
      }),
    ).toBe(false);
    expect(() =>
      ledger.indexFanoutDetailMaterial({
        detailFanoutWorkKey: detailFanout.workKey,
        lineNfcHash,
        now: start,
        promptMaterialHash: sha256("collision"),
        sourceWorkKey: detailClaim.work.workKey,
      }),
    ).toThrow("FANOUT_DETAIL_MATERIAL_COLLISION");
    const stranded = ledger.claim("stranding-fixture", start + 1, 1_000, [
      FANOUT_DETAIL_KIND,
    ]);
    if (!stranded) throw new Error("detail fanout claim missing");
    ledger.retry(
      stranded,
      "FANOUT_EXCEPTION",
      start + 24 * 60 * 60_000,
      start + 2,
    );

    const restarted = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => ({
          errorCode: "PUBLICATION_RATE_LIMITED",
          retryAt: start + 26 * 60 * 60_000,
          state: "retry_wait" as const,
        }),
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol: () => ({ status: "waiting" }),
        collected: () => null,
        enrichment: () => null,
      },
    });
    const result = await restarted.cycle({
      maximum: 1,
      now: () => start + 7 * 60 * 60_000,
    });
    expect(result.translatedDetailsPrioritized).toBe(1);
    expect(ledger.get(detailFanout.workKey)).toMatchObject({
      lastErrorCode: "FANOUT_EXCEPTION",
      priority: 1_000,
      state: "retry_wait",
    });
    await restarted.cycle({
      maximum: 1,
      now: () => start + 7 * 60 * 60_000 + 1,
    });
    expect(ledger.get(detailFanout.workKey)).toMatchObject({
      lastErrorCode: "PUBLICATION_RATE_LIMITED",
      priority: 1_000,
      state: "retry_wait",
    });
    ledger.close();
  });

  it("reindexes a completed detail when durable author metadata arrives later", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-metadata-revision-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: DETAIL_INPUT,
        inputHash: inputHash(DETAIL_INPUT),
        kind: collectionWorkKinds().poemDetail,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const claim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!claim) throw new Error("detail claim missing");
    ledger.succeed(claim, artifact.hash, 2);
    const reconciler = new FanoutReconciler({
      artifacts,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol: () => ({ status: "waiting" }),
        collected: () => null,
        enrichment: () => null,
      },
    });
    const startedAt = Date.now() + 1_000;
    await reconciler.cycle({ maximum: 1, now: () => startedAt });
    const promptMaterialHash = sha256(
      sourcePromptMaterialHashBody({
        authorArabic: MAPPING.authorNameArabic,
        linesArabic: DETAIL.source.lines,
        titleArabic: DETAIL.source.title,
      }),
    );
    expect(ledger.fanoutDetailWorkKeysForMaterial(promptMaterialHash)).toEqual(
      [],
    );
    ledger.recordSourceAuthorMetadata(
      DETAIL.source.author.href,
      MAPPING.authorNameArabic,
      "recovery-generation",
      startedAt + 1,
    );
    await reconciler.cycle({
      maximum: 1,
      now: () => startedAt + 30_001,
    });
    expect(
      ledger.fanoutDetailWorkKeysForMaterial(promptMaterialHash),
    ).toHaveLength(1);
    ledger.recordSourceAuthorMetadata(
      DETAIL.source.author.href,
      "شاعر مصحح",
      "corrected-generation",
      startedAt + 30_002,
    );
    await expect(
      reconciler.cycle({
        maximum: 1,
        now: () => startedAt + 60_002,
      }),
    ).resolves.toBeDefined();
    expect(
      ledger.fanoutDetailWorkKeysForMaterial(promptMaterialHash),
    ).toHaveLength(1);
    expect(
      ledger.fanoutDetailWorkKeysForMaterial(
        sha256(
          sourcePromptMaterialHashBody({
            authorArabic: "شاعر مصحح",
            linesArabic: DETAIL.source.lines,
            titleArabic: DETAIL.source.title,
          }),
        ),
      ),
    ).toEqual([]);
    ledger.close();
  });

  it("reuses an exact completed Sol artifact without a paid seed dependency", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-sol-reuse-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    ledger.recordSourceAuthorMetadata(
      DETAIL.source.author.href,
      MAPPING.authorNameArabic,
      "fixture-generation",
      1,
    );
    const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    const detailInput = {
      authorHref: DETAIL.source.author.href,
      poemHref: DETAIL.source.href,
    };
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: detailInput,
        inputHash: inputHash(detailInput),
        kind: collectionWorkKinds().poemDetail,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const detailClaim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!detailClaim) throw new Error("detail claim missing");
    ledger.succeed(detailClaim, detailArtifact.hash, 2);
    const prepared = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const reusableOutput = {
      schemaId: "saqi.poem-enrichment-output",
      schemaVersion: 2,
      translation: { lines: ["first", "second"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            segments: [
              {
                kind: "word",
                meaning: "opening hemistich",
                surface: "صدر",
                tokenIndex: 0,
              },
            ],
          },
        ],
        tokenizerVersion: "saqi-orthographic-v1",
      },
    };
    const legacyArtifact = await artifacts.put(
      `${canonicalJson({
        generationAttemptId: "legacy-generation",
        input: prepared.enrichmentInput,
        model: ENRICHMENT_PROVIDER_SPECS.sol.model,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        output: reusableOutput,
        outputHash: sha256(canonicalJson(reusableOutput)),
        pipelineVersion: SOL_PIPELINE_VERSION,
        provider: "sol",
        reasoningEffort: ENRICHMENT_PROVIDER_SPECS.sol.reasoningEffort,
        reviewAttemptIds: ["legacy-r1", "legacy-r2"],
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
      })}\n`,
    );
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: prepared.enrichmentInput,
        inputHash: inputHash(prepared.enrichmentInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${prepared.enrichmentInput.schemaId}@1`,
      },
      3,
    );
    const legacyClaim = ledger.claim("sol", 3, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!legacyClaim) throw new Error("legacy Sol claim missing");
    ledger.succeed(legacyClaim, legacyArtifact.hash, 4);
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol: () => ({ status: "waiting" }),
        collected: () => ({
          binding: bindingFor(prepared.enrichmentInput),
          mapping: MAPPING,
          observedAt: "2026-08-25T12:00:00.000Z",
          writerEpoch: 1,
        }),
        enrichment: () => null,
      },
    });
    const start = Date.now() + 1_000;
    const first = await reconciler.cycle({ maximum: 2, now: () => start });
    expect(first).toMatchObject({ pendingResolution: 1, solSeeded: 0 });
    await reconciler.cycle({ maximum: 2, now: () => start + 1 });

    const solFanout = ledger.listWorkDefinitionsAfter(
      null,
      FANOUT_SOL_KIND,
      10,
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
    ).items[0];
    if (!solFanout) throw new Error("Sol fanout missing");
    const reusableClaim = ledger.claim(
      "reuse-publication",
      start + 6 * 60 * 60_000 + 1,
      1_000,
      [FANOUT_SOL_KIND],
    );
    if (!reusableClaim) throw new Error("reusable fanout claim missing");
    const receipt = await artifacts.put("reuse-complete\n");
    ledger.succeed(reusableClaim, receipt.hash, start + 6 * 60 * 60_000 + 2);
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === FANOUT_DETAIL_KIND)?.byState
        .succeeded,
    ).toBe(1);
    ledger.close();
  });

  it("replays an attached Sol publication and wakes its exact sibling fanout", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-sol-attached-sibling-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const prepared = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const output = {
      schemaId: "saqi.poem-enrichment-output",
      schemaVersion: 2,
      translation: { lines: ["first", "second"] },
      wordGlosses: {
        lines: [
          {
            lineIndex: 0,
            segments: [
              {
                kind: "word",
                meaning: "opening hemistich",
                surface: "صدر",
                tokenIndex: 0,
              },
            ],
          },
        ],
        tokenizerVersion: "saqi-orthographic-v1",
      },
    };
    const artifact = await artifacts.put(
      `${canonicalJson({
        generationAttemptId: "legacy-generation",
        input: prepared.enrichmentInput,
        model: ENRICHMENT_PROVIDER_SPECS.sol.model,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        output,
        outputHash: sha256(canonicalJson(output)),
        pipelineVersion: SOL_PIPELINE_VERSION,
        provider: "sol",
        reasoningEffort: ENRICHMENT_PROVIDER_SPECS.sol.reasoningEffort,
        reviewAttemptIds: ["legacy-r1", "legacy-r2"],
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
      })}\n`,
    );
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: prepared.enrichmentInput,
        inputHash: inputHash(prepared.enrichmentInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: `${prepared.enrichmentInput.schemaId}@1`,
      },
      1,
    );
    const sourceClaim = ledger.claim("sol", 1, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!sourceClaim) throw new Error("legacy Sol claim missing");
    ledger.succeed(sourceClaim, artifact.hash, 2);
    const siblingInput = {
      lane: "sol" as const,
      source: { artifactHash: artifact.hash, workKey: source.workKey },
    };
    const sibling = ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: siblingInput,
        inputHash: inputHash(siblingInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      3,
    );
    const siblingClaim = ledger.claim("defer-sibling", 3, 1_000, [
      FANOUT_SOL_KIND,
    ]);
    if (!siblingClaim) throw new Error("sibling fanout claim missing");
    ledger.operatorRelease(
      siblingClaim,
      "FANOUT_RESOLUTION_PENDING",
      4,
      4 + 6 * 60 * 60_000,
    );
    expect(
      ledger.indexReusableEnrichment({
        fanoutWorkKey: sibling.workKey,
        modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        outputArtifactHash: artifact.hash,
        promptMaterialHash: sha256(
          sourcePromptMaterialHashBody(prepared.enrichmentInput),
        ),
        translationWorkKey: source.workKey,
      }),
    ).toBe(true);
    const attachingInput = {
      lane: "sol" as const,
      modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      source: { artifactHash: artifact.hash, workKey: source.workKey },
    };
    const attaching = ledger.seed(
      {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        input: attachingInput,
        inputHash: inputHash(attachingInput),
        kind: FANOUT_SOL_KIND,
        priority: 200,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      },
      5,
    );
    const approvedSol = vi.fn(() => ({
      binding: bindingFor(prepared.enrichmentInput),
      status: "resolved" as const,
    }));
    const retireApprovedSol = vi.fn();
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource: async () => {
          throw new Error("not called");
        },
        seedCollection: async () => {
          throw new Error("not called");
        },
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        approvedSol,
        collected: () => null,
        enrichment: () => null,
        retireApprovedSol,
      },
    });

    await reconciler.cycle({ maximum: 1, now: () => 5 });
    expect(ledger.get(attaching.workKey)?.state).toBe("succeeded");
    expect(ledger.get(sibling.workKey)).toMatchObject({
      availableAt: 5,
      state: "pending",
    });
    expect(
      ledger.listFanoutPriorityWorkKeys([FANOUT_SOL_KIND], 10, 5),
    ).toContain(sibling.workKey);
    expect(approvedSol).toHaveBeenCalledOnce();

    await reconciler.cycle({ maximum: 1, now: () => 6 });
    expect(ledger.get(sibling.workKey)?.state).toBe("succeeded");
    expect(approvedSol).toHaveBeenCalledOnce();
    expect(retireApprovedSol).toHaveBeenCalledExactlyOnceWith(
      prepared.enrichmentInput,
      ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      sibling.workKey,
    );
    expect(
      ledger.approvedPublicationAttachmentForTranslation(
        source.workKey,
        artifact.hash,
      ),
    ).toEqual({
      binding: bindingFor(prepared.enrichmentInput),
      publicationPriority: 200,
    });
    expect(
      ledger.status().kindProgress.find(({ kind }) => kind === FANOUT_SOL_KIND)
        ?.byState.succeeded,
    ).toBe(2);
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === DIRECT_ENRICHMENT_PUBLICATION_KIND,
        )?.total,
    ).toBe(1);
    expect(
      ledger.get(
        ledger.attachApprovedBindingAndSeedPublication(
          source.workKey,
          artifact.hash,
          bindingFor(prepared.enrichmentInput),
          {
            implementationVersion:
              SOURCE_BOUND_PUBLICATION_IMPLEMENTATION_VERSION,
            priority: 200,
          },
          7,
        ).publicationWorkKey,
      )?.state,
    ).toBe("pending");
    ledger.close();
  });

  it("autonomously scans, skips unresolved identities, and replays every seed boundary exactly once", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    const detailInput = {
      authorHref: "https://source.invalid/writers/a",
      poemHref: "https://source.invalid/works/42",
    };
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: detailInput,
        inputHash: inputHash(detailInput),
        kind: collectionWorkKinds().poemDetail,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const detailClaim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!detailClaim) throw new Error("detail claim missing");
    ledger.succeed(detailClaim, detailArtifact.hash, 2);

    const publication = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called while seeding");
        },
      }),
      ledger,
      owner: "publication-test",
    });
    let resolved = false;
    const boundaryCrashes = new Set<string>();
    let solWorkKey = "";
    const sol = {
      seed(input: PoemEnrichmentInput, priority = 0) {
        const seeded = ledger.seed({
          implementationVersion: SOL_PIPELINE_VERSION,
          input,
          inputHash: inputHash(input),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority,
          schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
        });
        solWorkKey = seeded.workKey;
        return seeded;
      },
    };
    const reconciler = new FanoutReconciler({
      afterBoundary: (boundary) => {
        if (!boundaryCrashes.has(boundary)) {
          boundaryCrashes.add(boundary);
          throw new Error(`crash:${boundary}`);
        }
      },
      artifacts,
      batchSize: 10,
      ledger,
      publication,
      resolvers: {
        collected: () =>
          resolved
            ? {
                binding: bindingFor(
                  prepareCollectedPoem(
                    DETAIL,
                    MAPPING,
                    "2026-08-25T12:00:00.000Z",
                    1,
                  ).enrichmentInput,
                ),
                mapping: MAPPING,
                observedAt: "2026-08-25T12:00:00.000Z",
                writerEpoch: 1,
              }
            : null,
        enrichment: () => ({ expectedPointerVersion: null, writerEpoch: 1 }),
      },
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: sol.seed.bind(sol),
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      owner: "fanout-test",
    });
    const start = Date.now() + 1_000;
    const unresolved = await reconciler.cycle({
      maximum: 10,
      now: () => start,
    });
    expect(unresolved).toMatchObject({ pendingResolution: 0, retried: 2 });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === SOL_ENRICHMENT_WORK_KIND),
    ).toBeUndefined();

    resolved = true;
    await reconciler.cycle({ maximum: 10, now: () => start + 400_000 }); // crash after Sol seed
    await reconciler.cycle({ maximum: 10, now: () => start + 500_000 }); // replay completes
    await reconciler.cycle({ maximum: 10, now: () => start + 600_000 }); // replay completes
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === COLLECTION_PUBLICATION_WORK_KIND,
        ),
    ).toBeUndefined();
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === SOL_ENRICHMENT_WORK_KIND)
        ?.total,
    ).toBe(1);
    expect(ledger.get(solWorkKey)?.priority).toBe(270);

    const prepared = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const output = {
      insights: {
        culturalSignificance: "c",
        historicalContext: "h",
        literaryDevices: ["d"],
        notableLines: [{ explanation: "e", line: "صدر" }],
        summary: "s",
        themes: ["t"],
      },
      translation: { lines: ["line"] },
    };
    const solArtifact = {
      generationAttemptId: "g",
      input: bindCollectedPoem(
        prepared.enrichmentInput,
        bindingFor(prepared.enrichmentInput),
      ),
      output,
      outputHash: sha256(canonicalJson(output)),
      pipelineVersion: SOL_PIPELINE_VERSION,
      reviewAttemptIds: ["r1", "r2"],
      reviews: [
        {
          fidelityScore: 100,
          findings: [],
          insightScore: 100,
          verdict: "pass",
        },
        { fidelityScore: 99, findings: [], insightScore: 99, verdict: "pass" },
      ],
    };
    const storedSol = await artifacts.put(`${canonicalJson(solArtifact)}\n`);
    const solClaim = ledger.claim("sol", start + 700_000, 1_000, [
      SOL_ENRICHMENT_WORK_KIND,
    ]);
    if (!solClaim) throw new Error("sol claim missing");
    ledger.succeed(solClaim, storedSol.hash, start + 700_001);

    await reconciler.cycle({
      maximum: 10,
      now: () => start + 800_000,
    }); // crash after enrichment publication seed
    await reconciler.cycle({
      maximum: 10,
      now: () => start + 900_000,
    }); // replay completes
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === ENRICHMENT_PUBLICATION_WORK_KIND,
        ),
    ).toBeUndefined();
    expect(boundaryCrashes).toEqual(
      new Set(["legacy-sol-backfill", "scan", "sol"]),
    );
    ledger.close();
  });

  it("bootstraps an unmapped collection once and waits for its canonical binding across a crash", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-collection-bootstrap-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: DETAIL_INPUT,
        inputHash: inputHash(DETAIL_INPUT),
        kind: collectionWorkKinds().poemDetail,
        priority: 1_000,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const detailClaim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!detailClaim) throw new Error("detail claim missing");
    ledger.succeed(detailClaim, detailArtifact.hash, 2);

    const publicationLane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called while seeding");
        },
      }),
      ledger,
    });
    const seedCollection = vi.fn(
      publicationLane.seedCollection.bind(publicationLane),
    );
    const admitSource = vi.fn(async () => {
      throw new Error("legacy v2 admission must not run");
    });
    let canonicalBindingAvailable = false;
    let crashed = false;
    const prepared = prepareCollectedPoem(
      DETAIL,
      { ...MAPPING, canonicalPoemId: null },
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const solSeed = vi.fn((input: PoemEnrichmentInput, priority = 0) =>
      ledger.seed({
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority,
        schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
      }),
    );
    const reconciler = new FanoutReconciler({
      afterBoundary: (boundary) => {
        if (boundary === "collection-publication" && !crashed) {
          crashed = true;
          throw new Error("crash:collection-publication");
        }
      },
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: solSeed,
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource,
        seedCollection,
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        collected: () => ({
          ...(canonicalBindingAvailable
            ? { binding: bindingFor(prepared.enrichmentInput) }
            : {}),
          mapping: canonicalBindingAvailable
            ? MAPPING
            : { ...MAPPING, canonicalPoemId: null },
          observedAt: crashed
            ? "2026-08-25T12:05:00.000Z"
            : "2026-08-25T12:00:00.000Z",
          writerEpoch: crashed ? 2 : 1,
        }),
        enrichment: () => null,
      },
    });
    const start = Date.now() + 1_000;
    const crashedCycle = await reconciler.cycle({
      maximum: 10,
      now: () => start,
    });
    expect(crashedCycle.retried).toBeGreaterThan(0);
    expect(seedCollection).toHaveBeenCalledOnce();
    expect(admitSource).not.toHaveBeenCalled();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === COLLECTION_PUBLICATION_WORK_KIND,
        )?.total,
    ).toBe(1);

    const replay = await reconciler.cycle({
      maximum: 10,
      now: () => start + 30_001,
    });
    expect(replay.pendingResolution).toBe(1);
    expect(seedCollection).toHaveBeenCalledTimes(2);
    expect(admitSource).not.toHaveBeenCalled();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === COLLECTION_PUBLICATION_WORK_KIND,
        )?.total,
    ).toBe(1);

    canonicalBindingAvailable = true;
    await reconciler.cycle({
      maximum: 10,
      now: () => start + 5 * 60_000 + 30_002,
    });
    expect(seedCollection).toHaveBeenCalledTimes(2);
    expect(solSeed).toHaveBeenCalledOnce();
    expect(admitSource).not.toHaveBeenCalled();
    ledger.close();
  });

  it("turns a demand-driven AUTHOR_NOT_FOUND response into one artifact-bound collection bootstrap", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-demand-bootstrap-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
    ledger.recordSourceAuthorMetadata(
      DETAIL.source.author.href,
      MAPPING.authorNameArabic,
      "fixture-generation",
      1,
    );
    ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: DETAIL_INPUT,
        inputHash: inputHash(DETAIL_INPUT),
        kind: collectionWorkKinds().poemDetail,
        priority: 1_000,
        schemaVersion: collectorSchemaVersion(),
      },
      1,
    );
    const detailClaim = ledger.claim("collector", 1, 1_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!detailClaim) throw new Error("detail claim missing");
    ledger.succeed(detailClaim, detailArtifact.hash, 2);

    const publicationLane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called while seeding");
        },
      }),
      ledger,
    });
    const seedCollection = vi.fn(
      publicationLane.seedCollection.bind(publicationLane),
    );
    const admitSource = vi.fn(async (item, _signal?: AbortSignal) => ({
      actionHash: "a".repeat(64),
      responseHash: "b".repeat(64),
      result: {
        observedAt: "2026-08-25T12:00:00.000Z",
        results: [
          {
            admissionId: item.admissionId,
            code: "AUTHOR_NOT_FOUND" as const,
            message: "Author is not in the production catalog",
            retryable: false,
            status: "rejected" as const,
          },
        ],
        schemaId: "saqi.source-admission",
        schemaVersion: 2,
        writerEpoch: 7,
      },
      state: "confirmed" as const,
    }));
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: [
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          seed: () => {
            throw new Error("not called before canonical binding");
          },
          workKind: SOL_ENRICHMENT_WORK_KIND,
        },
      ],
      ledger,
      publication: {
        admitSource,
        seedCollection,
        seedEnrichment: async () => {
          throw new Error("not called");
        },
      },
      resolvers: {
        collected: () => null,
        enrichment: () => null,
      },
    });
    const start = Date.now() + 1_000;
    const controller = new AbortController();
    const first = await reconciler.cycle({
      maximum: 10,
      now: () => start,
      signal: controller.signal,
    });
    expect(first).toMatchObject({
      collectionPublicationsSeeded: 1,
      deadLettered: 0,
      pendingResolution: 1,
    });
    expect(admitSource).toHaveBeenCalledOnce();
    expect(admitSource.mock.calls[0]?.[1]).toBe(controller.signal);
    expect(seedCollection).toHaveBeenCalledOnce();
    expect(
      ledger
        .status()
        .kindProgress.find(
          ({ kind }) => kind === COLLECTION_PUBLICATION_WORK_KIND,
        )?.total,
    ).toBe(1);

    const replay = await reconciler.cycle({
      maximum: 10,
      now: () => start + 5 * 60_000 + 1,
    });
    expect(replay.pendingResolution).toBe(1);
    expect(admitSource).toHaveBeenCalledOnce();
    expect(seedCollection).toHaveBeenCalledOnce();

    const collection = ledger.listWorkDefinitionsAfter(
      null,
      COLLECTION_PUBLICATION_WORK_KIND,
      10,
      {
        implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
        schemaVersion: PUBLICATION_SCHEMA_VERSION,
      },
    ).items[0];
    if (!collection) throw new Error("collection publication missing");
    const publicationClaim = ledger.claim(
      "publication",
      start + 5 * 60_000 + 2,
      1_000,
      [COLLECTION_PUBLICATION_WORK_KIND],
    );
    if (!publicationClaim) throw new Error("publication claim missing");
    expect(publicationClaim.work.workKey).toBe(collection.workKey);
    ledger.deadLetter(
      publicationClaim,
      "PUBLICATION_LOCAL_STATE_CORRUPT",
      start + 5 * 60_000 + 3,
    );
    const terminal = await reconciler.cycle({
      maximum: 10,
      now: () => start + 10 * 60_000 + 2,
    });
    expect(terminal).toMatchObject({ deadLettered: 1, pendingResolution: 0 });
    expect(
      ledger.listWorkDefinitionsAfter(null, FANOUT_DETAIL_KIND, 10, {
        implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
        schemaVersion: FANOUT_SCHEMA_VERSION,
      }).items[0],
    ).toMatchObject({
      lastErrorCode: "COLLECTION_PUBLICATION_TERMINAL",
      state: "dead_letter",
    });
    ledger.close();
  });

  it.each(["PUBLICATION_BATCHED", "PUBLICATION_SUPERSEDED"] as const)(
    "follows a %s collection successor through retry and success",
    async (supersededCode) => {
      const root = mkdtempSync(join(tmpdir(), "fanout-publication-successor-"));
      const artifacts = new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      });
      const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
      const detailArtifact = await artifacts.put(`${canonicalJson(DETAIL)}\n`);
      ledger.seed(
        {
          implementationVersion: collectorImplementationVersion(),
          input: DETAIL_INPUT,
          inputHash: inputHash(DETAIL_INPUT),
          kind: collectionWorkKinds().poemDetail,
          priority: 1_000,
          schemaVersion: collectorSchemaVersion(),
        },
        1,
      );
      const detailClaim = ledger.claim("collector", 1, 1_000, [
        collectionWorkKinds().poemDetail,
      ]);
      if (!detailClaim) throw new Error("detail claim missing");
      ledger.succeed(detailClaim, detailArtifact.hash, 2);
      const publicationLane = new PublicationLane({
        artifacts,
        client: new PublicationClient({
          endpoint: "https://example.test/api/corpus-import",
          transport: async () => {
            throw new Error("not called while seeding");
          },
        }),
        ledger,
      });
      const seedCollection = vi.fn(
        publicationLane.seedCollection.bind(publicationLane),
      );
      const reconciler = new FanoutReconciler({
        artifacts,
        batchSize: 10,
        enrichment: [
          {
            implementationVersion: SOL_PIPELINE_VERSION,
            modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            seed: () => {
              throw new Error("not called before canonical binding");
            },
            workKind: SOL_ENRICHMENT_WORK_KIND,
          },
        ],
        ledger,
        publication: {
          admitSource: async () => {
            throw new Error("not called for a mapping-only resolution");
          },
          seedCollection,
          seedEnrichment: async () => {
            throw new Error("not called");
          },
        },
        resolvers: {
          collected: () => ({
            mapping: { ...MAPPING, canonicalPoemId: null },
            observedAt: "2026-08-25T12:00:00.000Z",
            writerEpoch: 1,
          }),
          enrichment: () => null,
        },
      });
      const start = Date.now() + 1_000;
      await reconciler.cycle({ maximum: 10, now: () => start });
      const original = ledger.listWorkDefinitionsAfter(
        null,
        COLLECTION_PUBLICATION_WORK_KIND,
        10,
        {
          implementationVersion: PUBLICATION_IMPLEMENTATION_VERSION,
          schemaVersion: PUBLICATION_SCHEMA_VERSION,
        },
      ).items[0];
      if (!original) throw new Error("original publication missing");
      const originalClaim = ledger.claim("publication", start + 1, 1_000, [
        COLLECTION_PUBLICATION_WORK_KIND,
      ]);
      if (!originalClaim) throw new Error("original publication claim missing");
      const originalChunks = seedCollection.mock.calls[0]?.[0];
      const originalChunk = originalChunks?.[0];
      if (originalChunk?.action.action !== "stage-and-plan") {
        throw new Error("original publication chunk missing");
      }
      const [successor] = await publicationLane.seedCollection(
        [
          {
            action: {
              ...originalChunk.action,
              input: {
                ...originalChunk.action.input,
                bundle: {
                  ...originalChunk.action.input.bundle,
                  writerEpoch: 2,
                },
              },
            },
            sources: originalChunk.sources,
          },
        ],
        1_000,
      );
      if (!successor) throw new Error("successor publication missing");
      ledger.checkpoint(
        originalClaim,
        {
          artifactHash: null,
          kind: "publication-successor",
          payload: { successorWorkKey: successor.workKey },
        },
        start + 2,
      );
      ledger.deadLetter(originalClaim, supersededCode, start + 3);

      const followed = await reconciler.cycle({
        maximum: 10,
        now: () => start + 5 * 60_000 + 1,
      });
      expect(followed).toMatchObject({
        deadLettered: 0,
        pendingResolution: 1,
      });
      const fanoutDetail = ledger.listWorkDefinitionsAfter(
        null,
        FANOUT_DETAIL_KIND,
        10,
        {
          implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
          schemaVersion: FANOUT_SCHEMA_VERSION,
        },
      ).items[0];
      if (!fanoutDetail) throw new Error("detail fanout missing");
      const successorPhase = ledger.latestCheckpoint(
        fanoutDetail.workKey,
        "fanout-detail-phase",
      );
      if (!successorPhase?.artifactHash) {
        throw new Error("successor phase missing");
      }
      const successorPhaseContents = await artifacts.read(
        successorPhase.artifactHash,
      );
      const successorPhaseRaw: unknown = JSON.parse(
        successorPhaseContents.toString("utf8"),
      );
      expect(successorPhaseRaw).toMatchObject({
        collectionPublicationWorkKey: successor.workKey,
      });
      const successorClaim = ledger.claim(
        "publication",
        start + 5 * 60_000 + 2,
        1_000,
        [COLLECTION_PUBLICATION_WORK_KIND],
      );
      if (!successorClaim) throw new Error("successor claim missing");
      expect(successorClaim.work.workKey).toBe(successor.workKey);
      ledger.retry(
        successorClaim,
        "PUBLICATION_TEST_RETRY",
        start + 5 * 60_000 + 4,
        start + 5 * 60_000 + 3,
      );
      const retrying = await reconciler.cycle({
        maximum: 10,
        now: () => start + 10 * 60_000 + 2,
      });
      expect(retrying).toMatchObject({
        deadLettered: 0,
        pendingResolution: 1,
      });
      const finalClaim = ledger.claim(
        "publication",
        start + 10 * 60_000 + 3,
        1_000,
        [COLLECTION_PUBLICATION_WORK_KIND],
      );
      if (!finalClaim) throw new Error("retried successor claim missing");
      ledger.succeed(finalClaim, detailArtifact.hash, start + 10 * 60_000 + 4);
      const succeeded = await reconciler.cycle({
        maximum: 10,
        now: () => start + 15 * 60_000 + 3,
      });
      expect(succeeded).toMatchObject({
        deadLettered: 0,
        pendingResolution: 1,
      });
      expect(seedCollection).toHaveBeenCalledOnce();
      ledger.close();
    },
  );

  it("publishes only completed Codex work while consuming retired history", async () => {
    const root = mkdtempSync(join(tmpdir(), "fanout-providers-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const publicationLane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("not called while seeding");
        },
      }),
      ledger,
      owner: "publication-provider-test",
    });
    const publicationActions: CorpusImportAction[] = [];
    const publication = {
      admitSource: publicationLane.admitSource.bind(publicationLane),
      seedCollection: publicationLane.seedCollection.bind(publicationLane),
      seedEnrichment(
        action: CorpusImportAction,
        source: { artifactHash: string; workKey: string },
        priority = 0,
      ) {
        publicationActions.push(action);
        return publicationLane.seedEnrichment(action, source, priority);
      },
    };
    const prepared = prepareCollectedPoem(
      DETAIL,
      MAPPING,
      "2026-08-25T12:00:00.000Z",
      1,
    );
    const output = {
      insights: {
        culturalSignificance: "c",
        historicalContext: "h",
        literaryDevices: ["d"],
        notableLines: [{ explanation: "e", line: "صدر" }],
        summary: "s",
        themes: ["t"],
      },
      translation: { lines: ["first hemistich", "second hemistich"] },
    };
    const reviews = [
      {
        fidelityScore: 100,
        findings: [],
        insightScore: 100,
        verdict: "pass" as const,
      },
      {
        fidelityScore: 99,
        findings: [],
        insightScore: 99,
        verdict: "pass" as const,
      },
    ];
    const providerNow = Date.now() + 1_000;
    // Historical identities exercise publication compatibility only. They are
    // deliberately local test data and grant no executable provider support.
    const publicationSpecs = {
      retiredA: {
        model: "retired-model-a",
        modelKey: "retired-model-a",
        pipelineVersion: "retired-model-a-v1",
        reasoningEffort: "high",
      },
      retiredB: {
        model: "retired-model-b",
        modelKey: "retired-model-b",
        pipelineVersion: "retired-model-b-v1",
        reasoningEffort: "max",
      },
      sol: ENRICHMENT_PROVIDER_SPECS.sol,
    } as const;
    const providers = await Promise.all(
      (["sol", "retiredA", "retiredB"] as const).map(async (provider) => {
        const spec = publicationSpecs[provider];
        const workKind =
          provider === "sol"
            ? SOL_ENRICHMENT_WORK_KIND
            : `poem-enrichment-${spec.modelKey}`;
        const seeded = ledger.seed(
          {
            implementationVersion: spec.pipelineVersion,
            input: prepared.enrichmentInput,
            inputHash: inputHash(prepared.enrichmentInput),
            kind: workKind,
            priority: 200,
            schemaVersion: `${prepared.enrichmentInput.schemaId}@${String(prepared.enrichmentInput.schemaVersion)}`,
          },
          providerNow,
        );
        const claim = ledger.claim(provider, providerNow, 1_000, [workKind], {
          implementationVersion: spec.pipelineVersion,
        });
        if (!claim) throw new Error(`${provider} claim missing`);
        const stored = await artifacts.put(
          `${canonicalJson({
            generationAttemptId: `${provider}-generation`,
            input: prepared.enrichmentInput,
            model: spec.model,
            modelKey: spec.modelKey,
            output,
            outputHash: sha256(canonicalJson(output)),
            pipelineVersion: spec.pipelineVersion,
            provider,
            reasoningEffort: spec.reasoningEffort,
            reviewAttemptIds: [`${provider}-r1`, `${provider}-r2`],
            reviews,
          })}\n`,
        );
        ledger.succeed(claim, stored.hash, providerNow + 1);
        return {
          implementationVersion: spec.pipelineVersion,
          modelKey: spec.modelKey,
          workKind,
          workKey: seeded.workKey,
        };
      }),
    );
    const pointerVersions: Record<string, null | number> = {
      [publicationSpecs.sol.modelKey]: 7,
      [publicationSpecs.retiredA.modelKey]: null,
      [publicationSpecs.retiredB.modelKey]: 2,
    };
    const resolvedModels: string[] = [];
    const resolvedFanoutWorkKeys: string[] = [];
    expect(
      () =>
        new FanoutReconciler({
          artifacts,
          enrichment: [providers[1]!],
          ledger,
          publication,
          resolvers: { collected: () => null, enrichment: () => null },
        }),
    ).toThrow("FANOUT_CODEX_PROFILE_REQUIRED");
    const reconciler = new FanoutReconciler({
      artifacts,
      batchSize: 10,
      enrichment: providers.filter(
        ({ modelKey }) => modelKey === publicationSpecs.sol.modelKey,
      ),
      ledger,
      owner: "fanout-provider-test",
      publication,
      resolvers: {
        collected: () => null,
        enrichment: (_source, _input, profile, fanoutWorkKey) => {
          resolvedModels.push(profile.modelKey);
          resolvedFanoutWorkKeys.push(fanoutWorkKey);
          return {
            expectedPointerVersion: pointerVersions[profile.modelKey] ?? null,
            writerEpoch: 3,
          };
        },
      },
    });
    try {
      const first = await reconciler.cycle({
        maximum: 10,
        now: () => providerNow + 2,
      });
      expect(first).toMatchObject({
        enrichmentPublicationsSeeded: 1,
        scannedEnrichments: 0,
        scannedSol: 1,
      });
      expect(first.enrichmentCursors).toEqual({
        [publicationSpecs.sol.modelKey]: expect.any(Number),
      });
      expect(resolvedModels).toEqual([publicationSpecs.sol.modelKey]);
      expect(new Set(resolvedFanoutWorkKeys).size).toBe(1);
      expect(
        resolvedFanoutWorkKeys.every((workKey) =>
          /^[a-f\d]{64}$/.test(workKey),
        ),
      ).toBe(true);
      expect(
        publicationActions.map((action) => {
          if (action.action !== "publish-enrichment")
            throw new Error("unexpected publication action");
          return [
            action.input.artifact.modelKey,
            action.input.publication.expectedPointerVersion,
          ];
        }),
      ).toEqual([
        [
          publicationSpecs.sol.modelKey,
          pointerVersions[publicationSpecs.sol.modelKey],
        ],
      ]);
      expect(
        ledger
          .status()
          .kindProgress.find(
            ({ kind }) => kind === ENRICHMENT_PUBLICATION_WORK_KIND,
          )?.total,
      ).toBe(1);

      await reconciler.cycle({
        maximum: 10,
        now: () => providerNow + 30_003,
      });
      expect(
        ledger
          .status()
          .kindProgress.find(
            ({ kind }) => kind === ENRICHMENT_PUBLICATION_WORK_KIND,
          )?.total,
      ).toBe(1);
      expect(publicationActions).toHaveLength(1);
    } finally {
      ledger.close();
    }
  });
});

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
