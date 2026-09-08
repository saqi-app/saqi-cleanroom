import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  type PoemEnrichmentInput,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  ProductionResolutionResponseBodySchema,
  ProductionResolutionResponseSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { afterEach, describe, expect, it } from "vitest";

import { LegacySourceBindingReconciler } from "../enrichment/legacy-source-binding-reconciler.js";
import {
  POEM_ENRICHMENT_SCHEMA_VERSION,
  POEM_ENRICHMENT_V2_SCHEMA_VERSION,
  SOL_ENRICHMENT_WORK_KIND,
} from "../enrichment/sol-coordinator.js";
import {
  ENRICHMENT_PROVIDER_SPECS,
  SOL_PIPELINE_VERSION,
} from "../enrichment/sol-runner.js";
import { Ledger } from "../persistence/ledger.js";
import {
  ProductionResolutionDemandCache,
  type ProductionResolutionDemandClaim,
} from "../persistence/production-resolution-demand-cache.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";

const ROOTS: string[] = [];
const NOW = Date.parse("2026-09-02T12:00:00.000Z");

describe("legacy source binding reconciliation", () => {
  afterEach(async () => {
    await Promise.all(ROOTS.map((root) => rm(root, { recursive: true })));
    ROOTS.length = 0;
  });

  it("preserves attempted v2 work and its paid checkpoint identity", async () => {
    const root = await mkdtemp(
      join(tmpdir(), "legacy-source-binding-started-"),
    );
    ROOTS.push(root);
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const cache = new ProductionResolutionDemandCache({
      now: () => NOW,
      path: join(root, "resolution.sqlite3"),
    });
    const input = legacyInput();
    const definition = {
      implementationVersion: "sol-word-gloss-v2",
      input,
      inputHash: inputHash(input),
      kind: SOL_ENRICHMENT_WORK_KIND,
      priority: 0,
      schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
    };
    const source = ledger.seed(definition, NOW);
    const claim = ledger.claim("worker", NOW, 100, [SOL_ENRICHMENT_WORK_KIND]);
    if (!claim) throw new Error("Expected claim");
    ledger.retry(claim, "TEST_RETRY", NOW + 1, NOW);
    expect(
      new LegacySourceBindingReconciler({ cache, ledger }).cycle(NOW + 1),
    ).toMatchObject({ scanned: 1, seeded: 0, superseded: 0 });
    expect(ledger.get(source.workKey)).toMatchObject({
      implementationVersion: "sol-word-gloss-v2",
      state: "retry_wait",
      attemptCount: 1,
    });
    expect(() =>
      ledger.supersedeUnclaimed(
        source.workKey,
        { ...definition, implementationVersion: "sol-word-gloss-v3" },
        "TEST_UPGRADE",
        NOW,
      ),
    ).toThrow("Legacy work supersession lost its source row");
    expect(ledger.status().kindProgress[0]?.total).toBe(1);
    cache.close();
    ledger.close();
  });

  it.each(["sol-word-gloss-v2", "sol-word-gloss-v3"])(
    "binds %s work without upgrading its pipeline version",
    async (implementationVersion) => {
      const root = await mkdtemp(join(tmpdir(), "legacy-source-binding-"));
      ROOTS.push(root);
      const ledger = Ledger.open(join(root, "ledger.sqlite3"));
      const cache = new ProductionResolutionDemandCache({
        now: () => NOW,
        path: join(root, "resolution.sqlite3"),
      });
      const input = legacyInput();
      const source = ledger.seed(
        {
          implementationVersion,
          input,
          inputHash: inputHash(input),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: 270,
          schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
        },
        NOW,
      );
      const reconciler = new LegacySourceBindingReconciler({
        cache,
        ledger,
      });

      expect(reconciler.cycle(NOW)).toMatchObject({
        pendingResolution: 1,
        seeded: 0,
      });
      const claim = requiredClaim(cache.claim());
      const currentRevisionId = sha256("revision:current");
      cache.complete(claim, response(claim, input, currentRevisionId));
      expect(reconciler.cycle(NOW)).toMatchObject({
        pendingResolution: 0,
        seeded: 1,
        superseded: 1,
      });

      expect(
        ledger.listReadyWork(
          SOL_ENRICHMENT_WORK_KIND,
          10,
          {
            implementationVersion,
            schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
          },
          NOW,
        ),
      ).toEqual([]);
      const [replacement] = ledger.listReadyWork(
        SOL_ENRICHMENT_WORK_KIND,
        10,
        {
          implementationVersion,
          schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
        },
        NOW,
      );
      expect(replacement).toMatchObject({
        priority: 270,
        state: "pending",
        input: {
          authorArabic: "الشاعر الموثق",
          canonicalBinding: {
            poemId: input.poemId,
            sourceRevisionId: currentRevisionId,
          },
          schemaVersion: 2,
          sourceRevisionId: currentRevisionId,
        },
      });
      expect(ledger.get(source.workKey)).toMatchObject({
        lastErrorCode: null,
        state: "imported",
      });
      expect(
        ledger
          .status()
          .affectedByErrorCode.find(
            ({ code }) => code === "SOURCE_BINDING_SUPERSEDED",
          ),
      ).toBeUndefined();
      expect(source.inserted).toBe(true);
      expect(
        ledger.rollbackUnattemptedSupersession(
          source.workKey,
          replacement?.workKey ?? "",
          NOW + 1,
        ),
      ).toBe(true);
      expect(ledger.get(source.workKey)?.state).toBe("pending");
      expect(ledger.get(replacement?.workKey ?? "")?.state).toBe("imported");
      cache.close();
      ledger.close();
    },
  );

  it("classifies a noncanonical legacy poem id and advances instead of faulting the lane", async () => {
    const root = await mkdtemp(
      join(tmpdir(), "legacy-source-binding-invalid-id-"),
    );
    ROOTS.push(root);
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const cache = new ProductionResolutionDemandCache({
      now: () => NOW,
      path: join(root, "resolution.sqlite3"),
    });
    const input = { ...legacyInput(), poemId: "legacy-poem-id" };
    const source = ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 110,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW,
    );
    const reconciler = new LegacySourceBindingReconciler({ cache, ledger });

    expect(reconciler.cycle(NOW)).toMatchObject({
      conflicts: 1,
      scanned: 1,
    });
    expect(ledger.get(source.workKey)).toMatchObject({
      lastErrorCode: "LEGACY_ENRICHMENT_CANONICAL_ID_INVALID",
      state: "dead_letter",
    });
    expect(() => reconciler.cycle(NOW + 1)).not.toThrow();
    cache.close();
    ledger.close();
  });

  it("classifies a malformed row without starving valid work in the same window", async () => {
    const root = await mkdtemp(join(tmpdir(), "legacy-source-poison-"));
    ROOTS.push(root);
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const cache = new ProductionResolutionDemandCache({
      now: () => NOW,
      path: join(root, "resolution.sqlite3"),
    });
    const malformed = { poemId: sha256("malformed") };
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: malformed,
        inputHash: inputHash(malformed),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 300,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW,
    );
    const valid = legacyInput();
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: valid,
        inputHash: inputHash(valid),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW,
    );

    const summary = new LegacySourceBindingReconciler({
      batchSize: 2,
      cache,
      ledger,
    }).cycle(NOW);
    expect(summary).toMatchObject({
      conflicts: 1,
      pendingResolution: 1,
      scanned: 2,
    });
    expect(
      ledger
        .status()
        .affectedByErrorCode.find(
          ({ code }) => code === "LEGACY_ENRICHMENT_INPUT_INVALID",
        ),
    ).toMatchObject({ count: 1 });
    cache.close();
    ledger.close();
  });

  it("classifies an isolated production identity conflict as terminal", async () => {
    const root = await mkdtemp(join(tmpdir(), "legacy-source-conflict-"));
    ROOTS.push(root);
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const cache = new ProductionResolutionDemandCache({
      now: () => NOW,
      path: join(root, "resolution.sqlite3"),
    });
    const input = legacyInput();
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input,
        inputHash: inputHash(input),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 270,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW,
    );
    const reconciler = new LegacySourceBindingReconciler({ cache, ledger });
    expect(reconciler.cycle(NOW).pendingResolution).toBe(1);
    const claim = requiredClaim(cache.claim());
    expect(
      cache.bisect(
        claim,
        NOW + 60_000,
        "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
      ),
    ).toBe(0);

    expect(reconciler.cycle(NOW)).toMatchObject({
      conflicts: 1,
      pendingResolution: 0,
      seeded: 0,
    });
    expect(
      ledger
        .status()
        .affectedByErrorCode.find(
          ({ code }) =>
            code === "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
        ),
    ).toMatchObject({ count: 1 });
    cache.close();
    ledger.close();
  });

  it.each([
    null,
    "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
    "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
    "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
  ])(
    "recovers missing IDs across restart with fingerprint outcome %s",
    async (failure) => {
      const root = await mkdtemp(join(tmpdir(), "legacy-fingerprint-"));
      ROOTS.push(root);
      const ledger = Ledger.open(join(root, "ledger.sqlite3"));
      let cache = new ProductionResolutionDemandCache({
        now: () => NOW,
        path: join(root, "resolution.sqlite3"),
      });
      const input = legacyInput();
      const source = ledger.seed(
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          input,
          inputHash: inputHash(input),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: 270,
          schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
        },
        NOW,
      );
      let reconciler = new LegacySourceBindingReconciler({ cache, ledger });
      expect(reconciler.cycle(NOW).pendingResolution).toBe(1);
      const canonical = requiredClaim(cache.claim());
      cache.bisect(canonical, NOW + 60_000);
      expect(reconciler.cycle(NOW)).toMatchObject({
        pendingResolution: 1,
        conflicts: 0,
      });
      cache.close();
      cache = new ProductionResolutionDemandCache({
        now: () => NOW,
        path: join(root, "resolution.sqlite3"),
      });
      reconciler = new LegacySourceBindingReconciler({ cache, ledger });
      expect(reconciler.cycle(NOW)).toMatchObject({
        pendingResolution: 1,
        conflicts: 0,
      });
      const fingerprint = requiredClaim(cache.claim());
      expect(fingerprint.request.schemaVersion).toBe(3);
      if (failure) {
        cache.bisect(fingerprint, NOW + 60_000, failure);
        expect(reconciler.cycle(NOW)).toMatchObject({
          conflicts: 1,
          pendingResolution: 0,
          seeded: 0,
        });
        expect(ledger.get(source.workKey)).toMatchObject({
          state: "dead_letter",
          lastErrorCode: failure,
        });
        expect(reconciler.cycle(NOW)).toMatchObject({ scanned: 0, seeded: 0 });
        expect(cache.claim()).toBeNull();
        cache.close();
        ledger.close();
        return;
      }
      const { manifestHash: _manifestHash, ...resolved } = response(
        fingerprint,
        input,
      );
      const canonicalId = sha256("replacement-poem");
      const body = ProductionResolutionResponseBodySchema.parse({
        ...resolved,
        schemaVersion: 2,
        targets: resolved.targets.map((target) => ({
          ...target,
          authorNameArabic: input.authorArabic,
          poemId: canonicalId,
          activeSourceFingerprint: {
            algorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
            lineNfcHash: sha256(sourceLineNfcHashBody(input.linesArabic)),
            promptMaterialHash: sha256(sourcePromptMaterialHashBody(input)),
          },
        })),
      });
      cache.complete(
        fingerprint,
        ProductionResolutionResponseSchema.parse({
          ...body,
          manifestHash: sha256(canonicalJson(body)),
        }),
      );
      expect(reconciler.cycle(NOW)).toMatchObject({
        pendingResolution: 0,
        seeded: 1,
        superseded: 1,
      });
      expect(ledger.get(source.workKey)?.state).toBe("imported");
      const [replacement] = ledger.listReadyWork(
        SOL_ENRICHMENT_WORK_KIND,
        10,
        {
          implementationVersion: SOL_PIPELINE_VERSION,
          schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
        },
        NOW,
      );
      expect(replacement?.input).toMatchObject({
        canonicalBinding: { poemId: canonicalId },
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
        authorArabic: input.authorArabic,
      });
      expect(reconciler.cycle(NOW).seeded).toBe(0);
      cache.close();
      ledger.close();
    },
  );

  it("persists a cursor so an unresolved high-priority row cannot starve resolved work", async () => {
    const root = await mkdtemp(join(tmpdir(), "legacy-source-cursor-"));
    ROOTS.push(root);
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const cache = new ProductionResolutionDemandCache({
      now: () => NOW,
      path: join(root, "resolution.sqlite3"),
    });
    const high = legacyInput("high");
    const low = legacyInput("low");
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: high,
        inputHash: inputHash(high),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 300,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW,
    );
    ledger.seed(
      {
        implementationVersion: SOL_PIPELINE_VERSION,
        input: low,
        inputHash: inputHash(low),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 200,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      NOW + 1,
    );
    cache.registerPublicationDemand(
      low.poemId,
      ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      low.sourceRevisionId,
      200,
    );
    const claim = requiredClaim(cache.claim());
    cache.complete(claim, response(claim, low));

    expect(
      new LegacySourceBindingReconciler({ batchSize: 1, cache, ledger }).cycle(
        NOW + 2,
      ),
    ).toMatchObject({ pendingResolution: 1, seeded: 0 });
    expect(
      new LegacySourceBindingReconciler({ batchSize: 1, cache, ledger }).cycle(
        NOW + 3,
      ),
    ).toMatchObject({ pendingResolution: 0, seeded: 1, superseded: 1 });
    expect(
      new LegacySourceBindingReconciler({ batchSize: 1, cache, ledger }).cycle(
        NOW + 4,
      ),
    ).toMatchObject({ pendingResolution: 1, seeded: 0 });

    cache.close();
    ledger.close();
  });
});

function legacyInput(id = "1"): PoemEnrichmentInput {
  return {
    authorArabic: "اسم قديم",
    linesArabic: ["صدر", "عجز"],
    poemId: sha256(`poem:${id}`),
    schemaId: "saqi.poem-enrichment-input",
    schemaVersion: 1,
    sourceContentSha256: sha256(canonicalJson({ content: ["صدر", "عجز"] })),
    sourceRevisionId: sha256(`revision:${id}`),
    titleArabic: "قصيدة",
  };
}

function requiredClaim(
  claim: null | ProductionResolutionDemandClaim,
): ProductionResolutionDemandClaim {
  if (!claim) throw new Error("Expected production resolution demand");
  return claim;
}

function response(
  claim: ProductionResolutionDemandClaim,
  input: PoemEnrichmentInput,
  currentRevisionId = input.sourceRevisionId,
) {
  const body = ProductionResolutionResponseBodySchema.parse({
    expiresAt: new Date(NOW + 15 * 60_000).toISOString(),
    observedAt: new Date(NOW).toISOString(),
    schemaId: "saqi.production-resolution-response",
    schemaVersion: 1,
    scopeHash: claim.scopeHash,
    targets: [
      {
        authorId: sha256("author:1"),
        authorNameArabic: "الشاعر الموثق",
        currentSourceNfcSha256: sha256(
          sourceLineNfcHashBody(input.linesArabic),
        ),
        currentSourceRevisionId: currentRevisionId,
        modelPointers: [{ modelKey: "sol-5.6", pointerVersion: 1 }],
        poemId: input.poemId,
        sourceAuthorSlug: "author-1",
        sourcePoemId: "1",
        sourcePointerVersion: 2,
      },
    ],
    writerEpoch: 7,
  });
  return ProductionResolutionResponseSchema.parse({
    ...body,
    manifestHash: sha256(canonicalJson(body)),
  });
}
