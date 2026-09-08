import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  canonicalPoemBindingIdBody,
  type CorpusImportAction,
  ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_ID,
  ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_VERSION,
  ENRICHMENT_PUBLICATION_SCHEMA_ID,
  ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
  EnrichmentPublicationV2RequestSchema,
  MAX_CORPUS_IMPORT_BYTES,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { describe, expect, it, vi } from "vitest";

import { ArtifactStore } from "../persistence/artifact-store";
import { Ledger } from "../persistence/ledger";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key";
import { prepareCollectedPoem } from "../publication/corpus-import-actions";
import {
  PublicationClient,
  type PublicationTransport,
} from "../publication/publication-client";
import {
  MAX_DIRECT_PUBLICATION_BATCH_ITEMS,
  MAX_DIRECT_PUBLICATION_BATCH_REQUESTS,
  MAX_DIRECT_PUBLICATION_CLAIMS,
  planCollectionPublicationChunks,
  prepareBoundEnrichmentPublication,
  prepareEnrichmentPublication,
  PublicationLane,
} from "../publication/publication-lane";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const HASH = "a".repeat(64);

function collected(id: number) {
  const numericId = String(id);
  return prepareCollectedPoem(
    {
      artifactSchemaVersion: 1,
      collectedBy: "fixture",
      source: {
        author: {
          canonicalId: "source:author:a",
          href: "https://source.invalid/writers/a",
          path: "/writers/a",
          slug: "a",
        },
        canonicalId: `source:poem:${numericId}`,
        href: `https://source.invalid/works/${numericId}`,
        lines: ["صدر", "عجز"],
        numericId,
        slug: `work-${numericId}`,
        structure: "classical",
        title: `قصيدة ${numericId}`,
        verses: 1,
      },
      sourceHash: HASH,
      workKey: "fixture",
    },
    {
      authorId: "author-1",
      authorNameArabic: "شاعر",
      poemId: `poem-${numericId}`,
      sourceAuthorSlug: "a",
      sourcePoemId: numericId,
    },
    "2026-08-25T12:00:00.000Z",
    1,
  );
}

function collectedArtifact(id: number) {
  const numericId = String(id);
  return {
    source: {
      author: {
        canonicalId: "source:author:a",
        href: "https://source.invalid/writers/a",
        path: "/writers/a",
        slug: "a",
      },
      canonicalId: `source:poem:${numericId}`,
      href: `https://source.invalid/works/${numericId}`,
      lines: ["صدر", "عجز"],
      numericId,
      slug: `work-${numericId}`,
      structure: "classical",
      title: `قصيدة ${numericId}`,
      verses: 1,
    },
  } as const;
}

function enrichmentArtifact(
  index = 1,
  lineCount = 1,
  translatedLineLength = 4,
) {
  const linesArabic = Array.from({ length: lineCount }, () => "صدر");
  const output = {
    schemaId: "saqi.poem-enrichment-output",
    schemaVersion: 2,
    translation: {
      lines: linesArabic.map(() => "x".repeat(translatedLineLength)),
    },
    wordGlosses: {
      lines: linesArabic.map((_, lineIndex) => ({
        segments: [
          {
            kind: "word",
            meaning: "opening hemistich",
            surface: "صدر",
            tokenIndex: 0,
          },
        ],
        lineIndex,
      })),
      tokenizerVersion: "saqi-orthographic-v1",
    },
  };
  const sourceRevisionId =
    index === 1 ? HASH : sha256(`revision-${String(index)}`);
  return {
    generationAttemptId: `g-${String(index)}`,
    input: {
      authorArabic: "شاعر",
      linesArabic,
      poemId: `poem-${String(index)}`,
      schemaId: "saqi.poem-enrichment-input",
      schemaVersion: 1,
      sourceContentSha256: sha256(canonicalJson(linesArabic)),
      sourceRevisionId,
      titleArabic: "قصيدة",
    },
    output,
    outputHash: sha256(canonicalJson(output)),
    pipelineVersion: "sol-word-gloss-v2",
    reviewAttemptIds: [`r1-${String(index)}`, `r2-${String(index)}`],
    reviews: [
      { fidelityScore: 100, findings: [], insightScore: 100, verdict: "pass" },
      { fidelityScore: 99, findings: [], insightScore: 99, verdict: "pass" },
    ],
  } as const;
}

function boundEnrichmentArtifact(
  index = 1,
  lineCount = 1,
  translatedLineLength = 4,
) {
  const legacy = enrichmentArtifact(index, lineCount, translatedLineLength);
  const identity = {
    authorId: "author-1",
    authorNameArabic: legacy.input.authorArabic,
    externalPoemId: String(41 + index),
    lineNfcHash: sha256(sourceLineNfcHashBody(legacy.input.linesArabic)),
    poemId: legacy.input.poemId,
    promptMaterialHash: sha256(
      sourcePromptMaterialHashBody({
        authorArabic: legacy.input.authorArabic,
        linesArabic: legacy.input.linesArabic,
        titleArabic: legacy.input.titleArabic,
      }),
    ),
    schemaId: "saqi.canonical-poem-binding" as const,
    schemaVersion: 1 as const,
    sourceName: "source" as const,
    sourceRevisionId: legacy.input.sourceRevisionId,
  };
  const binding = {
    admissionEvidence: {
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5" as const,
      issuedAt: "2026-09-01T12:00:00Z",
      sourcePointerVersion: 7,
    },
    bindingId: sha256(canonicalPoemBindingIdBody(identity)),
    ...identity,
  };
  return {
    artifact: {
      ...legacy,
      input: {
        ...legacy.input,
        canonicalBinding: binding,
        schemaVersion: 2 as const,
      },
      model: "gpt-5.6-sol" as const,
      modelKey: "sol-5.6" as const,
      provider: "sol" as const,
      reasoningEffort: "high" as const,
    },
    binding,
  };
}

function succeededSource(
  ledger: Ledger,
  artifactHash: string,
  value = "source",
) {
  const input = { value };
  const seeded = ledger.seed(
    {
      implementationVersion: "v1",
      input,
      inputHash: inputHash(input),
      kind: "source",
      priority: 0,
      schemaVersion: "source@1",
    },
    0,
  );
  const claim = ledger.claim("fixture", 0, 1_000, ["source"]);
  if (!claim) throw new Error("claim missing");
  ledger.succeed(claim, artifactHash, 1);
  return seeded.workKey;
}

async function seedBoundPublication(
  ledger: Ledger,
  artifacts: ArtifactStore,
  index: number,
  options: { lineCount?: number; translatedLineLength?: number } = {},
) {
  const { artifact, binding } = boundEnrichmentArtifact(
    index,
    options.lineCount,
    options.translatedLineLength,
  );
  const stored = await artifacts.put(`${canonicalJson(artifact)}\n`);
  const input = artifact.input;
  const translation = ledger.seed(
    {
      implementationVersion: "sol-5.6",
      input,
      inputHash: inputHash(input),
      kind: "poem-enrichment-sol",
      priority: 100,
      schemaVersion: "saqi.poem-enrichment-input@2",
    },
    index * 10,
  );
  const translationClaim = ledger.claim(
    `sol-${String(index)}`,
    index * 10 + 1,
    1_000,
    ["poem-enrichment-sol"],
  );
  if (!translationClaim) throw new Error("translation claim missing");
  const direct = ledger.completeApprovedAndSeedPublication(
    translationClaim,
    stored.hash,
    binding,
    { implementationVersion: "source-bound-publication-v1", priority: 100 },
    index * 10 + 2,
  );
  return { direct, translation };
}

function publishedOutcome(
  item: ReturnType<typeof prepareBoundEnrichmentPublication>,
  pointerVersion: number,
) {
  return {
    receipt: {
      actionHash: item.actionHash,
      artifactId: item.artifact.id,
      committedAt: "2026-09-01T12:01:00Z",
      modelKey: item.artifact.modelKey,
      outcome: "published" as const,
      poemId: item.binding.poemId,
      pointerVersion,
      promptVersion: item.artifact.promptVersion,
      publicationIntentId: item.publicationIntentId,
      schemaId: ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_ID,
      schemaVersion: ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_VERSION,
      sourceRevisionId: item.artifact.sourceRevisionId,
      writerEpoch: 4,
    },
    status: "published" as const,
  };
}

describe("publication lane", () => {
  it("treats a superseded legacy publication as terminal rather than retrying", async () => {
    const { artifact, binding } = boundEnrichmentArtifact();
    const transport = vi.fn(async () => ({
      body: JSON.stringify({
        error: "LEGACY_SOL_PUBLICATION_SUPERSEDED",
        ok: false,
        retryable: false,
      }),
      status: 409,
    }));
    const client = new PublicationClient({
      endpoint: "https://ops.saqi.app/api/corpus-import",
      transport,
    });
    await expect(
      client.sendBoundEnrichment({
        items: [
          prepareBoundEnrichmentPublication(artifact, binding, "b".repeat(64)),
        ],
        schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
        schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
      }),
    ).resolves.toEqual({
      errorCode: "LEGACY_SOL_PUBLICATION_SUPERSEDED",
      state: "rejected",
    });
    expect(transport).toHaveBeenCalledOnce();
  });

  it.each([
    ["sol-word-gloss-v2", "high"],
    ["sol-word-gloss-v3", "medium"],
  ])(
    "preserves exact publication provenance for %s",
    (pipelineVersion, reasoningEffort) => {
      const { artifact, binding } = boundEnrichmentArtifact();
      expect(
        prepareBoundEnrichmentPublication(
          { ...artifact, pipelineVersion, reasoningEffort },
          binding,
          "b".repeat(64),
        ),
      ).toMatchObject({
        artifact: { promptVersion: pipelineVersion, reasoningEffort },
      });
    },
  );

  it.each([
    ["sol-word-gloss-v2", "medium"],
    ["sol-word-gloss-v3", "high"],
  ])(
    "rejects mismatched publication provenance for %s",
    (pipelineVersion, reasoningEffort) => {
      const { artifact, binding } = boundEnrichmentArtifact();
      expect(() =>
        prepareBoundEnrichmentPublication(
          { ...artifact, pipelineVersion, reasoningEffort },
          binding,
          "b".repeat(64),
        ),
      ).toThrow("BOUND_ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
    },
  );

  it("prepares an approved legacy artifact only with an exact production binding", () => {
    const { artifact: boundArtifact, binding } = boundEnrichmentArtifact();
    expect(() =>
      prepareBoundEnrichmentPublication(boundArtifact, binding, "b".repeat(64)),
    ).not.toThrow();
    const { canonicalBinding: _canonicalBinding, ...legacyInput } =
      boundArtifact.input;
    const legacyArtifact = {
      ...boundArtifact,
      input: { ...legacyInput, schemaVersion: 1 as const },
    };

    expect(
      prepareBoundEnrichmentPublication(
        legacyArtifact,
        binding,
        "b".repeat(64),
      ),
    ).toMatchObject({
      artifact: { sourceRevisionId: binding.sourceRevisionId },
      binding: { bindingId: binding.bindingId },
    });
    expect(() =>
      prepareBoundEnrichmentPublication(
        {
          ...legacyArtifact,
          input: { ...legacyArtifact.input, titleArabic: "عنوان مختلف" },
        },
        binding,
        "b".repeat(64),
      ),
    ).toThrow("BOUND_ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
  });

  it("publishes a source-bound translation through v2 and confirms both ledger rows atomically", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const { artifact, binding } = boundEnrichmentArtifact();
    const stored = await artifacts.put(`${canonicalJson(artifact)}\n`);
    const input = artifact.input;
    const translation = ledger.seed(
      {
        implementationVersion: "sol-5.6",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 100,
        schemaVersion: "saqi.poem-enrichment-input@2",
      },
      1,
    );
    const translationClaim = ledger.claim("sol", 2, 1_000, [
      "poem-enrichment-sol",
    ]);
    if (!translationClaim) throw new Error("translation claim missing");
    const direct = ledger.completeApprovedAndSeedPublication(
      translationClaim,
      stored.hash,
      binding,
      { implementationVersion: "source-bound-publication-v1", priority: 100 },
      3,
    );
    let requests = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body, url }) => {
          requests += 1;
          expect(url).toBe("http://localhost/api/v2/enrichment-publications");
          const request = JSON.parse(body) as {
            items: {
              actionHash: string;
              artifact: {
                id: string;
                modelKey: "sol-5.6";
                promptVersion: "sol-word-gloss-v2";
                sourceRevisionId: string;
              };
              binding: { poemId: string };
              publicationIntentId: string;
            }[];
          };
          const item = request.items[0];
          if (!item) throw new Error("publication item missing");
          return {
            body: JSON.stringify({
              results: [
                {
                  receipt: {
                    actionHash: item.actionHash,
                    artifactId: item.artifact.id,
                    committedAt: "2026-09-01T12:01:00Z",
                    modelKey: item.artifact.modelKey,
                    outcome: "published",
                    poemId: item.binding.poemId,
                    pointerVersion: 8,
                    promptVersion: item.artifact.promptVersion,
                    publicationIntentId: item.publicationIntentId,
                    schemaId: ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_ID,
                    schemaVersion:
                      ENRICHMENT_PUBLICATION_RECEIPT_SCHEMA_VERSION,
                    sourceRevisionId: item.artifact.sourceRevisionId,
                    writerEpoch: 4,
                  },
                  status: "published",
                },
              ],
              schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
              schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "publication",
    });

    const summary = await lane.run(undefined, {
      maximum: 1,
      now: () => 4,
    });
    expect(summary.confirmed).toBe(1);
    expect(requests).toBe(1);
    expect(ledger.get(translation.workKey)?.state).toBe("imported");
    expect(ledger.get(direct.publicationWorkKey)).toMatchObject({
      outputArtifactHash: expect.stringMatching(/^[a-f\d]{64}$/),
      state: "succeeded",
    });
    ledger.close();
  });

  it.each(["EAGAIN", "EBUSY", "EINTR", "EIO", "EMFILE", "ENFILE"])(
    "retries transient artifact %s without losing accepted publication",
    async (code) => {
      const root = mkdtempSync(join(tmpdir(), "publication-artifact-io-"));
      const artifacts = new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      });
      const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
      const { direct, translation } = await seedBoundPublication(
        ledger,
        artifacts,
        1,
      );
      vi.spyOn(artifacts, "read").mockRejectedValueOnce(
        Object.assign(new Error("temporary read failure"), { code }),
      );
      const transport = vi.fn<PublicationTransport>(async ({ body }) => {
        const request = EnrichmentPublicationV2RequestSchema.parse(
          JSON.parse(body),
        );
        return {
          status: 200,
          body: JSON.stringify({
            schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
            schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            results: request.items.map((item) => publishedOutcome(item, 8)),
          }),
        };
      });
      const lane = new PublicationLane({
        artifacts,
        ledger,
        owner: "publication",
        client: new PublicationClient({
          endpoint: "https://ops.saqi.app/api/corpus-import",
          transport,
        }),
      });
      await expect(
        lane.run(undefined, { maximum: 1, now: () => 20 }),
      ).resolves.toMatchObject({ retried: 1, deadLettered: 0 });
      expect(transport).not.toHaveBeenCalled();
      const pending = ledger.get(direct.publicationWorkKey);
      expect(pending).toMatchObject({
        state: "retry_wait",
        lastErrorCode: "BOUND_PUBLICATION_ARTIFACT_IO_UNAVAILABLE",
      });
      expect(ledger.get(translation.workKey)?.state).toBe("succeeded");
      await expect(
        lane.run(undefined, {
          maximum: 1,
          now: () => (pending?.availableAt ?? 0) + 1,
        }),
      ).resolves.toMatchObject({ confirmed: 1 });
      expect(transport).toHaveBeenCalledOnce();
      expect(ledger.get(translation.workKey)?.state).toBe("imported");
      ledger.close();
    },
  );

  it.each(["invalid-json", "invalid-schema", "missing-or-corrupt"])(
    "keeps %s publication artifacts terminal",
    async (failure) => {
      const root = mkdtempSync(join(tmpdir(), "publication-corrupt-"));
      const artifacts = new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      });
      const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
      const { direct } = await seedBoundPublication(ledger, artifacts, 1);
      const read = vi.spyOn(artifacts, "read");
      if (failure === "missing-or-corrupt") {
        read.mockRejectedValueOnce(new Error("Artifact is missing or corrupt"));
      } else {
        read.mockResolvedValueOnce(
          Buffer.from(failure === "invalid-json" ? "not JSON" : "{}"),
        );
      }
      const transport = vi.fn<PublicationTransport>();
      const lane = new PublicationLane({
        artifacts,
        ledger,
        owner: "publication",
        client: new PublicationClient({
          endpoint: "https://ops.saqi.app/api/corpus-import",
          transport,
        }),
      });
      await expect(
        lane.run(undefined, { maximum: 1, now: () => 20 }),
      ).resolves.toMatchObject({ deadLettered: 1, retried: 0 });
      expect(ledger.get(direct.publicationWorkKey)).toMatchObject({
        state: "dead_letter",
        lastErrorCode: "BOUND_PUBLICATION_LOCAL_STATE_CORRUPT",
      });
      expect(transport).not.toHaveBeenCalled();
      ledger.close();
    },
  );

  it("publishes fifty direct claims through five concurrent requests and maps reversed outcomes by publication intent", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-batch-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const seeded = [];
    for (let index = 1; index <= MAX_DIRECT_PUBLICATION_CLAIMS; index += 1) {
      seeded.push(await seedBoundPublication(ledger, artifacts, index));
    }
    const requestSizes: number[] = [];
    const release = Promise.withResolvers<undefined>();
    let activeRequests = 0;
    let maximumActiveRequests = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          const request = EnrichmentPublicationV2RequestSchema.parse(
            JSON.parse(body),
          );
          activeRequests += 1;
          maximumActiveRequests = Math.max(
            maximumActiveRequests,
            activeRequests,
          );
          requestSizes.push(request.items.length);
          if (activeRequests === MAX_DIRECT_PUBLICATION_BATCH_REQUESTS)
            release.resolve(undefined);
          await release.promise;
          activeRequests -= 1;
          return {
            body: JSON.stringify({
              results: request.items
                .map((item, index) => publishedOutcome(item, index + 1))
                .toReversed(),
              schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
              schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "publication",
    });

    await expect(
      lane.run(undefined, {
        maximum: MAX_DIRECT_PUBLICATION_CLAIMS,
        now: () => Date.now(),
      }),
    ).resolves.toMatchObject({
      claimed: MAX_DIRECT_PUBLICATION_CLAIMS,
      confirmed: MAX_DIRECT_PUBLICATION_CLAIMS,
    });
    expect(requestSizes).toEqual(
      Array.from(
        { length: MAX_DIRECT_PUBLICATION_BATCH_REQUESTS },
        () => MAX_DIRECT_PUBLICATION_BATCH_ITEMS,
      ),
    );
    expect(maximumActiveRequests).toBe(MAX_DIRECT_PUBLICATION_BATCH_REQUESTS);
    for (const { direct, translation } of seeded) {
      expect(ledger.get(translation.workKey)?.state).toBe("imported");
      expect(ledger.get(direct.publicationWorkKey)?.state).toBe("succeeded");
    }
    ledger.close();
  });

  it("splits direct requests by their exact serialized UTF-8 byte size", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-bytes-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    for (let index = 1; index <= 3; index += 1) {
      await seedBoundPublication(ledger, artifacts, index, {
        lineCount: 160,
        translatedLineLength: 5_000,
      });
    }
    const requestBytes: number[] = [];
    const requestItems: number[] = [];
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          const request = EnrichmentPublicationV2RequestSchema.parse(
            JSON.parse(body),
          );
          requestBytes.push(Buffer.byteLength(body));
          requestItems.push(request.items.length);
          return {
            body: JSON.stringify({
              results: request.items.map((item, index) =>
                publishedOutcome(item, index + 1),
              ),
              schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
              schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "publication",
    });

    await expect(
      lane.run(undefined, { maximum: 3, now: () => Date.now() }),
    ).resolves.toMatchObject({ claimed: 3, confirmed: 3 });
    expect(requestItems).toEqual([2, 1]);
    expect(
      requestBytes.every((bytes) => bytes <= MAX_CORPUS_IMPORT_BYTES),
    ).toBe(true);
    ledger.close();
  });

  it("renews every direct claim while one batched request is in flight", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-heartbeat-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    for (let index = 1; index <= 3; index += 1) {
      await seedBoundPublication(ledger, artifacts, index);
    }
    let now = 1_000;
    const heartbeat = Promise.withResolvers<undefined>();
    const originalRenewMany = ledger.renewMany.bind(ledger);
    const renewMany = vi
      .spyOn(ledger, "renewMany")
      .mockImplementation(
        (claims, renewedAt, leaseDurationMs, allowExpired) => {
          const result = originalRenewMany(
            claims,
            renewedAt,
            leaseDurationMs,
            allowExpired,
          );
          heartbeat.resolve(undefined);
          return result;
        },
      );
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          now = 1_050;
          await heartbeat.promise;
          now = 1_125;
          const request = EnrichmentPublicationV2RequestSchema.parse(
            JSON.parse(body),
          );
          return {
            body: JSON.stringify({
              results: request.items.map((item, index) =>
                publishedOutcome(item, index + 1),
              ),
              schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
              schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            }),
            status: 200,
          };
        },
      }),
      leaseDurationMs: 100,
      leaseHeartbeatMs: 20,
      ledger,
      owner: "publication",
    });

    await expect(
      lane.run(undefined, { maximum: 3, now: () => now }),
    ).resolves.toMatchObject({ claimed: 3, confirmed: 3 });
    expect(renewMany).toHaveBeenCalledOnce();
    expect(renewMany.mock.calls[0]?.[0]).toHaveLength(3);
    ledger.close();
  });

  it("releases every direct claim after an aborted unknown outcome and replays the batch idempotently", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-abort-batch-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const seeded = [];
    for (let index = 1; index <= 12; index += 1)
      seeded.push(await seedBoundPublication(ledger, artifacts, index));
    const started = Promise.withResolvers<undefined>();
    let calls = 0;
    let replaying = false;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: ({ body, signal }) => {
          calls += 1;
          if (!replaying) {
            started.resolve(undefined);
            return new Promise((_resolve, reject) => {
              signal?.addEventListener(
                "abort",
                () => reject(new Error("outcome unknown after send")),
                { once: true },
              );
            });
          }
          const request = EnrichmentPublicationV2RequestSchema.parse(
            JSON.parse(body),
          );
          return Promise.resolve({
            body: JSON.stringify({
              results: request.items.map((item, index) =>
                publishedOutcome(item, index + 1),
              ),
              schemaId: ENRICHMENT_PUBLICATION_SCHEMA_ID,
              schemaVersion: ENRICHMENT_PUBLICATION_SCHEMA_VERSION,
            }),
            status: 200,
          });
        },
      }),
      ledger,
      owner: "publication",
    });
    const controller = new AbortController();
    const firstRun = lane.run(controller.signal, {
      maximum: 12,
      now: () => Date.now(),
    });
    await started.promise;
    controller.abort(new Error("operator stop"));

    await expect(firstRun).resolves.toMatchObject({
      claimed: 12,
      confirmed: 0,
      stopped: "aborted",
    });
    for (const { direct } of seeded)
      expect(ledger.get(direct.publicationWorkKey)).toMatchObject({
        attemptCount: 0,
        lastErrorCode: "PUBLICATION_OPERATOR_STOP",
        state: "pending",
      });
    replaying = true;
    await expect(
      lane.run(undefined, { maximum: 12, now: () => Date.now() }),
    ).resolves.toMatchObject({ claimed: 12, confirmed: 12 });
    expect(calls).toBe(4);
    ledger.close();
  });

  it("releases a partially assembled direct batch when publication is paused", async () => {
    const root = mkdtempSync(join(tmpdir(), "bound-publication-pause-batch-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const seeded = [];
    for (let index = 1; index <= 3; index += 1)
      seeded.push(await seedBoundPublication(ledger, artifacts, index));
    let pauseChecks = 0;
    let requests = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: () => {
          requests += 1;
          throw new Error("paused publication must not reach transport");
        },
      }),
      ledger,
      owner: "publication",
    });

    await expect(
      lane.run(undefined, {
        maximum: 3,
        now: () => Date.now(),
        paused: () => {
          pauseChecks += 1;
          return pauseChecks > 1;
        },
      }),
    ).resolves.toMatchObject({ claimed: 1, confirmed: 0, stopped: "paused" });
    expect(requests).toBe(0);
    expect(
      seeded.map(({ direct }) => ledger.get(direct.publicationWorkKey)?.state),
    ).toEqual(["pending", "pending", "pending"]);
    expect(
      ledger.get(seeded[0]?.direct.publicationWorkKey ?? "")?.attemptCount,
    ).toBe(0);
    ledger.close();
  });

  it("builds deterministic, bounded, stable-ID chunks independent of input order", () => {
    const sources = Array.from({ length: 55 }, (_, index) => ({
      artifactHash: String(index + 1)
        .padStart(64, "a")
        .slice(-64),
      workKey: String(index + 1)
        .padStart(64, "b")
        .slice(-64),
    }));
    const candidates = sources.map((source, index) => ({
      source,
      stageAction: collected(index + 1).stageAction,
    }));
    const forward = planCollectionPublicationChunks(candidates);
    const reverse = planCollectionPublicationChunks(candidates.toReversed());
    expect(forward).toEqual(reverse);
    expect(
      forward.map(({ action }) =>
        action.action === "stage-and-plan" ? action.input.records.length : 0,
      ),
    ).toEqual([50, 5]);
    for (const { action } of forward)
      expect(Buffer.byteLength(canonicalJson(action))).toBeLessThan(
        2 * 1_024 * 1_024,
      );
  });

  it("uses the complete immutable record body for chunk identity", () => {
    const first = collected(1);
    const action = first.stageAction;
    if (action.action !== "stage-and-plan") throw new Error("stage action");
    const source = { artifactHash: "a".repeat(64), workKey: "b".repeat(64) };
    const observedLater = {
      ...action,
      input: {
        ...action.input,
        records: action.input.records.map((record) => ({
          ...record,
          observedAt: "2026-08-26T12:00:00.000Z",
        })),
      },
    } satisfies CorpusImportAction;
    const [originalChunk] = planCollectionPublicationChunks([
      { source, stageAction: action },
    ]);
    const [laterChunk] = planCollectionPublicationChunks([
      { source, stageAction: observedLater },
    ]);
    if (
      originalChunk?.action.action !== "stage-and-plan" ||
      laterChunk?.action.action !== "stage-and-plan"
    )
      throw new Error("chunk missing");
    expect(laterChunk.action.input.bundle.id).not.toBe(
      originalChunk.action.input.bundle.id,
    );
  });

  it("compacts queued single-record collection jobs before remote publication", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-batch-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const candidates = [];
    for (const id of [1, 2]) {
      const stored = await artifacts.put(
        `${canonicalJson(collectedArtifact(id))}\n`,
      );
      const source = {
        artifactHash: stored.hash,
        workKey: succeededSource(ledger, stored.hash, `source-${String(id)}`),
      };
      candidates.push({ source, stageAction: collected(id).stageAction });
    }
    const recordsByBundle = new Map<string, number>();
    const stageSizes: number[] = [];
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          const action = JSON.parse(body) as {
            action: string;
            bundleId?: string;
            expectedPlanHash?: string;
            input?: {
              bundle: { id: string; writerEpoch: number };
              records: unknown[];
            };
          };
          if (action.action === "stage-and-plan") {
            const size = action.input?.records.length ?? 0;
            const bundleId = action.input?.bundle.id ?? "";
            stageSizes.push(size);
            recordsByBundle.set(bundleId, size);
            return {
              body: JSON.stringify({
                ok: true,
                result: {
                  bundleId,
                  items: Array.from({ length: size }, () => ({})),
                  planHash: HASH,
                  writerEpoch: action.input?.bundle.writerEpoch,
                },
              }),
              status: 200,
            };
          }
          const size = recordsByBundle.get(action.bundleId ?? "") ?? 0;
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                advancedPointers: size,
                bundleId: action.bundleId,
                insertedRevisions: size,
                planHash: action.expectedPlanHash,
                reusedRevisions: 0,
                unchangedPointers: 0,
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "test",
    });
    for (const candidate of candidates)
      await lane.seedCollection(
        planCollectionPublicationChunks([candidate]),
        200,
      );

    await expect(
      lane.run(undefined, { maximum: 50, now: () => Date.now() + 1_000 }),
    ).resolves.toMatchObject({ claimed: 1, confirmed: 1 });
    expect(stageSizes).toEqual([2]);
    for (const candidate of candidates)
      expect(ledger.get(candidate.source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("replays server-confirmed promote after a lost response and imports only after confirmation", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-lane-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(collectedArtifact(1))}\n`,
    );
    const sourceArtifact = storedSourceArtifact.hash;
    const source = {
      artifactHash: sourceArtifact,
      workKey: succeededSource(ledger, sourceArtifact),
    };
    let promoteCalls = 0;
    let stageCalls = 0;
    const transport: PublicationTransport = async ({ body }) => {
      const action = JSON.parse(body) as {
        action: string;
        bundleId?: string;
        expectedPlanHash?: string;
        input?: { bundle: { id: string; writerEpoch: number } };
      };
      if (action.action === "stage-and-plan") {
        stageCalls += 1;
        return {
          body: JSON.stringify({
            ok: true,
            result: {
              bundleId: action.input!.bundle.id,
              items: [{}],
              planHash: HASH,
              writerEpoch: action.input!.bundle.writerEpoch,
            },
          }),
          status: 200,
        };
      }
      promoteCalls += 1;
      if (promoteCalls === 1) throw new Error("response lost after commit");
      return {
        body: JSON.stringify({
          ok: true,
          result: {
            advancedPointers: 1,
            bundleId: action.bundleId,
            insertedRevisions: 1,
            planHash: action.expectedPlanHash,
            reusedRevisions: 0,
            unchangedPointers: 0,
          },
        }),
        status: 200,
      };
    };
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        now: () => 1_000,
        transport,
      }),
      ledger,
      owner: "test",
    });
    await lane.seedCollection(
      planCollectionPublicationChunks([
        { source, stageAction: collected(1).stageAction },
      ]),
      0,
    );
    const runAt = Date.now() + 1_000;
    const firstRun = await lane.run(undefined, {
      maximum: 1,
      now: () => runAt,
    });
    expect(firstRun.retried).toBe(1);
    expect(ledger.get(source.workKey)?.state).toBe("succeeded");
    const secondRun = await lane.run(undefined, {
      maximum: 1,
      now: () => runAt + 100_000,
    });
    expect(secondRun.confirmed).toBe(1);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    expect(stageCalls).toBe(1);
    expect(promoteCalls).toBe(2);
    ledger.close();
  });

  it("resumes from a durable publish receipt without repeating the remote call", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-enrichment-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const solArtifact = enrichmentArtifact();
    const provenanceArtifact = {
      ...solArtifact,
      model: "gpt-5.6-sol",
      modelKey: "sol-5.6",
      provider: "sol" as const,
      reasoningEffort: "high",
    };
    expect(() =>
      prepareEnrichmentPublication(
        { ...provenanceArtifact, provider: "retired-provider" },
        {
          expectedPointerVersion: null,
          source: { artifactHash: HASH, workKey: HASH },
          taskKey: HASH,
          writerEpoch: 1,
        },
      ),
    ).toThrow("ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
    expect(() =>
      prepareEnrichmentPublication(
        { ...provenanceArtifact, reviewAttemptIds: ["r1", "r1"] },
        {
          expectedPointerVersion: null,
          source: { artifactHash: HASH, workKey: HASH },
          taskKey: HASH,
          writerEpoch: 1,
        },
      ),
    ).toThrow("ENRICHMENT_ARTIFACT_NOT_PUBLISHABLE");
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(solArtifact)}\n`,
    );
    const sourceArtifact = storedSourceArtifact.hash;
    const source = {
      artifactHash: sourceArtifact,
      workKey: succeededSource(ledger, sourceArtifact),
    };
    const action = prepareEnrichmentPublication(solArtifact, {
      expectedPointerVersion: null,
      source,
      taskKey: source.workKey,
      writerEpoch: 1,
    });
    let calls = 0;
    let crash = true;
    const lane = new PublicationLane({
      artifacts,
      beforeMarkImported: () => {
        if (crash) {
          crash = false;
          throw new Error("crash boundary");
        }
      },
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async () => {
          calls += 1;
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                authorSlug: "author-1",
                poemId: "poem-1",
                pointerVersion: 1,
                state: "published",
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "test",
    });
    await lane.seedEnrichment(action, source);
    const runAt = Date.now() + 1_000;
    const firstRun = await lane.run(undefined, {
      maximum: 1,
      now: () => runAt,
    });
    expect(firstRun.retried).toBe(1);
    expect(ledger.get(source.workKey)?.state).toBe("succeeded");
    const secondRun = await lane.run(undefined, {
      maximum: 1,
      now: () => runAt + 100_000,
    });
    expect(secondRun.confirmed).toBe(1);
    expect(calls).toBe(1);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("replays the same collection-promotion wake after a crash without repeating remote calls", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-collection-wake-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(collectedArtifact(1))}\n`,
    );
    const source = {
      artifactHash: storedSourceArtifact.hash,
      workKey: succeededSource(ledger, storedSourceArtifact.hash),
    };
    const wakeEvents: unknown[] = [];
    let crash = true;
    let remoteCalls = 0;
    const lane = new PublicationLane({
      artifacts,
      beforeMarkImported: () => {
        if (crash) {
          crash = false;
          throw new Error("crash after collection wake");
        }
      },
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          remoteCalls += 1;
          const action: unknown = JSON.parse(body);
          const parsed = action as {
            action: string;
            bundleId?: string;
            expectedPlanHash?: string;
            input?: { bundle: { id: string; writerEpoch: number } };
          };
          return parsed.action === "stage-and-plan"
            ? {
                body: JSON.stringify({
                  ok: true,
                  result: {
                    bundleId: parsed.input?.bundle.id,
                    items: [{}],
                    planHash: HASH,
                    writerEpoch: parsed.input?.bundle.writerEpoch,
                  },
                }),
                status: 200,
              }
            : {
                body: JSON.stringify({
                  ok: true,
                  result: {
                    advancedPointers: 1,
                    bundleId: parsed.bundleId,
                    insertedRevisions: 1,
                    planHash: parsed.expectedPlanHash,
                    reusedRevisions: 0,
                    unchangedPointers: 0,
                  },
                }),
                status: 200,
              };
        },
      }),
      ledger,
      onCollectionPromoted: (event) => {
        wakeEvents.push(event);
      },
      owner: "test",
    });
    const [seeded] = await lane.seedCollection(
      planCollectionPublicationChunks([
        { source, stageAction: collected(1).stageAction },
      ]),
    );
    if (!seeded) throw new Error("publication seed missing");
    const runAt = Date.now() + 1_000;
    await expect(
      lane.run(undefined, { maximum: 1, now: () => runAt }),
    ).resolves.toMatchObject({ retried: 1 });
    await expect(
      lane.run(undefined, { maximum: 1, now: () => runAt + 100_000 }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(remoteCalls).toBe(2);
    expect(wakeEvents).toEqual([
      {
        eventId: seeded.workKey,
        targets: [{ sourceAuthorSlug: "a", sourcePoemId: "1" }],
      },
      {
        eventId: seeded.workKey,
        targets: [{ sourceAuthorSlug: "a", sourcePoemId: "1" }],
      },
    ]);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("creates one durable successor after a typed pointer conflict", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-successor-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = enrichmentArtifact();
    const storedArtifact = await artifacts.put(`${canonicalJson(artifact)}\n`);
    const artifactHash = storedArtifact.hash;
    const source = {
      artifactHash,
      workKey: succeededSource(ledger, artifactHash),
    };
    const action = prepareEnrichmentPublication(artifact, {
      expectedPointerVersion: 1,
      source,
      taskKey: source.workKey,
      writerEpoch: 1,
    });
    let calls = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          calls += 1;
          return calls === 1
            ? {
                body: JSON.stringify({
                  conflict: { kind: "pointer", retryable: true },
                  error: "PUBLICATION_POINTER_CONFLICT",
                  ok: false,
                }),
                status: 409,
              }
            : {
                body: JSON.stringify({
                  ok: true,
                  result: {
                    authorSlug: "author-1",
                    poemId: "poem-1",
                    pointerVersion: 3,
                    state: "published",
                  },
                }),
                status: 200,
              };
        },
      }),
      ledger,
      refreshEnrichment: () => ({
        expectedPointerVersion: 2,
        writerEpoch: 2,
      }),
    });
    const original = await lane.seedEnrichment(action, source);
    const first = await lane.run(undefined, {
      maximum: 1,
      now: () => Date.now() + 1_000,
    });
    expect(first.deadLettered).toBe(1);
    const successor = ledger.latestCheckpoint(
      original.workKey,
      "publication-successor",
    )?.payload["successorWorkKey"];
    expect(successor).toEqual(expect.stringMatching(/^[a-f\d]{64}$/));
    await expect(
      lane.run(undefined, { maximum: 1, now: () => Date.now() + 2_000 }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(calls).toBe(2);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("keeps a conflict attempt-neutral until refresh produces a distinct successor", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-stale-refresh-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = enrichmentArtifact();
    const storedArtifact = await artifacts.put(`${canonicalJson(artifact)}\n`);
    const artifactHash = storedArtifact.hash;
    const source = {
      artifactHash,
      workKey: succeededSource(ledger, artifactHash),
    };
    const action = prepareEnrichmentPublication(artifact, {
      expectedPointerVersion: 1,
      source,
      taskKey: source.workKey,
      writerEpoch: 1,
    });
    let refreshed = { expectedPointerVersion: 1, writerEpoch: 1 };
    let calls = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          calls += 1;
          if (calls <= 2)
            return {
              body: JSON.stringify({
                conflict: { kind: "pointer", retryable: true },
                error: "PUBLICATION_POINTER_CONFLICT",
                ok: false,
              }),
              status: 409,
            };
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                authorSlug: "author-1",
                poemId: "poem-1",
                pointerVersion: 3,
                state: "published",
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      refreshEnrichment: () => refreshed,
    });
    const original = await lane.seedEnrichment(action, source);
    let now = Date.now() + 1_000;

    await expect(
      lane.run(undefined, { maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ deadLettered: 0, retried: 1 });
    expect(ledger.get(original.workKey)).toMatchObject({
      attemptCount: 0,
      state: "pending",
    });
    expect(
      ledger.latestCheckpoint(original.workKey, "publication-successor"),
    ).toBeNull();

    refreshed = { expectedPointerVersion: 2, writerEpoch: 2 };
    now += 31_000;
    await expect(
      lane.run(undefined, { maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ deadLettered: 1 });
    const successor = ledger.latestCheckpoint(
      original.workKey,
      "publication-successor",
    )?.payload["successorWorkKey"];
    expect(successor).not.toBe(original.workKey);

    await expect(
      lane.run(undefined, { maximum: 1, now: () => now + 1 }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("replans collection under the refreshed writer epoch after conflict", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-writer-successor-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(collectedArtifact(1))}\n`,
    );
    const sourceArtifact = storedSourceArtifact.hash;
    const source = {
      artifactHash: sourceArtifact,
      workKey: succeededSource(ledger, sourceArtifact),
    };
    let calls = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async ({ body }) => {
          calls += 1;
          const action = JSON.parse(body) as CorpusImportAction;
          if (calls === 1)
            return {
              body: JSON.stringify({
                conflict: { kind: "writer_epoch", retryable: true },
                error: "PUBLICATION_WRITER_EPOCH_CONFLICT",
                ok: false,
              }),
              status: 409,
            };
          if (action.action === "stage-and-plan")
            return {
              body: JSON.stringify({
                ok: true,
                result: {
                  bundleId: action.input.bundle.id,
                  items: [{}],
                  planHash: HASH,
                  writerEpoch: 2,
                },
              }),
              status: 200,
            };
          if (action.action !== "promote")
            throw new Error("Expected promotion action");
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                advancedPointers: 1,
                bundleId: action.bundleId,
                insertedRevisions: 1,
                planHash: action.expectedPlanHash,
                reusedRevisions: 0,
                unchangedPointers: 0,
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      refreshWriterEpoch: () => 2,
    });
    const [original] = await lane.seedCollection(
      planCollectionPublicationChunks([
        { source, stageAction: collected(1).stageAction },
      ]),
    );
    if (!original) throw new Error("publication seed missing");
    await expect(
      lane.run(undefined, { maximum: 1, now: () => Date.now() + 1_000 }),
    ).resolves.toMatchObject({ deadLettered: 1 });
    expect(
      ledger.latestCheckpoint(original.workKey, "publication-successor")
        ?.payload["successorWorkKey"],
    ).toEqual(expect.stringMatching(/^[a-f\d]{64}$/));
    await expect(
      lane.run(undefined, { maximum: 1, now: () => Date.now() + 2_000 }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(calls).toBe(3);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("releases an aborted publication without consuming its attempt budget", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-abort-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(collectedArtifact(1))}\n`,
    );
    const sourceArtifact = storedSourceArtifact.hash;
    const source = {
      artifactHash: sourceArtifact,
      workKey: succeededSource(ledger, sourceArtifact),
    };
    const started = Promise.withResolvers<undefined>();
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: ({ signal }) =>
          new Promise((_resolve, reject) => {
            started.resolve(undefined);
            signal?.addEventListener(
              "abort",
              () =>
                reject(
                  signal.reason instanceof Error
                    ? signal.reason
                    : new Error("Publication aborted"),
                ),
              { once: true },
            );
          }),
      }),
      ledger,
      owner: "test",
    });
    const [seeded] = await lane.seedCollection(
      planCollectionPublicationChunks([
        { source, stageAction: collected(1).stageAction },
      ]),
      0,
    );
    if (!seeded) throw new Error("publication seed missing");
    const controller = new AbortController();
    const run = lane.run(controller.signal, {
      maximum: 1,
      now: () => Date.now(),
    });
    await started.promise;
    controller.abort(new Error("operator stop"));

    await expect(run).resolves.toMatchObject({ stopped: "aborted" });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "PUBLICATION_OPERATOR_STOP",
      state: "pending",
    });
    expect(ledger.get(source.workKey)?.state).toBe("succeeded");
    ledger.close();
  });

  it("checkpoints a confirmed phase before honoring a racing operator stop", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-abort-confirmed-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const storedSourceArtifact = await artifacts.put(
      `${canonicalJson(collectedArtifact(1))}\n`,
    );
    const sourceArtifact = storedSourceArtifact.hash;
    const source = {
      artifactHash: sourceArtifact,
      workKey: succeededSource(ledger, sourceArtifact),
    };
    const controller = new AbortController();
    let promoteCalls = 0;
    let stageCalls = 0;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        allowInsecureLocalhost: true,
        endpoint: "http://localhost/api/corpus-import",
        transport: async ({ body }) => {
          const action = JSON.parse(body) as {
            action: string;
            bundleId?: string;
            expectedPlanHash?: string;
            input?: { bundle: { id: string; writerEpoch: number } };
          };
          if (action.action === "stage-and-plan") {
            stageCalls += 1;
            controller.abort(new Error("operator stop after remote success"));
            return {
              body: JSON.stringify({
                ok: true,
                result: {
                  bundleId: action.input!.bundle.id,
                  items: [{}],
                  planHash: HASH,
                  writerEpoch: action.input!.bundle.writerEpoch,
                },
              }),
              status: 200,
            };
          }
          promoteCalls += 1;
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                advancedPointers: 1,
                bundleId: action.bundleId,
                insertedRevisions: 1,
                planHash: action.expectedPlanHash,
                reusedRevisions: 0,
                unchangedPointers: 0,
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "test",
    });
    const [seeded] = await lane.seedCollection(
      planCollectionPublicationChunks([
        { source, stageAction: collected(1).stageAction },
      ]),
      0,
    );
    if (!seeded) throw new Error("publication seed missing");

    await expect(
      lane.run(controller.signal, { maximum: 1, now: () => Date.now() }),
    ).resolves.toMatchObject({ stopped: "aborted" });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "PUBLICATION_OPERATOR_STOP",
      state: "pending",
    });
    expect(stageCalls).toBe(1);
    expect(promoteCalls).toBe(0);

    await expect(
      lane.run(undefined, { maximum: 1, now: () => Date.now() }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(stageCalls).toBe(1);
    expect(promoteCalls).toBe(1);
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });
});
