import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  normalizeProductionResolutionRequest,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  ProductionResolutionRequestSchema,
  ProductionResolutionResponseBodySchema,
  PublicationIdentitySchema,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { afterEach, describe, expect, expectTypeOf, it } from "vitest";

import { collectionWorkKinds } from "../collection/collection-scheduler";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import type { WorkItem } from "../persistence/schema";
import {
  fetchScopedProductionResolution,
  productionResolutionManifestHash,
  requestScopedProductionResolution,
  requestScopedProductionResolutionWithTransport,
  ScopedProductionResolutionStore,
  writeScopedProductionResolution,
} from "../persistence/scoped-production-resolution";
import { SqliteQueryValidationError } from "../persistence/sqlite-query";
import {
  canonicalJson,
  inputHash,
  sha256,
  workKey,
} from "../persistence/work-key";
import { prepareCollectedPoem } from "../publication/corpus-import-actions";

const ROOTS: string[] = [];
const OBSERVED_AT = "2026-08-28T15:00:00.000Z";
const EXPIRES_AT = "2026-08-28T15:15:00.000Z";
const POEM_ID = "98b26349-3ff2-4541-a1d4-5058cd140886";
const AUTHOR_ID = "29a49374-9b1c-4c4f-8b17-1a5e29d887be";
const REVISION_ID = "a".repeat(64);

afterEach(() => {
  for (const root of ROOTS) rmSync(root, { force: true, recursive: true });
  ROOTS.length = 0;
});

describe("scoped production resolution", () => {
  it("atomically materializes an exact v3 scope and never synthesizes misses", async () => {
    const output = outputPath();
    const request = productionRequest();
    const response = productionResponse(request);
    const first = await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response,
    });
    expect(first).toMatchObject({
      modelCount: 1,
      poemCount: 1,
      promotion: "created",
      schemaVersion: 3,
      writerEpoch: 9,
    });
    expectTypeOf(first.poemCount).toEqualTypeOf<number>();
    expectTypeOf(first.writerEpoch).toEqualTypeOf<number>();
    expect(statSync(output).mode & 0o777).toBe(0o600);

    const store = await ScopedProductionResolutionStore.open(output, {
      now: () => Date.parse(OBSERVED_AT),
    });
    const resolution = await store.resolve(sourceWork(123), detail(123));
    expect(resolution).toMatchObject({
      mapping: {
        authorId: AUTHOR_ID,
        canonicalPoemId: POEM_ID,
        poemId: POEM_ID,
        sourceAuthorSlug: "Ibn-Farkoun",
        sourcePoemId: "123",
      },
      writerEpoch: 9,
    });
    if (!resolution) throw new Error("expected exact resolution");
    const prepared = prepareCollectedPoem(
      detail(123),
      resolution.mapping,
      resolution.observedAt,
      resolution.writerEpoch,
    );
    expect(prepared.stageAction).toMatchObject({
      action: "stage-and-plan",
      input: { records: [{ canonicalPoemId: POEM_ID }] },
    });
    await expect(
      store.resolve(sourceWork(999), detail(999)),
    ).resolves.toBeNull();
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true, REVISION_ID),
    ).resolves.toEqual({ expectedPointerVersion: 4, writerEpoch: 9 });
    await expect(
      store.resolvePublication(POEM_ID, "historical-model", false, REVISION_ID),
    ).resolves.toBeNull();
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true, "b".repeat(64)),
    ).resolves.toBeNull();

    const replay = await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response,
    });
    expect(replay.promotion).toBe("replayed");
    const replacement = productionResponse(request, 5, REVISION_ID, {
      expiresAt: "2026-08-28T15:16:00.000Z",
      observedAt: "2026-08-28T15:01:00.000Z",
    });
    const replacementReport = await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response: replacement,
    });
    expect(replacementReport.promotion).toBe("replaced");
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true, REVISION_ID),
    ).resolves.toEqual({ expectedPointerVersion: 5, writerEpoch: 9 });
    const withoutModelPointer = productionResponse(request, null, REVISION_ID, {
      expiresAt: "2026-08-28T15:17:00.000Z",
      observedAt: "2026-08-28T15:02:00.000Z",
    });
    await writeScopedProductionResolution({
      now: () => Date.parse(withoutModelPointer.observedAt),
      output,
      request,
      response: withoutModelPointer,
    });
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true, REVISION_ID),
    ).resolves.toEqual({ expectedPointerVersion: null, writerEpoch: 9 });
    const preAdoption = productionResponse(request, null, null, {
      expiresAt: "2026-08-28T15:18:00.000Z",
      observedAt: "2026-08-28T15:03:00.000Z",
    });
    await writeScopedProductionResolution({
      now: () => Date.parse(preAdoption.observedAt),
      output,
      request,
      response: preAdoption,
    });
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true, REVISION_ID),
    ).resolves.toBeNull();
    await expect(
      store.resolvePublication(POEM_ID, "sol-5.6", true),
    ).resolves.toBeNull();
    store.close();
  });

  it("rejects malformed rows without leaking row content", async () => {
    const output = outputPath();
    const request = productionRequest();
    await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response: productionResponse(request),
    });
    const secret = `private-${"x".repeat(10_001)}`;
    const database = new Database(output);
    database
      .prepare("UPDATE poem_resolution SET author_name_arabic = ?")
      .run(secret);
    database.close();

    let caught: unknown;
    try {
      await ScopedProductionResolutionStore.open(output, {
        now: () => Date.parse(OBSERVED_AT),
      });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(SqliteQueryValidationError);
    expect(caught).toMatchObject({
      cardinality: "many",
      operation: "scopedProductionResolution.poems",
      paths: [["author_name_arabic"]],
      rowIndex: 0,
    });
    expect(String(caught)).not.toContain(secret);
    expect(caught).not.toHaveProperty("cause");
  });

  it("reports a missing required metadata row as a cardinality failure", async () => {
    const output = outputPath();
    const request = productionRequest();
    await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response: productionResponse(request),
    });
    const database = new Database(output);
    database.prepare("DELETE FROM resolution_meta").run();
    database.close();

    await expect(
      ScopedProductionResolutionStore.open(output, {
        now: () => Date.parse(OBSERVED_AT),
      }),
    ).rejects.toMatchObject({
      cardinality: "required",
      operation: "scopedProductionResolution.meta",
      paths: [[]],
      rowIndex: null,
    });
  });

  it("rejects a response whose exact scope or manifest was changed", async () => {
    const request = productionRequest();
    const response = productionResponse(request);
    await expect(
      writeScopedProductionResolution({
        now: () => Date.parse(OBSERVED_AT),
        output: outputPath(),
        request,
        response: { ...response, scopeHash: "b".repeat(64) },
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_SCOPE_HASH_MISMATCH");
    await expect(
      writeScopedProductionResolution({
        now: () => Date.parse(OBSERVED_AT),
        output: outputPath(),
        request,
        response: { ...response, manifestHash: "c".repeat(64) },
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_MANIFEST_HASH_MISMATCH");
  });

  it("replaces an expired snapshot without weakening promotion monotonicity", async () => {
    const output = outputPath();
    const request = productionRequest();
    await writeScopedProductionResolution({
      now: () => Date.parse(OBSERVED_AT),
      output,
      request,
      response: productionResponse(request),
    });
    await expect(
      writeScopedProductionResolution({
        now: () => Date.parse(OBSERVED_AT),
        output,
        request,
        response: productionResponse(request, 5),
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_SNAPSHOT_EQUIVOCATION");

    const refreshedObservedAt = "2026-08-28T15:20:00.000Z";
    const refreshed = productionResponse(request, 4, REVISION_ID, {
      expiresAt: "2026-08-28T15:35:00.000Z",
      observedAt: refreshedObservedAt,
    });
    await expect(
      writeScopedProductionResolution({
        now: () => Date.parse(refreshedObservedAt),
        output,
        request,
        response: refreshed,
      }),
    ).resolves.toMatchObject({
      observedAt: refreshedObservedAt,
      promotion: "replaced",
      writerEpoch: 9,
    });
    const store = await ScopedProductionResolutionStore.open(output, {
      now: () => Date.parse(refreshedObservedAt),
    });
    const storedReport = await store.report();
    expect(storedReport.manifestSha256).toBe(refreshed.manifestHash);
    store.close();

    const newerEpoch = productionResponse(request, 4, REVISION_ID, {
      expiresAt: "2026-08-28T15:50:00.000Z",
      observedAt: "2026-08-28T15:36:00.000Z",
      writerEpoch: 10,
    });
    await writeScopedProductionResolution({
      now: () => Date.parse(newerEpoch.observedAt),
      output,
      request,
      response: newerEpoch,
    });
    const epochRollback = productionResponse(request, 4, REVISION_ID, {
      expiresAt: "2026-08-28T16:10:00.000Z",
      observedAt: "2026-08-28T15:55:00.000Z",
      writerEpoch: 9,
    });
    await expect(
      writeScopedProductionResolution({
        now: () => Date.parse(epochRollback.observedAt),
        output,
        request,
        response: epochRollback,
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_WRITER_EPOCH_ROLLBACK");
  });

  it("fetches through environment-only Access authentication", async () => {
    const request = productionRequest();
    const response = productionResponse(request);
    const seen: Readonly<Record<string, string>>[] = [];
    const report = await fetchScopedProductionResolution({
      auth: {
        allowedOrigins: ["https://ops.saqi.app"],
        clientIdEnvironment: "TEST_ACCESS_ID",
        clientSecretEnvironment: "TEST_ACCESS_SECRET",
        identityEndpoint: "https://ops.saqi.app/api/corpus-import",
        mode: "service_token",
      },
      endpoint: "https://ops.saqi.app/api/corpus-resolution",
      environment: {
        TEST_ACCESS_ID: "id-from-environment",
        TEST_ACCESS_SECRET: "secret-from-environment",
      },
      fetcher: (fetchRequest) => {
        if (fetchRequest.method === "GET")
          return Promise.resolve({
            body: JSON.stringify(
              PublicationIdentitySchema.parse({
                databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
                schemaId: "saqi.publication-identity",
                schemaVersion: 1,
                service: "saqi-production",
              }),
            ),
            status: 200,
          });
        seen.push(fetchRequest.headers);
        expect(JSON.parse(fetchRequest.body ?? "")).toEqual(
          normalizeProductionResolutionRequest(request),
        );
        return Promise.resolve({
          body: JSON.stringify({ ok: true, result: response }),
          status: 200,
        });
      },
      now: () => Date.parse(OBSERVED_AT),
      output: outputPath(),
      request,
    });
    expect(report.poemCount).toBe(1);
    expect(seen).toEqual([
      expect.objectContaining({
        "CF-Access-Client-Id": "id-from-environment",
        "CF-Access-Client-Secret": "secret-from-environment",
      }),
    ]);
  });

  it("requests through an already authenticated shared transport", async () => {
    const request = productionRequest();
    let calls = 0;
    await expect(
      requestScopedProductionResolutionWithTransport({
        endpoint: "https://ops.saqi.app/api/corpus-resolution",
        now: () => Date.parse(OBSERVED_AT),
        request,
        transport: (transportRequest) => {
          calls += 1;
          expect(transportRequest.url).toBe(
            "https://ops.saqi.app/api/corpus-resolution",
          );
          expect(JSON.parse(transportRequest.body)).toEqual(
            normalizeProductionResolutionRequest(request),
          );
          return Promise.resolve({
            body: JSON.stringify({
              ok: true,
              result: productionResponse(request),
            }),
            status: 200,
          });
        },
      }),
    ).resolves.toMatchObject({ response: { writerEpoch: 9 } });
    expect(calls).toBe(1);
  });

  it("accepts the schema-v2 response negotiated by a fingerprint request", async () => {
    const request = ProductionResolutionRequestSchema.parse({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 3,
      targets: [
        {
          fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
          lineNfcHash: "b".repeat(64),
          modelKeys: ["sol-5.6"],
          promptMaterialHash: "c".repeat(64),
        },
      ],
    });
    const responseBody = ProductionResolutionResponseBodySchema.parse({
      expiresAt: EXPIRES_AT,
      observedAt: OBSERVED_AT,
      schemaId: "saqi.production-resolution-response",
      schemaVersion: 2,
      scopeHash: sha256(canonicalJson(request)),
      targets: [
        {
          activeSourceFingerprint: {
            algorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
            lineNfcHash: "b".repeat(64),
            promptMaterialHash: "c".repeat(64),
          },
          authorId: AUTHOR_ID,
          authorNameArabic: "شاعر",
          currentSourceNfcSha256: "b".repeat(64),
          currentSourceRevisionId: REVISION_ID,
          modelPointers: [],
          poemId: POEM_ID,
          sourceAuthorSlug: "author",
          sourcePoemId: "1",
          sourcePointerVersion: 1,
        },
      ],
      writerEpoch: 9,
    });
    const response = {
      ...responseBody,
      manifestHash: sha256(canonicalJson(responseBody)),
    };

    await expect(
      requestScopedProductionResolutionWithTransport({
        endpoint: "https://ops.saqi.app/api/corpus-resolution",
        now: () => Date.parse(OBSERVED_AT),
        request,
        transport: () =>
          Promise.resolve({
            body: JSON.stringify({ ok: true, result: response }),
            status: 200,
          }),
      }),
    ).resolves.toMatchObject({ response: { schemaVersion: 2 } });
  });

  it("preserves an allowlisted production conflict code", async () => {
    await expect(
      requestScopedProductionResolution({
        auth: {
          allowedOrigins: ["https://ops.saqi.app"],
          clientIdEnvironment: "TEST_ACCESS_ID",
          clientSecretEnvironment: "TEST_ACCESS_SECRET",
          identityEndpoint: "https://ops.saqi.app/api/corpus-import",
          mode: "service_token",
        },
        endpoint: "https://ops.saqi.app/api/corpus-resolution",
        environment: {
          TEST_ACCESS_ID: "id",
          TEST_ACCESS_SECRET: "secret",
        },
        fetcher: (request) =>
          Promise.resolve(
            request.method === "GET"
              ? {
                  body: JSON.stringify({
                    databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
                    schemaId: "saqi.publication-identity",
                    schemaVersion: 1,
                    service: "saqi-production",
                  }),
                  status: 200,
                }
              : {
                  body: JSON.stringify({
                    error: "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
                    ok: false,
                  }),
                  status: 409,
                },
          ),
        request: productionRequest(),
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED");
  });
});

function productionRequest() {
  return ProductionResolutionRequestSchema.parse({
    schemaId: "saqi.production-resolution-request",
    schemaVersion: 1,
    targets: [
      {
        modelKeys: ["sol-5.6"],
        sourceAuthorSlug: "Ibn-Farkoun",
        sourcePoemId: "123",
      },
    ],
  });
}

function productionResponse(
  request: ReturnType<typeof productionRequest>,
  pointerVersion: null | number = 4,
  currentSourceRevisionId: null | string = REVISION_ID,
  metadata: {
    readonly expiresAt?: string;
    readonly observedAt?: string;
    readonly writerEpoch?: number;
  } = {},
) {
  const body = ProductionResolutionResponseBodySchema.parse({
    expiresAt: metadata.expiresAt ?? EXPIRES_AT,
    observedAt: metadata.observedAt ?? OBSERVED_AT,
    schemaId: "saqi.production-resolution-response",
    schemaVersion: 1,
    scopeHash: sha256(
      canonicalJson(normalizeProductionResolutionRequest(request)),
    ),
    targets: [
      {
        authorId: AUTHOR_ID,
        authorNameArabic: "ابن فركون",
        currentSourceRevisionId,
        modelPointers:
          pointerVersion === null
            ? []
            : [{ modelKey: "sol-5.6", pointerVersion }],
        poemId: POEM_ID,
        sourceAuthorSlug: "Ibn-Farkoun",
        sourcePoemId: "123",
        sourcePointerVersion: currentSourceRevisionId === null ? null : 3,
      },
    ],
    writerEpoch: metadata.writerEpoch ?? 9,
  });
  return {
    ...body,
    manifestHash: productionResolutionManifestHash(body),
  };
}

function outputPath(): string {
  const root = mkdtempSync(join(tmpdir(), "saqi-scoped-resolution-"));
  ROOTS.push(root);
  return join(root, "resolution.sqlite3");
}

function sourceWork(numericId: number): WorkItem {
  const input = {
    authorHref: "https://source.invalid/writers/Ibn-Farkoun",
    poemHref: `https://source.invalid/works/${String(numericId)}`,
  };
  const definition = {
    implementationVersion: collectorImplementationVersion(),
    input,
    inputHash: inputHash(input),
    kind: collectionWorkKinds().poemDetail,
    priority: 0,
    schemaVersion: collectorSchemaVersion(),
  };
  return {
    ...definition,
    attemptCount: 1,
    availableAt: 1,
    createdAt: 1,
    lastErrorCode: null,
    leaseEpoch: 1,
    leaseExpiresAt: null,
    leaseOwner: null,
    leaseToken: null,
    outputArtifactHash: "d".repeat(64),
    state: "succeeded",
    updatedAt: 1,
    workKey: workKey(definition),
  };
}

function detail(numericId: number) {
  const work = sourceWork(numericId);
  const source = {
    author: {
      canonicalId: "source:author:Ibn-Farkoun",
      href: "https://source.invalid/writers/Ibn-Farkoun",
      path: "/writers/Ibn-Farkoun",
      slug: "Ibn-Farkoun",
    },
    canonicalId: `source:poem:${String(numericId)}`,
    href: `https://source.invalid/works/${String(numericId)}`,
    lines: ["صدر", "عجز"],
    numericId: String(numericId),
    slug: `work-${String(numericId)}`,
    structure: "classical",
    title: "قصيدة",
    verses: 1,
  };
  return {
    artifactSchemaVersion: 1,
    collectedBy: collectorImplementationVersion(),
    source,
    sourceHash: sha256(canonicalJson(source)),
    workKey: work.workKey,
  };
}
