import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  ProductionResolutionRequestSchema,
  ProductionResolutionResponseBodySchema,
  ProductionResolutionResponseSchema,
} from "@saqi/precedent-iso";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ProductionResolutionDemandCache } from "../persistence/production-resolution-demand-cache.js";
import { canonicalJson, sha256 } from "../persistence/work-key.js";
import {
  DemandDrivenProductionResolutionRefresher,
  type DemandDrivenProductionResolutionRefreshResult,
} from "../publication/demand-driven-production-resolution-refresher.js";
import { createAuthenticatedPublicationTransport } from "../publication/publication-auth-client.js";

const ROOTS: string[] = [];
const NOW = Date.parse("2026-08-31T12:00:00.000Z");
const ORIGIN = "https://ops.saqi.app";

describe("demand-driven production resolution refresher", () => {
  afterEach(async () => {
    await Promise.all(ROOTS.map((root) => rm(root, { recursive: true })));
    ROOTS.length = 0;
  });

  it("skips authorization and claims when no demand is due", async () => {
    const fixture = await createFixture();
    const authorize = vi.fn(() => fixture.auth.preflight());
    const claim = vi.spyOn(fixture.cache, "claim");
    const prune = vi.spyOn(fixture.cache, "pruneExpiredScopes");

    await expect(
      fixture.refresher({ authorize }).runOnce(),
    ).resolves.toMatchObject({
      state: "idle",
    });
    expect(authorize).not.toHaveBeenCalled();
    expect(claim).not.toHaveBeenCalled();
    expect(prune).toHaveBeenCalledOnce();
    expect(fixture.resolutionRequests()).toBe(0);
    fixture.cache.close();
  });

  it("skips authorization for fresh cached demand until its scope expires", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    await fixture.refresher().runOnce();
    const authorize = vi.fn(() => fixture.auth.preflight());
    const refresher = fixture.refresher({ authorize });

    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(authorize).not.toHaveBeenCalled();
    fixture.advance(15 * 60_000 + 1);
    await refresher.runOnce();
    expect(authorize).toHaveBeenCalledOnce();
    expect(fixture.resolutionRequests()).toBe(2);
    fixture.cache.close();
  });

  it("imports a fixed request once and merges its exact response", async () => {
    const fixture = await createFixture();
    const request = ProductionResolutionRequestSchema.parse({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 1,
      targets: [
        {
          modelKeys: ["sol-5.6"],
          sourceAuthorSlug: "author",
          sourcePoemId: "1",
        },
      ],
    });
    const bootstrap = join(fixture.root, "bootstrap.json");
    await writeFile(bootstrap, canonicalJson(request), { mode: 0o600 });
    const refresher = fixture.refresher({ bootstrapRequestPath: bootstrap });

    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(fixture.cache.counts()).toEqual({
      demands: 1,
      requests: 1,
      scopes: 1,
    });
    fixture.cache.close();
  });

  it("normalizes stale bootstrap models onto the active profile", async () => {
    const fixture = await createFixture();
    const bootstrap = join(fixture.root, "stale-bootstrap.json");
    await writeFile(
      bootstrap,
      canonicalJson({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 1,
        targets: [
          {
            modelKeys: ["retired-model", "sol-5.6"],
            sourceAuthorSlug: "active-author",
            sourcePoemId: "1",
          },
          {
            modelKeys: ["retired-model"],
            sourceAuthorSlug: "retired-only-author",
            sourcePoemId: "2",
          },
        ],
      }),
      { mode: 0o600 },
    );
    const refresher = fixture.refresher({ bootstrapRequestPath: bootstrap });

    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(fixture.resolutionRequests()).toBe(1);
    expect(fixture.cache.counts().demands).toBe(1);
    fixture.cache.close();
  });

  it("loads an inactive-only bootstrap once without issuing a request", async () => {
    const fixture = await createFixture();
    const bootstrap = join(fixture.root, "inactive-bootstrap.json");
    await writeFile(
      bootstrap,
      canonicalJson({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 1,
        targets: [
          {
            modelKeys: ["retired-model"],
            sourceAuthorSlug: "retired-only-author",
            sourcePoemId: "2",
          },
        ],
      }),
      { mode: 0o600 },
    );
    const refresher = fixture.refresher({ bootstrapRequestPath: bootstrap });

    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(fixture.resolutionRequests()).toBe(0);
    expect(fixture.cache.counts().demands).toBe(0);
    fixture.cache.close();
  });

  it("bisects a 409 scope and advances both exact children", async () => {
    const fixture = await createFixture({ rejectMultiTarget: true });
    for (let index = 1; index <= 4; index += 1)
      fixture.cache.registerDemand({
        modelKeys: ["sol-5.6"],
        priority: 1,
        sourceAuthorSlug: `author-${String(index)}`,
        sourcePoemId: String(index),
      });
    const refresher = fixture.refresher();

    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "split",
    });
    const states = await drain(refresher, 10);
    expect(states.filter((state) => state === "split")).toHaveLength(2);
    expect(states.filter((state) => state === "merged")).toHaveLength(4);
    expect(fixture.cache.counts().scopes).toBe(4);
    fixture.cache.close();
  });

  it("merges an exact canonical v2 request end to end", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      sha256("poem:canonical"),
      "sol-5.6",
      sha256("revision:canonical"),
      200,
    );

    await expect(fixture.refresher().runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(
      fixture.cache.resolvePublication(
        sha256("poem:canonical"),
        "sol-5.6",
        sha256("revision:canonical"),
      ),
    ).toEqual({
      expectedPointerVersion: 1,
      sourceRevisionId: sha256("revision:canonical"),
      writerEpoch: 7,
    });
    fixture.cache.close();
  });

  it("defers a rejected canonical singleton without sleeping unrelated work", async () => {
    const fixture = await createFixture({ rejectCanonical: true });
    fixture.cache.registerPublicationDemand(
      sha256("poem:canonical"),
      "sol-5.6",
      sha256("revision:canonical"),
      200,
    );
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 1,
      sourceAuthorSlug: "author-1",
      sourcePoemId: "1",
    });

    await expect(fixture.refresher().runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    await expect(fixture.refresher().runOnce()).resolves.toEqual({
      errorCode: "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
      nextWakeAt: NOW,
      state: "retry_wait",
    });
    fixture.cache.close();
  });

  it("immediately wakes only acknowledged fanout waiters and durably retries the rest", async () => {
    const fixture = await createFixture();
    const waiters = [
      {
        modelKey: "sol-5.6",
        poemId: sha256("poem:wake-1"),
        priority: 200,
        sourceRevisionId: sha256("revision:wake-1"),
        workKey: sha256("fanout:wake-1"),
      },
      {
        modelKey: "sol-5.6",
        poemId: sha256("poem:wake-1"),
        priority: 200,
        sourceRevisionId: sha256("revision:wake-1"),
        workKey: sha256("fanout:wake-2"),
      },
    ] as const;
    for (const waiter of waiters)
      expect(fixture.cache.resolveOrRegisterPublication(waiter).status).toBe(
        "waiting",
      );
    const delivered: string[][] = [];
    const refresher = fixture.refresher({
      wakePublications: (workKeys) => {
        delivered.push([...workKeys]);
        return {
          acknowledge: delivered.length === 1 ? workKeys.slice(0, 1) : workKeys,
        };
      },
    });

    await expect(refresher.runOnce()).resolves.toMatchObject({
      nextWakeAt: NOW,
      state: "merged",
      targets: 1,
    });
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toHaveLength(2);
    expect(fixture.cache.listPublicationWakeups()).toHaveLength(1);

    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(delivered).toHaveLength(2);
    expect(delivered[1]).toHaveLength(1);
    expect(fixture.cache.listPublicationWakeups()).toEqual([]);
    fixture.cache.close();
  });

  it("delivers large mixed wakeup backlogs in bounded batches until drained", async () => {
    const fixture = await createFixture();
    const publication = Array.from({ length: 450 }, (_, index) => ({
      modelKey: "sol-5.6",
      poemId: sha256(`poem:publication-${String(index)}`),
      resolution: {
        expectedPointerVersion: 1,
        sourceRevisionId: sha256(`revision:publication-${String(index)}`),
        writerEpoch: 7,
      },
      workKey: sha256(`fanout:publication-${String(index)}`),
    }));
    const fingerprint = Array.from({ length: 450 }, (_, index) => ({
      workKey: sha256(`fanout:fingerprint-${String(index)}`),
    }));
    fingerprint[0] = { workKey: publication[0]!.workKey };
    const firstPagePublicationKey = publication[200]!.workKey;
    const nextPublicationKey = publication[201]!.workKey;
    const firstPageFingerprintKey = fingerprint[200]!.workKey;
    const nextFingerprintKey = fingerprint[201]!.workKey;
    vi.spyOn(fixture.cache, "listPublicationWakeups").mockImplementation(
      (limit = 100) => publication.slice(0, limit),
    );
    vi.spyOn(fixture.cache, "listFingerprintWakeups").mockImplementation(
      (limit = 100) => fingerprint.slice(0, limit),
    );
    vi.spyOn(fixture.cache, "acknowledgePublicationWakeups").mockImplementation(
      (workKeys) => removeAcknowledged(publication, workKeys),
    );
    vi.spyOn(fixture.cache, "acknowledgeFingerprintWakeups").mockImplementation(
      (workKeys) => removeAcknowledged(fingerprint, workKeys),
    );
    const delivered: string[][] = [];
    const refresher = fixture.refresher({
      wakePublications: (workKeys) => {
        delivered.push([...workKeys]);
        return { acknowledge: workKeys };
      },
    });

    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(delivered).toHaveLength(2);
    expect(delivered.every((workKeys) => workKeys.length <= 500)).toBe(true);
    expect(delivered[0]).toContain(firstPagePublicationKey);
    expect(delivered[0]).toContain(firstPageFingerprintKey);
    expect(delivered[0]!.indexOf(firstPagePublicationKey)).toBeLessThan(
      delivered[0]!.indexOf(nextPublicationKey),
    );
    expect(delivered[0]!.indexOf(firstPageFingerprintKey)).toBeLessThan(
      delivered[0]!.indexOf(nextFingerprintKey),
    );
    expect(delivered.flat()).toHaveLength(899);
    expect(new Set(delivered.flat())).toHaveLength(899);
    expect(publication).toEqual([]);
    expect(fingerprint).toEqual([]);

    publication.push(
      ...Array.from({ length: 600 }, (_, index) => ({
        modelKey: "sol-5.6",
        poemId: sha256(`poem:top-up-${String(index)}`),
        resolution: {
          expectedPointerVersion: 1,
          sourceRevisionId: sha256(`revision:top-up-${String(index)}`),
          writerEpoch: 7,
        },
        workKey: sha256(`fanout:top-up-${String(index)}`),
      })),
    );
    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(delivered.slice(2).map((workKeys) => workKeys.length)).toEqual([
      500, 100,
    ]);
    expect(publication).toEqual([]);
    fixture.cache.close();
  });

  it("delivers a terminal canonical waiter without waiting for fanout backoff", async () => {
    const fixture = await createFixture();
    const workKey = sha256("terminal-canonical:fanout");
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: sha256("terminal-canonical:poem"),
      priority: 200,
      sourceRevisionId: sha256("terminal-canonical:revision"),
      workKey,
    });
    const claim = fixture.cache.claim();
    expect(claim).not.toBeNull();
    if (!claim) throw new Error("EXPECTED_RESOLUTION_CLAIM");
    fixture.cache.retry(
      claim,
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      NOW + 30 * 60_000,
    );
    const delivered: string[][] = [];
    const refresher = fixture.refresher({
      wakePublications: (workKeys) => {
        delivered.push([...workKeys]);
        return { acknowledge: workKeys };
      },
    });

    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(delivered).toEqual([[workKey]]);
    expect(fixture.cache.listTerminalCanonicalWakeups()).toEqual([]);
    expect(fixture.cache.counts().demands).toBe(0);
    fixture.cache.close();
  });

  it("delivers pending wakes before pruning an expired scope during an idle cycle", async () => {
    const fixture = await createFixture();
    const waiter = {
      modelKey: "sol-5.6",
      poemId: sha256("poem:idle-prune"),
      priority: 200,
      sourceRevisionId: sha256("revision:idle-prune"),
      workKey: sha256("fanout:idle-prune"),
    };
    expect(fixture.cache.resolveOrRegisterPublication(waiter).status).toBe(
      "waiting",
    );
    await expect(fixture.refresher().runOnce()).resolves.toMatchObject({
      state: "merged",
    });
    fixture.advance(15 * 60_000 + 1);

    const prune = vi.spyOn(fixture.cache, "pruneExpiredScopes");
    const delivered: string[][] = [];
    const refresher = fixture.refresher({
      wakePublications: (workKeys) => {
        expect(prune).not.toHaveBeenCalled();
        delivered.push([...workKeys]);
        return { acknowledge: workKeys };
      },
    });
    await expect(refresher.runOnce()).resolves.toMatchObject({
      pruned: { models: 1, scopes: 1, targets: 1 },
      state: "idle",
    });
    expect(delivered).toEqual([[waiter.workKey]]);
    expect(prune).toHaveBeenCalledOnce();
    fixture.cache.close();
  });

  it("prunes on cadence while continuous claims still execute", async () => {
    const fixture = await createFixture();
    const prune = vi.spyOn(fixture.cache, "pruneExpiredScopes");
    const refresher = fixture.refresher();
    const register = (index: number) =>
      fixture.cache.registerDemand({
        modelKeys: ["sol-5.6"],
        priority: 100,
        sourceAuthorSlug: `author-${String(index)}`,
        sourcePoemId: String(index),
      });

    register(1);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(prune).toHaveBeenCalledOnce();

    register(2);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(prune).toHaveBeenCalledOnce();

    fixture.advance(5 * 60_000);
    register(3);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(prune).toHaveBeenCalledTimes(2);
    fixture.cache.close();
  });

  it("does not retry a completed request when post-completion pruning fails", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    vi.spyOn(fixture.cache, "pruneExpiredScopes").mockImplementationOnce(() => {
      throw new Error("PRUNE_FAILED");
    });
    const refresher = fixture.refresher();

    await expect(refresher.runOnce()).rejects.toThrow("PRUNE_FAILED");
    expect(fixture.resolutionRequests()).toBe(1);
    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    expect(fixture.resolutionRequests()).toBe(1);
    expect(fixture.cache.counts()).toEqual({
      demands: 1,
      requests: 1,
      scopes: 1,
    });
    fixture.cache.close();
  });

  it("retries a scope-mismatched response without leaving its claim running", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    vi.spyOn(fixture.cache, "complete").mockImplementationOnce(() => {
      throw new Error("PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH");
    });
    const refresher = fixture.refresher();

    await expect(refresher.runOnce()).resolves.toEqual({
      errorCode: "PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH",
      nextWakeAt: NOW + 30_000,
      state: "retry_wait",
    });
    expect(fixture.resolutionRequests()).toBe(1);
    await expect(refresher.runOnce()).resolves.toMatchObject({ state: "idle" });
    fixture.advance(30_000);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
    });
    expect(fixture.resolutionRequests()).toBe(2);
    fixture.cache.close();
  });

  it("does not claim or increment attempts while shared authorization is gated", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    const claim = vi.spyOn(fixture.cache, "claim");
    const authorize = vi
      .fn()
      .mockResolvedValueOnce({
        errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
        state: "retry_wait",
        status: authStatus({ paused: true, retryAt: NOW + 60_000 }),
      })
      .mockResolvedValueOnce({ state: "ready", status: authStatus() });
    const refresher = fixture.refresher({ authorize });

    await expect(refresher.runOnce()).resolves.toEqual({
      errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
      nextWakeAt: NOW + 60_000,
      state: "auth_wait",
    });
    expect(claim).not.toHaveBeenCalled();
    expect(fixture.cache.counts().requests).toBe(0);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(claim).toHaveBeenCalledOnce();
    fixture.cache.close();
  });

  it("continues bounded expired-scope pruning during an auth outage", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    await expect(fixture.refresher().runOnce()).resolves.toMatchObject({
      state: "merged",
    });
    fixture.advance(15 * 60_000 + 1);
    const prune = vi.spyOn(fixture.cache, "pruneExpiredScopes");
    const refresher = fixture.refresher({
      authorize: () =>
        Promise.resolve({
          errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
          state: "retry_wait",
          status: authStatus({ paused: true, retryAt: NOW + 30 * 60_000 }),
        }),
    });

    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "auth_wait",
    });
    expect(prune).toHaveBeenCalledOnce();
    expect(fixture.cache.counts()).toEqual({
      demands: 1,
      requests: 1,
      scopes: 0,
    });
    fixture.cache.close();
  });

  it("backs off a due authorization probe without claiming", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    const claim = vi.spyOn(fixture.cache, "claim");
    const refresher = fixture.refresher({
      authorize: () =>
        Promise.resolve({
          errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
          state: "retry_wait",
          status: authStatus({ paused: true, retryAt: NOW }),
        }),
    });

    await expect(refresher.runOnce()).resolves.toEqual({
      errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
      nextWakeAt: NOW + 30_000,
      state: "auth_wait",
    });
    expect(claim).not.toHaveBeenCalled();
    fixture.cache.close();
  });

  it("recovers a shared POST rejection without repeated claim churn", async () => {
    const fixture = await createFixture({ rejectFirstPost: true });
    fixture.cache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "author",
      sourcePoemId: "1",
    });
    const claim = vi.spyOn(fixture.cache, "claim");
    const refresher = fixture.refresher();

    await expect(refresher.runOnce()).resolves.toMatchObject({
      errorCode: "PRODUCTION_RESOLUTION_AUTH_REJECTED",
      state: "retry_wait",
    });
    expect(claim).toHaveBeenCalledOnce();
    expect(fixture.auth.status()).toMatchObject({
      paused: true,
      pauseReason: "rejected",
    });
    const authorize = vi.spyOn(fixture.auth, "preflight");
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "idle",
    });
    expect(authorize).not.toHaveBeenCalled();
    expect(claim).toHaveBeenCalledOnce();

    fixture.advance(60_000);
    await expect(refresher.runOnce()).resolves.toMatchObject({
      state: "merged",
      targets: 1,
    });
    expect(claim).toHaveBeenCalledTimes(2);
    expect(authorize).toHaveBeenCalledOnce();
    expect(fixture.auth.status().paused).toBe(false);
    fixture.cache.close();
  });
});

type RefreshState = DemandDrivenProductionResolutionRefreshResult["state"];

async function drain(
  refresher: DemandDrivenProductionResolutionRefresher,
  remaining: number,
): Promise<readonly RefreshState[]> {
  const states: RefreshState[] = [];
  for (let index = 0; index < remaining; index += 1) {
    const result = await refresher.runOnce();
    if (result.state === "idle") return states;
    states.push(result.state);
  }
  throw new Error("Resolution refresher did not drain");
}

async function createFixture(
  options: {
    readonly rejectCanonical?: boolean;
    readonly rejectFirstPost?: boolean;
    readonly rejectMultiTarget?: boolean;
  } = {},
) {
  const root = await mkdtemp(join(tmpdir(), "demand-resolution-refresh-"));
  ROOTS.push(root);
  let now = NOW;
  const cache = new ProductionResolutionDemandCache({
    now: () => now,
    path: join(root, "cache.sqlite3"),
  });
  const environment = {
    TEST_ACCESS_ID: "id",
    TEST_ACCESS_SECRET: "secret",
  };
  let resolutionRequests = 0;
  const auth = createAuthenticatedPublicationTransport({
    config: {
      allowedOrigins: [ORIGIN],
      clientIdEnvironment: "TEST_ACCESS_ID",
      clientSecretEnvironment: "TEST_ACCESS_SECRET",
      identityEndpoint: `${ORIGIN}/api/corpus-import`,
      mode: "service_token",
    },
    environment,
    fetcher: async (request) => {
      if (request.method === "GET")
        return {
          body: JSON.stringify({
            databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
            schemaId: "saqi.publication-identity",
            schemaVersion: 1,
            service: "saqi-production",
          }),
          status: 200,
        };
      resolutionRequests += 1;
      if (options.rejectFirstPost && resolutionRequests === 1)
        return { body: "rejected", status: 401 };
      const input = ProductionResolutionRequestSchema.parse(
        JSON.parse(request.body ?? "null"),
      );
      if (options.rejectMultiTarget && input.targets.length > 1)
        return { body: "conflict", status: 409 };
      if (options.rejectCanonical && input.schemaVersion === 2)
        return {
          body: JSON.stringify({
            error: "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
            ok: false,
          }),
          status: 409,
        };
      return {
        body: JSON.stringify({ ok: true, result: response(input, now) }),
        status: 200,
      };
    },
    now: () => now,
  });
  return {
    advance: (duration: number) => {
      now += duration;
    },
    auth,
    cache,
    refresher: (
      overrides: {
        readonly authorize?: ConstructorParameters<
          typeof DemandDrivenProductionResolutionRefresher
        >[0]["authorize"];
        readonly bootstrapRequestPath?: string;
        readonly wakePublications?: (workKeys: readonly string[]) => {
          readonly acknowledge: readonly string[];
        };
      } = {},
    ) =>
      new DemandDrivenProductionResolutionRefresher({
        auth,
        authorize: (signal) => auth.preflight(signal),
        ...overrides,
        cache,
        endpoint: `${ORIGIN}/api/corpus-resolution`,
        now: () => now,
        retryBackoffMs: 30_000,
        unresolvedRetryMs: 30 * 60_000,
      }),
    resolutionRequests: () => resolutionRequests,
    root,
  };
}

function response(
  request: ReturnType<typeof ProductionResolutionRequestSchema.parse>,
  now: number,
) {
  const body = ProductionResolutionResponseBodySchema.parse({
    expiresAt: new Date(now + 15 * 60_000).toISOString(),
    observedAt: new Date(now).toISOString(),
    schemaId: "saqi.production-resolution-response",
    schemaVersion: 1,
    scopeHash: sha256(canonicalJson(request)),
    targets: request.targets.map((target) => {
      if (!("sourcePoemId" in target) && !("poemId" in target))
        throw new Error("Unexpected fingerprint target in source fixture");
      const source =
        "sourcePoemId" in target
          ? {
              poemId: sha256(`poem:${target.sourcePoemId}`),
              revisionId: sha256(`revision:${target.sourcePoemId}`),
              sourceAuthorSlug: target.sourceAuthorSlug,
              sourcePoemId: target.sourcePoemId,
            }
          : {
              poemId: target.poemId,
              revisionId: target.sourceRevisionId,
              sourceAuthorSlug: "canonical-author",
              sourcePoemId: "999",
            };
      return {
        authorId: sha256(`author:${source.sourcePoemId}`),
        authorNameArabic: "شاعر",
        currentSourceNfcSha256: sha256(`source:${source.sourcePoemId}`),
        currentSourceRevisionId: source.revisionId,
        modelPointers: target.modelKeys.map((modelKey) => ({
          modelKey,
          pointerVersion: 1,
        })),
        poemId: source.poemId,
        sourceAuthorSlug: source.sourceAuthorSlug,
        sourcePoemId: source.sourcePoemId,
        sourcePointerVersion: 1,
      };
    }),
    writerEpoch: 7,
  });
  return ProductionResolutionResponseSchema.parse({
    ...body,
    manifestHash: sha256(canonicalJson(body)),
  });
}

function authStatus(
  overrides: Partial<{
    paused: boolean;
    retryAt: null | number;
  }> = {},
) {
  return {
    consecutiveRejections: 0,
    expiresAt: null,
    mode: "service_token" as const,
    pauseReason: overrides.paused ? ("rejected" as const) : null,
    paused: overrides.paused ?? false,
    retryAt: overrides.retryAt ?? null,
  };
}

function removeAcknowledged(
  queue: { readonly workKey: string }[],
  workKeys: readonly string[],
): number {
  const acknowledged = new Set(workKeys);
  const retained = queue.filter(({ workKey }) => !acknowledged.has(workKey));
  const removed = queue.length - retained.length;
  queue.splice(0, queue.length, ...retained);
  return removed;
}
