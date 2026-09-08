import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  canonicalPoemBindingIdBody,
  normalizeProductionResolutionRequest,
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  ProductionResolutionResponseBodySchema,
  ProductionResolutionResponseSchema,
  SAQI_PRODUCTION_DATABASE_ID,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { collectionWorkKinds } from "../collection/collection-scheduler.js";
import {
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector.js";
import {
  ProductionResolutionDemandCache,
  type ProductionResolutionDemandClaim,
} from "../persistence/production-resolution-demand-cache.js";
import type { WorkItem } from "../persistence/schema.js";
import { SqliteQueryValidationError } from "../persistence/sqlite-query.js";
import {
  canonicalJson,
  inputHash,
  sha256,
  workKey,
} from "../persistence/work-key.js";

const ROOTS: string[] = [];
const START = Date.parse("2026-08-31T12:00:00.000Z");

describe("production resolution demand cache", () => {
  afterEach(async () => {
    await Promise.all(ROOTS.map((root) => rm(root, { recursive: true })));
    ROOTS.length = 0;
  });

  it("retires only canonical demand that cannot wake a publication", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(1),
      "sol-5.6",
      revisionId(1),
      100,
    );
    expect(
      fixture.cache.resolveOrRegisterPublication({
        modelKey: "sol-5.6",
        poemId: poemId(2),
        priority: 200,
        sourceRevisionId: revisionId(2),
        workKey: sha256("fanout:bound-publication"),
      }),
    ).toEqual({ status: "waiting" });
    const mixed = requiredClaim(fixture.cache.claim());
    expect(mixed.request.targets).toHaveLength(2);
    fixture.cache.retry(mixed, "PRODUCTION_RESOLUTION_HTTP_503", fixture.now());

    expect(fixture.cache.retireUnboundCanonicalDemands()).toBe(1);
    expect(fixture.cache.counts().demands).toBe(1);
    const bound = requiredClaim(fixture.cache.claim());
    expect(bound.request.targets).toEqual([
      {
        modelKeys: ["sol-5.6"],
        poemId: poemId(2),
        sourceRevisionId: revisionId(2),
      },
    ]);
    expect(fixture.cache.retireUnboundCanonicalDemands()).toBe(0);
    fixture.cache.close();
  });

  it("retires source refresh demand without removing publication demand", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: poemId(2),
      priority: 200,
      sourceRevisionId: revisionId(2),
      workKey: sha256("fanout:source-retirement-bound"),
    });
    const source = requiredClaim(fixture.cache.claim());
    expect(source.request.schemaVersion).toBe(1);
    fixture.cache.retry(
      source,
      "PRODUCTION_RESOLUTION_HTTP_503",
      fixture.now(),
    );

    expect(fixture.cache.retireSourceDemands()).toBe(1);
    expect(fixture.cache.counts().demands).toBe(1);
    const publication = requiredClaim(fixture.cache.claim());
    expect(publication.request.schemaVersion).toBe(2);
    expect(fixture.cache.retireSourceDemands()).toBe(0);
    fixture.cache.close();
  });

  it("retires inactive model scheduling state idempotently", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const database = new Database(fixture.path);
    database
      .prepare(
        `INSERT INTO resolution_demand(
           source_poem_id, source_author_slug, model_key, priority,
           first_seen_at, last_seen_at
         ) VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run("2", "author-2", "retired-model", 100, START, START);
    database
      .prepare(
        `INSERT INTO resolution_canonical_demand(
           poem_id, source_revision_id, model_key, priority,
           first_seen_at, last_seen_at
         ) VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(poemId(3), revisionId(3), "retired-model", 200, START, START);
    database
      .prepare(
        `INSERT INTO publication_waiter(
           work_key, poem_id, model_key, source_revision_id,
           priority, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        sha256("fanout:retired-model"),
        poemId(3),
        "retired-model",
        revisionId(3),
        200,
        START,
        START,
      );
    database.close();

    expect(fixture.cache.retireInactiveModelState(["sol-5.6"])).toEqual({
      canonicalDemands: 1,
      fingerprintDemands: 0,
      fingerprintWaiters: 0,
      publicationWaiters: 1,
      sourceDemands: 1,
      supersededRequests: 0,
    });
    expect(fixture.cache.retireInactiveModelState(["sol-5.6"])).toEqual({
      canonicalDemands: 0,
      fingerprintDemands: 0,
      fingerprintWaiters: 0,
      publicationWaiters: 0,
      sourceDemands: 0,
      supersededRequests: 0,
    });
    const claim = requiredClaim(fixture.cache.claim());
    expect(claim.request.targets).toEqual([
      {
        modelKeys: ["sol-5.6"],
        sourceAuthorSlug: "author-1",
        sourcePoemId: "1",
      },
    ]);
    fixture.cache.close();
  });

  it("claims deterministic bounded Codex-only scopes", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 60; index += 1)
      fixture.cache.registerDemand(demand(index));

    const first = requiredClaim(fixture.cache.claim());
    expect(first.request.targets).toHaveLength(50);
    expect(Buffer.byteLength(canonicalJson(first.request))).toBeLessThanOrEqual(
      131_072,
    );
    expect(first.request.targets[0]).toMatchObject({ sourcePoemId: "1" });
    fixture.cache.complete(first, response(first, fixture.now(), 60_000));

    const second = requiredClaim(fixture.cache.claim());
    expect(second.request.targets).toHaveLength(10);
    fixture.cache.complete(second, response(second, fixture.now()));

    expect(fixture.cache.counts()).toEqual({
      demands: 60,
      requests: 2,
      scopes: 2,
    });
    expect(
      fixture.cache.resolvePublication(poemId(1), "sol-5.6", revisionId(1)),
    ).toEqual({
      expectedPointerVersion: 1,
      sourceRevisionId: revisionId(1),
      writerEpoch: 7,
    });
    expect(
      fixture.cache.resolvePublication(poemId(60), "retired-model"),
    ).toBeNull();
    expect(fixture.cache.claim()).toBeNull();
    fixture.advance(60_001);
    expect(
      fixture.cache.resolvePublication(poemId(1), "sol-5.6", revisionId(1)),
    ).toBeNull();
    expect(
      fixture.cache.resolvePublication(poemId(60), "retired-model"),
    ).toBeNull();
    expect(fixture.cache.counts().scopes).toBe(2);
    fixture.cache.close();
  });

  it("refreshes a conflicted publication from the durable demand cache", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const initial = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(initial, response(initial, fixture.now()));
    const eventId = sha256("publication-conflict:1");

    expect(
      fixture.cache.refreshPublicationConflict({
        eventId,
        expectedPointerVersion: 1,
        expectedWriterEpoch: 7,
        modelKey: "sol-5.6",
        poemId: poemId(1),
        priority: 100,
        sourceRevisionId: revisionId(1),
      }),
    ).toBeNull();
    expect(
      fixture.cache.resolvePublication(poemId(1), "sol-5.6", revisionId(1)),
    ).toBeNull();

    fixture.cache.close();
    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      owner: "conflict-restart",
      path: fixture.path,
    });
    const refresh = requiredClaim(reopened.claim());
    reopened.complete(
      refresh,
      response(refresh, fixture.now(), 15 * 60_000, true, {
        pointerVersion: 2,
        writerEpoch: 8,
      }),
    );
    expect(
      reopened.refreshPublicationConflict({
        eventId,
        expectedPointerVersion: 1,
        expectedWriterEpoch: 7,
        modelKey: "sol-5.6",
        poemId: poemId(1),
        priority: 100,
        sourceRevisionId: revisionId(1),
      }),
    ).toEqual({
      expectedPointerVersion: 2,
      sourceRevisionId: revisionId(1),
      writerEpoch: 8,
    });
    reopened.close();
  });

  it("refreshes every collection target before returning a new writer epoch", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    fixture.cache.registerDemand(demand(2));
    const initial = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(initial, response(initial, fixture.now()));
    const input = {
      eventId: sha256("collection-conflict:1-2"),
      expectedWriterEpoch: 7,
      targets: [
        { sourceAuthorSlug: "author-1", sourcePoemId: "1" },
        { sourceAuthorSlug: "author-2", sourcePoemId: "2" },
      ],
    } as const;

    expect(fixture.cache.refreshCollectionConflict(input)).toBeNull();
    const refresh = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(
      refresh,
      response(refresh, fixture.now(), 15 * 60_000, true, {
        pointerVersion: 1,
        writerEpoch: 8,
      }),
    );
    expect(fixture.cache.refreshCollectionConflict(input)).toBe(8);
    fixture.cache.close();
  });

  it("prunes expired scopes in deterministic bounded pages and reissues their exact demand", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 60; index += 1)
      fixture.cache.registerDemand(demand(index));
    const first = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(first, response(first, fixture.now(), 1_000));
    const second = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(second, response(second, fixture.now(), 1_000));
    fixture.advance(1_001);

    const firstPrune = fixture.cache.pruneExpiredScopes(1);
    expect(firstPrune.scopes).toBe(1);
    expect(fixture.cache.counts().scopes).toBe(1);
    const secondPrune = fixture.cache.pruneExpiredScopes(1);
    expect(secondPrune.scopes).toBe(1);
    expect(
      [firstPrune, secondPrune].toSorted(
        (left, right) => left.models - right.models,
      ),
    ).toEqual([
      { models: 10, scopes: 1, targets: 10 },
      { models: 50, scopes: 1, targets: 50 },
    ]);
    expect(fixture.cache.pruneExpiredScopes()).toEqual({
      models: 0,
      scopes: 0,
      targets: 0,
    });

    const reissued = requiredClaim(fixture.cache.claim());
    expect(reissued.request.targets).toHaveLength(50);
    fixture.cache.close();
  });

  it("rejects retired publication-model demand from retained source identity", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const first = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(first, response(first, fixture.now()));

    expect(() =>
      fixture.cache.registerPublicationDemand(
        poemId(1),
        "retired-model",
        revisionId(1),
        100,
      ),
    ).toThrow();
    fixture.cache.close();
  });

  it("reissues a source resolution after collection promotion without losing a concurrent wake", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const inFlight = requiredClaim(fixture.cache.claim());
    const event = {
      eventId: "e".repeat(64),
      targets: [{ sourceAuthorSlug: "author-1", sourcePoemId: "1" }],
    };
    expect(fixture.cache.wakeSourceResolution(event)).toBe(true);
    expect(fixture.cache.wakeSourceResolution(event)).toBe(false);

    // The pre-promotion response races in after the event. The durable event
    // must invalidate it and force one fresh request on the next claim.
    fixture.cache.complete(inFlight, response(inFlight, fixture.now()));
    const refreshed = requiredClaim(fixture.cache.claim());
    expect(refreshed.request).toEqual(inFlight.request);
    fixture.cache.complete(refreshed, response(refreshed, fixture.now()));
    expect(fixture.cache.claim()).toBeNull();

    // A replay of the same publication event must not invalidate fresh state.
    expect(fixture.cache.wakeSourceResolution(event)).toBe(false);
    expect(fixture.cache.claim()).toBeNull();
    fixture.cache.close();
  });

  it("wakes an unresolved source singleton before its deferred retry deadline", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const unresolved = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      unresolved,
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      fixture.now() + 30 * 60_000,
    );
    expect(fixture.cache.claim()).toBeNull();

    expect(
      fixture.cache.wakeSourceResolution({
        eventId: "d".repeat(64),
        targets: [{ sourceAuthorSlug: "author-1", sourcePoemId: "1" }],
      }),
    ).toBe(true);
    const immediate = requiredClaim(fixture.cache.claim());
    expect(immediate.request).toEqual(unresolved.request);
    fixture.cache.close();
  });

  it("normalizes shuffled multi-author refresh events and rejects event-id reuse with different targets", async () => {
    const fixture = await createFixture();
    const eventId = "f".repeat(64);
    const targets = [
      { sourceAuthorSlug: "z-author", sourcePoemId: "2" },
      { sourceAuthorSlug: "a-author", sourcePoemId: "10" },
      { sourceAuthorSlug: "b-author", sourcePoemId: "2" },
    ];
    expect(fixture.cache.wakeSourceResolution({ eventId, targets })).toBe(true);
    expect(
      fixture.cache.wakeSourceResolution({
        eventId,
        targets: targets.toReversed(),
      }),
    ).toBe(false);
    expect(() =>
      fixture.cache.wakeSourceResolution({
        eventId,
        targets: targets.slice(1),
      }),
    ).toThrow("SOURCE_RESOLUTION_REFRESH_EVENT_CONFLICT");
    fixture.cache.close();
  });

  it("refreshes an unexpired source scope that predates NFC fingerprints", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const first = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(
      first,
      response(first, fixture.now(), 15 * 60_000, false),
    );

    fixture.cache.registerDemand(demand(1));
    const refresh = requiredClaim(fixture.cache.claim());
    expect(refresh.request).toEqual(first.request);

    fixture.cache.close();
  });

  it("rebases only when the active source has the same NFC fingerprint", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(source, response(source, fixture.now()));

    expect(
      fixture.cache.resolvePublication(poemId(1), "sol-5.6", revisionId(2)),
    ).toBeNull();
    expect(
      fixture.cache.resolvePublication(
        poemId(1),
        "sol-5.6",
        revisionId(2),
        "c".repeat(64),
      ),
    ).toEqual({
      expectedPointerVersion: 1,
      sourceRevisionId: revisionId(1),
      writerEpoch: 7,
    });
    fixture.cache.close();
  });

  it("durably wakes an exact NFC publication waiter and retires its canonical retry", async () => {
    const fixture = await createFixture();
    const waiting = fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: poemId(7),
      priority: 200,
      sourceNfcSha256: "c".repeat(64),
      sourceRevisionId: revisionId(8),
      workKey: "f".repeat(64),
    });
    expect(waiting).toEqual({ status: "waiting" });
    expect(fixture.cache.getPublicationFingerprint("f".repeat(64))).toBe(
      "c".repeat(64),
    );
    expect(() =>
      fixture.cache.resolveOrRegisterPublication({
        modelKey: "sol-5.6",
        poemId: poemId(8),
        priority: 200,
        sourceNfcSha256: "d".repeat(64),
        sourceRevisionId: revisionId(8),
        workKey: "f".repeat(64),
      }),
    ).toThrow("PRODUCTION_RESOLUTION_WAITER_IDENTITY_CONFLICT");
    fixture.cache.close();
    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });

    reopened.registerDemand(demand(7));
    const source = requiredClaim(reopened.claim());
    expect(source.request.schemaVersion).toBe(1);
    reopened.complete(source, response(source, fixture.now()));

    expect(reopened.listPublicationWakeups()).toEqual([
      {
        modelKey: "sol-5.6",
        poemId: poemId(7),
        resolution: {
          expectedPointerVersion: 1,
          sourceRevisionId: revisionId(7),
          writerEpoch: 7,
        },
        workKey: "f".repeat(64),
      },
    ]);
    expect(reopened.claim()).toBeNull();
    expect(reopened.acknowledgePublicationWakeups(["f".repeat(64)])).toBe(1);
    expect(reopened.listPublicationWakeups()).toEqual([]);
    expect(reopened.getPublicationFingerprint("f".repeat(64))).toBe(
      "c".repeat(64),
    );
    reopened.retirePublicationWaiter("f".repeat(64));
    expect(reopened.getPublicationFingerprint("f".repeat(64))).toBeNull();
    expect(reopened.listPublicationWakeups()).toEqual([]);
    reopened.close();
  });

  it("does not reclaim a running canonical request retired by source resolution", async () => {
    const fixture = await createFixture();
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: poemId(7),
      priority: 200,
      sourceNfcSha256: "c".repeat(64),
      sourceRevisionId: revisionId(8),
      workKey: "a".repeat(64),
    });
    const canonical = requiredClaim(fixture.cache.claim());
    expect(canonical.request.schemaVersion).toBe(2);

    fixture.cache.registerDemand(demand(7));
    const source = requiredClaim(fixture.cache.claim());
    expect(source.request.schemaVersion).toBe(1);
    fixture.cache.complete(source, response(source, fixture.now()));

    fixture.cache.retry(
      canonical,
      "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
      fixture.now(),
    );
    expect(fixture.cache.claim()).toBeNull();
    fixture.cache.close();
  });

  it("resolves atomically without retaining a waiter when cache already matches", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(source, response(source, fixture.now()));

    expect(
      fixture.cache.resolveOrRegisterPublication({
        modelKey: "sol-5.6",
        poemId: poemId(1),
        priority: 200,
        sourceNfcSha256: "c".repeat(64),
        sourceRevisionId: revisionId(2),
        workKey: "e".repeat(64),
      }),
    ).toEqual({
      resolution: {
        expectedPointerVersion: 1,
        sourceRevisionId: revisionId(1),
        writerEpoch: 7,
      },
      status: "resolved",
    });
    expect(fixture.cache.getPublicationFingerprint("e".repeat(64))).toBeNull();
    fixture.cache.close();
  });

  it("persists and resolves canonical publication demand without cached source identity", async () => {
    const fixture = await createFixture();
    expect(
      fixture.cache.registerPublicationDemand(
        poemId(7),
        "sol-5.6",
        revisionId(7),
        200,
      ),
    ).toBe(true);
    fixture.cache.close();

    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    const claim = requiredClaim(reopened.claim());
    expect(claim.request.targets).toEqual([
      {
        modelKeys: ["sol-5.6"],
        poemId: poemId(7),
        sourceRevisionId: revisionId(7),
      },
    ]);
    reopened.complete(claim, response(claim, fixture.now()));
    expect(
      reopened.resolvePublication(poemId(7), "sol-5.6", revisionId(7)),
    ).toEqual({
      expectedPointerVersion: 1,
      sourceRevisionId: revisionId(7),
      writerEpoch: 7,
    });
    reopened.close();
  });

  it("isolates canonical publication demand while bootstrapping source provenance first", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );

    const source = requiredClaim(fixture.cache.claim());
    expect(source.request).toMatchObject({
      schemaVersion: 1,
      targets: [{ sourceAuthorSlug: "author-1", sourcePoemId: "1" }],
    });
    expect(source.request.targets).toHaveLength(1);
    fixture.cache.complete(source, response(source, fixture.now()));

    const canonical = requiredClaim(fixture.cache.claim());
    expect(canonical.request).toMatchObject({
      schemaVersion: 2,
      targets: [{ poemId: poemId(7), sourceRevisionId: revisionId(7) }],
    });
    expect(canonical.request.targets).toHaveLength(1);
    fixture.cache.close();
  });

  it("bounds source-first work so canonical publication cannot starve", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 201; index += 1)
      fixture.cache.registerDemand(demand(index));
    fixture.cache.registerPublicationDemand(
      poemId(500),
      "sol-5.6",
      revisionId(500),
      200,
    );

    for (let index = 0; index < 4; index += 1) {
      const source = requiredClaim(fixture.cache.claim());
      expect(source.request.schemaVersion).toBe(1);
      fixture.cache.complete(source, response(source, fixture.now()));
    }
    expect(fixture.cache.metrics().sourceBurst).toBe(4);

    const canonical = requiredClaim(fixture.cache.claim());
    expect(canonical.request).toMatchObject({
      schemaVersion: 2,
      targets: [{ poemId: poemId(500), sourceRevisionId: revisionId(500) }],
    });
    expect(fixture.cache.metrics().sourceBurst).toBe(0);
    fixture.cache.close();
  });

  it("claims a due source retry ahead of canonical publication demand", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(source, "PRODUCTION_RESOLUTION_RETRY", fixture.now());
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );

    const retriedSource = requiredClaim(fixture.cache.claim());
    expect(retriedSource.scopeHash).toBe(source.scopeHash);
    expect(retriedSource.request.schemaVersion).toBe(1);
    expect(fixture.cache.counts().requests).toBe(1);
    fixture.cache.close();
  });

  it("does not immediately rebuild a rejected singleton canonical request", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );
    const rejected = requiredClaim(fixture.cache.claim());

    expect(fixture.cache.bisect(rejected, fixture.now() + 60_000)).toBe(0);
    expect(fixture.cache.claim()).toBeNull();
    expect(fixture.cache.counts().requests).toBe(1);

    fixture.advance(60_000);
    const retry = requiredClaim(fixture.cache.claim());
    expect(retry.scopeHash).toBe(rejected.scopeHash);
    expect(retry.leaseEpoch).toBe(rejected.leaseEpoch + 1);
    expect(fixture.cache.counts().requests).toBe(1);
    fixture.cache.close();
  });

  it("wakes an exact Sol waiter when a canonical singleton becomes terminal", async () => {
    const fixture = await createFixture();
    const input = { ...legacyInput(7), poemId: poemId(7) };
    const workKeyValue = sha256("canonical-terminal-waiter");
    expect(
      fixture.cache.resolveOrRegisterPublication({
        modelKey: "sol-5.6",
        poemId: input.poemId,
        priority: 200,
        sourceRevisionId: input.sourceRevisionId,
        workKey: workKeyValue,
      }).status,
    ).toBe("waiting");
    const canonical = requiredClaim(fixture.cache.claim());
    expect(canonical.request.schemaVersion).toBe(2);
    expect(
      fixture.cache.resolveOrRegisterFingerprintEnrichment({
        input,
        modelKey: "sol-5.6",
        priority: 200,
        workKey: workKeyValue,
      }).status,
    ).toBe("waiting");

    fixture.cache.retry(
      canonical,
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      fixture.now() + 60_000,
    );

    expect(fixture.cache.listFingerprintWakeups()).toEqual([
      { workKey: workKeyValue },
    ]);
    fixture.cache.close();
  });

  it("repairs an acknowledged terminal singleton wakeup once on restart", async () => {
    const fixture = await createFixture();
    const input = { ...legacyInput(8), poemId: poemId(8) };
    const workKeyValue = sha256("canonical-terminal-backlog");
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: input.poemId,
      priority: 200,
      sourceRevisionId: input.sourceRevisionId,
      workKey: workKeyValue,
    });
    const canonical = requiredClaim(fixture.cache.claim());
    fixture.cache.resolveOrRegisterFingerprintEnrichment({
      input,
      modelKey: "sol-5.6",
      priority: 200,
      workKey: workKeyValue,
    });
    fixture.cache.retry(
      canonical,
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      fixture.now() + 60_000,
    );
    expect(fixture.cache.acknowledgeFingerprintWakeups([workKeyValue])).toBe(1);
    fixture.cache.close();

    const legacy = new Database(fixture.path);
    legacy
      .prepare("DELETE FROM resolution_cache_migration WHERE version = 15")
      .run();
    legacy.close();
    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });

    expect(reopened.listFingerprintWakeups()).toEqual([
      { workKey: workKeyValue },
    ]);
    reopened.close();
  });

  it("admits uncovered canonical demand ahead of an unrelated due retry", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      100,
    );
    const rejected = requiredClaim(fixture.cache.claim());
    expect(fixture.cache.bisect(rejected, fixture.now())).toBe(0);
    fixture.cache.registerPublicationDemand(
      poemId(8),
      "sol-5.6",
      revisionId(8),
      200,
    );

    const fresh = requiredClaim(fixture.cache.claim());
    expect(fresh.scopeHash).not.toBe(rejected.scopeHash);
    expect(fresh.request).toMatchObject({
      schemaVersion: 2,
      targets: [{ poemId: poemId(8), sourceRevisionId: revisionId(8) }],
    });
    fixture.cache.close();
  });

  it("retries higher-priority canonical demand before an older retry", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      100,
    );
    const older = requiredClaim(fixture.cache.claim());
    expect(fixture.cache.bisect(older, fixture.now())).toBe(0);
    fixture.cache.registerPublicationDemand(
      poemId(8),
      "sol-5.6",
      revisionId(8),
      200,
    );
    const newer = requiredClaim(fixture.cache.claim());
    expect(fixture.cache.bisect(newer, fixture.now())).toBe(0);

    const prepare = vi.spyOn(Database.prototype, "prepare");
    const prioritized = requiredClaim(fixture.cache.claim());
    const selectionSql = prepare.mock.calls
      .map(([statement]) => statement)
      .find((statement) => statement.includes("WITH selected AS MATERIALIZED"));
    prepare.mockRestore();
    expect(prioritized.scopeHash).toBe(newer.scopeHash);
    if (!selectionSql) throw new Error("Bound request selection was not used");
    const inspected = new Database(fixture.path, { readonly: true });
    try {
      const plan = inspected
        .prepare(`EXPLAIN QUERY PLAN ${selectionSql}`)
        .all(fixture.now());
      expect(plan).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ detail: "MATERIALIZE selected" }),
          expect.objectContaining({
            detail: "SEARCH request USING PRIMARY KEY (scope_hash=?)",
          }),
        ]),
      );
    } finally {
      inspected.close();
    }
    fixture.cache.close();
  });

  it("claims fingerprint demand and resolves only the exact two-hash input", async () => {
    const fixture = await createFixture();
    const higher = legacyInput(2);
    fixture.cache.registerFingerprintDemand(higher, "sol-5.6", 200);

    const claim = requiredClaim(fixture.cache.claim());
    expect(claim.request.schemaVersion).toBe(3);
    if (claim.request.schemaVersion !== 3)
      throw new Error("Expected fingerprint request");
    expect(claim.request.targets[0]).toMatchObject({
      fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
      lineNfcHash: sha256(sourceLineNfcHashBody(higher.linesArabic)),
      promptMaterialHash: sha256(
        sourcePromptMaterialHashBody({
          authorArabic: higher.authorArabic,
          linesArabic: higher.linesArabic,
          titleArabic: higher.titleArabic,
        }),
      ),
    });
    fixture.cache.complete(claim, fingerprintResponse(claim, fixture.now(), 2));

    const resolution = fixture.cache.resolveFingerprintEnrichment(
      higher,
      "sol-5.6",
    );
    expect(resolution).toMatchObject({
      binding: {
        authorNameArabic: "شاعر موثق 2",
        lineNfcHash: sha256(sourceLineNfcHashBody(higher.linesArabic)),
        poemId: poemId(2),
        promptMaterialHash: sha256(
          sourcePromptMaterialHashBody({
            authorArabic: higher.authorArabic,
            linesArabic: higher.linesArabic,
            titleArabic: higher.titleArabic,
          }),
        ),
        sourceRevisionId: revisionId(2),
      },
      status: "resolved",
    });
    expect(
      fixture.cache.resolveFingerprintEnrichment(
        { ...higher, authorArabic: "مختلف" },
        "sol-5.6",
      ),
    ).toEqual({ status: "pending" });
    expect(fixture.cache.claim()).toBeNull();
    fixture.cache.close();
  });

  it("plans canonical enrichment resolution from the poem target before the model index", async () => {
    const fixture = await createFixture();
    const input = { ...legacyInput(1), poemId: poemId(1) };
    fixture.cache.registerPublicationDemand(
      input.poemId,
      "sol-5.6",
      input.sourceRevisionId,
      200,
    );
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(
      claim,
      response(
        claim,
        fixture.now(),
        15 * 60_000,
        sha256(sourceLineNfcHashBody(input.linesArabic)),
      ),
    );
    expect(
      fixture.cache.resolveCanonicalEnrichment(input, "sol-5.6"),
    ).toMatchObject({ status: "resolved" });

    const database = new Database(fixture.path, { readonly: true });
    const plan = database
      .prepare(
        `EXPLAIN QUERY PLAN
         SELECT target.source_poem_id, model.model_key
           FROM resolution_target target
           CROSS JOIN resolution_scope scope
           CROSS JOIN resolution_model model
          WHERE target.poem_id = ?
            AND scope.scope_hash = target.scope_hash
            AND model.scope_hash = target.scope_hash
            AND model.source_poem_id = target.source_poem_id
            AND model.source_author_slug = target.source_author_slug
            AND model.model_key = ? AND scope.expires_at > ?
          ORDER BY scope.observed_at DESC, scope.scope_hash DESC LIMIT 1`,
      )
      .all(input.poemId, "sol-5.6", fixture.now()) as {
      readonly detail: string;
    }[];
    database.close();
    expect(plan.map(({ detail: queryDetail }) => queryDetail)).toEqual([
      expect.stringMatching(
        /SEARCH target USING (?:COVERING )?INDEX resolution_target_canonical_lookup/,
      ),
      expect.stringContaining("SEARCH scope USING PRIMARY KEY"),
      expect.stringContaining(
        "SEARCH model USING COVERING INDEX resolution_model_lookup",
      ),
      expect.stringContaining("USE TEMP B-TREE FOR ORDER BY"),
    ]);
    fixture.cache.close();
  });

  it("orders uncovered canonical and fingerprint demand by priority", async () => {
    const fixture = await createFixture();
    fixture.cache.registerFingerprintDemand(legacyInput(1), "sol-5.6", 100);
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );
    expect(requiredClaim(fixture.cache.claim()).request.schemaVersion).toBe(2);
    fixture.cache.close();

    const reverse = await createFixture();
    reverse.cache.registerFingerprintDemand(legacyInput(2), "sol-5.6", 200);
    reverse.cache.registerPublicationDemand(
      poemId(8),
      "sol-5.6",
      revisionId(8),
      100,
    );
    expect(requiredClaim(reverse.cache.claim()).request.schemaVersion).toBe(3);
    reverse.cache.close();
  });

  it("bounds canonical mode bursts so fingerprint demand cannot starve", async () => {
    const fixture = await createFixture();
    fixture.cache.registerFingerprintDemand(legacyInput(1), "sol-5.6", 100);

    for (let index = 10; index < 14; index += 1) {
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        200,
      );
      const canonical = requiredClaim(fixture.cache.claim());
      expect(canonical.request.schemaVersion).toBe(2);
      fixture.cache.complete(canonical, response(canonical, fixture.now()));
    }

    fixture.cache.registerPublicationDemand(
      poemId(14),
      "sol-5.6",
      revisionId(14),
      200,
    );
    expect(fixture.cache.metrics().canonicalModeBurst).toBe(4);
    expect(requiredClaim(fixture.cache.claim()).request.schemaVersion).toBe(3);
    expect(fixture.cache.metrics().canonicalModeBurst).toBe(0);
    fixture.cache.close();
  });

  it("counts legacy source claims toward fingerprint mode fairness", async () => {
    const fixture = await createFixture();
    fixture.cache.registerFingerprintDemand(legacyInput(1), "sol-5.6", 100);
    fixture.cache.registerPublicationDemand(
      poemId(20),
      "sol-5.6",
      revisionId(20),
      200,
    );

    for (let index = 10; index < 14; index += 1) {
      fixture.cache.registerDemand(demand(index));
      const source = requiredClaim(fixture.cache.claim());
      expect(source.request.schemaVersion).toBe(1);
      fixture.cache.complete(source, response(source, fixture.now()));
    }

    expect(fixture.cache.metrics().canonicalModeBurst).toBe(4);
    fixture.cache.registerDemand(demand(14));
    const fingerprint = requiredClaim(fixture.cache.claim());
    expect(fingerprint.request.schemaVersion).toBe(3);
    expect(fixture.cache.metrics().sourceBurst).toBe(4);
    fixture.cache.retry(
      fingerprint,
      "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      fixture.now() + 60_000,
    );
    expect(requiredClaim(fixture.cache.claim()).request.schemaVersion).toBe(2);
    fixture.cache.close();
  });

  it("bounds fingerprint split continuation across restart", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 6; index += 1) {
      fixture.cache.registerFingerprintDemand(
        legacyInput(index),
        "sol-5.6",
        100,
      );
      insertFingerprintSplitRequest(fixture.path, [index], fixture.now());
    }

    fixture.cache.registerDemand(demand(10));
    fixture.cache.registerPublicationDemand(
      poemId(20),
      "sol-5.6",
      revisionId(20),
      200,
    );

    const firstChild = requiredClaim(fixture.cache.claim());
    expect(firstChild.request.schemaVersion).toBe(3);
    fixture.cache.retry(
      firstChild,
      "PUBLICATION_AUTH_NETWORK_ERROR",
      fixture.now() + 60_000,
    );
    const secondChild = requiredClaim(fixture.cache.claim());
    expect(secondChild.request.schemaVersion).toBe(3);
    fixture.cache.retry(
      secondChild,
      "PUBLICATION_AUTH_NETWORK_ERROR",
      fixture.now() + 60_000,
    );
    expect(fixture.cache.metrics().fingerprintSplitBurst).toBe(2);
    fixture.cache.close();

    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    const source = requiredClaim(reopened.claim());
    expect(source.request.schemaVersion).toBe(1);
    reopened.complete(source, response(source, fixture.now()));

    for (let count = 0; count < 2; count += 1) {
      const child = requiredClaim(reopened.claim());
      expect(child.request.schemaVersion).toBe(3);
      reopened.retry(
        child,
        "PUBLICATION_AUTH_NETWORK_ERROR",
        fixture.now() + 60_000,
      );
    }
    const canonical = requiredClaim(reopened.claim());
    expect(canonical.request.schemaVersion).toBe(2);
    reopened.close();
  });

  it("preserves canonical fairness across sustained source and split work", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 10; index += 1) {
      fixture.cache.registerFingerprintDemand(
        legacyInput(index),
        "sol-5.6",
        100,
      );
      insertFingerprintSplitRequest(fixture.path, [index], fixture.now());
    }
    fixture.cache.registerPublicationDemand(
      poemId(20),
      "sol-5.6",
      revisionId(20),
      200,
    );

    const claimTwoSplitChildren = () => {
      for (let count = 0; count < 2; count += 1) {
        const child = requiredClaim(fixture.cache.claim());
        expect(child.request.schemaVersion).toBe(3);
        fixture.cache.retry(
          child,
          "PUBLICATION_AUTH_NETWORK_ERROR",
          fixture.now() + 60_000,
        );
      }
    };
    for (let cycle = 0; cycle < 4; cycle += 1) {
      fixture.cache.registerDemand(demand(100 + cycle));
      claimTwoSplitChildren();
      const source = requiredClaim(fixture.cache.claim());
      expect(source.request.schemaVersion).toBe(1);
      fixture.cache.complete(source, response(source, fixture.now()));
    }

    fixture.cache.registerDemand(demand(104));
    claimTwoSplitChildren();
    expect(fixture.cache.metrics()).toMatchObject({
      canonicalModeBurst: 4,
      fingerprintSplitBurst: 2,
      sourceBurst: 4,
    });
    const canonical = requiredClaim(fixture.cache.claim());
    expect(canonical.request.schemaVersion).toBe(2);
    fixture.cache.close();
  });

  it("does not let split fairness postpone an attempted fingerprint retry", async () => {
    const fixture = await createFixture();
    fixture.cache.registerFingerprintDemand(legacyInput(1), "sol-5.6", 100);
    const attemptedFingerprint = requiredClaim(fixture.cache.claim());
    expect(attemptedFingerprint.request.schemaVersion).toBe(3);
    fixture.cache.retry(
      attemptedFingerprint,
      "PRODUCTION_RESOLUTION_SCHEMA_NEGOTIATION_MISMATCH",
      fixture.now(),
    );

    for (let index = 10; index < 14; index += 1) {
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        200,
      );
      const canonical = requiredClaim(fixture.cache.claim());
      expect(canonical.request.schemaVersion).toBe(2);
      fixture.cache.complete(canonical, response(canonical, fixture.now()));
    }
    fixture.cache.registerPublicationDemand(
      poemId(14),
      "sol-5.6",
      revisionId(14),
      200,
    );

    for (let index = 2; index <= 3; index += 1) {
      fixture.cache.registerFingerprintDemand(
        legacyInput(index),
        "sol-5.6",
        100,
      );
      insertFingerprintSplitRequest(fixture.path, [index], fixture.now());
      const split = requiredClaim(fixture.cache.claim());
      expect(split.request.schemaVersion).toBe(3);
      fixture.cache.retry(
        split,
        "PUBLICATION_AUTH_NETWORK_ERROR",
        fixture.now() + 60_000,
      );
    }

    expect(fixture.cache.metrics()).toMatchObject({
      canonicalModeBurst: 4,
      fingerprintSplitBurst: 2,
    });
    const retry = requiredClaim(fixture.cache.claim());
    expect(retry.scopeHash).toBe(attemptedFingerprint.scopeHash);
    expect(retry.request.schemaVersion).toBe(3);
    expect(fixture.cache.metrics().canonicalModeBurst).toBe(0);
    fixture.cache.close();
  });

  it("builds singleton fingerprint scopes for reconciliation", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 4; index += 1)
      fixture.cache.registerFingerprintDemand(
        legacyInput(index),
        "sol-5.6",
        100,
      );

    const first = requiredClaim(fixture.cache.claim());
    expect(first.request.schemaVersion).toBe(3);
    expect(first.request.targets).toHaveLength(1);
    fixture.cache.retry(
      first,
      "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      fixture.now() + 60_000,
    );
    const second = requiredClaim(fixture.cache.claim());
    expect(second.request.schemaVersion).toBe(3);
    expect(second.request.targets).toHaveLength(1);
    fixture.cache.close();
  });

  it("retains terminal singleton fingerprint failures for reconciliation", async () => {
    const fixture = await createFixture();
    const input = legacyInput(1);
    fixture.cache.registerFingerprintDemand(input, "sol-5.6", 200);
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      claim,
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
      fixture.now() + 60_000,
    );

    expect(fixture.cache.terminalFingerprintFailure(input, "sol-5.6")).toBe(
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
    );
    fixture.cache.retireFingerprintDemand(input, "sol-5.6");
    expect(fixture.cache.terminalFingerprintFailure(input, "sol-5.6")).toBe(
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
    );
    fixture.advance(60_000);
    expect(fixture.cache.claim()).toBeNull();
    fixture.cache.close();
  });

  it("durably wakes an exact fingerprint waiter after resolution", async () => {
    const fixture = await createFixture();
    const input = legacyInput(2);
    const waiterKey = sha256("fingerprint-waiter:resolved");
    expect(
      fixture.cache.resolveOrRegisterFingerprintEnrichment({
        input,
        modelKey: "sol-5.6",
        priority: 200,
        workKey: waiterKey,
      }),
    ).toEqual({ status: "waiting" });
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(claim, fingerprintResponse(claim, fixture.now(), 2));
    expect(fixture.cache.listFingerprintWakeups()).toEqual([
      { workKey: waiterKey },
    ]);
    expect(
      fixture.cache.resolveOrRegisterFingerprintEnrichment({
        input,
        modelKey: "sol-5.6",
        priority: 200,
        workKey: waiterKey,
      }),
    ).toMatchObject({ status: "resolved" });
    expect(fixture.cache.listFingerprintWakeups()).toEqual([]);
    fixture.cache.close();
  });

  it("wakes and retires a terminal fingerprint waiter", async () => {
    const fixture = await createFixture();
    const input = legacyInput(1);
    const waiterKey = sha256("fingerprint-waiter:terminal");
    fixture.cache.resolveOrRegisterFingerprintEnrichment({
      input,
      modelKey: "sol-5.6",
      priority: 200,
      workKey: waiterKey,
    });
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      claim,
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
      fixture.now() + 60_000,
    );
    expect(fixture.cache.listFingerprintWakeups()).toEqual([
      { workKey: waiterKey },
    ]);
    fixture.cache.retireFingerprintWaiter(waiterKey);
    expect(fixture.cache.listFingerprintWakeups()).toEqual([]);
    fixture.cache.retireFingerprintDemand(input, "sol-5.6");
    fixture.cache.close();
  });

  it("wakes a canonical-only waiter to register its fingerprint fallback", async () => {
    const fixture = await createFixture();
    const waiterKey = sha256("canonical-waiter:terminal");
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: poemId(1),
      priority: 200,
      sourceRevisionId: revisionId(1),
      workKey: waiterKey,
    });
    const claim = requiredClaim(fixture.cache.claim());
    expect(claim.request.schemaVersion).toBe(2);
    fixture.cache.retry(
      claim,
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      fixture.now() + 60_000,
    );

    expect(fixture.cache.listTerminalCanonicalWakeups()).toEqual([
      { workKey: waiterKey },
    ]);
    fixture.cache.retirePublicationWaiter(waiterKey);
    expect(fixture.cache.listTerminalCanonicalWakeups()).toEqual([]);
    fixture.cache.close();
  });

  it("immediately repairs a v8 generic singleton fingerprint conflict", async () => {
    const fixture = await createFixture();
    const input = legacyInput(1);
    fixture.cache.registerFingerprintDemand(input, "sol-5.6", 200);
    const failed = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      failed,
      "PRODUCTION_RESOLUTION_HTTP_409",
      fixture.now() + 30 * 60_000,
    );
    fixture.cache.close();

    const database = new Database(fixture.path);
    database
      .prepare("DELETE FROM resolution_cache_migration WHERE version = 9")
      .run();
    database.close();

    const migrated = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    const repaired = requiredClaim(migrated.claim());
    expect(repaired.scopeHash).toBe(failed.scopeHash);
    expect(repaired.request.schemaVersion).toBe(3);
    migrated.close();
  });

  it("reopens a v9 cache through the pinned scheduler migrations", async () => {
    const fixture = await createFixture();
    fixture.cache.close();
    const database = new Database(fixture.path);
    database.exec(`
      ALTER TABLE resolution_scheduler DROP COLUMN fingerprint_split_burst;
      ALTER TABLE resolution_scheduler DROP COLUMN canonical_mode_burst;
      DELETE FROM resolution_cache_migration WHERE version IN (10, 11);
    `);
    database.close();

    const migrated = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    expect(migrated.metrics()).toMatchObject({
      canonicalModeBurst: 0,
      fingerprintSplitBurst: 0,
    });
    migrated.close();

    const verified = new Database(fixture.path, { readonly: true });
    expect(
      verified
        .prepare(
          "SELECT version FROM resolution_cache_migration WHERE version >= 10 ORDER BY version",
        )
        .pluck()
        .all(),
    ).toEqual([10, 11, 12, 13, 14, 15, 16, 17]);
    verified.close();
  });

  it("compacts only unattempted legacy fingerprint split children", async () => {
    const fixture = await createFixture();
    const waiterKey = sha256("fingerprint-migration-waiter");
    fixture.cache.resolveOrRegisterFingerprintEnrichment({
      input: legacyInput(1),
      modelKey: "sol-5.6",
      priority: 200,
      workKey: waiterKey,
    });
    for (let index = 2; index <= 4; index += 1)
      fixture.cache.registerFingerprintDemand(
        legacyInput(index),
        "sol-5.6",
        200,
      );
    fixture.cache.close();

    const obsolete = insertFingerprintSplitRequest(
      fixture.path,
      [1, 2],
      fixture.now(),
    );
    const attempted = insertFingerprintSplitRequest(
      fixture.path,
      [3, 4],
      fixture.now(),
    );
    const legacy = new Database(fixture.path);
    legacy
      .prepare(
        "UPDATE resolution_request SET attempt_count = 1 WHERE scope_hash = ?",
      )
      .run(attempted);
    legacy
      .prepare("DELETE FROM resolution_cache_migration WHERE version = 12")
      .run();
    legacy.close();

    const migrated = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    const inspected = new Database(fixture.path, { readonly: true });
    expect(
      inspected
        .prepare(
          "SELECT state FROM resolution_request WHERE scope_hash IN (?, ?) ORDER BY scope_hash",
        )
        .all(obsolete, attempted),
    ).toEqual(
      [
        { scope_hash: obsolete, state: "superseded" },
        { scope_hash: attempted, state: "retry_wait" },
      ]
        .toSorted((left, right) =>
          left.scope_hash.localeCompare(right.scope_hash),
        )
        .map(({ state }) => ({ state })),
    );
    expect(
      inspected
        .prepare("SELECT count(*) FROM resolution_fingerprint_demand")
        .pluck()
        .get(),
    ).toBe(4);
    expect(
      inspected
        .prepare("SELECT count(*) FROM resolution_fingerprint_waiter")
        .pluck()
        .get(),
    ).toBe(1);
    inspected.close();

    const exact = requiredClaim(migrated.claim());
    expect(exact.request.schemaVersion).toBe(3);
    expect(exact.request.targets).toHaveLength(1);
    migrated.close();

    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    const verified = new Database(fixture.path, { readonly: true });
    expect(
      verified
        .prepare(
          "SELECT count(*) FROM resolution_cache_migration WHERE version = 12",
        )
        .pluck()
        .get(),
    ).toBe(1);
    verified.close();
    reopened.close();
  });

  it("rejects reuse of a fingerprint waiter key for another identity", async () => {
    const fixture = await createFixture();
    const waiterKey = sha256("fingerprint-waiter:conflict");
    fixture.cache.resolveOrRegisterFingerprintEnrichment({
      input: legacyInput(1),
      modelKey: "sol-5.6",
      priority: 100,
      workKey: waiterKey,
    });
    expect(() =>
      fixture.cache.resolveOrRegisterFingerprintEnrichment({
        input: legacyInput(2),
        modelKey: "sol-5.6",
        priority: 100,
        workKey: waiterKey,
      }),
    ).toThrow("PRODUCTION_RESOLUTION_WAITER_IDENTITY_CONFLICT");
    fixture.cache.close();
  });

  it("admits canonical demand while a source retry is deferred", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      source,
      "PRODUCTION_RESOLUTION_RETRY",
      fixture.now() + 60_000,
    );
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );

    const canonical = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(canonical, response(canonical, fixture.now()));
    fixture.advance(60_000);
    const resumed = requiredClaim(fixture.cache.claim());

    expect(resumed.scopeHash).toBe(source.scopeHash);
    expect(resumed.leaseEpoch).toBe(source.leaseEpoch + 1);
    expect(resumed.request.schemaVersion).toBe(1);
    expect(fixture.cache.counts().requests).toBe(2);
    fixture.cache.close();
  });

  it("preempts sustained canonical arrivals with newly due source work", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      source,
      "PRODUCTION_RESOLUTION_RETRY",
      fixture.now() + 60_000,
    );

    for (let index = 10; index < 14; index += 1) {
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        200,
      );
      const canonical = requiredClaim(fixture.cache.claim());
      expect(canonical.request.schemaVersion).toBe(2);
      fixture.cache.complete(canonical, response(canonical, fixture.now()));
    }

    fixture.cache.registerPublicationDemand(
      poemId(14),
      "sol-5.6",
      revisionId(14),
      200,
    );
    fixture.advance(60_000);
    const fair = requiredClaim(fixture.cache.claim());
    expect(fair.scopeHash).toBe(source.scopeHash);
    expect(fair.request.schemaVersion).toBe(1);
    expect(fixture.cache.metrics().canonicalBurst).toBe(0);
    fixture.cache.close();
  });

  it("persists the canonical burst across restart", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const source = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(
      source,
      "PRODUCTION_RESOLUTION_RETRY",
      fixture.now() + 60_000,
    );
    for (let index = 10; index < 14; index += 1) {
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        200,
      );
      const canonical = requiredClaim(fixture.cache.claim());
      fixture.cache.complete(canonical, response(canonical, fixture.now()));
    }
    fixture.cache.close();

    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    reopened.registerPublicationDemand(
      poemId(14),
      "sol-5.6",
      revisionId(14),
      200,
    );
    fixture.advance(60_000);
    expect(requiredClaim(reopened.claim()).request.schemaVersion).toBe(1);
    reopened.close();
  });

  it("preserves distinct canonical revisions without revoking a running lease", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(7),
      200,
    );
    const running = requiredClaim(fixture.cache.claim());
    fixture.cache.registerPublicationDemand(
      poemId(7),
      "sol-5.6",
      revisionId(8),
      200,
    );

    expect(() =>
      fixture.cache.complete(running, response(running, fixture.now())),
    ).not.toThrow();
    const current = requiredClaim(fixture.cache.claim());
    expect(current.request).toMatchObject({
      schemaVersion: 2,
      targets: [{ poemId: poemId(7), sourceRevisionId: revisionId(8) }],
    });
    fixture.cache.close();
  });

  it("selects orphan request keys without loading intact request bodies", async () => {
    const fixture = await createFixture();
    const prepare = vi.spyOn(Database.prototype, "prepare");
    let statements: string[];
    try {
      fixture.cache.claim();
      statements = prepare.mock.calls
        .map(([statement]) => statement)
        .filter((statement) =>
          /UPDATE resolution_request[\s\S]*LEFT JOIN resolution_(canonical|fingerprint)_demand/.test(
            statement,
          ),
        );
    } finally {
      prepare.mockRestore();
    }
    expect(statements).toHaveLength(2);
    const inspected = new Database(fixture.path, { readonly: true });
    try {
      for (const statement of statements) {
        const plan = inspected
          .prepare(`EXPLAIN QUERY PLAN ${statement}`)
          .all(fixture.now());
        expect(plan).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              detail:
                "SEARCH resolution_request USING PRIMARY KEY (scope_hash=?)",
            }),
            expect.objectContaining({
              detail:
                "SEARCH resolution_request USING COVERING INDEX resolution_request_ready (state=?)",
            }),
          ]),
        );
      }
    } finally {
      inspected.close();
      fixture.cache.close();
    }
  });

  it.each(["canonical", "fingerprint"] as const)(
    "supersedes a deferred %s request after its demand disappears",
    async (mode) => {
      const fixture = await createFixture();
      if (mode === "fingerprint")
        fixture.cache.registerFingerprintDemand(
          legacyInput(19),
          "sol-5.6",
          200,
        );
      else
        fixture.cache.registerPublicationDemand(
          poemId(19),
          "sol-5.6",
          revisionId(19),
          200,
        );
      const orphaned = requiredClaim(fixture.cache.claim());
      fixture.cache.retry(
        orphaned,
        "PRODUCTION_RESOLUTION_RETRY",
        fixture.now(),
      );
      const database = new Database(fixture.path);
      database
        .prepare(
          mode === "canonical"
            ? "DELETE FROM resolution_canonical_demand"
            : "DELETE FROM resolution_fingerprint_demand",
        )
        .run();
      database.close();

      expect(fixture.cache.claim()).toBeNull();
      const inspected = new Database(fixture.path, { readonly: true });
      expect(
        inspected
          .prepare("SELECT state FROM resolution_request WHERE scope_hash = ?")
          .pluck()
          .get(orphaned.scopeHash),
      ).toBe("superseded");
      inspected.close();
      fixture.cache.close();
    },
  );

  it("coalesces two pending revisions without deleting either demand", async () => {
    const fixture = await createFixture();
    fixture.cache.registerPublicationDemand(
      poemId(17),
      "sol-5.6",
      revisionId(17),
      200,
    );
    fixture.cache.registerPublicationDemand(
      poemId(17),
      "sol-5.6",
      revisionId(18),
      200,
    );

    const request = requiredClaim(fixture.cache.claim()).request;
    expect(request.schemaVersion).toBe(2);
    if (request.schemaVersion !== 2)
      throw new Error("Expected canonical request");
    expect(
      request.targets
        .map((target) =>
          "sourceRevisionId" in target ? target.sourceRevisionId : "",
        )
        .toSorted((left, right) => left.localeCompare(right)),
    ).toEqual(
      [revisionId(17), revisionId(18)].toSorted((left, right) =>
        left.localeCompare(right),
      ),
    );
    fixture.cache.close();
  });

  it("resolves a collected identity from a merged integer timestamp", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const claim = requiredClaim(fixture.cache.claim());
    const source = sourceWork(1);
    const artifact = detail(source, 1);
    const lineNfcHash = sha256(sourceLineNfcHashBody(artifact.source.lines));
    fixture.cache.complete(
      claim,
      response(claim, fixture.now(), 15 * 60_000, lineNfcHash),
    );
    const bindingIdentity = {
      authorId: sha256("author:1"),
      authorNameArabic: "شاعر 1",
      externalPoemId: "1",
      lineNfcHash,
      poemId: poemId(1),
      promptMaterialHash: sha256(
        sourcePromptMaterialHashBody({
          authorArabic: "شاعر 1",
          linesArabic: artifact.source.lines,
          titleArabic: artifact.source.title,
        }),
      ),
      schemaId: "saqi.canonical-poem-binding" as const,
      schemaVersion: 1 as const,
      sourceName: "source" as const,
      sourceRevisionId: revisionId(1),
    };

    expect(fixture.cache.resolveCollected(source, artifact)).toEqual({
      binding: {
        ...bindingIdentity,
        admissionEvidence: {
          databaseId: SAQI_PRODUCTION_DATABASE_ID,
          issuedAt: new Date(START).toISOString(),
          sourcePointerVersion: 1,
        },
        bindingId: sha256(canonicalPoemBindingIdBody(bindingIdentity)),
      },
      mapping: {
        authorId: sha256("author:1"),
        authorNameArabic: "شاعر 1",
        canonicalPoemId: poemId(1),
        poemId: poemId(1),
        sourceAuthorSlug: "author-1",
        sourcePoemId: "1",
      },
      observedAt: new Date(START).toISOString(),
      writerEpoch: 7,
    });
    fixture.cache.close();
  });

  it("rejects a partial response transactionally without writing a scope", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    fixture.cache.registerDemand(demand(2));
    const claim = requiredClaim(fixture.cache.claim());
    const output = response(claim, fixture.now());
    const partialBody = ProductionResolutionResponseBodySchema.parse({
      expiresAt: output.expiresAt,
      observedAt: output.observedAt,
      schemaId: output.schemaId,
      schemaVersion: output.schemaVersion,
      scopeHash: output.scopeHash,
      targets: output.targets.slice(0, 1),
      writerEpoch: output.writerEpoch,
    });
    const partial = ProductionResolutionResponseSchema.parse({
      ...partialBody,
      manifestHash: sha256(canonicalJson(partialBody)),
    });

    expect(() => fixture.cache.complete(claim, partial)).toThrow(
      "PRODUCTION_RESOLUTION_RESPONSE_SCOPE_MISMATCH",
    );
    expect(fixture.cache.counts().scopes).toBe(0);
    expect(fixture.cache.complete(claim, output)).toBe("merged");
    fixture.cache.close();
  });

  it("replays idempotently and survives restart without rebuilding fresh work", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const claim = requiredClaim(fixture.cache.claim());
    const output = response(claim, fixture.now());
    expect(fixture.cache.complete(claim, output)).toBe("merged");
    fixture.cache.close();

    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    expect(reopened.complete(claim, output)).toBe("replayed");
    expect(reopened.claim()).toBeNull();
    expect(reopened.counts()).toEqual({ demands: 1, requests: 1, scopes: 1 });
    reopened.close();
  });

  it("reopens demand after expiry while retaining the prior validated scope", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const first = requiredClaim(fixture.cache.claim());
    fixture.cache.complete(first, response(first, fixture.now(), 60_000));
    fixture.advance(60_001);

    const refresh = requiredClaim(fixture.cache.claim());
    expect(refresh.scopeHash).toBe(first.scopeHash);
    fixture.cache.complete(refresh, response(refresh, fixture.now(), 60_000));
    expect(fixture.cache.counts().scopes).toBe(1);
    expect(fixture.cache.resolvePublication(poemId(1), "sol-5.6")).toEqual({
      expectedPointerVersion: 1,
      sourceRevisionId: revisionId(1),
      writerEpoch: 7,
    });
    fixture.cache.close();
  });

  it("recovers an expired crash lease with a fenced epoch", async () => {
    const fixture = await createFixture(1_000);
    fixture.cache.registerDemand(demand(1));
    const abandoned = requiredClaim(fixture.cache.claim());
    fixture.cache.close();

    const restarted = new ProductionResolutionDemandCache({
      leaseMs: 1_000,
      now: fixture.clock,
      owner: "restart",
      path: fixture.path,
    });
    expect(restarted.claim()).toBeNull();
    fixture.advance(1_001);
    const recovered = requiredClaim(restarted.claim());
    expect(recovered.scopeHash).toBe(abandoned.scopeHash);
    expect(recovered.leaseEpoch).toBe(abandoned.leaseEpoch + 1);
    expect(() =>
      restarted.complete(abandoned, response(abandoned, fixture.now())),
    ).toThrow("PRODUCTION_RESOLUTION_LEASE_LOST");
    restarted.complete(recovered, response(recovered, fixture.now()));
    restarted.close();
  });

  it("bisects a rejected scope without duplicating demand", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 4; index += 1)
      fixture.cache.registerDemand(demand(index));
    const parent = requiredClaim(fixture.cache.claim());
    expect(fixture.cache.bisect(parent, fixture.now())).toBe(2);

    const left = requiredClaim(fixture.cache.claim());
    const right = requiredClaim(fixture.cache.claim());
    expect(
      [...left.request.targets, ...right.request.targets]
        .map((target) => {
          if (!("sourcePoemId" in target))
            throw new Error("EXPECTED_SOURCE_RESOLUTION_TARGET");
          return target.sourcePoemId;
        })
        .toSorted((a, b) => Number(a) - Number(b)),
    ).toEqual(["1", "2", "3", "4"]);
    expect(fixture.cache.counts()).toEqual({
      demands: 4,
      requests: 3,
      scopes: 0,
    });
    fixture.cache.close();
  });

  it.each(["succeeded", "superseded"])(
    "reactivates an exact %s split child without losing history",
    async (initialState) => {
      const fixture = await createFixture();
      for (let index = 1; index <= 4; index += 1)
        fixture.cache.registerPublicationDemand(
          poemId(index),
          "sol-5.6",
          revisionId(index),
          1_000,
        );
      const parent = requiredClaim(fixture.cache.claim());
      if (parent.request.schemaVersion !== 2)
        throw new Error("EXPECTED_CANONICAL_RESOLUTION_REQUEST");
      const child = normalizeProductionResolutionRequest({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 2,
        targets: parent.request.targets.slice(0, 2),
      });
      const childJson = canonicalJson(child);
      const childHash = sha256(childJson);
      const createdAt = fixture.now() - 2_000;
      const previousUpdatedAt = fixture.now() - 1_000;
      const database = new Database(fixture.path);
      database
        .prepare(
          `INSERT INTO resolution_request (
             scope_hash, request_json, state, retry_at, attempt_count,
             lease_epoch, last_error_code, created_at, updated_at
           ) VALUES (?, ?, ?, ?, 7, 9, ?, ?, ?)`,
        )
        .run(
          childHash,
          childJson,
          initialState,
          fixture.now() + 60_000,
          "STALE_TERMINAL_ERROR",
          createdAt,
          previousUpdatedAt,
        );
      database.close();

      expect(fixture.cache.bisect(parent, fixture.now())).toBe(2);
      const inspected = new Database(fixture.path, { readonly: true });
      expect(
        inspected
          .prepare(
            `SELECT state, retry_at, attempt_count, lease_epoch,
                    last_error_code, created_at, updated_at
               FROM resolution_request WHERE scope_hash = ?`,
          )
          .get(childHash),
      ).toEqual({
        attempt_count: 7,
        created_at: createdAt,
        last_error_code: null,
        lease_epoch: 9,
        retry_at: fixture.now(),
        state: "retry_wait",
        updated_at: fixture.now(),
      });
      inspected.close();
      fixture.cache.close();
    },
  );

  it("does not rewrite an exact active split child", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 4; index += 1)
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        1_000,
      );
    const parent = requiredClaim(fixture.cache.claim());
    if (parent.request.schemaVersion !== 2)
      throw new Error("EXPECTED_CANONICAL_RESOLUTION_REQUEST");
    const child = normalizeProductionResolutionRequest({
      schemaId: "saqi.production-resolution-request",
      schemaVersion: 2,
      targets: parent.request.targets.slice(0, 2),
    });
    const childJson = canonicalJson(child);
    const childHash = sha256(childJson);
    const database = new Database(fixture.path);
    database
      .prepare(
        `INSERT INTO resolution_request (
           scope_hash, request_json, state, retry_at, attempt_count,
           lease_owner, lease_token, lease_epoch, lease_expires_at,
           last_error_code, created_at, updated_at
         ) VALUES (?, ?, 'running', ?, 7, 'other-owner', 'other-token', 9,
                   ?, 'IN_FLIGHT_HISTORY', ?, ?)`,
      )
      .run(
        childHash,
        childJson,
        fixture.now() + 60_000,
        fixture.now() + 120_000,
        fixture.now() - 2_000,
        fixture.now() - 1_000,
      );
    database.close();

    expect(fixture.cache.bisect(parent, fixture.now())).toBe(2);
    const inspected = new Database(fixture.path, { readonly: true });
    expect(
      inspected
        .prepare("SELECT * FROM resolution_request WHERE scope_hash = ?")
        .get(childHash),
    ).toMatchObject({
      attempt_count: 7,
      last_error_code: "IN_FLIGHT_HISTORY",
      lease_epoch: 9,
      lease_expires_at: fixture.now() + 120_000,
      lease_owner: "other-owner",
      lease_token: "other-token",
      retry_at: fixture.now() + 60_000,
      state: "running",
      updated_at: fixture.now() - 1_000,
    });
    inspected.close();
    fixture.cache.close();
  });

  it("drains a canonical split child before unrelated source backlog", async () => {
    const fixture = await createFixture();
    for (let index = 1; index <= 4; index += 1)
      fixture.cache.registerPublicationDemand(
        poemId(index),
        "sol-5.6",
        revisionId(index),
        1_000,
      );
    const parent = requiredClaim(fixture.cache.claim());
    expect(parent.request.schemaVersion).toBe(2);
    expect(fixture.cache.bisect(parent, fixture.now())).toBe(2);
    fixture.cache.registerDemand(demand(100));

    expect(requiredClaim(fixture.cache.claim()).request.schemaVersion).toBe(2);
    fixture.cache.close();
  });

  it("fails closed on malformed persisted fingerprints without leaking them", async () => {
    const fixture = await createFixture();
    const waiterWorkKey = "f".repeat(64);
    fixture.cache.resolveOrRegisterPublication({
      modelKey: "sol-5.6",
      poemId: poemId(1),
      priority: 100,
      sourceRevisionId: revisionId(1),
      workKey: waiterWorkKey,
    });
    const secret = "private-corrupt-fingerprint";
    const database = new Database(fixture.path);
    database.pragma("ignore_check_constraints = ON");
    database
      .prepare(
        "UPDATE publication_waiter SET source_nfc_sha256 = ? WHERE work_key = ?",
      )
      .run(secret, waiterWorkKey);
    database.close();

    let caught: unknown;
    try {
      fixture.cache.getPublicationFingerprint(waiterWorkKey);
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(SqliteQueryValidationError);
    expect(caught).toMatchObject({
      cardinality: "scalar",
      operation: "productionResolutionDemand.publicationFingerprint",
      paths: [[]],
      rowIndex: null,
    });
    expect(String(caught)).not.toContain(secret);
    expect(caught).not.toHaveProperty("cause");
    fixture.cache.close();
  });

  it("reports missing scheduler state as a required-row cardinality failure", async () => {
    const fixture = await createFixture();
    const database = new Database(fixture.path);
    database.prepare("DELETE FROM resolution_scheduler").run();
    database.close();

    expect(() => fixture.cache.metrics()).toThrow(
      expect.objectContaining({
        cardinality: "required",
        operation: "productionResolutionDemand.scheduler",
        paths: [[]],
        rowIndex: null,
      }),
    );
    fixture.cache.close();
  });

  it("upgrades request membership probes to a covering index without changing requests", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(claim, "PRODUCTION_RESOLUTION_HTTP_503", fixture.now());
    fixture.cache.close();
    const legacy = new Database(fixture.path);
    const before = legacy
      .prepare("SELECT * FROM resolution_request ORDER BY scope_hash")
      .all();
    legacy.exec(
      "DROP INDEX resolution_request_identity_state; DELETE FROM resolution_cache_migration WHERE version = 17;",
    );
    legacy.close();
    const upgraded = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    upgraded.close();
    const repeated = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    repeated.close();
    const inspected = new Database(fixture.path, { readonly: true });
    expect(
      inspected
        .prepare("SELECT * FROM resolution_request ORDER BY scope_hash")
        .all(),
    ).toEqual(before);
    expect(
      inspected
        .prepare(
          "SELECT count(*) FROM resolution_cache_migration WHERE version = 17",
        )
        .pluck()
        .get(),
    ).toBe(1);
    const plan = z.array(z.object({ detail: z.string() })).parse(
      inspected
        .prepare(
          `EXPLAIN QUERY PLAN SELECT demand.poem_id
             FROM resolution_canonical_demand demand
             WHERE NOT EXISTS (
               SELECT 1 FROM resolution_canonical_request_member member
               JOIN resolution_request request USING(scope_hash)
               WHERE member.poem_id = demand.poem_id
                 AND member.source_revision_id = demand.source_revision_id
                 AND member.model_key = demand.model_key
                 AND request.state IN ('pending', 'running', 'retry_wait')
             ) ORDER BY demand.priority DESC LIMIT 150`,
        )
        .all(),
    );
    expect(plan.map((row) => row.detail).join("\n")).toContain(
      "COVERING INDEX resolution_request_identity_state",
    );
    inspected.close();
  });

  it("migrates and indexes request schema versions without hot-path JSON extraction", async () => {
    const fixture = await createFixture();
    fixture.cache.registerDemand(demand(1));
    const claim = requiredClaim(fixture.cache.claim());
    fixture.cache.retry(claim, "PRODUCTION_RESOLUTION_HTTP_503", fixture.now());
    fixture.cache.close();

    const legacy = new Database(fixture.path);
    legacy.exec(`
      DROP INDEX resolution_request_ready_schema;
      ALTER TABLE resolution_request DROP COLUMN schema_version;
      DELETE FROM resolution_cache_migration WHERE version = 16;
    `);
    legacy.close();
    const reopened = new ProductionResolutionDemandCache({
      now: fixture.clock,
      path: fixture.path,
    });
    reopened.close();

    const inspected = new Database(fixture.path, { readonly: true });
    const columns = inspected
      .prepare("PRAGMA table_xinfo(resolution_request)")
      .all() as { readonly hidden: number; readonly name: string }[];
    const plan = inspected
      .prepare(
        `EXPLAIN QUERY PLAN
         SELECT scope_hash, request_json, lease_epoch
           FROM resolution_request
          WHERE state IN ('pending', 'retry_wait') AND retry_at <= ?
            AND schema_version = 1
          ORDER BY created_at, scope_hash LIMIT 1`,
      )
      .all(fixture.now()) as { readonly detail: string }[];
    inspected.close();

    expect(columns).toContainEqual(
      expect.objectContaining({ hidden: 2, name: "schema_version" }),
    );
    expect(
      plan.map(({ detail: queryPlanDetail }) => queryPlanDetail).join("\n"),
    ).toContain("resolution_request_ready_schema");
    expect(
      plan.map(({ detail: queryPlanDetail }) => queryPlanDetail).join("\n"),
    ).not.toContain("SCAN resolution_request");
  });
});

async function createFixture(leaseMs = 5 * 60_000) {
  const root = await mkdtemp(join(tmpdir(), "resolution-demand-cache-"));
  ROOTS.push(root);
  const path = join(root, "cache.sqlite3");
  let now = START;
  const clock = () => now;
  return {
    advance: (duration: number) => {
      now += duration;
    },
    cache: new ProductionResolutionDemandCache({ leaseMs, now: clock, path }),
    clock,
    now: clock,
    path,
  };
}

function demand(index: number, modelKeys = ["sol-5.6"]) {
  return {
    modelKeys,
    priority: 100,
    sourceAuthorSlug: `author-${String(index)}`,
    sourcePoemId: String(index),
  };
}

function sourceWork(index: number): WorkItem {
  const input = {
    authorHref: `https://source.invalid/writers/author-${String(index)}`,
    poemHref: `https://source.invalid/works/${String(index)}`,
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

function detail(work: WorkItem, index: number) {
  const source = {
    author: {
      canonicalId: `source:author:author-${String(index)}`,
      href: `https://source.invalid/writers/author-${String(index)}`,
      path: `/writers/author-${String(index)}`,
      slug: `author-${String(index)}`,
    },
    canonicalId: `source:poem:${String(index)}`,
    href: `https://source.invalid/works/${String(index)}`,
    lines: ["صدر", "عجز"],
    numericId: String(index),
    slug: `work-${String(index)}`,
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

function legacyInput(index: number) {
  const linesArabic = [`صدر ${String(index)}`, `عجز ${String(index)}`];
  return {
    authorArabic: `شاعر قديم ${String(index)}`,
    linesArabic,
    poemId: sha256(`legacy-poem:${String(index)}`),
    schemaId: "saqi.poem-enrichment-input" as const,
    schemaVersion: 1 as const,
    sourceContentSha256: sha256(canonicalJson({ content: linesArabic })),
    sourceRevisionId: sha256(`legacy-revision:${String(index)}`),
    titleArabic: `قصيدة ${String(index)}`,
  };
}

function insertFingerprintSplitRequest(
  path: string,
  indices: readonly number[],
  now: number,
): string {
  const request = normalizeProductionResolutionRequest({
    schemaId: "saqi.production-resolution-request",
    schemaVersion: 3,
    targets: indices.map((index) => {
      const input = legacyInput(index);
      return {
        fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
        lineNfcHash: sha256(sourceLineNfcHashBody(input.linesArabic)),
        modelKeys: ["sol-5.6"],
        promptMaterialHash: sha256(
          sourcePromptMaterialHashBody({
            authorArabic: input.authorArabic,
            linesArabic: input.linesArabic,
            titleArabic: input.titleArabic,
          }),
        ),
      };
    }),
  });
  if (request.schemaVersion !== 3)
    throw new Error("Expected fingerprint request");
  const requestJson = canonicalJson(request);
  const scopeHash = sha256(requestJson);
  const database = new Database(path);
  const insert = database.transaction(() => {
    database
      .prepare(
        `INSERT INTO resolution_request (
           scope_hash, request_json, state, retry_at, attempt_count,
           lease_epoch, created_at, updated_at
         ) VALUES (?, ?, 'retry_wait', ?, 0, 0, ?, ?)`,
      )
      .run(scopeHash, requestJson, now, now, now);
    const member = database.prepare(
      `INSERT INTO resolution_fingerprint_request_member (
         scope_hash, algorithm, line_nfc_hash, prompt_material_hash, model_key
       ) VALUES (?, ?, ?, ?, ?)`,
    );
    for (const target of request.targets)
      for (const modelKey of target.modelKeys)
        member.run(
          scopeHash,
          target.fingerprintAlgorithm,
          target.lineNfcHash,
          target.promptMaterialHash,
          modelKey,
        );
  });
  insert();
  database.close();
  return scopeHash;
}

function fingerprintResponse(
  claim: ProductionResolutionDemandClaim,
  now: number,
  index: number,
) {
  if (claim.request.schemaVersion !== 3)
    throw new Error("Expected fingerprint request");
  const observedAt = Math.floor(now / 1_000) * 1_000;
  const body = ProductionResolutionResponseBodySchema.parse({
    expiresAt: new Date(observedAt + 15 * 60_000).toISOString(),
    observedAt: new Date(observedAt).toISOString(),
    schemaId: "saqi.production-resolution-response",
    schemaVersion: 2,
    scopeHash: claim.scopeHash,
    targets: claim.request.targets.map((target) => ({
      activeSourceFingerprint: {
        algorithm: target.fingerprintAlgorithm,
        lineNfcHash: target.lineNfcHash,
        promptMaterialHash: target.promptMaterialHash,
      },
      authorId: sha256(`author:${String(index)}`),
      authorNameArabic: `شاعر موثق ${String(index)}`,
      currentSourceNfcSha256: target.lineNfcHash,
      currentSourceRevisionId: revisionId(index),
      modelPointers: target.modelKeys.map((modelKey) => ({
        modelKey,
        pointerVersion: 1,
      })),
      poemId: poemId(index),
      sourceAuthorSlug: `author-${String(index)}`,
      sourcePoemId: String(index),
      sourcePointerVersion: 1,
    })),
    writerEpoch: 7,
  });
  return ProductionResolutionResponseSchema.parse({
    ...body,
    manifestHash: sha256(canonicalJson(body)),
  });
}

function response(
  claim: ProductionResolutionDemandClaim,
  now: number,
  lifetime = 15 * 60_000,
  nfcFingerprint: boolean | string = true,
  versions: { pointerVersion: number; writerEpoch: number } = {
    pointerVersion: 1,
    writerEpoch: 7,
  },
) {
  const observedAt = Math.floor(now / 1_000) * 1_000;
  const body = ProductionResolutionResponseBodySchema.parse({
    expiresAt: new Date(observedAt + lifetime).toISOString(),
    observedAt: new Date(observedAt).toISOString(),
    schemaId: "saqi.production-resolution-response",
    schemaVersion: 1,
    scopeHash: claim.scopeHash,
    targets: claim.request.targets.map((target) => {
      const index = Number(
        "sourcePoemId" in target
          ? target.sourcePoemId
          : "poemId" in target && target.poemId === poemId(7)
            ? 7
            : 1,
      );
      return {
        authorId: sha256(`author:${String(index)}`),
        authorNameArabic: `شاعر ${String(index)}`,
        ...(nfcFingerprint
          ? {
              currentSourceNfcSha256:
                typeof nfcFingerprint === "string"
                  ? nfcFingerprint
                  : "c".repeat(64),
            }
          : {}),
        currentSourceRevisionId:
          "sourceRevisionId" in target
            ? target.sourceRevisionId
            : revisionId(index),
        modelPointers: target.modelKeys.map((modelKey) => ({
          modelKey,
          pointerVersion: versions.pointerVersion,
        })),
        poemId: "poemId" in target ? target.poemId : poemId(index),
        sourceAuthorSlug:
          "sourceAuthorSlug" in target
            ? target.sourceAuthorSlug
            : `author-${String(index)}`,
        sourcePoemId:
          "sourcePoemId" in target ? target.sourcePoemId : String(index),
        sourcePointerVersion: 1,
      };
    }),
    writerEpoch: versions.writerEpoch,
  });
  return ProductionResolutionResponseSchema.parse({
    ...body,
    manifestHash: sha256(canonicalJson(body)),
  });
}

function poemId(index: number): string {
  return sha256(`poem:${String(index)}`);
}

function revisionId(index: number): string {
  return sha256(`revision:${String(index)}`);
}

function requiredClaim(
  claim: null | ProductionResolutionDemandClaim,
): ProductionResolutionDemandClaim {
  if (!claim) throw new Error("Expected a production resolution claim");
  return claim;
}
