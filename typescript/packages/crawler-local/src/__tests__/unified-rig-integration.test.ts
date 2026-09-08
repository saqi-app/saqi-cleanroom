import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { SAQI_PRODUCTION_DATABASE_ID } from "@saqi/precedent-iso";
import { describe, expect, it, onTestFinished, vi } from "vitest";

import {
  SourceBrowserError,
  SourceChromeCollector,
} from "../collection/collection-source-browser";
import {
  collectionWorkKinds,
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import { CollectorRecoveryController } from "../collection/collector-recovery";
import {
  POEM_ENRICHMENT_SCHEMA_VERSION,
  POEM_ENRICHMENT_V2_SCHEMA_VERSION,
  SOL_ENRICHMENT_WORK_KIND,
} from "../enrichment/sol-coordinator";
import { QuotaAwareSolLaneScheduler } from "../enrichment/sol-lane-scheduler";
import { ENRICHMENT_PROVIDER_SPECS } from "../enrichment/sol-runner";
import { CURRENT_SCHEMA_VERSION, Ledger } from "../persistence/ledger";
import { ProductionResolutionDemandCache } from "../persistence/production-resolution-demand-cache";
import { inputHash } from "../persistence/work-key";
import {
  FANOUT_IMPLEMENTATION_VERSION,
  FANOUT_SCHEMA_VERSION,
  FANOUT_SOL_KIND,
} from "../publication/fanout-reconciler";
import { parseScraperOperationConfig as parseProductionOperationConfig } from "../runtime/operations-contract";
import { readProviderExecutionHealth } from "../runtime/provider-execution-health";
import { type SupervisorLane, UnifiedSupervisor } from "../runtime/supervisor";
import {
  providerWaitIsActive,
  resolveApprovedSolBinding,
  retireApprovedSolResolution,
  UnifiedRigRuntime,
} from "../runtime/unified-rig";
import { importEmptyTestOperations } from "./support/import-empty-test-operations.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DIGEST = "d".repeat(64);
const OPEN = {
  circuitOpen: false,
  diskWritable: true,
  paused: false,
  quotaWaitUntil: null,
} as const;

function parseScraperOperationConfig(
  input: Record<string, unknown>,
): ReturnType<typeof parseProductionOperationConfig> {
  const resources =
    typeof input["resources"] === "object" && input["resources"] !== null
      ? input["resources"]
      : {};
  const retention =
    typeof input["retention"] === "object" && input["retention"] !== null
      ? input["retention"]
      : {};
  return parseProductionOperationConfig({
    ...input,
    resources: { enabled: false, minimumFreeDiskBytes: 0, ...resources },
    retention: { minimumFreeBytes: 0, ...retention },
  });
}

describe("unified rig restart integration", () => {
  it("rejects missing operation import before recovering expired leases", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-missing-operation-import-"));
    const database = join(root, "ledger.sqlite3");
    const ledger = Ledger.initialize(database);
    const key = ledger.seed(
      {
        implementationVersion: "test-v1",
        input: {},
        inputHash: inputHash({}),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
      1,
    ).workKey;
    ledger.armSolPaidUsageBudget(3);
    ledger.claim("crashed", 1, 10);
    const beforeWork = ledger.get(key);
    const beforeBudget = ledger.solPaidUsageBudgetStatus();
    const runtime = new UnifiedRigRuntime({
      config: parseScraperOperationConfig({
        schemaVersion: 1,
        collector: { enabled: false },
        retention: { enabled: false },
        sol: { enabled: true, concurrency: 1, initialConcurrency: 1 },
        startupReconciliation: { enabled: false },
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      now: () => 100,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    try {
      await expect(
        runtime.createLanes({ publicationAllowed: false, status: {} }),
      ).rejects.toThrow(
        "SQLITE_ROW_VALIDATION_FAILED:solOperation.importComplete",
      );
      expect(ledger.get(key)).toEqual(beforeWork);
      expect(ledger.solPaidUsageBudgetStatus()).toEqual(beforeBudget);
    } finally {
      runtime.close();
      ledger.close();
    }
  });

  it.each(["publication", "fanout", "local-enrichment-fanout-sol"])(
    "managed %s leaves expired work to the runtime recovery lane",
    async (laneName) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-early-lease-restart-"));
      const database = join(root, "ledger.sqlite3");
      const crashed = Ledger.initialize(database);
      const definition = {
        implementationVersion: "test-v1",
        input: { poem: "abandoned" },
        inputHash: inputHash({ poem: "abandoned" }),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      };
      const key = crashed.seed(definition, 100).workKey;
      const stale = crashed.claim("crashed-process", 100, 10);
      if (!stale) throw new Error("Expected abandoned claim");
      const activeKey = crashed.seed(
        {
          ...definition,
          input: { poem: "still-active" },
          inputHash: inputHash({ poem: "still-active" }),
        },
        100,
      ).workKey;
      const active = crashed.claim("active-process", 100, 100_000);
      if (!active) throw new Error("Expected live claim");
      crashed.pauseControls.set("paid", true);
      crashed.close();
      let now = 105;
      const run = vi.fn(() => {
        throw new Error("Lease maintenance must not launch paid work");
      });
      const runtime = new UnifiedRigRuntime({
        config: parseScraperOperationConfig({
          schemaVersion: 1,
          collector: { enabled: false },
          retention: { enabled: false },
          sol: { concurrency: 1, enabled: true, initialConcurrency: 1 },
          fanout: {
            enabled: true,
            resolutionFormat: "sqlite-v1",
            resolutionPath: join(root, "resolution.sqlite3"),
          },
          localFanout: {
            enabled: true,
            resolutionFormat: "sqlite-v1",
            resolutionPath: join(root, "resolution.sqlite3"),
          },
          publication: {
            enabled: true,
            endpoint: "https://ops.saqi.app/api/corpus-import",
          },
          startupReconciliation: { enabled: false },
          stateDirectory: root,
        }),
        configDigest: DIGEST,
        now: () => now,
        paths: {
          artifacts: join(root, "artifacts"),
          database,
          paused: join(root, "PAUSED"),
          root,
          schedulerState: join(root, "sol-scheduler.json"),
          solAttempts: join(root, "sol-attempts"),
        },
        solCoordinatorFactory: () => ({
          run,
          seed: () => {
            throw new Error("Unexpected seeding");
          },
        }),
        localEnrichmentResolver: { resolve: () => null },
        publicationAuth: {
          preflight: async () => {
            throw new Error("Unexpected preflight");
          },
          status: () => ({
            consecutiveRejections: 0,
            expiresAt: null,
            mode: "service_token",
            paused: false,
            pauseReason: null,
            retryAt: null,
          }),
          transport: async () => {
            throw new Error("Unexpected publication request");
          },
        },
      });
      const lanes = await runtime.createLanes({
        publicationAllowed: true,
        status: {},
      });
      const reader = Ledger.initialize(database);
      try {
        expect(reader.get(key)?.state).toBe("running");
        const recovery = lanes.find(({ name }) => name === "lease-recovery");
        expect(recovery).toBeDefined();
        if (!recovery) throw new Error("Expected periodic recovery lane");
        now = 30_105;
        const lane = lanes.find((candidate) => candidate.name === laneName);
        expect(lane).toBeDefined();
        await lane!.runOnce(new AbortController().signal);
        expect(reader.get(key)?.state).toBe("running");
        await expect(
          recovery.runOnce(new AbortController().signal),
        ).resolves.toEqual({
          nextWakeAt: 60_105,
          result: "recovered",
        });
        expect(reader.get(key)).toMatchObject({
          state: "pending",
          lastErrorCode: "LEASE_EXPIRED",
        });
        expect(reader.get(activeKey)?.state).toBe("running");
        expect(() => reader.succeed(stale, "a".repeat(64), now)).toThrow();
        await expect(
          recovery.runOnce(new AbortController().signal),
        ).resolves.toMatchObject({ result: "idle" });
        expect(reader.pauseControls.read().paidWorkPaused).toBe(true);
        expect(run).not.toHaveBeenCalled();
      } finally {
        reader.close();
        for (const lane of lanes) await lane.close();
        runtime.close();
      }
    },
  );

  it.each([
    { expected: false, providerUntil: 999 },
    { expected: false, providerUntil: 1_000 },
    { expected: true, providerUntil: 1_001 },
    { expected: false, providerUntil: null },
  ])(
    "reports provider deadline $providerUntil active=$expected",
    ({ expected, providerUntil }) => {
      expect(providerWaitIsActive(providerUntil, 1_000)).toBe(expected);
    },
  );

  it("surfaces a publication backlog that has never made progress", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-publication-stall-"));
    const database = join(root, "ledger.sqlite3");
    const now = 2 * 60 * 60_000;
    const ledger = Ledger.initialize(database);
    ledger.seed(
      {
        implementationVersion: "test-v1",
        input: { artifact: "pending" },
        inputHash: inputHash({ artifact: "pending" }),
        kind: "test-publication",
        priority: 0,
        schemaVersion: "test-v1",
      },
      1,
    );
    ledger.close();
    const authStatus = {
      consecutiveRejections: 0,
      expiresAt: null,
      mode: "service_token" as const,
      paused: false,
      pauseReason: null,
      retryAt: null,
    };
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      resources: { minimumFreeDiskBytes: 0 },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      sol: { enabled: false },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      publicationAuth: {
        preflight: vi.fn(() =>
          Promise.resolve({ state: "ready" as const, status: authStatus }),
        ),
        status: vi.fn(() => authStatus),
        transport: vi.fn(() => {
          throw new Error("Publication must not run during a status probe");
        }),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: true,
      status: { d1: { go: true } },
    });
    try {
      await runtime.status();
      expect(
        JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
      ).toMatchObject({
        growth: {
          production: {
            configured: true,
            lastSuccessAt: null,
            state: "stalled",
          },
        },
        state: "blocked",
      });
    } finally {
      for (const lane of lanes) await lane.close();
      runtime.close();
    }
  });

  it("constructs demand refresh while publication capacity is gated without preflight", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-gated-resolution-auth-"));
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      fanout: {
        refresh: {
          cachePath: join(root, "resolution-cache.sqlite3"),
          demandDriven: true,
          enabled: true,
        },
        resolutionPath: join(root, "resolution.sqlite3"),
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: {
        CF_ACCESS_CLIENT_ID: "id",
        CF_ACCESS_CLIENT_SECRET: "secret",
      },
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: { d1: { go: false } },
    });
    expect(lanes.map(({ name }) => name)).toContain(
      "production-resolution-demand-refresh",
    );
    expect(lanes.map(({ name }) => name)).toContain(
      "legacy-source-binding-reconciliation",
    );
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("gates demand claims through the injected shared publication auth", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-shared-resolution-auth-"));
    const cachePath = join(root, "resolution-cache.sqlite3");
    const bootstrapPath = join(root, "resolution-request.json");
    writeFileSync(
      bootstrapPath,
      JSON.stringify({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 1,
        targets: [
          {
            modelKeys: ["sol-5.6"],
            sourceAuthorSlug: "author",
            sourcePoemId: "1",
          },
        ],
      }),
    );
    const status = {
      consecutiveRejections: 1,
      expiresAt: null,
      mode: "service_token" as const,
      paused: true,
      pauseReason: "rejected" as const,
      retryAt: 160_000,
    };
    const sharedAuth = {
      preflight: vi.fn(() =>
        Promise.resolve({
          errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
          state: "retry_wait" as const,
          status,
        }),
      ),
      status: vi.fn(() => status),
      transport: vi.fn(() => {
        throw new Error("Gated resolution must not POST");
      }),
    };
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      fanout: {
        refresh: {
          cachePath,
          demandDriven: true,
          enabled: true,
          requestPath: bootstrapPath,
        },
        resolutionPath: join(root, "resolution.sqlite3"),
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      resources: { minimumFreeDiskBytes: 0 },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => 100_000,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      publicationAuth: sharedAuth,
    });
    let lanes: readonly SupervisorLane[] = [];
    try {
      lanes = await runtime.createLanes({
        publicationAllowed: false,
        status: { d1: { go: false } },
      });
      const refresh = lanes.find(
        ({ name }) => name === "production-resolution-demand-refresh",
      );
      expect(refresh).toBeDefined();
      await expect(
        refresh!.runOnce(new AbortController().signal),
      ).resolves.toEqual({
        nextWakeAt: 160_000,
        result: "auth_wait",
      });
      expect(sharedAuth.preflight).toHaveBeenCalled();
      expect(sharedAuth.transport).not.toHaveBeenCalled();
    } finally {
      for (const lane of lanes) await lane.close();
      runtime.close();
    }
    const cache = new ProductionResolutionDemandCache({
      now: () => 100_000,
      path: cachePath,
    });
    expect(cache.counts()).toMatchObject({ demands: 1, requests: 0 });
    cache.close();
  });

  it("surfaces a resolution refresh lane fault in pipeline health", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-resolution-health-"));
    const bootstrapPath = join(root, "resolution-request.json");
    writeFileSync(
      bootstrapPath,
      JSON.stringify({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 1,
        targets: Array.from({ length: 51 }, (_, index) => ({
          modelKeys: ["sol-5.6"],
          sourceAuthorSlug: "author",
          sourcePoemId: String(index),
        })),
      }),
    );
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      fanout: {
        refresh: {
          cachePath: join(root, "resolution-cache.sqlite3"),
          demandDriven: true,
          enabled: true,
          requestPath: bootstrapPath,
        },
        resolutionPath: join(root, "resolution.sqlite3"),
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      resources: { minimumFreeDiskBytes: 0 },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    let lanes: readonly SupervisorLane[] = [];
    try {
      lanes = await runtime.createLanes({
        publicationAllowed: false,
        status: { d1: { go: false } },
      });
      const refresh = lanes.find(
        ({ name }) => name === "production-resolution-demand-refresh",
      );
      expect(refresh).toBeDefined();
      await expect(
        refresh!.runOnce(new AbortController().signal),
      ).rejects.toThrow();
      await runtime.status();
      expect(
        JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
      ).toMatchObject({
        checks: expect.arrayContaining([
          expect.objectContaining({
            code: "PRODUCTION_RESOLUTION_REFRESH_UNAVAILABLE",
            state: "warning",
          }),
        ]),
      });
    } finally {
      for (const lane of lanes) await lane.close();
      runtime.close();
    }
  });

  it("interrupts a sleeping fanout lane when exact resolution work is woken", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-resolution-fanout-wake-"));
    const cachePath = join(root, "resolution-cache.sqlite3");
    const databasePath = join(root, "ledger.sqlite3");
    const now = 100_000;
    const ledger = Ledger.initialize(databasePath);
    const source = {
      artifactHash: "a".repeat(64),
      workKey: "b".repeat(64),
    };
    const definition = {
      implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
      input: { lane: "sol", modelKey: "sol-5.6", source },
      inputHash: inputHash({ lane: "sol", modelKey: "sol-5.6", source }),
      kind: FANOUT_SOL_KIND,
      priority: 200,
      schemaVersion: FANOUT_SCHEMA_VERSION,
    };
    const seeded = ledger.seed(definition, now);
    const claim = ledger.claim("test-fanout", now, 60_000, [FANOUT_SOL_KIND], {
      implementationVersion: FANOUT_IMPLEMENTATION_VERSION,
      schemaVersion: FANOUT_SCHEMA_VERSION,
    });
    if (claim?.work.workKey !== seeded.workKey)
      throw new Error("EXPECTED_FANOUT_CLAIM");
    ledger.retry(
      claim,
      "FANOUT_RESOLUTION_PENDING",
      now + 6 * 60 * 60_000,
      now,
    );
    ledger.close();

    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "legacy-poem",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "c".repeat(64),
      sourceRevisionId: "d".repeat(64),
      titleArabic: "عنوان",
    };
    const cache = new ProductionResolutionDemandCache({
      now: () => now,
      path: cachePath,
    });
    cache.resolveOrRegisterFingerprintEnrichment({
      input,
      modelKey: "sol-5.6",
      priority: 200,
      workKey: seeded.workKey,
    });
    const resolutionClaim = cache.claim();
    if (!resolutionClaim) throw new Error("EXPECTED_RESOLUTION_CLAIM");
    cache.retry(
      resolutionClaim,
      "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      now + 30 * 60_000,
    );
    cache.close();

    const authStatus = {
      consecutiveRejections: 0,
      expiresAt: null,
      mode: "service_token" as const,
      paused: false,
      pauseReason: null,
      retryAt: null,
    };
    const sharedAuth = {
      preflight: vi.fn(() =>
        Promise.resolve({ state: "ready" as const, status: authStatus }),
      ),
      status: vi.fn(() => authStatus),
      transport: vi.fn(() => {
        throw new Error("No resolution request should be due");
      }),
    };
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      fanout: {
        enabled: true,
        refresh: { cachePath, demandDriven: true, enabled: true },
        resolutionFormat: "sqlite-v1",
        resolutionPath: join(root, "resolution.sqlite3"),
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      resources: { minimumFreeDiskBytes: 0 },
      retention: { enabled: false, minimumFreeBytes: 0 },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true, initialConcurrency: 1 },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database: databasePath,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      publicationAuth: sharedAuth,
    });
    let lanes: readonly SupervisorLane[] = [];
    const controller = new AbortController();
    try {
      lanes = await runtime.createLanes({
        publicationAllowed: true,
        status: { d1: { go: true } },
      });
      const fanout = lanes.find(({ name }) => name === "fanout");
      const refresh = lanes.find(
        ({ name }) => name === "production-resolution-demand-refresh",
      );
      expect(fanout?.waitForNextRun).toBeDefined();
      expect(refresh).toBeDefined();
      const waiting = fanout!.waitForNextRun!(10_000, controller.signal);
      await refresh!.runOnce(controller.signal);
      await expect(
        Promise.race([
          waiting.then(() => "woken"),
          new Promise<string>((resolve) =>
            setTimeout(() => resolve("timeout"), 100),
          ),
        ]),
      ).resolves.toBe("woken");
      const verification = Ledger.initialize(databasePath);
      expect(verification.get(seeded.workKey)).toMatchObject({
        availableAt: now,
        lastErrorCode: "FANOUT_RESOLUTION_PENDING",
        state: "retry_wait",
      });
      verification.close();
    } finally {
      controller.abort();
      for (const lane of lanes) await lane.close();
      runtime.close();
    }
  });

  it("observes an external arm, claims only the reservation, and stops on its first 429", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-collector-recovery-rig-"));
    const database = join(root, "ledger.sqlite3");
    const now = 100_000;
    const ledger = Ledger.initialize(database);
    const seedDetail = (poemId: number, priority: number) => {
      const input = {
        authorHref: "https://source.invalid/writers/test",
        poemHref: `https://source.invalid/works/${String(poemId)}`,
      };
      return ledger.seed(
        {
          implementationVersion: collectorImplementationVersion(),
          input,
          inputHash: inputHash(input),
          kind: collectionWorkKinds().poemDetail,
          priority,
          schemaVersion: collectorSchemaVersion(),
        },
        now - 10,
      );
    };
    const reserved = seedDetail(1, 0);
    const reservedClaim = ledger.claim("fixture", now - 9, 10_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!reservedClaim) throw new Error("Expected recovery fixture claim");
    ledger.deadLetter(reservedClaim, "SOURCE_POEM_CONTENT_EMPTY", now - 8);
    const unrelated = seedDetail(2, 1_000);
    ledger.close();

    const browser = {
      close: vi.fn(() => Promise.resolve()),
      collectAuthorManifest: vi.fn(() => {
        throw new Error("Recovery must not collect author manifests");
      }),
      collectPoemDetail: vi.fn(() => {
        throw new SourceBrowserError(
          "SOURCE_RATE_LIMITED",
          "slow down",
          true,
          60_000,
        );
      }),
    };
    const createBrowser = vi
      .spyOn(SourceChromeCollector, "create")
      .mockResolvedValue(browser as unknown as SourceChromeCollector);
    const config = parseScraperOperationConfig({
      collector: {
        enabled: true,
        headless: true,
        maximumPerCycle: 1,
        recovery: { enabled: true },
      },
      retention: { enabled: false, minimumFreeBytes: 0 },
      restart: { idlePollMs: 5_000 },
      schemaVersion: 1,
      sol: { enabled: false },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    const sleeps: number[] = [];
    let recoveryLane: SupervisorLane | undefined;
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: async (preflight) => {
        const lanes = await runtime.createLanes(preflight);
        recoveryLane = lanes.find(({ name }) => name === "collector");
        return lanes;
      },
      paused: () => false,
      preflight: () => runtime.preflight(),
      resourcePressure: {
        snapshot: () =>
          Promise.resolve({
            availableDiskBytes: Number.MAX_SAFE_INTEGER,
            freeMemoryBytes: Number.MAX_SAFE_INTEGER,
            nextProbeAt: Number.MAX_SAFE_INTEGER,
            openFileDescriptors: 0,
            processRssBytes: 0,
            reasons: [],
            state: "ready" as const,
          }),
      },
      sleep: (milliseconds) => {
        sleeps.push(milliseconds);
        const operator = Ledger.open(database);
        // The collector and recovery lanes run concurrently. At this sleep
        // boundary the first 429 may be durably quarantined or the recovery
        // lane may already have released it; the final assertion below proves
        // the required recoverable state.
        expect(["dead_letter", "retry_wait"]).toContain(
          operator.get(reserved.workKey)?.state,
        );
        expect(operator.get(unrelated.workKey)?.state).toBe("pending");
        new CollectorRecoveryController({ ledger: operator }).arm(now);
        operator.close();
        return Promise.resolve();
      },
      status: () => runtime.status(),
    });
    try {
      await expect(
        supervisor.run(new AbortController().signal, { maximumCycles: 2 }),
      ).resolves.toMatchObject({ cycles: 2, stopped: "maximum" });
      expect(recoveryLane).toMatchObject({
        honorNextWakeAt: true,
        maximumSleepMs: 30_000,
      });
      expect(sleeps).toContain(config.restart.idlePollMs);
      const verification = Ledger.open(database, { readonly: true });
      expect(verification.get(reserved.workKey)?.state).toBe("retry_wait");
      expect(verification.get(unrelated.workKey)).toMatchObject({
        attemptCount: 0,
        state: "pending",
      });
      verification.close();
      expect(browser.collectPoemDetail).toHaveBeenCalledOnce();
      const status = await runtime.status();
      expect(status).toMatchObject({
        collectorRecovery: { reservation: { workKeys: [reserved.workKey] } },
      });
      // With independently scheduled lanes, the externally armed controller
      // may already have moved from the durable stop into observation.
      expect(status).toMatchObject({
        collectorRecovery: {
          phase: expect.stringMatching(/^(observing|stopped)$/),
        },
      });
    } finally {
      createBrowser.mockRestore();
      runtime.close();
    }
  });

  it("omits all source-collection lanes and browser construction when the collector is disabled", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-collector-disabled-"));
    const createBrowser = vi
      .spyOn(SourceChromeCollector, "create")
      .mockRejectedValue(new Error("source browser must not be constructed"));
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    try {
      const lanes = await runtime.createLanes({
        publicationAllowed: false,
        status: {},
      });
      expect(lanes.map(({ name }) => name)).not.toContain("collector");
      expect(lanes.map(({ name }) => name)).not.toContain("author-inventory");
      expect(createBrowser).not.toHaveBeenCalled();
      for (const lane of lanes) await lane.close();
    } finally {
      createBrowser.mockRestore();
      runtime.close();
    }
  });

  it("resolves approved legacy Sol work through canonical coordinates and a safe fingerprint fallback", () => {
    const registerCanonical = vi.fn(() => ({ status: "waiting" as const }));
    const registerFingerprint = vi.fn(() => ({ status: "waiting" as const }));
    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "11111111-1111-4111-8111-111111111111",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "عنوان",
    };
    const cache = {
      resolveCanonicalEnrichment: vi.fn(() => ({ status: "pending" as const })),
      resolveOrRegisterFingerprintEnrichment: registerFingerprint,
      resolveOrRegisterPublication: registerCanonical,
      retireCanonicalDemand: vi.fn(),
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter: vi.fn(),
      terminalCanonicalFailure: vi.fn(() => null),
      terminalFingerprintFailure: vi.fn(() => null),
    };

    expect(
      resolveApprovedSolBinding(cache, input, "sol-5.6", 270, "c".repeat(64)),
    ).toEqual({ status: "waiting" });
    expect(registerCanonical).toHaveBeenCalledExactlyOnceWith({
      modelKey: "sol-5.6",
      poemId: input.poemId,
      priority: 1_000,
      sourceNfcSha256: expect.stringMatching(/^[a-f\d]{64}$/),
      sourceRevisionId: input.sourceRevisionId,
      workKey: "c".repeat(64),
    });
    expect(registerFingerprint).toHaveBeenCalledExactlyOnceWith({
      input,
      modelKey: "sol-5.6",
      priority: 1_000,
      workKey: "c".repeat(64),
    });
  });

  it("retires every stale resolution waiter after local attachment replay", () => {
    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "11111111-1111-4111-8111-111111111111",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "عنوان",
    };
    const cache = {
      resolveCanonicalEnrichment: vi.fn(),
      resolveOrRegisterFingerprintEnrichment: vi.fn(),
      resolveOrRegisterPublication: vi.fn(),
      retireCanonicalDemand: vi.fn(),
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter: vi.fn(),
      terminalCanonicalFailure: vi.fn(),
      terminalFingerprintFailure: vi.fn(),
    };

    retireApprovedSolResolution(cache, input, "sol-5.6", "c".repeat(64));

    expect(cache.retireCanonicalDemand).toHaveBeenCalledExactlyOnceWith(
      input.poemId,
      "sol-5.6",
      input.sourceRevisionId,
    );
    expect(cache.retireFingerprintDemand).toHaveBeenCalledExactlyOnceWith(
      input,
      "sol-5.6",
    );
    expect(cache.retireFingerprintWaiter).toHaveBeenCalledExactlyOnceWith(
      "c".repeat(64),
    );
    expect(cache.retirePublicationWaiter).toHaveBeenCalledExactlyOnceWith(
      "c".repeat(64),
    );
  });

  it("uses a resolved fingerprint for UUID-shaped legacy work without registering a stale canonical waiter", () => {
    const binding = {
      admissionEvidence: {
        databaseId: SAQI_PRODUCTION_DATABASE_ID,
        issuedAt: "2026-09-03T00:00:00.000Z",
        sourcePointerVersion: 1,
      },
      authorId: "author-1",
      authorNameArabic: "شاعر",
      bindingId: "d".repeat(64),
      externalPoemId: "42",
      lineNfcHash: "e".repeat(64),
      poemId: "22222222-2222-4222-8222-222222222222",
      promptMaterialHash: "f".repeat(64),
      schemaId: "saqi.canonical-poem-binding",
      schemaVersion: 1,
      sourceName: "source",
      sourceRevisionId: "1".repeat(64),
    } as const;
    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "11111111-1111-4111-8111-111111111111",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "عنوان",
    };
    const registerCanonical = vi.fn(() => ({ status: "waiting" as const }));
    const retireCanonical = vi.fn();
    const retirePublicationWaiter = vi.fn();
    const cache = {
      resolveCanonicalEnrichment: vi.fn(() => ({ status: "pending" as const })),
      resolveOrRegisterFingerprintEnrichment: vi.fn(() => ({
        binding,
        status: "resolved" as const,
      })),
      resolveOrRegisterPublication: registerCanonical,
      retireCanonicalDemand: retireCanonical,
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter,
      terminalCanonicalFailure: vi.fn(() => null),
      terminalFingerprintFailure: vi.fn(() => null),
    };

    expect(
      resolveApprovedSolBinding(cache, input, "sol-5.6", 270, "c".repeat(64)),
    ).toEqual({ binding, status: "resolved" });
    expect(registerCanonical).not.toHaveBeenCalled();
    expect(retireCanonical).toHaveBeenCalledExactlyOnceWith(
      input.poemId,
      "sol-5.6",
      input.sourceRevisionId,
    );
    expect(retirePublicationWaiter).toHaveBeenCalledExactlyOnceWith(
      "c".repeat(64),
    );
  });

  it("retires a terminal canonical route while a viable fingerprint route remains", () => {
    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "11111111-1111-4111-8111-111111111111",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "عنوان",
    };
    const retireCanonicalDemand = vi.fn();
    const retirePublicationWaiter = vi.fn();
    const cache = {
      resolveCanonicalEnrichment: vi.fn(() => ({ status: "pending" as const })),
      resolveOrRegisterFingerprintEnrichment: vi.fn(() => ({
        status: "waiting" as const,
      })),
      resolveOrRegisterPublication: vi.fn(),
      retireCanonicalDemand,
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter,
      terminalCanonicalFailure: vi.fn(
        () => "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      ),
      terminalFingerprintFailure: vi.fn(() => null),
    };

    expect(
      resolveApprovedSolBinding(cache, input, "sol-5.6", 270, "c".repeat(64)),
    ).toEqual({ status: "waiting" });
    expect(retireCanonicalDemand).toHaveBeenCalledExactlyOnceWith(
      input.poemId,
      "sol-5.6",
      input.sourceRevisionId,
    );
    expect(retirePublicationWaiter).toHaveBeenCalledExactlyOnceWith(
      "c".repeat(64),
    );
    expect(cache.resolveOrRegisterPublication).not.toHaveBeenCalled();
    expect(cache.retireFingerprintDemand).not.toHaveBeenCalled();
  });

  it("returns a terminal conflict only after canonical and fingerprint routes fail", () => {
    const input = {
      authorArabic: "شاعر",
      linesArabic: ["سطر"],
      poemId: "11111111-1111-4111-8111-111111111111",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "عنوان",
    };
    const cache = {
      resolveCanonicalEnrichment: vi.fn(() => ({ status: "pending" as const })),
      resolveOrRegisterFingerprintEnrichment: vi.fn(() => ({
        status: "waiting" as const,
      })),
      resolveOrRegisterPublication: vi.fn(),
      retireCanonicalDemand: vi.fn(),
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter: vi.fn(),
      terminalCanonicalFailure: vi.fn(
        () => "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      ),
      terminalFingerprintFailure: vi.fn(
        () => "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      ),
    };

    expect(
      resolveApprovedSolBinding(cache, input, "sol-5.6", 270, "c".repeat(64)),
    ).toEqual({
      errorCode: "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      status: "conflict",
    });
    expect(cache.retireCanonicalDemand).toHaveBeenCalledOnce();
    expect(cache.retireFingerprintDemand).toHaveBeenCalledOnce();
    expect(cache.retirePublicationWaiter).toHaveBeenCalledOnce();
    expect(cache.retireFingerprintWaiter).toHaveBeenCalledOnce();
    expect(cache.resolveOrRegisterPublication).not.toHaveBeenCalled();
  });

  it("keeps fingerprint resolution for legacy Sol work without canonical coordinates", () => {
    const registerFingerprint = vi.fn(() => ({ status: "waiting" as const }));
    const input = {
      authorArabic: "المتنبي",
      linesArabic: ["على قدر أهل العزم تأتي العزائم"],
      poemId: "canary-al-mutanabbi-2026-08-25",
      schemaId: "saqi.poem-enrichment-input" as const,
      schemaVersion: 1 as const,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "b".repeat(64),
      titleArabic: "على قدر أهل العزم",
    };
    const cache = {
      resolveCanonicalEnrichment: vi.fn(() => {
        throw new Error("canonical resolution must not be called");
      }),
      resolveOrRegisterFingerprintEnrichment: registerFingerprint,
      resolveOrRegisterPublication: vi.fn(() => {
        throw new Error("canonical demand must not be registered");
      }),
      retireCanonicalDemand: vi.fn(),
      retireFingerprintDemand: vi.fn(),
      retireFingerprintWaiter: vi.fn(),
      retirePublicationWaiter: vi.fn(),
      terminalCanonicalFailure: vi.fn(() => null),
      terminalFingerprintFailure: vi.fn(() => null),
    };

    expect(
      resolveApprovedSolBinding(cache, input, "sol-5.6", 270, "c".repeat(64)),
    ).toEqual({ status: "waiting" });
    expect(registerFingerprint).toHaveBeenCalledExactlyOnceWith({
      input,
      modelKey: "sol-5.6",
      priority: 1_000,
      workKey: "c".repeat(64),
    });
  });

  it("keeps global work active while independently gating paid provider claims", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-paid-work-pause-"));
    const paidWorkPaused = join(root, "PAID_WORK_PAUSED");
    writeFileSync(paidWorkPaused, "paused\n");
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true, initialConcurrency: 1 },
      stateDirectory: root,
    });
    const run = vi.fn(() =>
      Promise.resolve({
        claimed: 0,
        deadLettered: 0,
        quotaWait: 0,
        retried: 0,
        retryAt: null,
        schedulerOutcome: "idle" as const,
        stopped: "idle" as const,
        succeeded: 0,
      }),
    );
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paidWorkPaused,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      solCoordinatorFactory: () => ({
        run,
        seed: () => {
          throw new Error("fixture coordinator does not seed");
        },
      }),
    });

    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const sol = lanes.find(({ name }) => name === "sol-1");
    expect(sol).toBeDefined();
    await expect(runtime.paused()).resolves.toBe(false);
    await expect(runtime.paidWorkPaused()).resolves.toBe(true);
    await expect(
      sol!.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({ result: "paused" });
    expect(run).not.toHaveBeenCalled();
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    ledger.pauseControls.set("paid", false);
    await expect(
      sol!.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({ result: "budget_exhausted" });
    expect(run).not.toHaveBeenCalled();
    await expect(runtime.providerExecutionStatus()).resolves.toMatchObject({
      providerExecution: {
        providers: [
          expect.objectContaining({
            progress: expect.objectContaining({ activeInvocations: 0 }),
          }),
        ],
      },
    });
    const freeRecovery = lanes.find(
      ({ name }) => name === "sol-artifact-reconciliation",
    );
    if (!freeRecovery) throw new Error("Free recovery lane missing");
    await freeRecovery.runOnce(new AbortController().signal);
    expect(run).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ artifactReconciliationOnly: true }),
    );
    run.mockClear();
    ledger.armSolPaidUsageBudget(3);
    await sol!.runOnce(new AbortController().signal);
    expect(run).toHaveBeenCalledOnce();
    ledger.close();
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("reports a durable Codex quota wait after runtime restart", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-codex-quota-health-"));
    const codexHome = join(root, "codex-home");
    mkdirSync(codexHome);
    writeFileSync(
      join(codexHome, "auth.json"),
      JSON.stringify({
        auth_mode: "chatgpt",
        tokens: { access_token: "test-token", account_id: "test-account" },
      }),
    );
    const database = join(root, "ledger.sqlite3");
    const paths = {
      artifacts: join(root, "artifacts"),
      database,
      paused: join(root, "PAUSED"),
      root,
      schedulerState: join(root, "sol-scheduler.json"),
      solAttempts: join(root, "sol-attempts"),
    };
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true, initialConcurrency: 1 },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    const ledger = Ledger.initialize(database);
    ledger.armSolPaidUsageBudget(12, false, 1);
    ledger.close();
    let now = 1_000;
    const quotaRun = vi.fn(() =>
      Promise.resolve({
        claimed: 1,
        deadLettered: 0,
        quotaWait: 1,
        retried: 0,
        retryAt: 2_000,
        schedulerOutcome: "quota_wait" as const,
        stopped: "maximum" as const,
        succeeded: 0,
      }),
    );
    const first = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: { CODEX_HOME: codexHome },
      now: () => now,
      paths,
      solCoordinatorFactory: () => ({
        run: quotaRun,
        seed: () => {
          throw new Error("Unexpected seeding");
        },
      }),
    });
    const firstLanes = await first.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const sol = firstLanes.find(({ name }) => name === "sol-1");
    if (!sol) throw new Error("Expected Sol lane");
    await sol.runOnce(new AbortController().signal);
    for (const lane of firstLanes) await lane.close();
    first.close();

    now = 1_100;
    const restarted = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: { CODEX_HOME: codexHome },
      now: () => now,
      paths,
      solCoordinatorFactory: () => ({
        run: () => {
          throw new Error(
            "Persisted quota health must not invoke provider work",
          );
        },
        seed: () => {
          throw new Error("Unexpected seeding");
        },
      }),
    });
    const restartedLanes = await restarted.createLanes({
      publicationAllowed: false,
      status: {},
    });
    await restarted.status();
    const execution = await readProviderExecutionHealth(
      join(root, "health", "provider-execution-latest.json"),
    );
    expect(execution.providers[0]).toMatchObject({
      admission: {
        primaryReason: "codex_quota_wait",
        retryAt: 2_000,
        state: "waiting",
      },
      gates: {
        quota: {
          errorCode: "CODEX_QUOTA_EXHAUSTED",
          retryAt: 2_000,
          state: "waiting",
        },
      },
    });
    for (const lane of restartedLanes) await lane.close();
    restarted.close();
  });

  it("releases its exact account-switch lease after an idle probe", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-graceful-lease-release-"));
    const fundedLedger = Ledger.initialize(join(root, "ledger.sqlite3"));
    fundedLedger.armSolPaidUsageBudget(12);
    fundedLedger.close();
    const codexHome = join(root, "codex-home");
    const authPath = join(codexHome, "auth.json");
    mkdirSync(codexHome);
    const writeAuth = (accountId: string) => {
      writeFileSync(
        authPath,
        JSON.stringify({
          auth_mode: "chatgpt",
          tokens: { access_token: `token-${accountId}`, account_id: accountId },
        }),
      );
    };
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: {
        concurrency: 1,
        enabled: true,
        initialConcurrency: 1,
        minimumLaunchIntervalMs: 0,
      },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    let normalRuns = 0;
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: { CODEX_HOME: codexHome },
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      solCoordinatorFactory: ({ owner }) => ({
        run: () => {
          if (owner.startsWith("sol-reconciliation-"))
            return Promise.resolve({
              claimed: 0,
              deadLettered: 0,
              quotaWait: 0,
              retried: 0,
              retryAt: null,
              schedulerOutcome: "idle" as const,
              stopped: "idle" as const,
              succeeded: 0,
            });
          normalRuns += 1;
          if (normalRuns === 1) writeAuth("account-b");
          return Promise.resolve({
            claimed: normalRuns === 1 ? 1 : 0,
            deadLettered: 0,
            quotaWait: 0,
            retried: 0,
            retryAt: null,
            schedulerOutcome:
              normalRuns === 1 ? ("success" as const) : ("idle" as const),
            stopped: "idle" as const,
            succeeded: normalRuns === 1 ? 1 : 0,
          });
        },
        seed: () => {
          throw new Error("fixture coordinator does not seed");
        },
      }),
    });
    writeAuth("account-a");
    const supervisor = new UnifiedSupervisor({
      config,
      configDigest: DIGEST,
      createLanes: (preflight) => runtime.createLanes(preflight),
      paused: () => false,
      preflight: () => runtime.preflight(),
      resourcePressure: {
        snapshot: () =>
          Promise.resolve({
            availableDiskBytes: Number.MAX_SAFE_INTEGER,
            freeMemoryBytes: Number.MAX_SAFE_INTEGER,
            nextProbeAt: Number.MAX_SAFE_INTEGER,
            openFileDescriptors: 0,
            processRssBytes: 0,
            reasons: [],
            state: "ready" as const,
          }),
      },
      sleep: () => Promise.resolve(),
      status: () => runtime.status(),
    });
    try {
      await expect(
        supervisor.run(new AbortController().signal, { maximumCycles: 3 }),
      ).resolves.toMatchObject({ cycles: 3, stopped: "maximum" });
      // The third run proves that the second run's idle account-switch probe
      // released its lease before supervisor drain/close.
      expect(normalRuns).toBe(3);
      await expect(runtime.status()).resolves.toMatchObject({
        solScheduler: {
          active: 0,
          recoveryCause: null,
          selectedConcurrency: 1,
        },
      });
    } finally {
      runtime.close();
    }
  });

  it("skips the legacy scan while preserving exact Sol and paid-operation recovery lanes", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-bounded-startup-"));
    const database = join(root, "ledger.sqlite3");
    const operationKey = "e".repeat(64);
    const seededLedger = Ledger.open(database);
    const seeded = seededLedger.seed({
      implementationVersion: "fixture-v1",
      input: {},
      inputHash: inputHash({}),
      kind: "fixture-paid-operation",
      priority: 0,
      schemaVersion: "fixture@1",
    });
    seededLedger.recordPaidOperationUnknown(
      seeded.workKey,
      operationKey,
      "fixture-attempt",
      1_000,
      1_000,
    );
    expect(seededLedger.doctor().schemaVersion).toBe(CURRENT_SCHEMA_VERSION);
    seededLedger.close();

    const legacyScan = vi
      .spyOn(Ledger.prototype, "listWorkDefinitionsAfter")
      .mockImplementation(() => {
        throw new Error("LEGACY_STARTUP_SCAN_MUST_NOT_RUN");
      });
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: {
        concurrency: 128,
        enabled: true,
        initialConcurrency: 128,
        minimumLaunchIntervalMs: 0,
      },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      solCoordinatorFactory: ({ index, ledger }) => ({
        run: () => {
          if (index === 128)
            ledger.recordPaidOperationReconciled(operationKey, 2_000);
          return Promise.resolve({
            claimed: index === 128 ? 1 : 0,
            deadLettered: 0,
            quotaWait: 0,
            retried: 0,
            retryAt: null,
            schedulerOutcome: "idle" as const,
            stopped: "idle" as const,
            succeeded: 0,
          });
        },
        seed: () => {
          throw new Error("fixture coordinator does not seed");
        },
      }),
    });

    try {
      const lanes = await runtime.createLanes({
        publicationAllowed: false,
        status: {},
      });
      expect(legacyScan).not.toHaveBeenCalled();
      expect(lanes.map(({ name }) => name)).toEqual(
        expect.arrayContaining([
          "sol-1",
          "sol-128",
          "sol-artifact-reconciliation",
        ]),
      );
      expect(lanes.filter(({ name }) => /^sol-\d+$/.test(name))).toHaveLength(
        128,
      );
      const reconciliation = lanes.find(
        ({ name }) => name === "sol-artifact-reconciliation",
      );
      expect(reconciliation).toBeDefined();
      await expect(
        reconciliation!.runOnce(new AbortController().signal),
      ).resolves.toMatchObject({
        result: "idle",
      });
      for (const lane of lanes) await lane.close();
    } finally {
      runtime.close();
      legacyScan.mockRestore();
    }

    const verified = Ledger.open(database);
    try {
      expect(verified.doctor().schemaVersion).toBe(CURRENT_SCHEMA_VERSION);
      expect(verified.paidOperationReconciliationStatus()).toMatchObject({
        reconciled: 1,
        unknown: 0,
      });
    } finally {
      verified.close();
    }
  });

  it("reports a persisted challenge gate truthfully after restart", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-origin-health-restart-"));
    const database = join(root, "ledger.sqlite3");
    const now = Date.now();
    const writer = Ledger.open(database);
    const origin = writer.claimOrigin("https://source.invalid", now, 10_000);
    if (origin.state !== "claimed") throw new Error("Expected origin lease");
    writer.failOrigin(origin.lease, now + 1, 0, {
      circuitBreakerAfter: 1,
      circuitBreakerCooldownMs: 60_000,
      retryAt: now + 60_000,
      stopReason: "SOURCE_HUMAN_REQUIRED",
    });
    writer.close();
    const config = parseScraperOperationConfig({
      collector: { enabled: true, headless: true },
      retention: { enabled: false },
      schemaVersion: 1,
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    await runtime.status();
    expect(
      JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
    ).toMatchObject({
      origins: [
        {
          state: "challenge_wait",
          stopReason: "SOURCE_HUMAN_REQUIRED",
        },
      ],
    });
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("starts collector and provider lanes while local resolution is missing", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-deferred-resolution-"));
    const resolutionPath = join(root, "not-yet-refreshed.sqlite3");
    const config = parseScraperOperationConfig({
      collector: { enabled: true, headless: true },
      fanout: {
        enabled: true,
        resolutionFormat: "sqlite-v1",
        resolutionPath,
      },
      localFanout: {
        enabled: true,
        resolutionFormat: "sqlite-v1",
        resolutionPath,
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    expect(lanes.map(({ name }) => name)).toEqual(
      expect.arrayContaining([
        "collector",
        "fanout",
        "sol-1",
        "local-enrichment-fanout-sol",
      ]),
    );
    await expect(runtime.status()).resolves.toMatchObject({
      providerHostAdmission: {
        activeProcesses: 0,
        available: true,
        reservedBufferedOutputBytes: 0,
        reservedMemoryBytes: 0,
      },
    });
    expect(
      JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
    ).toMatchObject({
      checks: expect.arrayContaining([
        expect.objectContaining({
          code: "SOL_LOCAL_FANOUT_RESOLUTION_UNAVAILABLE",
          detail: expect.stringContaining("ENOENT"),
          state: "warning",
        }),
        expect.objectContaining({
          code: "FANOUT_RESOLUTION_UNAVAILABLE",
          detail: expect.stringContaining("ENOENT"),
          state: "warning",
        }),
      ]),
    });
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("does not probe a stale snapshot when demand-driven resolution owns freshness", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-demand-resolution-"));
    const resolutionPath = join(root, "not-yet-refreshed.sqlite3");
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      fanout: {
        enabled: true,
        refresh: {
          cachePath: join(root, "resolution-demand.sqlite3"),
          demandDriven: true,
          enabled: true,
        },
      },
      localFanout: {
        enabled: true,
        resolutionFormat: "sqlite-v1",
        resolutionPath,
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
      },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    expect(lanes.map(({ name }) => name)).toEqual(
      expect.arrayContaining([
        "production-resolution-demand-refresh",
        "fanout",
        "local-enrichment-fanout-sol",
      ]),
    );
    await runtime.status();
    const health = JSON.parse(
      readFileSync(join(root, "health", "latest.json"), "utf8"),
    ) as { checks: { code: string }[] };
    expect(health.checks.map(({ code }) => code)).not.toContain(
      "SOL_LOCAL_FANOUT_RESOLUTION_UNAVAILABLE",
    );
    expect(health.checks.map(({ code }) => code)).not.toContain(
      "FANOUT_RESOLUTION_UNAVAILABLE",
    );
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("publishes provider queues for the active profile only", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-profile-health-"));
    const database = join(root, "ledger.sqlite3");
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      sol: { concurrency: 1, enabled: true },
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    await runtime.createLanes({ publicationAllowed: false, status: {} });
    const writer = Ledger.open(database);
    const currentInput = { poemId: "source-bound" };
    const dormantLegacyInput = { poemId: "dormant-legacy" };
    const obsoleteInput = { poemId: "obsolete" };
    const retiredProviderInput = { poemId: "retired-provider" };
    const current = writer.seed(
      {
        implementationVersion: ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion,
        input: currentInput,
        inputHash: inputHash(currentInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
      1,
    );
    writer.seed(
      {
        implementationVersion: ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion,
        input: dormantLegacyInput,
        inputHash: inputHash(dormantLegacyInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      1,
    );
    writer.seed(
      {
        implementationVersion: "retired-provider-v1",
        input: retiredProviderInput,
        inputHash: inputHash(retiredProviderInput),
        kind: "poem-enrichment-retired-provider",
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
      1,
    );
    writer.seed(
      {
        implementationVersion: "sol-obsolete-v1",
        input: obsoleteInput,
        inputHash: inputHash(obsoleteInput),
        kind: SOL_ENRICHMENT_WORK_KIND,
        priority: 0,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
      1,
    );
    const currentClaim = writer.claim(
      "current",
      2,
      100,
      [SOL_ENRICHMENT_WORK_KIND],
      {
        implementationVersion: ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion,
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
    );
    if (!currentClaim) throw new Error("Expected current profile claim");
    writer.succeed(currentClaim, "a".repeat(64), 3);
    expect(writer.get(current.workKey)?.state).toBe("succeeded");
    expect(writer.markImported(current.workKey, "a".repeat(64), 4)).toBe(
      "imported",
    );
    const obsoleteClaim = writer.claim(
      "obsolete",
      4,
      100,
      [SOL_ENRICHMENT_WORK_KIND],
      {
        implementationVersion: "sol-obsolete-v1",
        schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
      },
    );
    if (!obsoleteClaim) throw new Error("Expected obsolete profile claim");
    writer.operatorRelease(obsoleteClaim, "CODEX_OPERATION_OUTCOME_UNKNOWN", 5);
    writer.close();

    const runtimeStatus = (await runtime.status()) as {
      providerExecution: {
        providers: { admission: { primaryReason: string } }[];
      };
    };
    const health = JSON.parse(
      readFileSync(join(root, "health", "latest.json"), "utf8"),
    ) as {
      providers: {
        accepted: number;
        provider: string;
        unknownOperations: number;
      }[];
      queues: {
        deadLetter: number;
        kind: string;
        oldestReadyAt: null | number;
        pending: number;
        succeeded: number;
        total: number;
      }[];
    };
    expect(
      health.providers.find(({ provider }) => provider === "sol"),
    ).toMatchObject({ accepted: 1, unknownOperations: 0 });
    expect(health).not.toHaveProperty("providerExecution");
    const execution = await readProviderExecutionHealth(
      join(root, "health", "provider-execution-latest.json"),
    );
    expect(execution.providers[0]).toMatchObject({
      admission: { primaryReason: "budget_unarmed", state: "closed" },
      progress: { accepted: 1, terminalWork: 1 },
      provider: "sol",
      throughput: {
        generated: { lastAt: 3 },
        published: { lastAt: 4 },
        remaining: {
          endToEndPublication: 0,
          generatedAwaitingPublication: 0,
          generation: 0,
        },
      },
    });
    expect(runtimeStatus.providerExecution).toEqual(execution);
    expect(
      health.queues.find(({ kind }) => kind === SOL_ENRICHMENT_WORK_KIND),
    ).toMatchObject({
      deadLetter: 0,
      oldestReadyAt: null,
      pending: 0,
      succeeded: 1,
      total: 1,
    });
    expect(
      health.queues.some(
        ({ kind }) => kind === "poem-enrichment-retired-provider",
      ),
    ).toBe(false);
    runtime.close();
  });

  it("schedules collector cooldown at the persisted origin retry time", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-collector-cooldown-"));
    const database = join(root, "ledger.sqlite3");
    const now = Date.now();
    const ledger = Ledger.initialize(database);
    const origin = "https://source.invalid";
    let completedAt = now - 200;
    let retryAt = completedAt;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const originClaim = ledger.claimOrigin(origin, completedAt, 10_000);
      if (originClaim.state !== "claimed") throw new Error("expected lease");
      const failure = ledger.failOrigin(originClaim.lease, completedAt, 0, {
        circuitBreakerAfter: 3,
        circuitBreakerCooldownMs: 15 * 60_000,
        retryAt: completedAt,
      });
      retryAt = failure.nextAllowedAt;
      if (attempt < 3) completedAt = retryAt;
    }
    const input = { authorHref: "https://source.invalid/writers/test" };
    const pending = ledger.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input,
        inputHash: inputHash(input),
        kind: collectionWorkKinds().authorManifest,
        priority: 0,
        schemaVersion: collectorSchemaVersion(),
      },
      now - 100,
    );
    const eventsBefore = ledger.eventCount(pending.workKey);
    ledger.close();

    const config = parseScraperOperationConfig({
      collector: { enabled: true, headless: true, maximumPerCycle: 1 },
      retention: { enabled: false },
      schemaVersion: 1,
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    onTestFinished(async () => {
      try {
        const results = await Promise.allSettled(
          lanes.map(async (lane) => lane.close()),
        );
        const failures = results.filter(
          (result) => result.status === "rejected",
        );
        if (failures.length > 0) {
          throw new AggregateError(
            failures.map((result): unknown => result.reason),
            "Collector fixture lane cleanup failed",
          );
        }
      } finally {
        runtime.close();
      }
    });
    const collector = lanes.find(({ name }) => name === "collector");
    expect(collector).toBeDefined();
    await expect(
      collector!.runOnce(new AbortController().signal),
    ).resolves.toEqual({
      nextWakeAt: retryAt,
      result: "idle",
      urgentStatus: false,
    });
    const verification = Ledger.open(database);
    onTestFinished(() => verification.close());
    expect(verification.get(pending.workKey)).toMatchObject({
      attemptCount: 0,
      leaseOwner: null,
      state: "pending",
    });
    expect(verification.eventCount(pending.workKey)).toBe(eventsBefore);
  });

  it("publishes a blocked health check for a rejected duplicate", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-duplicate-health-"));
    const database = join(root, "ledger.sqlite3");
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      retention: { enabled: false },
      schemaVersion: 1,
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      paths: {
        artifacts: join(root, "artifacts"),
        database,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    await runtime.createLanes({ publicationAllowed: false, status: {} });
    const writer = Ledger.open(database);
    const input = { authorHref: "https://source.invalid/writers/poet-one" };
    writer.seed({
      implementationVersion: "test-v1",
      input,
      inputHash: inputHash(input),
      kind: "source_author_manifest",
      priority: 0,
      schemaVersion: "test-v1",
    });
    const claim = writer.claim("test", Date.now(), 1_000);
    if (!claim) throw new Error("Expected duplicate diagnostic claim");
    writer.deadLetter(claim, "SOURCE_POEM_DUPLICATE");
    writer.close();

    await runtime.status();
    expect(
      JSON.parse(readFileSync(join(root, "health", "latest.json"), "utf8")),
    ).toMatchObject({
      checks: [
        {
          code: "SOURCE_POEM_DUPLICATE",
          state: "blocked",
        },
      ],
      state: "blocked",
    });
    runtime.close();
  });

  it("fails publication closed before constructing resources", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-unified-preflight-"));
    const config = parseScraperOperationConfig({
      schemaVersion: 1,
      stateDirectory: root,
    });
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: {},
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    await expect(runtime.preflight()).resolves.toMatchObject({
      publicationAllowed: false,
      status: { d1: { blockers: expect.arrayContaining(["TIER_UNKNOWN"]) } },
    });
    expect(() => readFileSync(join(root, "ledger.sqlite3"))).toThrow();
  });

  it("requires the durable paid fence before continuous free publication starts", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-free-publication-"));
    const paidWorkPaused = join(root, "PAID_WORK_PAUSED");
    const resolutionCachePath = join(root, "resolution-demand.sqlite3");
    const resolutionRequestPath = join(root, "resolution-request.json");
    writeFileSync(
      resolutionRequestPath,
      JSON.stringify({
        schemaId: "saqi.production-resolution-request",
        schemaVersion: 1,
        targets: [
          {
            modelKeys: ["sol-5.6"],
            sourceAuthorSlug: "bootstrap-source",
            sourcePoemId: "2",
          },
        ],
      }),
    );
    const seededCache = new ProductionResolutionDemandCache({
      path: resolutionCachePath,
    });
    seededCache.registerPublicationDemand(
      inputHash({ poem: "orphan" }),
      "sol-5.6",
      "a".repeat(64),
      100,
    );
    seededCache.registerDemand({
      modelKeys: ["sol-5.6"],
      priority: 100,
      sourceAuthorSlug: "legacy-source",
      sourcePoemId: "1",
    });
    seededCache.close();
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      continuousFreePublication: true,
      fanout: {
        batchSize: 25,
        enabled: true,
        refresh: {
          cachePath: resolutionCachePath,
          enabled: true,
          requestPath: resolutionRequestPath,
        },
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
        maximumPerCycle: 10,
      },
      restart: { idlePollMs: 30_000 },
      schemaVersion: 1,
      sol: { enabled: false },
      startupReconciliation: { enabled: false },
      stateDirectory: root,
    });
    const createSolCoordinator = vi.fn(() => {
      throw new Error("PAID_SOL_COORDINATOR_MUST_NOT_BE_CONSTRUCTED");
    });
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: {},
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paidWorkPaused,
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      solCoordinatorFactory: createSolCoordinator,
    });
    await expect(runtime.preflight()).rejects.toThrow(
      "CONTINUOUS_FREE_PUBLICATION_REQUIRES_PAID_FENCE",
    );
    const controls = Ledger.open(join(root, "ledger.sqlite3"));
    controls.pauseControls.set("paid", true);
    controls.close();
    const preflight = await runtime.preflight();
    expect(preflight).toMatchObject({
      publicationAllowed: false,
    });
    const lanes = await runtime.createLanes(preflight);
    expect(lanes.map(({ name }) => name)).toEqual(
      expect.arrayContaining([
        "fanout",
        "publication",
        "production-resolution-demand-refresh",
      ]),
    );
    expect(lanes.map(({ name }) => name)).not.toContain("sol");
    expect(lanes.map(({ name }) => name)).not.toContain(
      "legacy-source-binding-reconciliation",
    );
    const refresh = lanes.find(
      ({ name }) => name === "production-resolution-demand-refresh",
    );
    expect(refresh).toBeDefined();
    await refresh!.runOnce(new AbortController().signal);
    const prunedCache = new ProductionResolutionDemandCache({
      path: resolutionCachePath,
    });
    expect(prunedCache.counts().demands).toBe(0);
    prunedCache.close();
    expect(createSolCoordinator).not.toHaveBeenCalled();
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("keeps a gated publication lane alive for periodic preflight recovery", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-publication-recovery-"));
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/publication",
      },
      retention: { enabled: false },
      restart: { errorBackoffMs: 2_000 },
      schemaVersion: 1,
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      environment: {},
      now: () => 1_000,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });

    const preflight = await runtime.preflight();
    expect(preflight.publicationAllowed).toBe(false);
    const lanes = await runtime.createLanes(preflight);
    const publication = lanes.find(({ name }) => name === "publication");
    expect(publication).toBeDefined();
    await expect(
      publication!.runOnce(new AbortController().signal),
    ).resolves.toEqual({ nextWakeAt: 61_000, result: "preflight_gated" });
    runtime.close();
  });

  it("drains source lineage through bounded authenticated requests without model quota", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-source-lineage-drain-"));
    let now = 10_000;
    const transport = vi.fn().mockResolvedValue({
      body: JSON.stringify({
        ok: true,
        result: { remaining: 80, state: "active" },
      }),
      status: 200,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config: parseScraperOperationConfig({
        collector: { enabled: false },
        publication: {
          enabled: true,
          endpoint: "https://ops.saqi.app/api/corpus-import",
        },
        retention: { enabled: false },
        schemaVersion: 1,
        sol: { enabled: false },
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      publicationAuth: {
        preflight: async () => {
          throw new Error("Unexpected preflight");
        },
        status: () => ({
          consecutiveRejections: 0,
          expiresAt: null,
          mode: "service_token",
          paused: false,
          pauseReason: null,
          retryAt: null,
        }),
        transport,
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: true,
      status: {},
    });
    const lineage = lanes.find(
      ({ name }) => name === "maintenance-source-lineage",
    );
    await expect(
      lineage?.runOnce(new AbortController().signal),
    ).resolves.toEqual({ nextWakeAt: 11_991, result: "progress" });
    expect(transport).toHaveBeenCalledWith(
      expect.objectContaining({
        body: '{"maxPages":2}',
        url: "https://ops.saqi.app/api/source-lineage-maintenance",
      }),
    );

    now = 12_000;
    transport.mockResolvedValueOnce({
      body: JSON.stringify({
        ok: true,
        result: { remaining: 0, state: "complete" },
      }),
      status: 200,
    });
    await expect(
      lineage?.runOnce(new AbortController().signal),
    ).resolves.toEqual({ nextWakeAt: 312_000, result: "complete" });
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("backs off the source-lineage lane when shared publication auth rejects", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-source-lineage-auth-"));
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config: parseScraperOperationConfig({
        collector: { enabled: false },
        publication: {
          enabled: true,
          endpoint: "https://ops.saqi.app/api/corpus-import",
        },
        restart: { errorBackoffMs: 4_000 },
        retention: { enabled: false },
        schemaVersion: 1,
        sol: { enabled: false },
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      now: () => 10_000,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
      publicationAuth: {
        preflight: async () => {
          throw new Error("Unexpected preflight");
        },
        status: () => ({
          consecutiveRejections: 1,
          expiresAt: null,
          mode: "service_token",
          paused: true,
          pauseReason: "rejected",
          retryAt: 14_000,
        }),
        transport: async () => ({
          authFailure: "rejected",
          body: "",
          status: 403,
        }),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: true,
      status: {},
    });
    const lineage = lanes.find(
      ({ name }) => name === "maintenance-source-lineage",
    );
    await expect(
      lineage?.runOnce(new AbortController().signal),
    ).resolves.toEqual({ nextWakeAt: 14_000, result: "auth-wait" });
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("rate limits retention scans independently of the supervisor poll", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-retention-cadence-"));
    let now = 1_000;
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      restart: { idlePollMs: 1_000 },
      retention: { apply: false, enabled: true },
      schemaVersion: 1,
      stateDirectory: root,
    });
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => now,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const retention = lanes.find(
      ({ name }) => name === "maintenance-retention",
    );
    expect(retention).toMatchObject({
      honorNextWakeAt: true,
      maximumSleepMs: 30_000,
    });
    await expect(
      retention!.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({
      result: "dry_run",
    });
    await expect(
      retention!.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({
      result: "waiting",
    });
    now += 60 * 60_000;
    await expect(
      retention!.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({
      result: "dry_run",
    });
    for (const lane of lanes) await lane.close();
    runtime.close();
  });

  it("backfills poem milestones in a bounded free maintenance lane", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-milestone-backfill-lane-"));
    const backfill = vi.spyOn(Ledger.prototype, "backfillSolPoemMilestones");
    await importEmptyTestOperations(root);
    const runtime = new UnifiedRigRuntime({
      config: parseScraperOperationConfig({
        collector: { enabled: false },
        retention: { enabled: false },
        schemaVersion: 1,
        sol: { enabled: true },
        startupReconciliation: { enabled: false },
        stateDirectory: root,
      }),
      configDigest: DIGEST,
      now: () => 1_000,
      paths: {
        artifacts: join(root, "artifacts"),
        database: join(root, "ledger.sqlite3"),
        paused: join(root, "PAUSED"),
        root,
        schedulerState: join(root, "sol-scheduler.json"),
        solAttempts: join(root, "sol-attempts"),
      },
    });
    const lanes = await runtime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const maintenance = lanes.find(
      ({ name }) => name === "maintenance-sol-poem-milestones",
    );
    expect(maintenance).toMatchObject({
      honorNextWakeAt: true,
      maximumSleepMs: 60 * 60_000,
    });
    expect(backfill).not.toHaveBeenCalled();
    await expect(
      maintenance!.runOnce(new AbortController().signal),
    ).resolves.toEqual({
      nextWakeAt: 1_000,
      result: "backfilling",
    });
    expect(backfill).toHaveBeenLastCalledWith("succeeded", 250, 1_000);
    await expect(
      maintenance!.runOnce(new AbortController().signal),
    ).resolves.toEqual({
      nextWakeAt: 60 * 60_000 + 1_000,
      result: "complete",
    });
    expect(backfill).toHaveBeenLastCalledWith("imported", 250, 1_000);
    for (const lane of lanes) await lane.close();
    runtime.close();
    backfill.mockRestore();
  });

  it("persists bounded retention progress across runtime restarts", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-retention-cursor-"));
    const attempts = join(root, "sol-attempts");
    mkdirSync(attempts);
    for (let index = 0; index < 251; index += 1) {
      const attemptId = `${index.toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`;
      mkdirSync(join(attempts, attemptId));
    }
    const config = parseScraperOperationConfig({
      collector: { enabled: false },
      restart: { idlePollMs: 1_000 },
      retention: { apply: false, enabled: true },
      schemaVersion: 1,
      stateDirectory: root,
    });
    const paths = {
      artifacts: join(root, "artifacts"),
      database: join(root, "ledger.sqlite3"),
      paused: join(root, "PAUSED"),
      root,
      schedulerState: join(root, "sol-scheduler.json"),
      solAttempts: attempts,
    };
    await importEmptyTestOperations(root);
    const firstRuntime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => 1_000,
      paths,
    });
    const firstLanes = await firstRuntime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const firstRetention = firstLanes.find(
      ({ name }) => name === "maintenance-retention",
    );
    await expect(
      firstRetention?.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({
      nextWakeAt: 31_000,
      result: "dry_run",
    });
    for (const lane of firstLanes) await lane.close();
    firstRuntime.close();

    const afterFirst = Ledger.open(paths.database);
    expect(
      JSON.parse(
        afterFirst.loadSchedulerState("retention-scan:sol")?.serialized ??
          "null",
      ),
    ).toMatchObject({ cursor: expect.any(String) });
    afterFirst.close();

    const secondRuntime = new UnifiedRigRuntime({
      config,
      configDigest: DIGEST,
      now: () => 6_000,
      paths,
    });
    const secondLanes = await secondRuntime.createLanes({
      publicationAllowed: false,
      status: {},
    });
    const secondRetention = secondLanes.find(
      ({ name }) => name === "maintenance-retention",
    );
    await expect(
      secondRetention?.runOnce(new AbortController().signal),
    ).resolves.toMatchObject({
      result: "dry_run",
    });
    for (const lane of secondLanes) await lane.close();
    secondRuntime.close();

    const afterSecond = Ledger.open(paths.database);
    expect(
      JSON.parse(
        afterSecond.loadSchedulerState("retention-scan:sol")?.serialized ??
          "null",
      ),
    ).toMatchObject({ cursor: null });
    afterSecond.close();
  });

  it("runs collection and bounded Sol in parallel while publication remains gated", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-unified-integration-"));
    const config = parseScraperOperationConfig({
      retention: { enabled: false },
      restart: { errorBackoffMs: 1_000, idlePollMs: 5_000 },
      schemaVersion: 1,
      sol: { concurrency: 2 },
      stateDirectory: root,
    });
    const schedulerLedger = Ledger.initialize(join(root, "ledger.sqlite3"));
    onTestFinished(() => schedulerLedger.close());
    const events: string[] = [];
    const sleeps: number[] = [];
    let publicationRuns = 0;
    let concurrentSol = 0;
    let maximumConcurrentSol = 0;
    let cycleBarrier = Promise.withResolvers<undefined>();
    let restartSolBarrier: null | PromiseWithResolvers<undefined> = null;

    const createSupervisor = (scheduler: QuotaAwareSolLaneScheduler) =>
      new UnifiedSupervisor({
        config,
        configDigest: DIGEST,
        createLanes: (preflight) => {
          events.push("resources");
          const lanes: SupervisorLane[] = [
            {
              name: "collector",
              close: () => {
                events.push("close:collector");
              },
              runOnce: async () => {
                events.push("start:collector");
                if (events.includes("start:sol"))
                  cycleBarrier.resolve(undefined);
                await cycleBarrier.promise;
                return { nextWakeAt: 2_000, result: "collected" };
              },
            },
            ...Array.from({ length: 2 }, (_, index): SupervisorLane => ({
              name: `sol-${String(index + 1)}`,
              close: () => {
                events.push(`close:sol-${String(index + 1)}`);
              },
              runOnce: async () => {
                const permit = await scheduler.acquire(OPEN);
                if (!permit)
                  return { nextWakeAt: 4_000, result: "at_capacity" };
                concurrentSol += 1;
                maximumConcurrentSol = Math.max(
                  maximumConcurrentSol,
                  concurrentSol,
                );
                events.push("start:sol");
                // Model real provider dispatch, which always crosses an I/O
                // boundary after admission and allows peer lanes to acquire.
                await new Promise<void>((resolvePromise) => {
                  setImmediate(resolvePromise);
                });
                if (restartSolBarrier !== null) {
                  if (concurrentSol === 2) restartSolBarrier.resolve(undefined);
                  await restartSolBarrier.promise;
                }
                if (events.includes("start:collector"))
                  cycleBarrier.resolve(undefined);
                await cycleBarrier.promise;
                concurrentSol -= 1;
                await permit.complete({ kind: "success" }, 1_100);
                return { nextWakeAt: 3_000, result: "succeeded" };
              },
            })),
          ];
          if (preflight?.publicationAllowed) {
            lanes.push({
              name: "publication",
              close: () => undefined,
              runOnce: () => {
                publicationRuns += 1;
                return Promise.resolve({ nextWakeAt: null, result: "sent" });
              },
            });
          }
          return lanes;
        },
        now: () => 1_000,
        paused: () => false,
        preflight: () => {
          events.push("preflight");
          return { publicationAllowed: false, status: { d1: { go: false } } };
        },
        resourcePressure: {
          snapshot: async () => ({
            availableDiskBytes: Number.MAX_SAFE_INTEGER,
            freeMemoryBytes: Number.MAX_SAFE_INTEGER,
            openFileDescriptors: 0,
            processRssBytes: 0,
            reasons: [],
            nextProbeAt: 1_000,
            state: "ready",
          }),
        },
        sleep: (milliseconds) => {
          sleeps.push(milliseconds);
          return Promise.resolve();
        },
      });

    const firstScheduler = new QuotaAwareSolLaneScheduler({
      ceiling: 2,
      configDigest: DIGEST,
      now: () => 1_000,
      promotionSuccesses: 2,
      stateKey: "test:sol",
      stateStore: schedulerLedger,
    });
    await createSupervisor(firstScheduler).run(new AbortController().signal, {
      maximumCycles: 2,
    });
    expect(events.indexOf("preflight")).toBeLessThan(
      events.indexOf("resources"),
    );
    expect(events).toContain("start:collector");
    expect(events).toContain("start:sol");
    expect(sleeps.toSorted((left, right) => left - right)).toEqual([
      1_000, 2_000, 2_000,
    ]);
    expect(publicationRuns).toBe(0);
    const firstSchedulerSnapshot = await firstScheduler.snapshot(OPEN);
    expect(firstSchedulerSnapshot.selectedConcurrency).toBe(2);

    maximumConcurrentSol = 0;
    cycleBarrier = Promise.withResolvers<undefined>();
    // Hold the first restarted invocation at the provider boundary until its
    // peer is admitted. Merely yielding one event-loop turn is insufficient:
    // scheduler admission performs asynchronous durable-state reads, so the
    // collector can otherwise release the first invocation before the second
    // lane has finished acquiring its permit. That race measured provider
    // latency rather than the scheduler's persisted capacity.
    restartSolBarrier = Promise.withResolvers<undefined>();
    const restartedScheduler = new QuotaAwareSolLaneScheduler({
      ceiling: 2,
      configDigest: DIGEST,
      now: () => 1_000,
      promotionSuccesses: 2,
      stateKey: "test:sol",
      stateStore: schedulerLedger,
    });
    await createSupervisor(restartedScheduler).run(
      new AbortController().signal,
      { maximumCycles: 1 },
    );
    expect(maximumConcurrentSol).toBe(2);
    expect(events).toEqual(
      expect.arrayContaining(["close:collector", "close:sol-1", "close:sol-2"]),
    );
    expect(
      JSON.parse(readFileSync(join(root, "status.json"), "utf8")),
    ).toMatchObject({
      preflight: { d1: { go: false } },
      state: "stopped",
    });
  });
});
