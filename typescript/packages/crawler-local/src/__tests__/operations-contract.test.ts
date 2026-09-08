import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import { describe, expect, it } from "vitest";

import {
  operationConfigDigestForSource,
  parseScraperOperationConfig,
} from "../runtime/operations-contract";

describe("scraper operations contract", () => {
  it("binds normalized source identity into the runtime digest", () => {
    const configuration = parseScraperOperationConfig({
      schemaVersion: 1,
      stateDirectory: "/tmp/saqi",
    });
    const first = operationConfigDigestForSource(configuration, {
      name: "archive-a",
      origin: "https://a.example",
    });

    expect(
      operationConfigDigestForSource(configuration, {
        name: "archive-b",
        origin: "https://b.example",
      }),
    ).not.toBe(first);
    expect(
      operationConfigDigestForSource(configuration, {
        name: "archive-a",
        origin: "https://a.example",
      }),
    ).toBe(first);
    expect(
      operationConfigDigestForSource(configuration, {
        name: "archive-a",
        origin: "https://a.example",
        profile: DEFAULT_SOURCE_ADAPTER_PROFILE,
      }),
    ).not.toBe(first);
  });

  it("allows a completed one-time enrichment reconciliation to be disabled", () => {
    expect(
      parseScraperOperationConfig({
        schemaVersion: 1,
        startupReconciliation: { enabled: false },
        stateDirectory: "/tmp/saqi",
      }).startupReconciliation,
    ).toEqual({ enabled: false });
  });

  it("applies conservative bounded defaults", () => {
    const configuration = parseScraperOperationConfig({
      schemaVersion: 1,
      stateDirectory: "./runtime",
    });
    expect(configuration).toMatchObject({
      baseline: { enabled: false },
      fanout: {
        enabled: false,
        refresh: {
          cachePath: null,
          demandDriven: true,
          enabled: false,
          requestPath: null,
        },
        resolutionFormat: null,
        resolutionPath: null,
      },
      inventory: { statusPath: null },
      localFanout: {
        enabled: false,
        resolutionFormat: null,
        resolutionPath: null,
      },
      collector: {
        detailBurst: 100,
        enabled: true,
        headless: false,
        maximumPerCycle: 25,
        minimumSourceGapMs: 13_000,
        recovery: { enabled: false },
      },
      logging: { maximumBytes: 25_165_824, retainedFiles: 7 },
      resources: {
        minimumFreeDiskBytes: 2_147_483_648,
        maximumProviderBufferedOutputBytes: 402_653_184,
        maximumProviderProcesses: 64,
        maximumProviderReservedMemoryBytes: 17_179_869_184,
        providerBufferedOutputReservationBytes: 6_291_456,
        providerProcessMemoryReservationBytes: 268_435_456,
      },
      retention: {
        apply: false,
        enabled: true,
        maximumAttemptsPerCycle: 25,
        maximumInputBytesPerCycle: 67_108_864,
        minimumFreeBytes: 2_147_483_648,
        safetyAgeMs: 604_800_000,
      },
      restart: {
        errorBackoffMs: 30_000,
        idlePollMs: 30_000,
        shutdownGraceMs: 120_000,
        startupTimeoutMs: 900_000,
      },
      startupReconciliation: { enabled: true },
      schemaId: "saqi.unified-rig",
      schemaVersion: 1,
      publication: {
        d1: { tier: "unknown" },
        enabled: false,
        endpoint: null,
      },
      sol: {
        concurrency: 2,
        enabled: true,
        minimumLaunchIntervalMs: 250,
        maximumPerCyclePerLane: 2,
        promotionSuccesses: 8,
      },
    });
    expect(Object.keys(configuration.sol)).toEqual([
      "concurrency",
      "enabled",
      "initialConcurrency",
      "minimumLaunchIntervalMs",
      "maximumPerCyclePerLane",
      "promotionSuccesses",
    ]);
  });

  it("admits only a bounded provider-free continuous publication mode", () => {
    const input = {
      collector: { enabled: false },
      continuousFreePublication: true,
      fanout: {
        batchSize: 25,
        enabled: true,
        refresh: {
          cachePath: "./resolution-demand.sqlite3",
          enabled: true,
        },
      },
      publication: {
        enabled: true,
        endpoint: "https://ops.saqi.app/api/corpus-import",
        maximumPerCycle: 50,
      },
      restart: { idlePollMs: 30_000 },
      schemaVersion: 1 as const,
      sol: { enabled: false },
      startupReconciliation: { enabled: false },
      stateDirectory: "/tmp/saqi-free-publication",
    };
    expect(parseScraperOperationConfig(input)).toMatchObject({
      continuousFreePublication: true,
      collector: { enabled: false },
      fanout: {
        batchSize: 25,
        enabled: true,
        refresh: { enabled: true },
        resolutionFormat: null,
        resolutionPath: null,
      },
      publication: { enabled: true, maximumPerCycle: 50 },
      sol: { enabled: false },
    });
    expect(() =>
      parseScraperOperationConfig({
        ...input,
        sol: { enabled: true },
      }),
    ).toThrow("Continuous free publication forbids");
    expect(() =>
      parseScraperOperationConfig({
        ...input,
        fanout: { ...input.fanout, batchSize: 26 },
      }),
    ).toThrow("fanout batchSize <= 25");
    expect(() =>
      parseScraperOperationConfig({
        ...input,
        publication: { ...input.publication, maximumPerCycle: 51 },
      }),
    ).toThrow("publication maximumPerCycle <= 50");
  });

  it("requires explicit files and endpoint for autonomous mutation lanes", () => {
    expect(() =>
      parseScraperOperationConfig({
        collector: { enabled: false, recovery: { enabled: true } },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled collector recovery requires collector");
    expect(() =>
      parseScraperOperationConfig({
        collector: { recovery: { enabled: true } },
        inventory: {
          enabled: true,
          productionAuthorsPath: "./authors.json",
          refreshGeneration: "test",
        },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Collector recovery cannot run with author inventory");
    expect(() =>
      parseScraperOperationConfig({
        baseline: { enabled: true },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled baseline requires");
    expect(() =>
      parseScraperOperationConfig({
        fanout: { enabled: true, resolutionPath: "./mapping.json" },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled fanout requires");
    expect(() =>
      parseScraperOperationConfig({
        fanout: { enabled: true },
        publication: {
          enabled: true,
          endpoint: "https://ops.saqi.app/api/corpus-import",
        },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("either demand refresh or a resolutionPath");
    expect(() =>
      parseScraperOperationConfig({
        localFanout: { enabled: true },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled localFanout requires a resolutionPath");
    expect(
      parseScraperOperationConfig(
        {
          localFanout: {
            enabled: true,
            resolutionFormat: "sqlite-v1",
            resolutionPath: "./resolution.sqlite",
          },
          schemaVersion: 1,
          stateDirectory: "/tmp/runtime",
        },
        "/tmp/config",
      ).localFanout,
    ).toEqual({
      batchSize: 250,
      enabled: true,
      resolutionFormat: "sqlite-v1",
      resolutionPath: "/tmp/config/resolution.sqlite",
    });
    expect(() =>
      parseScraperOperationConfig({
        publication: { enabled: true },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled publication requires endpoint");
    expect(() =>
      parseScraperOperationConfig({
        fanout: { refresh: { enabled: true } },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Enabled resolution refresh requires");
    expect(
      parseScraperOperationConfig(
        {
          fanout: {
            enabled: true,
            refresh: {
              cachePath: "./resolution-cache.sqlite3",
              enabled: true,
              requestPath: "./scope.json",
            },
            resolutionFormat: "sqlite-v1",
            resolutionPath: "./resolution.sqlite",
          },
          publication: {
            enabled: true,
            endpoint: "https://ops.saqi.app/api/corpus-import",
          },
          schemaVersion: 1,
          stateDirectory: "/tmp/runtime",
        },
        "/tmp/config",
      ).fanout.refresh.requestPath,
    ).toBe("/tmp/config/scope.json");
    expect(() =>
      parseScraperOperationConfig({
        fanout: { refresh: { demandDriven: false } },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow();
    expect(
      parseScraperOperationConfig(
        {
          fanout: {
            enabled: true,
            refresh: {
              cachePath: "./resolution-cache.sqlite3",
              demandDriven: true,
              enabled: true,
            },
          },
          publication: {
            enabled: true,
            endpoint: "https://ops.saqi.app/api/corpus-import",
          },
          schemaVersion: 1,
          stateDirectory: "/tmp/runtime",
        },
        "/tmp/config",
      ).fanout,
    ).toMatchObject({
      refresh: {
        cachePath: "/tmp/config/resolution-cache.sqlite3",
        demandDriven: true,
        enabled: true,
        requestPath: null,
      },
      resolutionFormat: null,
      resolutionPath: null,
    });
    expect(
      parseScraperOperationConfig({
        collector: { enabled: false },
        publication: {
          enabled: true,
          endpoint: "https://ops.saqi.app/api/corpus-import",
        },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }).publication.auth.identityEndpoint,
    ).toBe("https://ops.saqi.app/api/corpus-import");
  });

  it("fails legacy JSON resolution configs with an actionable migration code", () => {
    expect(() =>
      parseScraperOperationConfig({
        localFanout: { enabled: true, resolutionPath: "./mapping.json" },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("RESOLUTION_JSON_V1_RETIRED");
    expect(() =>
      parseScraperOperationConfig({
        localFanout: {
          enabled: true,
          resolutionPath: "./resolution.sqlite",
        },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("RESOLUTION_FORMAT_REQUIRED");
  });

  it("rejects unsafe source pacing and unknown configuration", () => {
    expect(() =>
      parseScraperOperationConfig({
        collector: { minimumSourceGapMs: 12_999 },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow();
    expect(() =>
      parseScraperOperationConfig({
        collector: { detailBurst: 0 },
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow();
    expect(() =>
      parseScraperOperationConfig({
        extra: true,
        schemaVersion: 1,
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow();
  });

  it("supports the 256-way Sol control", () => {
    expect(() =>
      parseScraperOperationConfig({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 257, enabled: true },
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow();

    expect(
      parseScraperOperationConfig({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 256, enabled: true },
        stateDirectory: "/tmp/runtime",
      }),
    ).toMatchObject({ sol: { concurrency: 256 } });

    expect(() =>
      parseScraperOperationConfig({
        collector: { enabled: false },
        schemaVersion: 1,
        sol: { concurrency: 256, enabled: true },
        stateDirectory: "/tmp/runtime",
      }),
    ).not.toThrow();
  });

  it("rejects removed provider configuration as an unknown key", () => {
    expect(() =>
      parseScraperOperationConfig({
        collector: { enabled: false },
        formerProvider: { enabled: true },
        schemaVersion: 1,
        sol: { enabled: true },
        stateDirectory: "/tmp/runtime",
      }),
    ).toThrow("Unrecognized key");
  });
});
