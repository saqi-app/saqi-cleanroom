import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import {
  evaluateLaunchdIdentity,
  evaluateLaunchdState,
  evaluateRuntimeAcknowledgement,
  evaluateRuntimeStatusIdentity,
  type LaunchdControlSnapshot,
  loadedServiceContractIssue,
  replacementAcknowledged,
  runOwnershipMatches,
  setLaunchdServiceEnabled,
  sourceIndependentStopPlan,
  withLaunchdServiceEnabled,
} from "../runtime/launchd-control.js";
import { serviceEnabledPath } from "../runtime/launchd-service.js";
import { readServiceEnabled } from "../runtime/service-enabled-control.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const DIGEST = "a".repeat(64);

describe("launchd control fencing", () => {
  it("preserves service intent when a derived mirror cannot be replaced or removed", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-service-mirror-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    readServiceEnabled(root);
    mkdirSync(serviceEnabledPath(root));
    await setLaunchdServiceEnabled(root, true);
    expect(readServiceEnabled(root)).toBe(true);
    await setLaunchdServiceEnabled(root, false);
    expect(readServiceEnabled(root)).toBe(false);
  });

  it("plans a minimal credential-free stop with conservative defaults", () => {
    expect(
      sourceIndependentStopPlan(
        "/tmp/config/rig.json",
        { stateDirectory: "../state" },
        undefined,
      ),
    ).toEqual({
      configDigest: "0".repeat(64),
      root: "/tmp/state",
      shutdownGraceMs: 120_000,
    });
  });

  it("uses only the acknowledged owner digest for credential-free stop", () => {
    expect(
      sourceIndependentStopPlan(
        "/tmp/config/rig.json",
        {
          restart: { shutdownGraceMs: 45_000 },
          stateDirectory: "/tmp/state",
        },
        DIGEST,
      ),
    ).toEqual({
      configDigest: DIGEST,
      root: "/tmp/state",
      shutdownGraceMs: 45_000,
    });
    expect(() =>
      sourceIndependentStopPlan(
        "/tmp/config/rig.json",
        { stateDirectory: "/tmp/state" },
        "not-an-owner-digest",
      ),
    ).toThrow();
  });

  it.each([
    [false, null, null, null],
    [false, 123, null, null],
    [true, 123, 123, null],
    [true, null, 123, "SERVICE_PID_MISMATCH"],
    [true, 123, null, "SERVICE_PID_MISMATCH"],
    [true, 123, 456, "SERVICE_PID_MISMATCH"],
  ] as const)(
    "checks running=%s launchdPid=%s lockPid=%s",
    (running, launchdPid, lockPid, expected) => {
      expect(evaluateLaunchdIdentity(running, launchdPid, lockPid)).toBe(
        expected,
      );
    },
  );

  it.each([
    [true, null, "fenced", "RUN_LOCK_MISSING"],
    [
      true,
      { configDigest: "b".repeat(64) },
      "running_outdated",
      "CONFIG_DIGEST_MISMATCH",
    ],
    [false, { configDigest: DIGEST }, "fenced", "RUN_LOCK_RETAINED"],
    [false, null, "stopped", null],
    [true, { configDigest: DIGEST }, "running", null],
  ] as const)(
    "evaluates running=%s lock=%j as %s",
    (running, lock, actualState, detail) => {
      expect(evaluateLaunchdState(running, lock, DIGEST)).toEqual({
        actualState,
        detail,
      });
    },
  );

  it("requires status to acknowledge the exact process, run, and config", () => {
    const lock = { configDigest: DIGEST, pid: 123, runId: "run-2" };
    const now = Date.parse("2026-08-29T05:00:00.000Z");
    expect(
      evaluateRuntimeStatusIdentity(true, 123, lock, null, DIGEST, null, now),
    ).toBe("SERVICE_STATUS_MISSING");
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        {
          configDigest: DIGEST,
          loadedConfigDigest: DIGEST,
          observedAt: "2026-08-29T05:00:00.000Z",
          ownerPid: 123,
          runId: "run-1",
          state: "running",
          startup: { lanesReady: true, phase: "ready" },
        },
        DIGEST,
        null,
        now,
      ),
    ).toBe("SERVICE_STATUS_IDENTITY_MISMATCH");
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        {
          configDigest: DIGEST,
          loadedConfigDigest: DIGEST,
          observedAt: "2026-08-29T05:00:00.000Z",
          ownerPid: 123,
          runId: "run-2",
          state: "paused",
          startup: { lanesReady: true, phase: "ready" },
        },
        DIGEST,
        null,
        now,
      ),
    ).toBeNull();
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        {
          configDigest: DIGEST,
          loadedConfigDigest: DIGEST,
          observedAt: "2026-08-29T05:00:00.000Z",
          ownerPid: 123,
          runId: "run-2",
          state: "starting",
          startup: { lanesReady: false, phase: "creating_lanes" },
        },
        DIGEST,
        null,
        now,
      ),
    ).toBe("SERVICE_STATUS_NOT_READY");
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        {
          configDigest: DIGEST,
          loadedConfigDigest: DIGEST,
          observedAt: "2026-08-29T05:00:00.000Z",
          ownerPid: 123,
          runId: "run-2",
        },
        DIGEST,
        null,
        now,
      ),
    ).toBeNull();
  });

  it("rejects stale status and the wrong immutable runtime", () => {
    const now = Date.parse("2026-08-29T05:05:00.000Z");
    const lock = { configDigest: DIGEST, pid: 123, runId: "run-2" };
    const status = {
      configDigest: DIGEST,
      loadedConfigDigest: DIGEST,
      observedAt: "2026-08-29T05:05:00.000Z",
      ownerPid: 123,
      runId: "run-2",
      state: "running" as const,
      startup: { lanesReady: true, phase: "ready" as const },
      runtimeRelease: {
        commit: "b".repeat(40),
        manifestPath: "/runtime/releases/b/runtime-release.json",
      },
    };
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        status,
        DIGEST,
        "c".repeat(40),
        now,
      ),
    ).toBe("SERVICE_RUNTIME_COMMIT_MISMATCH");
    expect(
      evaluateRuntimeStatusIdentity(
        true,
        123,
        lock,
        { ...status, observedAt: "2026-08-29T04:49:59.999Z" },
        DIGEST,
        "b".repeat(40),
        now,
      ),
    ).toBe("SERVICE_STATUS_STALE");
  });

  it.each(["fenced", "stopped"] as const)(
    "does not acknowledge a %s process as ready",
    (state) => {
      const now = Date.parse("2026-08-29T05:00:00.000Z");
      expect(
        evaluateRuntimeStatusIdentity(
          true,
          123,
          { configDigest: DIGEST, pid: 123, runId: "run-2" },
          {
            configDigest: DIGEST,
            loadedConfigDigest: DIGEST,
            observedAt: "2026-08-29T05:00:00.000Z",
            ownerPid: 123,
            runId: "run-2",
            state,
          },
          DIGEST,
          null,
          now,
        ),
      ).toBe("SERVICE_STATUS_NOT_READY");
    },
  );

  it.each([
    ["running", { lanesReady: false, phase: "creating_lanes" }],
    ["running", { lanesReady: false, phase: "ready" }],
    ["paused", { lanesReady: true, phase: "failed" }],
  ] as const)(
    "requires ready lanes for an explicit %s runtime",
    (state, startup) => {
      const now = Date.parse("2026-08-29T05:00:00.000Z");
      expect(
        evaluateRuntimeStatusIdentity(
          true,
          123,
          { configDigest: DIGEST, pid: 123, runId: "run-2" },
          {
            configDigest: DIGEST,
            loadedConfigDigest: DIGEST,
            observedAt: "2026-08-29T05:00:00.000Z",
            ownerPid: 123,
            runId: "run-2",
            startup,
            state,
          },
          DIGEST,
          null,
          now,
        ),
      ).toBe("SERVICE_STATUS_NOT_READY");
    },
  );

  it("preserves startup ownership without reporting readiness", () => {
    const running = { actualState: "running" as const, detail: null };
    expect(
      evaluateRuntimeAcknowledgement(running, "SERVICE_STATUS_NOT_READY"),
    ).toEqual({
      actualState: "starting",
      detail: "SERVICE_STATUS_NOT_READY",
    });
    expect(
      evaluateRuntimeAcknowledgement(running, "SERVICE_STATUS_STALE"),
    ).toEqual({
      actualState: "running_outdated",
      detail: "SERVICE_STATUS_STALE",
    });
    expect(evaluateRuntimeAcknowledgement(running, null)).toEqual(running);
  });
});

describe("loaded launchd contract", () => {
  it.each([
    [false, null, null],
    [true, null, "SERVICE_EXIT_TIMEOUT_TOO_SHORT"],
    [true, 2_114, "SERVICE_EXIT_TIMEOUT_TOO_SHORT"],
    [true, 2_115, null],
    [true, 2_160, null],
  ] as const)("evaluates loaded=%s timeout=%s", (loaded, timeout, expected) => {
    expect(loadedServiceContractIssue(loaded, timeout, 2_115)).toBe(expected);
  });

  it("durably distinguishes operator stop from an unexpected successful exit", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-service-enabled-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    const path = serviceEnabledPath(root);
    await setLaunchdServiceEnabled(root, true);
    expect(readServiceEnabled(root)).toBe(true);
    expect(JSON.parse(readFileSync(path, "utf8"))).toMatchObject({
      schemaVersion: 1,
    });

    await setLaunchdServiceEnabled(root, true);
    expect(existsSync(path)).toBe(true);
    await setLaunchdServiceEnabled(root, false);
    expect(readServiceEnabled(root)).toBe(false);
    expect(existsSync(path)).toBe(false);
    await setLaunchdServiceEnabled(root, false);
    expect(existsSync(path)).toBe(false);
  });
});

describe("durable desired-running replacement", () => {
  it.each(["REPLACEMENT_SIGNAL_FAILED", "LAUNCHD_RESTART_NOT_ACKNOWLEDGED"])(
    "preserves SERVICE_ENABLED when replacement fails with %s",
    async (code) => {
      const root = mkdtempSync(join(tmpdir(), "saqi-restart-enabled-"));
      Ledger.initialize(join(root, "ledger.sqlite3")).close();
      let replacementStarted = false;
      await expect(
        withLaunchdServiceEnabled(root, () => {
          replacementStarted = true;
          expect(existsSync(serviceEnabledPath(root))).toBe(true);
          return Promise.reject(new Error(code));
        }),
      ).rejects.toThrow(code);
      expect(replacementStarted).toBe(true);
      expect(readServiceEnabled(root)).toBe(true);
      expect(existsSync(serviceEnabledPath(root))).toBe(true);
    },
  );

  it("requires a new run with the exact config and managed runtime", () => {
    const previousRunId = "00000000-0000-4000-8000-000000000001";
    const desiredRuntimeCommit = "b".repeat(40);
    const acknowledged = serviceSnapshot({
      desiredRuntimeCommit,
      runId: "00000000-0000-4000-8000-000000000002",
      runtimeCommit: desiredRuntimeCommit,
    });
    expect(replacementAcknowledged(acknowledged, previousRunId)).toBe(true);
    expect(
      replacementAcknowledged(
        { ...acknowledged, runId: previousRunId },
        previousRunId,
      ),
    ).toBe(false);
    expect(
      replacementAcknowledged(
        { ...acknowledged, loadedConfigDigest: "c".repeat(64) },
        previousRunId,
      ),
    ).toBe(false);
    expect(
      replacementAcknowledged(
        { ...acknowledged, runtimeCommit: "d".repeat(40) },
        previousRunId,
      ),
    ).toBe(false);
    expect(
      replacementAcknowledged(
        { ...acknowledged, serviceEnabled: false },
        previousRunId,
      ),
    ).toBe(false);
  });

  it("accepts an unmanaged replacement without inventing a runtime commit", () => {
    expect(
      replacementAcknowledged(
        serviceSnapshot({
          desiredRuntimeCommit: null,
          runId: "00000000-0000-4000-8000-000000000002",
          runtimeCommit: null,
        }),
        "00000000-0000-4000-8000-000000000001",
      ),
    ).toBe(true);
  });

  it("refuses a signal when the PID is reused by a replacement run", () => {
    const ownerPid = 123;
    const previousRunId = "00000000-0000-4000-8000-000000000001";
    const first = { pid: ownerPid, runId: previousRunId };
    expect(runOwnershipMatches(first, first, ownerPid, previousRunId)).toBe(
      true,
    );
    expect(
      runOwnershipMatches(
        first,
        {
          pid: ownerPid,
          runId: "00000000-0000-4000-8000-000000000002",
        },
        ownerPid,
        previousRunId,
      ),
    ).toBe(false);
    expect(runOwnershipMatches(first, null, ownerPid, previousRunId)).toBe(
      false,
    );
  });
});

function serviceSnapshot(
  overrides: Partial<LaunchdControlSnapshot> = {},
): LaunchdControlSnapshot {
  return {
    action: "restart",
    actualState: "running",
    allowedActions: ["restart", "status", "stop"],
    configDigest: DIGEST,
    desiredConfigDigest: DIGEST,
    desiredRuntimeCommit: null,
    detail: null,
    loadedConfigDigest: DIGEST,
    observedAt: "2026-08-29T05:00:00.000Z",
    ownerPid: 123,
    runId: "00000000-0000-4000-8000-000000000002",
    runtimeCommit: null,
    schemaId: "saqi.launchd-control",
    schemaVersion: 1,
    serviceEnabled: true,
    ...overrides,
  };
}
