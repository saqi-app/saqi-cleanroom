import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import {
  preflightLaunchdService,
  renderLaunchdService,
} from "../runtime/launchd-service.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "saqi-launchd-"));
  const state = join(root, "state");
  const logs = join(root, "logs & audit");
  mkdirSync(state);
  Ledger.initialize(join(state, "ledger.sqlite3")).close();
  mkdirSync(logs);
  const executablePath = join(root, "saqi-crawler");
  writeFileSync(executablePath, "#!/usr/bin/env node\n");
  chmodSync(executablePath, 0o700);
  const configPath = join(root, "rig.json");
  writeFileSync(
    configPath,
    JSON.stringify({
      retention: { minimumFreeBytes: 0 },
      schemaVersion: 1,
      stateDirectory: state,
    }),
  );
  return {
    configPath,
    executablePath,
    exitTimeOutSeconds: 135,
    label: "net.saqi.crawler.test",
    standardErrorPath: join(logs, "stderr.log"),
    standardOutPath: join(logs, "stdout.log"),
    throttleIntervalSeconds: 60,
    workingDirectory: root,
    state,
  };
}

describe("launchd service packaging", () => {
  it("renders escaped boot checks and unsuccessful-exit recovery without file authority", async () => {
    const { state: _state, ...options } = fixture();
    const plist = await renderLaunchdService(options);
    expect(plist).toContain("logs &amp; audit");
    expect(plist).toContain("<key>RunAtLoad</key>\n  <true/>");
    expect(plist).toContain("<string>run-service</string>");
    expect(plist).not.toContain("<string>run</string>");
    expect(plist).toContain(
      "<key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>",
    );
    expect(plist).toContain(
      "<key>ThrottleInterval</key>\n  <integer>60</integer>",
    );
    expect(plist).toContain("<string>Background</string>");
    expect(plist).toContain(
      "<key>SoftResourceLimits</key>\n  <dict>\n    <key>NumberOfFiles</key>\n    <integer>4096</integer>",
    );
    expect(plist).toContain(
      "<key>HardResourceLimits</key>\n  <dict>\n    <key>NumberOfFiles</key>\n    <integer>4096</integer>",
    );
    expect(plist).toContain("EnvironmentVariables");
    expect(plist).toContain("<key>CODEX_HOME</key>");
    expect(plist).toContain("<key>HOME</key>");
    expect(plist).toContain("<key>PATH</key>");
    expect(plist).not.toContain("TOKEN");
    expect(plist).not.toContain("SAQI_SOURCE_BASE_URL");
    expect(plist).not.toContain("SAQI_SOURCE_NAME");
    expect(plist).not.toContain("<key>KeepAlive</key>\n  <true/>");
    expect(plist).not.toContain("<key>PathState</key>");
    expect(plist).not.toContain("SERVICE_ENABLED");
    if (process.platform === "darwin") {
      const path = join(options.workingDirectory, "rendered.plist");
      writeFileSync(path, plist);
      expect(() => execFileSync("plutil", ["-lint", path])).not.toThrow();
    }
  });

  it("rejects restart-thrashing throttle values and unknown keys", async () => {
    const { state: _firstState, ...first } = fixture();
    await expect(
      renderLaunchdService({ ...first, throttleIntervalSeconds: 29 }),
    ).rejects.toThrow();
    const { state: _state, ...options } = fixture();
    await expect(
      renderLaunchdService({ ...options, secret: "no" }),
    ).rejects.toThrow();
  });

  it("preflights built CLI, config, state, logs, disk, and singleton lock read-only", async () => {
    const { state, ...options } = fixture();
    const preflightOptions = {
      ...options,
      codexHomePath: join(options.workingDirectory, "missing-codex-home"),
    };
    await expect(
      preflightLaunchdService(preflightOptions),
    ).resolves.toMatchObject({
      codexAuth: {
        authPathHash: expect.stringMatching(/^[a-f\d]{64}$/),
        homePathHash: expect.stringMatching(/^[a-f\d]{64}$/),
        state: "missing",
      },
      issues: [],
      ok: true,
    });
    writeFileSync(
      join(state, "RUN.lock"),
      `${JSON.stringify({
        configDigest: "a".repeat(64),
        pid: process.pid,
        runId: "11111111-1111-4111-8111-111111111111",
        schemaVersion: 1,
        startedAt: new Date().toISOString(),
      })}\n`,
    );
    await expect(
      preflightLaunchdService(preflightOptions),
    ).resolves.toMatchObject({
      issues: [
        expect.objectContaining({
          code: "CONFIG_INVALID",
          message: "RUNTIME_OWNER_LEGACY_LOCK_PRESENT",
        }),
      ],
      ok: false,
    });
  });

  it("classifies explicit Codex authentication without reading or returning secrets", async () => {
    const { state: _state, ...options } = fixture();
    const codexHomePath = join(options.workingDirectory, "private-codex-home");
    mkdirSync(codexHomePath);
    writeFileSync(
      join(codexHomePath, "auth.json"),
      '{"access_token":"never-return-this-secret"}',
      { mode: 0o600 },
    );

    const report = await preflightLaunchdService({
      ...options,
      codexHomePath,
    });
    expect(report.codexAuth).toEqual({
      authPathHash: expect.stringMatching(/^[a-f\d]{64}$/),
      homePathHash: expect.stringMatching(/^[a-f\d]{64}$/),
      resolution: "explicit",
      state: "readable",
    });
    expect(JSON.stringify(report.codexAuth)).not.toContain(codexHomePath);
    expect(JSON.stringify(report.codexAuth)).not.toContain(
      "never-return-this-secret",
    );

    chmodSync(join(codexHomePath, "auth.json"), 0o000);
    const unreadable = await preflightLaunchdService({
      ...options,
      codexHomePath,
    });
    expect(unreadable.codexAuth.state).toBe("unreadable");
  });

  it("requires launchd to allow shutdown grace plus child cleanup", async () => {
    const { state: _state, ...options } = fixture();
    writeFileSync(
      options.configPath,
      JSON.stringify({
        restart: { shutdownGraceMs: 180_000 },
        retention: { minimumFreeBytes: 0 },
        schemaVersion: 1,
        stateDirectory: _state,
      }),
    );
    await expect(
      preflightLaunchdService({ ...options, exitTimeOutSeconds: 195 }),
    ).resolves.toMatchObject({ issues: [], ok: true });
    await expect(
      preflightLaunchdService({ ...options, exitTimeOutSeconds: 180 }),
    ).resolves.toMatchObject({
      issues: [expect.objectContaining({ code: "EXIT_TIMEOUT_TOO_SHORT" })],
      ok: false,
    });
  });
});
import { execFileSync } from "node:child_process";
