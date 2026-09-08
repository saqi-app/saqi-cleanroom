import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const CLI = resolve(import.meta.dirname, "../cli.ts");

interface InstallServiceOutput {
  readonly report: {
    readonly codexAuth: {
      readonly resolution: string;
      readonly state: string;
    };
    readonly options: { readonly codexHomePath: string };
    readonly plist: string;
  };
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "saqi-launchd-cli-"));
  const stateDirectory = join(root, "state");
  const logs = join(root, "logs");
  const codexHome = join(root, "codex-home");
  mkdirSync(stateDirectory);
  Ledger.initialize(join(stateDirectory, "ledger.sqlite3")).close();
  mkdirSync(logs);
  mkdirSync(codexHome);
  writeFileSync(join(codexHome, "auth.json"), "private credential material", {
    mode: 0o600,
  });
  const executable = join(root, "saqi-crawler");
  writeFileSync(executable, "#!/usr/bin/env node\n");
  chmodSync(executable, 0o700);
  const config = join(root, "rig.json");
  writeFileSync(
    config,
    JSON.stringify({
      retention: { minimumFreeBytes: 0 },
      schemaVersion: 1,
      stateDirectory,
    }),
  );
  return {
    arguments: [
      "install-service",
      "--dry-run",
      "--config",
      config,
      "--executable",
      executable,
      "--workdir",
      root,
      "--stdout",
      join(logs, "stdout.log"),
      "--stderr",
      join(logs, "stderr.log"),
    ],
    codexHome,
  } as const;
}

function runCli(
  commandArguments: readonly string[],
  environment: NodeJS.ProcessEnv,
): InstallServiceOutput {
  return JSON.parse(
    execFileSync(
      process.execPath,
      ["--import", "tsx", CLI, ...commandArguments],
      { encoding: "utf8", env: environment, stdio: "pipe" },
    ),
  ) as InstallServiceOutput;
}

describe("launchd CLI credential home", () => {
  it("resolves and propagates an explicit --codex-home", () => {
    const target = fixture();
    const output = runCli(
      [...target.arguments, "--codex-home", target.codexHome],
      process.env,
    );

    expect(output.report.codexAuth).toEqual({
      authPathHash: expect.stringMatching(/^[a-f\d]{64}$/),
      homePathHash: expect.stringMatching(/^[a-f\d]{64}$/),
      resolution: "explicit",
      state: "readable",
    });
    expect(output.report.options.codexHomePath).toBe(target.codexHome);
    expect(output.report.plist).toContain(
      `<string>${target.codexHome}</string>`,
    );
    expect(JSON.stringify(output.report.codexAuth)).not.toContain(
      "private credential material",
    );
  });

  it("defaults to the invoking CODEX_HOME", () => {
    const target = fixture();
    const output = runCli(target.arguments, {
      ...process.env,
      CODEX_HOME: target.codexHome,
    });

    expect(output.report.codexAuth).toMatchObject({
      resolution: "environment",
      state: "readable",
    });
    expect(output.report.options.codexHomePath).toBe(target.codexHome);
    expect(output.report.plist).toContain(
      `<string>${target.codexHome}</string>`,
    );
  });

  it("defaults to the invoking user's .codex directory when CODEX_HOME is unset", () => {
    const target = fixture();
    const { CODEX_HOME: _codexHome, ...environment } = process.env;
    const output = runCli(target.arguments, environment);
    const expected = resolve(homedir(), ".codex");

    expect(output.report.codexAuth.resolution).toBe("default");
    expect(output.report.options.codexHomePath).toBe(expected);
    expect(output.report.plist).toContain(`<string>${expected}</string>`);
  });
});
