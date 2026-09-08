import { execFileSync } from "node:child_process";
import { existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import { describe, expect, it, vi } from "vitest";

import {
  cliSourceInitialization,
  loadCliSourceConfiguration,
} from "../runtime/cli-source-policy.js";
import { loadLaunchdSourceConfiguration } from "../runtime/source-keychain.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const CLI = resolve(import.meta.dirname, "../cli.ts");
const PROFILE = JSON.stringify(DEFAULT_SOURCE_ADAPTER_PROFILE);

describe("CLI source initialization policy", () => {
  it.each(["available", "missing"])(
    "actual config-bound init uses only %s managed Keychain authority",
    (mode) => {
      const root = trackedMkdtempSync(join(tmpdir(), "saqi-init-authority-"));
      const state = join(root, "state");
      const config = join(root, "rig.json");
      const preload = join(root, "keychain-fixture.mjs");
      writeFileSync(
        config,
        JSON.stringify({ schemaVersion: 1, stateDirectory: state }),
      );
      writeFileSync(
        preload,
        `
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { promisify } from "node:util";
const replacement = () => { throw new Error("Unexpected synchronous fixture call"); };
replacement[promisify.custom] = async (file, args) => {
  if (file !== "/usr/bin/security") throw new Error("Unexpected fixture subprocess");
  if (${JSON.stringify(mode)} === "missing") throw new Error("Fixture Keychain unavailable");
  return { stdout: args.includes("saqi-source-name") ? "managed-source" : args.includes("saqi-source-base-url") ? "https://managed.example" : ${JSON.stringify(JSON.stringify(DEFAULT_SOURCE_ADAPTER_PROFILE))}, stderr: "" };
};
childProcess.execFile = replacement;
syncBuiltinESMExports();
`,
      );
      const environment = {
        ...process.env,
        SAQI_SOURCE_NAME: "environment-source",
        SAQI_SOURCE_BASE_URL: "https://environment.example",
        SAQI_SOURCE_ADAPTER_CONFIG: PROFILE,
      };
      const invoke = () =>
        execFileSync(
          process.execPath,
          [
            "--import",
            preload,
            "--import",
            "tsx",
            CLI,
            "init",
            "--config",
            config,
          ],
          {
            encoding: "utf8",
            env: environment,
            stdio: "pipe",
            timeout: 20_000,
          },
        );
      if (mode === "missing") {
        expect(invoke).toThrow("SOURCE_KEYCHAIN_UNAVAILABLE");
        expect(existsSync(state)).toBe(false);
      } else {
        expect(invoke()).toContain('"initialized": true');
        expect(
          execFileSync(
            process.execPath,
            [
              "--import",
              preload,
              "--import",
              "tsx",
              CLI,
              "resume-paid",
              "--config",
              config,
              "--maximum-sol-operations",
              "3",
            ],
            {
              encoding: "utf8",
              env: environment,
              stdio: "pipe",
              timeout: 20_000,
            },
          ),
        ).toContain('"remainingOperations": 3');
        // The persisted identity must match managed authority, not poisoned env.
        expect(
          execFileSync(
            process.execPath,
            ["--import", "tsx", CLI, "init", "--state-dir", state],
            {
              encoding: "utf8",
              env: {
                ...environment,
                SAQI_SOURCE_NAME: "managed-source",
                SAQI_SOURCE_BASE_URL: "https://managed.example",
                SAQI_SOURCE_ADAPTER_CONFIG: PROFILE,
              },
              stdio: "pipe",
              timeout: 20_000,
            },
          ),
        ).toContain('"initialized": true');
      }
    },
  );

  it.each(["run-service", "service-control", "set-concurrency"])(
    "loads managed command %s from the launchd Keychain authority",
    (command) => {
      expect(cliSourceInitialization(command)).toBe("keychain");
    },
  );

  it.each(["doctor", "init", "resume-paid", "status", "verify"])(
    "loads config-bound ledger command %s from Keychain",
    (command) => {
      expect(
        cliSourceInitialization(command, [command, "--config", "rig.json"]),
      ).toBe("keychain");
      expect(
        cliSourceInitialization(command, [command, "--state-dir", "state"]),
      ).toBe("environment");
    },
  );

  it("keeps emergency service stop independent of Keychain", () => {
    expect(
      cliSourceInitialization("service-control", [
        "service-control",
        "--action",
        "stop",
      ]),
    ).toBe("none");
  });

  it("does not call a source reader for emergency service stop", async () => {
    const loadManagedSource = vi.fn();

    await expect(
      loadCliSourceConfiguration({
        command: "service-control",
        commandArguments: ["service-control", "--action", "stop"],
        environment: {
          SAQI_SOURCE_NAME: "must-not-be-read",
        },
        loadManagedSource,
      }),
    ).resolves.toBeNull();
    expect(loadManagedSource).not.toHaveBeenCalled();
  });

  it.each([
    "--help",
    "-h",
    "help",
    "health",
    "fetch-resolution",
    "install-service",
    "install-runtime",
    "import-sol-operations",
    "runtime-retention",
    "pause",
    "resume",
    "pause-paid",
    "validate-resolution",
  ])("keeps source-independent command %s independent", (command) => {
    expect(cliSourceInitialization(command)).toBe("none");
  });

  it.each(["run", "init", "status", "doctor", "seed-author", undefined])(
    "keeps ordinary command %s on explicit environment configuration",
    (command) => {
      expect(cliSourceInitialization(command)).toBe("environment");
    },
  );

  it.each(["--config", "--state-dir"])(
    "keeps health independent of source readers for %s",
    async (pathOption) => {
      const loadManagedSource = vi.fn();
      await expect(
        loadCliSourceConfiguration({
          command: "health",
          commandArguments: ["health", pathOption, "state"],
          environment: { SAQI_SOURCE_NAME: "poisoned-partial-value" },
          loadManagedSource,
        }),
      ).resolves.toBeNull();
      expect(loadManagedSource).not.toHaveBeenCalled();
    },
  );

  it("prints help without consulting an incomplete source environment", () => {
    const output = execFileSync(
      process.execPath,
      ["--import", "tsx", CLI, "--help"],
      {
        encoding: "utf8",
        env: {
          HOME: process.env["HOME"] ?? "",
          PATH: process.env["PATH"] ?? "",
          SAQI_SOURCE_NAME: "would-fail-if-consulted",
        },
        stdio: "pipe",
      },
    );

    expect(output).toContain("Usage: saqi-crawler <command>");
  });

  it("rejects ambiguous config and state-directory targets", () => {
    expect(() =>
      execFileSync(
        process.execPath,
        [
          "--import",
          "tsx",
          CLI,
          "health",
          "--config",
          "rig.json",
          "--state-dir",
          "state",
        ],
        { encoding: "utf8", stdio: "pipe" },
      ),
    ).toThrow("--config and --state-dir are mutually exclusive");
  });

  it.each(["service-control", "set-concurrency", "status", "init"])(
    "wires managed command %s to Keychain with source env absent",
    async (command) => {
      const loadManagedSource = vi.fn().mockResolvedValue({
        name: "private-source-name",
        origin: "https://private-source.example",
      });

      await expect(
        loadCliSourceConfiguration({
          command,
          commandArguments:
            command === "status" || command === "init"
              ? [command, "--config", "rig.json"]
              : [],
          environment: {},
          loadManagedSource,
        }),
      ).resolves.toEqual({
        name: "private-source-name",
        origin: "https://private-source.example",
      });
      expect(loadManagedSource).toHaveBeenCalledOnce();
    },
  );

  it("keeps managed wiring failures value-free with source env absent", async () => {
    const privateValue = "private-source-value-must-not-leak";
    const result = loadCliSourceConfiguration({
      command: "service-control",
      environment: {},
      loadManagedSource: () =>
        loadLaunchdSourceConfiguration(async (_account, service) => {
          if (service === "saqi-source-name") return "archive";
          throw new Error(privateValue);
        }),
    });

    await expect(result).rejects.toThrow("SOURCE_KEYCHAIN_UNAVAILABLE");
    await expect(result).rejects.not.toThrow(privateValue);
  });

  it("does not fall back to environment when config-bound init Keychain fails", async () => {
    const loadManagedSource = vi
      .fn()
      .mockRejectedValue(new Error("SOURCE_KEYCHAIN_UNAVAILABLE"));
    await expect(
      loadCliSourceConfiguration({
        command: "init",
        commandArguments: ["init", "--config", "rig.json"],
        environment: {
          SAQI_SOURCE_NAME: "environment-source",
          SAQI_SOURCE_BASE_URL: "https://environment.example",
        },
        loadManagedSource,
      }),
    ).rejects.toThrow("SOURCE_KEYCHAIN_UNAVAILABLE");
    expect(loadManagedSource).toHaveBeenCalledOnce();
  });
});
