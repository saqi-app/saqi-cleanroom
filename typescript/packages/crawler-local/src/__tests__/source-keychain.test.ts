import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  configureSource,
  currentSource,
  DEFAULT_SOURCE_ADAPTER_PROFILE,
} from "@saqi/source-adapter";
import { describe, expect, it, vi } from "vitest";

import {
  loadLaunchdDesiredConfigDigest,
  loadLaunchdSourceConfiguration,
} from "../runtime/source-keychain.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

describe("launchd source Keychain loading", () => {
  it("loads and validates all conventional Keychain entries", async () => {
    const readSecret = vi.fn(async (_account: string, service: string) => {
      if (service === "saqi-source-name") return "archive\n";
      if (service === "saqi-source-base-url") return "https://poetry.example\n";
      return JSON.stringify(DEFAULT_SOURCE_ADAPTER_PROFILE);
    });

    await expect(loadLaunchdSourceConfiguration(readSecret)).resolves.toEqual({
      name: "archive",
      origin: "https://poetry.example",
      profile: DEFAULT_SOURCE_ADAPTER_PROFILE,
    });
    expect(readSecret.mock.calls).toEqual([
      ["saqi-publication", "saqi-source-name"],
      ["saqi-publication", "saqi-source-base-url"],
      ["saqi-publication", "saqi-source-adapter-v1"],
    ]);
  });

  it.each(["missing-secret-value", "not-a-valid-origin-secret"])(
    "fails closed without returning a missing or invalid value: %s",
    async (privateValue) => {
      const readSecret = vi.fn(async (_account: string, service: string) => {
        if (service === "saqi-source-name") return "archive";
        if (service === "saqi-source-adapter-v1")
          return JSON.stringify(DEFAULT_SOURCE_ADAPTER_PROFILE);
        if (privateValue === "missing-secret-value")
          throw new Error(privateValue);
        return privateValue;
      });

      const result = loadLaunchdSourceConfiguration(readSecret);

      await expect(result).rejects.toThrow("SOURCE_KEYCHAIN_UNAVAILABLE");
      await expect(result).rejects.not.toThrow(privateValue);
    },
  );

  it("fails closed for malformed adapter JSON without exposing it", async () => {
    const privateValue = "private malformed adapter value";
    const result = loadLaunchdSourceConfiguration(async (_account, service) => {
      if (service === "saqi-source-name") return "archive";
      if (service === "saqi-source-base-url") return "https://poetry.example";
      return privateValue;
    });

    await expect(result).rejects.toThrow("SOURCE_KEYCHAIN_UNAVAILABLE");
    await expect(result).rejects.not.toThrow(privateValue);
  });

  it("detects a Keychain source switch without mutating the active source", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-source-digest-"));
    const configPath = join(root, "config.json");
    writeFileSync(
      configPath,
      JSON.stringify({ schemaVersion: 1, stateDirectory: root }),
    );
    configureSource({ name: "active", origin: "https://active.example" });
    let desired = { name: "first", origin: "https://first.example" };
    const readSecret = async (_account: string, service: string) => {
      if (service === "saqi-source-name") return desired.name;
      if (service === "saqi-source-base-url") return desired.origin;
      return JSON.stringify(DEFAULT_SOURCE_ADAPTER_PROFILE);
    };

    const first = await loadLaunchdDesiredConfigDigest(configPath, readSecret);
    desired = { name: "second", origin: "https://second.example" };
    const second = await loadLaunchdDesiredConfigDigest(configPath, readSecret);

    expect(second).not.toBe(first);
    expect(currentSource()).toEqual({
      name: "active",
      origin: "https://active.example",
    });
    configureSource({ name: "source", origin: "https://source.invalid" });
  });
});
