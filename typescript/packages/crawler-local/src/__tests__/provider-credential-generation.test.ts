import { execFileSync } from "node:child_process";
import {
  closeSync,
  constants,
  mkdirSync,
  openSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  providerCredentialGeneration,
  providerCredentialSnapshot,
} from "../enrichment/provider-credential-generation";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

function codexAuth(accountId: string, accessToken: string): string {
  return JSON.stringify({
    auth_mode: "chatgpt",
    tokens: { account_id: accountId, access_token: accessToken },
  });
}

describe("provider credential generations", () => {
  it("rejects a writerless FIFO without blocking and recovers after replacement", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-fifo-"));
    const root = join(home, ".codex");
    mkdirSync(root, { recursive: true });
    const path = join(root, "auth.json");
    execFileSync("mkfifo", [path], { timeout: 1_000 });
    let neededRescue = false;
    // A regressed blocking open must fail the assertion without leaving a
    // blocked libuv reader behind after the test times out.
    const rescue = setInterval(() => {
      neededRescue = true;
      const writer = openSync(path, constants.O_RDWR | constants.O_NONBLOCK);
      closeSync(writer);
    }, 1_000);
    try {
      await expect(
        providerCredentialSnapshot("sol", {}, home),
      ).resolves.toEqual({
        state: "transient_unavailable",
      });
      expect(neededRescue).toBe(false);
    } finally {
      clearInterval(rescue);
    }
    const replacement = join(root, "auth.replacement.json");
    writeFileSync(replacement, codexAuth("account-one", "token-one"));
    renameSync(replacement, path);
    await expect(
      providerCredentialSnapshot("sol", {}, home),
    ).resolves.toMatchObject({
      state: "observed",
    });
  });

  it("rejects a directory at the credential path", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-directory-"));
    mkdirSync(join(home, ".codex", "auth.json"), { recursive: true });
    await expect(providerCredentialSnapshot("sol", {}, home)).resolves.toEqual({
      state: "transient_unavailable",
    });
  });

  it("tracks Codex account switches without treating token refresh as a switch", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-account-"));
    const root = join(home, ".codex");
    mkdirSync(root, { recursive: true });
    const path = join(root, "auth.json");
    writeFileSync(path, codexAuth("account-one", "token-one"));
    const firstSnapshot = await providerCredentialSnapshot("sol", {}, home);
    const first = await providerCredentialGeneration("sol", {}, home);
    writeFileSync(path, codexAuth("account-one", "token-two"));
    const refreshedSnapshot = await providerCredentialSnapshot("sol", {}, home);
    const refreshed = await providerCredentialGeneration("sol", {}, home);
    writeFileSync(path, codexAuth("account-two", "token-three"));
    const switched = await providerCredentialGeneration("sol", {}, home);

    expect(refreshed).toBe(first);
    expect(firstSnapshot).toMatchObject({ state: "observed" });
    expect(refreshedSnapshot).toMatchObject({
      accountGeneration: first,
      state: "observed",
    });
    if (
      firstSnapshot.state === "observed" &&
      refreshedSnapshot.state === "observed"
    )
      expect(refreshedSnapshot.materialGeneration).not.toBe(
        firstSnapshot.materialGeneration,
      );
    expect(switched).not.toBe(first);
    expect(switched).not.toContain("account-two");
  });

  it("returns null when no known credential material exists", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-no-auth-"));
    await expect(
      providerCredentialGeneration("sol", {}, home),
    ).resolves.toBeNull();
    await expect(providerCredentialSnapshot("sol", {}, home)).resolves.toEqual({
      state: "absent",
    });
  });

  it.each([
    "{",
    JSON.stringify({ tokens: {} }),
    JSON.stringify({ tokens: { account_id: "account-one" } }),
    "secret",
  ])("fails closed when Codex auth JSON is incomplete: %s", async (content) => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-partial-"));
    const root = join(home, ".codex");
    mkdirSync(root, { recursive: true });
    writeFileSync(join(root, "auth.json"), content);

    await expect(providerCredentialSnapshot("sol", {}, home)).resolves.toEqual({
      state: "transient_unavailable",
    });
    await expect(
      providerCredentialGeneration("sol", {}, home),
    ).resolves.toBeNull();
  });

  it("re-observes after a truncate-write stabilizes without invoking a provider", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-truncate-race-"));
    const root = join(home, ".codex");
    const path = join(root, "auth.json");
    mkdirSync(root, { recursive: true });
    writeFileSync(path, codexAuth("account-one", "token-one"));
    const attempts: number[] = [];

    const observed = await providerCredentialSnapshot("sol", {}, home, {
      afterDescriptorRead: ({ attempt }) => {
        if (attempt === 0) writeFileSync(path, "{");
      },
      beforeAttempt: ({ attempt }) => {
        attempts.push(attempt);
        if (attempt === 1)
          writeFileSync(path, codexAuth("account-one", "token-two"));
      },
    });
    const stable = await providerCredentialSnapshot("sol", {}, home);

    expect(attempts).toEqual([0, 1]);
    expect(observed).toEqual(stable);
    expect(observed.state).toBe("observed");
  });

  it("bounds persistent malformed credential re-observation and fails closed", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-malformed-"));
    const root = join(home, ".codex");
    mkdirSync(root, { recursive: true });
    writeFileSync(join(root, "auth.json"), "{");
    const attempts: number[] = [];

    await expect(
      providerCredentialSnapshot("sol", {}, home, {
        beforeAttempt: ({ attempt }) => {
          attempts.push(attempt);
        },
      }),
    ).resolves.toEqual({ state: "transient_unavailable" });
    expect(attempts).toEqual([0, 1, 2]);
  });

  it("rejects an atomically replaced pathname and returns only the new inode", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-codex-rename-race-"));
    const root = join(home, ".codex");
    const path = join(root, "auth.json");
    const replacement = join(root, "auth.replacement.json");
    mkdirSync(root, { recursive: true });
    writeFileSync(path, codexAuth("account-one", "token-one"));
    const attempts: number[] = [];

    const observed = await providerCredentialSnapshot("sol", {}, home, {
      afterDescriptorRead: ({ attempt }) => {
        if (attempt !== 0) return;
        writeFileSync(replacement, codexAuth("account-two", "token-two"), {
          mode: 0o600,
        });
        renameSync(replacement, path);
      },
      beforeAttempt: ({ attempt }) => {
        attempts.push(attempt);
      },
    });
    const stable = await providerCredentialSnapshot("sol", {}, home);

    expect(attempts).toEqual([0, 1]);
    expect(observed).toEqual(stable);
    expect(observed).toMatchObject({ state: "observed" });
  });

  it("does not treat a credential directory override as credential material", async () => {
    const home = mkdtempSync(join(tmpdir(), "saqi-auth-location-"));
    await expect(
      providerCredentialSnapshot(
        "sol",
        { CODEX_HOME: join(home, "missing") },
        home,
      ),
    ).resolves.toEqual({ state: "absent" });
  });
});
