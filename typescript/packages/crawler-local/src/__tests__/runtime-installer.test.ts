import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readlinkSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  installRuntime,
  readRuntimeReleaseIdentity,
  retainRuntimeReleases,
} from "../runtime/runtime-installer.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

describe("immutable runtime installer", () => {
  it("builds every runtime workspace dependency before smoke testing", async () => {
    const fixture = repositoryFixture("build-plan");
    const commands: string[] = [];
    await installRuntime(options(fixture, fixture.first), {
      run: (command, argumentsList, cwd) => {
        commands.push([command, ...argumentsList].join(" "));
        if (commands.length === 1) {
          const yarnDirectory = join(cwd, ".yarn");
          mkdirSync(join(yarnDirectory, "cache"), { recursive: true });
          writeFileSync(
            join(yarnDirectory, "cache", "build-only.zip"),
            "cache",
          );
          writeFileSync(join(yarnDirectory, "install-state.gz"), "state");
        }
      },
    });
    expect(commands).toEqual([
      "corepack yarn install --immutable",
      "corepack yarn workspace @saqi/source-adapter build",
      "corepack yarn workspace @saqi/precedent-iso build",
      "corepack yarn workspace @saqi/crawler-local build",
      "corepack yarn workspaces focus @saqi/crawler-local --production",
    ]);
    const installedTypescript = join(
      fixture.releaseRoot,
      "current",
      "typescript",
    );
    expect(existsSync(join(installedTypescript, ".yarn", "cache"))).toBe(false);
    expect(
      existsSync(join(installedTypescript, ".yarn", "install-state.gz")),
    ).toBe(false);
  });

  it("refuses dirty source and retains a verified prior release", async () => {
    const fixture = repositoryFixture("lifecycle");
    const firstResult = await install(fixture, fixture.first);
    expect(firstResult.reused).toBe(false);
    expect(readlinkSync(join(fixture.releaseRoot, "current"))).toBe(
      join("releases", fixture.first),
    );
    expect(lstatSync(firstResult.release).isDirectory()).toBe(true);
    const firstManifest = JSON.parse(
      readFileSync(join(firstResult.release, "runtime-release.json"), "utf8"),
    );
    expect(firstManifest).toMatchObject({
      commit: fixture.first,
      nodeVersion: process.version,
      schemaId: "saqi.runtime-release",
      schemaVersion: 2,
    });
    expect(firstManifest.closureSha256).toMatch(/^[0-9a-f]{64}$/u);
    await expect(
      readRuntimeReleaseIdentity(
        join(
          fixture.releaseRoot,
          "current/typescript/packages/crawler-local/dist/cli.js",
        ),
      ),
    ).resolves.toMatchObject({
      closureSha256: firstManifest.closureSha256,
      commit: fixture.first,
      manifestPath: join(firstResult.release, "runtime-release.json"),
    });
    const releaseMode = lstatSync(firstResult.release).mode & 0o777;
    chmodSync(firstResult.release, releaseMode | 0o200);
    await expect(
      readRuntimeReleaseIdentity(
        join(
          fixture.releaseRoot,
          "current/typescript/packages/crawler-local/dist/cli.js",
        ),
      ),
    ).rejects.toThrow("RUNTIME_RELEASE_NOT_IMMUTABLE");
    chmodSync(firstResult.release, releaseMode);

    writeFileSync(fixture.module, "module.exports = 'dirty';\n");
    await expect(install(fixture, fixture.first)).rejects.toThrow(
      "RUNTIME_SOURCE_DIRTY",
    );

    git(fixture.repository, ["add", "."]);
    git(fixture.repository, ["commit", "--quiet", "-m", "second"]);
    const second = git(fixture.repository, ["rev-parse", "HEAD"]).trim();
    const secondResult = await install(fixture, second);
    expect(readlinkSync(join(fixture.releaseRoot, "current"))).toBe(
      join("releases", second),
    );
    expect(readlinkSync(join(fixture.releaseRoot, "previous"))).toBe(
      join("releases", fixture.first),
    );
    expect(lstatSync(firstResult.release).isDirectory()).toBe(true);
    const reusedSecond = await install(fixture, second);
    expect(reusedSecond.reused).toBe(true);

    const installedModule = join(
      secondResult.release,
      "typescript/packages/crawler-local/dist/module.js",
    );
    chmodSync(installedModule, 0o644);
    writeFileSync(installedModule, "module.exports = 'tampered';\n");
    chmodSync(installedModule, 0o444);
    await expect(install(fixture, second)).rejects.toThrow(
      "RUNTIME_EXECUTABLE_CLOSURE_MISMATCH",
    );

    rmSync(fixture.repository, { force: true, recursive: true });
    expect(() =>
      execFileSync(
        join(
          fixture.releaseRoot,
          "current/typescript/packages/crawler-local/dist/cli.js",
        ),
        ["--help"],
      ),
    ).not.toThrow();
  });

  it("serializes installers and recovers a dead-owner lock", async () => {
    const fixture = repositoryFixture("locking");
    let contentionChecked = false;
    await installRuntime(options(fixture, fixture.first), {
      prepareRelease: async () => {
        await expect(install(fixture, fixture.first)).rejects.toThrow(
          "RUNTIME_INSTALL_LOCKED",
        );
        contentionChecked = true;
      },
    });
    expect(contentionChecked).toBe(true);

    rmSync(join(fixture.releaseRoot, "current"));
    writeFileSync(
      join(fixture.releaseRoot, ".install.lock"),
      `${JSON.stringify({
        nonce: randomUUID(),
        pid: 2_147_483_647,
        schemaId: "saqi.runtime-install-lock",
        schemaVersion: 1,
        startedAt: Date.now() - 60_000,
      })}\n`,
      { mode: 0o600 },
    );
    const recovered = await install(fixture, fixture.first);
    expect(recovered.reused).toBe(true);
  });

  it("shares the install lock with retention", async () => {
    const fixture = repositoryFixture("retention-locking");
    await installRuntime(options(fixture, fixture.first), {
      prepareRelease: async () => {
        await expect(
          retainRuntimeReleases({
            releaseRoot: fixture.releaseRoot,
            repository: fixture.repository,
          }),
        ).rejects.toThrow("RUNTIME_INSTALL_LOCKED");
      },
    });
  });

  it("plans safely by default and applies only a bounded verified subset", async () => {
    const fixture = repositoryFixture("retention");
    const anchored: string[] = [fixture.first];
    await install(fixture, fixture.first);
    for (let index = 1; index < 7; index += 1) {
      const version = String(index);
      writeFileSync(fixture.module, `module.exports = '${version}';\n`);
      git(fixture.repository, ["add", "."]);
      git(fixture.repository, [
        "commit",
        "--quiet",
        "-m",
        `release-${version}`,
      ]);
      const commit = git(fixture.repository, ["rev-parse", "HEAD"]).trim();
      anchored.push(commit);
      await install(fixture, commit);
    }

    writeFileSync(fixture.module, "module.exports = 'unanchored';\n");
    git(fixture.repository, ["add", "."]);
    git(fixture.repository, ["commit", "--quiet", "-m", "unanchored"]);
    const unanchored = git(fixture.repository, ["rev-parse", "HEAD"]).trim();
    await install(fixture, unanchored);
    writeFileSync(fixture.module, "module.exports = 'temporary-current';\n");
    git(fixture.repository, ["add", "."]);
    git(fixture.repository, ["commit", "--quiet", "-m", "temporary-current"]);
    const temporaryCurrent = git(fixture.repository, [
      "rev-parse",
      "HEAD",
    ]).trim();
    await install(fixture, temporaryCurrent);
    git(fixture.repository, ["reset", "--hard", "--quiet", anchored.at(-1)!]);
    await install(fixture, anchored.at(-1)!);

    const releases = join(fixture.releaseRoot, "releases");
    const malformed = join(releases, "manual-backup");
    const writableUnknown = join(releases, "f".repeat(40));
    const symlinkUnknown = join(releases, "e".repeat(40));
    mkdirSync(malformed);
    mkdirSync(writableUnknown);
    symlinkSync(anchored[0]!, symlinkUnknown, "dir");

    const dryRun = await retainRuntimeReleases({
      releaseRoot: fixture.releaseRoot,
      repository: fixture.repository,
    });
    expect(dryRun.applied).toBe(false);
    expect(dryRun.deleted).toEqual([]);
    expect(
      dryRun.entries.filter((entry) => entry.reason === "retention_candidate"),
    ).toHaveLength(3);
    expect(
      dryRun.entries.filter((entry) => entry.reason === "newest_rollback"),
    ).toHaveLength(3);
    expect(
      dryRun.entries
        .filter((entry) => entry.reason === "newest_rollback")
        .map((entry) => entry.commit),
    ).toEqual(expect.arrayContaining(anchored.slice(3, 6)));
    expect(dryRun.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ commit: unanchored, reason: "unanchored" }),
        expect.objectContaining({
          commit: anchored.at(-1),
          reason: "active_ref",
        }),
        expect.objectContaining({
          commit: temporaryCurrent,
          reason: "active_ref",
        }),
        expect.objectContaining({
          commit: "manual-backup",
          reason: "malformed_or_unknown",
        }),
        expect.objectContaining({
          commit: "f".repeat(40),
          reason: "verification_failed",
        }),
        expect.objectContaining({
          commit: "e".repeat(40),
          reason: "verification_failed",
        }),
      ]),
    );
    for (const entry of dryRun.entries)
      expect(existsSync(entry.path)).toBe(true);

    await expect(
      retainRuntimeReleases({
        apply: true,
        releaseRoot: fixture.releaseRoot,
        repository: fixture.repository,
      }),
    ).rejects.toThrow("RUNTIME_RETENTION_APPLY_REQUIRES_MAXIMUM_DELETIONS");

    const applied = await retainRuntimeReleases({
      apply: true,
      maximumDeletions: 1,
      releaseRoot: fixture.releaseRoot,
      repository: fixture.repository,
    });
    expect(applied.deleted).toHaveLength(1);
    expect(existsSync(join(releases, applied.deleted[0]!))).toBe(false);
    expect(existsSync(join(releases, unanchored))).toBe(true);
    expect(existsSync(join(releases, anchored.at(-1)!))).toBe(true);
    expect(existsSync(join(releases, temporaryCurrent))).toBe(true);
    expect(existsSync(malformed)).toBe(true);
    expect(existsSync(writableUnknown)).toBe(true);
    expect(lstatSync(symlinkUnknown).isSymbolicLink()).toBe(true);
  }, 15_000);

  it("rejects symlinked release-root boundaries", async () => {
    const fixture = repositoryFixture("symlink");
    mkdirSync(fixture.releaseRoot);
    const outside = join(fixture.root, "outside");
    mkdirSync(outside);
    symlinkSync(outside, join(fixture.releaseRoot, "releases"), "dir");
    await expect(install(fixture, fixture.first)).rejects.toThrow(
      "RUNTIME_RELEASES_SYMLINK",
    );

    const rootFixture = repositoryFixture("root-symlink");
    const rootOutside = join(rootFixture.root, "root-outside");
    mkdirSync(rootOutside);
    symlinkSync(rootOutside, rootFixture.releaseRoot, "dir");
    await expect(install(rootFixture, rootFixture.first)).rejects.toThrow(
      "RUNTIME_RELEASE_ROOT_SYMLINK",
    );
  });

  it("recovers after publication before the current-link switch", async () => {
    const fixture = repositoryFixture("crash");
    await expect(
      installRuntime(options(fixture, fixture.first), {
        failpoint: (name) => {
          if (name === "release-published") throw new Error("CRASH_INJECTED");
        },
        prepareRelease: () => undefined,
      }),
    ).rejects.toThrow("CRASH_INJECTED");
    expect(() => lstatSync(join(fixture.releaseRoot, "current"))).toThrow();
    const recovered = await install(fixture, fixture.first);
    expect(recovered.reused).toBe(true);
    expect(readlinkSync(join(fixture.releaseRoot, "current"))).toBe(
      join("releases", fixture.first),
    );
  });
});

interface RepositoryFixture {
  readonly first: string;
  readonly module: string;
  readonly releaseRoot: string;
  readonly repository: string;
  readonly root: string;
}

function repositoryFixture(name: string): RepositoryFixture {
  const root = mkdtempSync(join(tmpdir(), `saqi-runtime-${name}-`));
  const repository = join(root, "repository");
  const releaseRoot = join(root, "installed");
  mkdirSync(repository);
  git(repository, ["init", "--quiet"]);
  git(repository, ["config", "user.email", "test@saqi.app"]);
  git(repository, ["config", "user.name", "Saqi Test"]);
  const dist = join(repository, "typescript/packages/crawler-local/dist");
  mkdirSync(dist, { recursive: true });
  const cli = join(dist, "cli.js");
  const module = join(dist, "module.js");
  writeFileSync(cli, "#!/usr/bin/env node\nprocess.exit(0);\n", {
    mode: 0o755,
  });
  chmodSync(cli, 0o755);
  writeFileSync(module, "module.exports = 'first';\n");
  git(repository, ["add", "."]);
  git(repository, ["commit", "--quiet", "-m", "first"]);
  const first = git(repository, ["rev-parse", "HEAD"]).trim();
  return { first, module, releaseRoot, repository, root };
}

function options(
  fixture: RepositoryFixture,
  commit: string,
): { commit: string; releaseRoot: string; repository: string } {
  return {
    commit,
    releaseRoot: fixture.releaseRoot,
    repository: fixture.repository,
  };
}

function install(fixture: RepositoryFixture, commit: string) {
  return installRuntime(options(fixture, commit), {
    prepareRelease: () => undefined,
  });
}

function git(repository: string, argumentsList: readonly string[]): string {
  return execFileSync("git", [...argumentsList], {
    cwd: repository,
    encoding: "utf8",
  });
}
