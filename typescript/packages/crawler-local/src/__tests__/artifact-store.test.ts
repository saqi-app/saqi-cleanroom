import { existsSync, mkdirSync, utimesSync, writeFileSync } from "node:fs";
import { open, readdir, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test, vi } from "vitest";

import {
  ArtifactStore,
  cleanupStaleArtifactTemporaries,
  DiskPressureError,
  verifyArtifactInventory,
} from "../persistence/artifact-store.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

vi.mock("node:fs/promises", { spy: true });

describe("content-addressed artifact store", () => {
  test("cleanup tolerates removed directories but preserves permission errors", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-cleanup-race-"));
    vi.mocked(readdir).mockRejectedValueOnce(
      Object.assign(new Error("removed"), { code: "ENOENT" }),
    );
    await expect(cleanupStaleArtifactTemporaries(root)).resolves.toEqual({
      bytesRemoved: 0,
      filesRemoved: 0,
      scanned: 0,
      truncated: false,
    });
    const denied = Object.assign(new Error("denied"), { code: "EACCES" });
    vi.mocked(readdir).mockRejectedValueOnce(denied);
    await expect(cleanupStaleArtifactTemporaries(root)).rejects.toBe(denied);
  });

  test("verifies referenced and unreferenced artifacts once without losing corruption reports", async () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifact-inventory-")),
      { minimumFreeBytes: 0 },
    );
    const good = await store.put("good");
    const corrupt = await store.put("referenced corrupt");
    const orphan = await store.put("unreferenced corrupt");
    writeFileSync(corrupt.path, "changed");
    writeFileSync(orphan.path, "changed");
    const verify = vi.spyOn(store, "verify");
    const missing = "a".repeat(64);

    const report = await verifyArtifactInventory(store, [
      good.hash,
      corrupt.hash,
      missing,
      missing,
    ]);

    expect(report).toMatchObject({ checked: 3, referenced: 4 });
    expect(
      report.corrupt.map((result) => result.expectedHash).toSorted(),
    ).toEqual([corrupt.hash, orphan.hash].toSorted());
    expect(
      report.missingOrCorruptReferences.map((result) => result.expectedHash),
    ).toEqual([corrupt.hash, missing, missing]);
    expect(verify).toHaveBeenCalledTimes(4);
    for (const hash of [good.hash, corrupt.hash, orphan.hash, missing])
      expect(
        verify.mock.calls.filter(([value]) => value === hash),
      ).toHaveLength(1);
  });

  test("bounds absent-inventory reference reads and propagates verification failures", async () => {
    let active = 0;
    let maximum = 0;
    const verify = vi.fn(async (hash: string) => {
      active += 1;
      maximum = Math.max(maximum, active);
      await Promise.resolve();
      active -= 1;
      return { actualHash: null, expectedHash: hash, ok: false, path: hash };
    });
    const store = { verify, verifyAll: async () => [] };
    const hashes = Array.from({ length: 1000 }, (_, index) =>
      index.toString(16).padStart(64, "0"),
    );
    const report = await verifyArtifactInventory(store, hashes);
    expect(maximum).toBe(1);
    expect(report.missingOrCorruptReferences).toHaveLength(hashes.length);
    const failure = new Error("unreadable artifact");
    verify.mockRejectedValueOnce(failure);
    await expect(verifyArtifactInventory(store, ["0".repeat(64)])).rejects.toBe(
      failure,
    );
    await expect(
      verifyArtifactInventory(
        {
          ...store,
          verifyAll: async () => {
            throw failure;
          },
        },
        [],
      ),
    ).rejects.toBe(failure);
  });

  test.each([0, 2 * 1024 * 1024 + 1])(
    "reads and validates a %i-byte artifact in one file pass",
    async (size) => {
      const store = new ArtifactStore(
        mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
        { minimumFreeBytes: 0 },
      );
      const content = Buffer.alloc(size, 0xab);
      const artifact = await store.put(content);
      vi.mocked(open).mockClear();
      vi.mocked(readFile).mockClear();

      const read = await store.read(artifact.hash);
      expect(read.equals(content)).toBe(true);

      expect(readFile).toHaveBeenCalledExactlyOnceWith(artifact.path);
      expect(open).not.toHaveBeenCalled();
    },
  );

  test("validates the returned bytes even when the on-disk artifact is valid", async () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
      { minimumFreeBytes: 0 },
    );
    const artifact = await store.put("expected");
    vi.mocked(readFile).mockResolvedValueOnce(Buffer.from("replaced"));

    await expect(store.read(artifact.hash)).rejects.toThrow(
      `Artifact is missing or corrupt: ${artifact.hash}`,
    );
  });

  test("preserves missing, invalid-hash, and filesystem error behavior", async () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
      { minimumFreeBytes: 0 },
    );
    const hash = "a".repeat(64);
    await expect(store.read(hash)).rejects.toThrow(
      `Artifact is missing or corrupt: ${hash}`,
    );
    await expect(store.read("../outside")).rejects.toThrow("Invalid SHA-256");
    const artifact = await store.put("readable");
    const error = Object.assign(new Error("Permission denied"), {
      code: "EACCES",
    });
    vi.mocked(readFile).mockRejectedValueOnce(error);
    await expect(store.read(artifact.hash)).rejects.toBe(error);
  });

  test("duplicate publication converges on one verified artifact", async () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
      { minimumFreeBytes: 0 },
    );
    const first = await store.put("immutable");
    const second = await store.put(Buffer.from("immutable"));
    expect(second).toEqual(first);
    const stored = await store.read(first.hash);
    expect(stored.toString()).toBe("immutable");
    await expect(store.verifyAll()).resolves.toHaveLength(1);
  });

  test("detects corruption and refuses to bless overwritten content", async () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
      { minimumFreeBytes: 0 },
    );
    const artifact = await store.put("expected");
    writeFileSync(artifact.path, "corrupt");
    await expect(store.verify(artifact.hash)).resolves.toMatchObject({
      ok: false,
    });
    await expect(store.read(artifact.hash)).rejects.toThrow("corrupt");
    await expect(store.put("expected")).rejects.toThrow(
      "Existing artifact is corrupt",
    );
  });

  test("rejects path traversal disguised as an artifact identity", () => {
    const store = new ArtifactStore(
      mkdtempSync(join(tmpdir(), "saqi-artifacts-")),
      { minimumFreeBytes: 0 },
    );
    expect(() => store.path("../outside")).toThrow("Invalid SHA-256");
  });

  test("fails clearly before writing under configured disk pressure", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-artifacts-"));
    const store = new ArtifactStore(root, {
      minimumFreeBytes: Number.MAX_SAFE_INTEGER,
    });
    await expect(store.put("cannot fit safely")).rejects.toThrow(
      "ARTIFACT_STORE_DISK_PRESSURE",
    );
    await expect(store.assertWritableCapacity()).rejects.toThrow(
      DiskPressureError,
    );
    await expect(store.capacity()).resolves.toMatchObject({ writable: false });
    await expect(store.verifyAll()).resolves.toEqual([]);
  });

  test("reclaims only old unpublished CAS temporaries", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-artifacts-"));
    const prefix = join(root, "aa");
    mkdirSync(prefix);
    const oldTemporary = join(
      prefix,
      `.${"a".repeat(64)}.123.00000000-0000-4000-8000-000000000000.tmp`,
    );
    const recentTemporary = join(
      prefix,
      `.${"b".repeat(64)}.123.00000000-0000-4000-8000-000000000000.tmp`,
    );
    const durable = join(prefix, "c".repeat(62));
    writeFileSync(oldTemporary, "old-partial");
    writeFileSync(recentTemporary, "active-partial");
    writeFileSync(durable, "durable");
    utimesSync(oldTemporary, new Date(0), new Date(0));

    await expect(
      cleanupStaleArtifactTemporaries(root, 1_000, 2_000),
    ).resolves.toEqual({
      bytesRemoved: 11,
      filesRemoved: 1,
      scanned: 2,
      truncated: false,
    });
    expect(existsSync(oldTemporary)).toBe(false);
    expect(existsSync(recentTemporary)).toBe(true);
    expect(existsSync(durable)).toBe(true);
  });
});
