import {
  existsSync,
  linkSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  symlinkSync,
  truncateSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";

import { describe, expect, it } from "vitest";

import { SolAttemptRetention } from "../enrichment/sol-attempt-retention.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const ELIGIBLE = "11111111-1111-4111-8111-111111111111";
const PROTECTED = "22222222-2222-4222-8222-222222222222";
const POLICY = {
  maximumAttempts: 10,
  maximumInputBytes: 1_000_000,
  minimumFreeBytes: 0,
};

function eligibility(
  eligibleAttemptIds: ReadonlySet<string> = new Set([ELIGIBLE]),
  protectedAttemptIds: ReadonlySet<string> = new Set([PROTECTED]),
) {
  return {
    eligibleAttemptIds,
    nextEligibleAt: null,
    protectedAttemptIds,
    withAttemptReservation: (attemptId: string, operation: () => void) => {
      if (
        !eligibleAttemptIds.has(attemptId) ||
        protectedAttemptIds.has(attemptId)
      )
        return false;
      operation();
      return true;
    },
  };
}

function fixture(): string {
  const root = mkdtempSync(join(tmpdir(), "saqi-retention-"));
  for (const attemptId of [ELIGIBLE, PROTECTED]) {
    const directory = join(root, attemptId);
    mkdirSync(directory);
    writeFileSync(
      join(directory, "manifest.json"),
      `${JSON.stringify({ attemptId, kind: "generation" })}\n`,
    );
    writeFileSync(join(directory, "input.json"), "must remain\n");
    writeFileSync(join(directory, "result.json"), "must remain\n");
    writeFileSync(join(directory, "candidate-output.json"), "must remain\n");
    writeFileSync(join(directory, "last-message.json"), "must remain\n");
    writeFileSync(join(directory, "output-schema.json"), "must remain\n");
    writeFileSync(join(directory, "events.jsonl"), "event\n".repeat(100));
    writeFileSync(join(directory, "stderr.log"), "warning\n".repeat(100));
  }
  return root;
}

describe("Sol attempt retention", () => {
  it("bounds a filesystem pass with a resumable attempt cursor", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const first = await retention.plan(eligibility(), {
      ...POLICY,
      maximumScannedAttempts: 1,
      scanCursor: null,
    });
    expect(first).toMatchObject({
      scanComplete: false,
      scannedAttempts: 1,
      scanCursor: ELIGIBLE,
    });
    expect(first.candidates.map(({ attemptId }) => attemptId)).toEqual([
      ELIGIBLE,
    ]);

    const second = await retention.plan(eligibility(), {
      ...POLICY,
      maximumScannedAttempts: 1,
      scanCursor: first.scanCursor,
    });
    expect(second).toMatchObject({
      scanComplete: true,
      scannedAttempts: 1,
      scanCursor: null,
      skippedProtectedOrUnclassified: 1,
    });
  });

  it("has identical bounded dry-run/apply projection and preserves durable files", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility();
    const planned = await retention.plan(retentionEligibility, POLICY);
    await expect(retention.plan(retentionEligibility, POLICY)).resolves.toEqual(
      planned,
    );
    expect(planned).toMatchObject({
      applied: false,
      archivedAttempts: 0,
      scannedAttempts: 2,
      skippedProtectedOrUnclassified: 1,
    });
    expect(planned.candidates.map(({ attemptId }) => attemptId)).toEqual([
      ELIGIBLE,
    ]);
    const applied = await retention.apply(retentionEligibility, POLICY);
    expect(applied.candidates).toEqual(planned.candidates);
    expect(applied.archivedAttempts).toBe(1);
    expect(applied.quarantinedBytes).toBe(
      applied.reclaimableAfterExplicitPurgeBytes,
    );
    const directory = join(root, ELIGIBLE);
    expect(readFileSync(join(directory, "input.json"), "utf8")).toBe(
      "must remain\n",
    );
    expect(readFileSync(join(directory, "result.json"), "utf8")).toBe(
      "must remain\n",
    );
    expect(existsSync(join(directory, "events.jsonl"))).toBe(false);
    expect(
      existsSync(join(directory, "diagnostics.quarantine", "events.jsonl")),
    ).toBe(true);
    const archive = JSON.parse(
      gunzipSync(
        readFileSync(join(directory, "diagnostics.archive.json.gz")),
      ).toString("utf8"),
    ) as { files: { name: string }[] };
    expect(archive.files.map(({ name }) => name)).toEqual([
      "events.jsonl",
      "stderr.log",
    ]);
    const converged = await retention.plan(retentionEligibility, POLICY);
    expect(converged.candidates).toEqual([]);
    expect(converged.reclaimableAfterExplicitPurgeBytes).toBe(
      applied.reclaimableAfterExplicitPurgeBytes,
    );
    expect(existsSync(join(root, PROTECTED, "events.jsonl"))).toBe(true);
  });

  it("resumes a partial quarantine after verifying the existing archive", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    renameSync(
      join(root, ELIGIBLE, "diagnostics.quarantine", "events.jsonl"),
      join(root, ELIGIBLE, "events.jsonl"),
    );
    const applied = await retention.apply(retentionEligibility, POLICY);
    expect(applied).toMatchObject({
      archivedAttempts: 1,
      quarantinedBytes: 1_400,
      reclaimableAfterExplicitPurgeBytes: 1_400,
    });
    expect(existsSync(join(root, ELIGIBLE, "events.jsonl"))).toBe(false);
  });

  it("reclaims verified raw diagnostics while preserving durable evidence", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const directory = join(root, ELIGIBLE);
    const applied = await retention.apply(
      eligibility(new Set([ELIGIBLE]), new Set()),
      { ...POLICY, purgeArchivedDiagnostics: true },
    );
    expect(applied).toMatchObject({
      archivedAttempts: 1,
      quarantinedBytes: 0,
      reclaimableAfterExplicitPurgeBytes: 0,
    });

    expect(existsSync(join(directory, "diagnostics.quarantine"))).toBe(false);
    expect(existsSync(join(directory, "diagnostics.archive.json.gz"))).toBe(
      true,
    );
    expect(readFileSync(join(directory, "input.json"), "utf8")).toBe(
      "must remain\n",
    );
    expect(readFileSync(join(directory, "result.json"), "utf8")).toBe(
      "must remain\n",
    );
  });

  it("reclaims an existing verified quarantine after purge is enabled", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);

    const planned = await retention.plan(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(planned.candidates.map(({ attemptId }) => attemptId)).toEqual([
      ELIGIBLE,
    ]);

    const applied = await retention.apply(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(applied).toMatchObject({
      applied: true,
      archivedAttempts: 1,
      quarantinedBytes: 0,
      reclaimableAfterExplicitPurgeBytes: 0,
    });
    const directory = join(root, ELIGIBLE);
    expect(existsSync(join(directory, "diagnostics.quarantine"))).toBe(false);
    expect(existsSync(join(directory, "diagnostics.archive.json.gz"))).toBe(
      true,
    );
    const converged = await retention.plan(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(converged.candidates).toEqual([]);
  });

  it("fails closed on a corrupt archive before purging quarantine", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const directory = join(root, ELIGIBLE);
    writeFileSync(join(directory, "diagnostics.archive.json.gz"), "corrupt");

    await expect(
      retention.plan(retentionEligibility, {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow();
    expect(
      existsSync(join(directory, "diagnostics.quarantine", "events.jsonl")),
    ).toBe(true);
  });

  it("fails closed when quarantined bytes differ from the verified archive", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const path = join(root, ELIGIBLE, "diagnostics.quarantine", "stderr.log");
    const original = readFileSync(path, "utf8");
    writeFileSync(path, "x".repeat(original.length));

    await expect(
      retention.plan(retentionEligibility, {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention diagnostic changed");
    expect(existsSync(path)).toBe(true);
  });

  it("rejects a symlinked quarantine without touching its target", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const directory = join(root, ELIGIBLE);
    const quarantine = join(directory, "diagnostics.quarantine");
    const outside = join(root, "outside");
    renameSync(quarantine, outside);
    symlinkSync(outside, quarantine);

    await expect(
      retention.plan(retentionEligibility, {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention purge directory invalid");
    expect(existsSync(join(outside, "events.jsonl"))).toBe(true);
  });

  it("rejects unknown quarantine entries before deleting diagnostics", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const quarantine = join(root, ELIGIBLE, "diagnostics.quarantine");
    writeFileSync(join(quarantine, "unexpected.txt"), "preserve\n");

    await expect(
      retention.plan(retentionEligibility, {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention purge entry invalid");
    expect(existsSync(join(quarantine, "events.jsonl"))).toBe(true);
    expect(existsSync(join(quarantine, "stderr.log"))).toBe(true);
  });

  it("resumes a partially deleted purge tombstone idempotently", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const directory = join(root, ELIGIBLE);
    const pending = join(directory, "diagnostics.purge.pending");
    renameSync(join(directory, "diagnostics.quarantine"), pending);
    unlinkSync(join(pending, "events.jsonl"));

    const partiallyPurged = await retention.plan(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(partiallyPurged.quarantinedBytes).toBe(800);

    const applied = await retention.apply(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(applied.archivedAttempts).toBe(1);
    expect(existsSync(pending)).toBe(false);
    const converged = await retention.plan(retentionEligibility, {
      ...POLICY,
      purgeArchivedDiagnostics: true,
    });
    expect(converged.candidates).toEqual([]);
  });

  it("rejects hardlinked diagnostics before purging either link", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const directory = join(root, ELIGIBLE);
    const diagnostic = join(directory, "events.jsonl");
    const outsideLink = join(root, "events-hardlink.jsonl");
    linkSync(diagnostic, outsideLink);

    await expect(
      retention.apply(eligibility(new Set([ELIGIBLE]), new Set()), {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention diagnostic invalid");
    expect(existsSync(diagnostic)).toBe(true);
    expect(existsSync(outsideLink)).toBe(true);
  });

  it("rejects sparse diagnostics before purging them", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const diagnostic = join(root, ELIGIBLE, "events.jsonl");
    truncateSync(diagnostic, 100_000);

    await expect(
      retention.apply(eligibility(new Set([ELIGIBLE]), new Set()), {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention diagnostic invalid");
    expect(existsSync(diagnostic)).toBe(true);
  });

  it("rejects hardlinked archives before purging raw diagnostics", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const directory = join(root, ELIGIBLE);
    const archive = join(directory, "diagnostics.archive.json.gz");
    const outsideLink = join(root, "archive-hardlink.json.gz");
    linkSync(archive, outsideLink);

    await expect(
      retention.plan(retentionEligibility, {
        ...POLICY,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("Retention archive invalid");
    expect(
      existsSync(join(directory, "diagnostics.quarantine", "events.jsonl")),
    ).toBe(true);
    expect(existsSync(outsideLink)).toBe(true);
  });

  it("fails closed if a prior archive is corrupt", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    writeFileSync(
      join(root, ELIGIBLE, "diagnostics.archive.json.gz"),
      "corrupt",
    );
    await expect(
      retention.plan(retentionEligibility, POLICY),
    ).rejects.toThrow();
    expect(
      existsSync(join(root, ELIGIBLE, "diagnostics.quarantine", "stderr.log")),
    ).toBe(true);
  });

  it("preflights archive space before moving any diagnostic", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await expect(
      retention.apply(retentionEligibility, {
        ...POLICY,
        minimumFreeBytes: Number.MAX_SAFE_INTEGER,
      }),
    ).rejects.toThrow("SOL_RETENTION_DISK_PRESSURE");
    expect(existsSync(join(root, ELIGIBLE, "events.jsonl"))).toBe(true);
    expect(
      existsSync(join(root, ELIGIBLE, "diagnostics.archive.json.gz")),
    ).toBe(false);
  });

  it("reclaims one bounded verified candidate below the configured reserve", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    writeFileSync(
      join(root, ELIGIBLE, "events.jsonl"),
      "meaningful diagnostic\n".repeat(100_000),
    );
    await retention.apply(retentionEligibility, {
      ...POLICY,
      maximumInputBytes: 4_000_000,
    });
    const applied = await retention.apply(retentionEligibility, {
      ...POLICY,
      maximumInputBytes: 4_000_000,
      minimumFreeBytes: Number.MAX_SAFE_INTEGER,
      purgeArchivedDiagnostics: true,
    });
    expect(applied.archivedAttempts).toBe(1);
    expect(existsSync(join(root, ELIGIBLE, "events.jsonl"))).toBe(false);
    expect(existsSync(join(root, ELIGIBLE, "diagnostics.quarantine"))).toBe(
      false,
    );
    expect(
      existsSync(join(root, ELIGIBLE, "diagnostics.archive.json.gz")),
    ).toBe(true);
  });

  it("automatically reclaims only verified archives below the configured reserve", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    const retentionEligibility = eligibility(new Set([ELIGIBLE]), new Set());
    await retention.apply(retentionEligibility, POLICY);
    const directory = join(root, ELIGIBLE);

    const applied = await retention.apply(retentionEligibility, {
      ...POLICY,
      minimumFreeBytes: Number.MAX_SAFE_INTEGER,
    });

    expect(applied).toMatchObject({
      applied: true,
      archivedAttempts: 1,
      quarantinedBytes: 0,
      reclaimableAfterExplicitPurgeBytes: 0,
    });
    expect(existsSync(join(directory, "diagnostics.quarantine"))).toBe(false);
    expect(existsSync(join(directory, "diagnostics.archive.json.gz"))).toBe(
      true,
    );
    expect(readFileSync(join(directory, "input.json"), "utf8")).toBe(
      "must remain\n",
    );
    expect(readFileSync(join(directory, "result.json"), "utf8")).toBe(
      "must remain\n",
    );
  });

  it("never creates a new archive below the configured reserve", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    await expect(
      retention.apply(eligibility(new Set([ELIGIBLE]), new Set()), {
        ...POLICY,
        minimumFreeBytes: Number.MAX_SAFE_INTEGER,
        purgeArchivedDiagnostics: true,
      }),
    ).rejects.toThrow("SOL_RETENTION_DISK_PRESSURE");
    expect(existsSync(join(root, ELIGIBLE, "events.jsonl"))).toBe(true);
    expect(
      existsSync(join(root, ELIGIBLE, "diagnostics.archive.json.gz")),
    ).toBe(false);
  });

  it("retains accounting for quarantines outside a purge candidate", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    await retention.apply(eligibility(new Set([PROTECTED]), new Set()), POLICY);
    const applied = await retention.apply(
      eligibility(new Set([ELIGIBLE]), new Set()),
      { ...POLICY, purgeArchivedDiagnostics: true },
    );
    expect(applied).toMatchObject({
      archivedAttempts: 1,
      quarantinedBytes: 1_400,
      reclaimableAfterExplicitPurgeBytes: 1_400,
    });
    expect(existsSync(join(root, PROTECTED, "diagnostics.quarantine"))).toBe(
      true,
    );
  });

  it("revalidates ledger eligibility immediately before mutation", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    await expect(
      retention.apply(
        {
          ...eligibility(new Set([ELIGIBLE]), new Set()),
          withAttemptReservation: () => false,
        },
        { ...POLICY, purgeArchivedDiagnostics: true },
      ),
    ).rejects.toThrow("SOL_RETENTION_ATTEMPT_NO_LONGER_ELIGIBLE");
    expect(existsSync(join(root, ELIGIBLE, "events.jsonl"))).toBe(true);
    expect(
      existsSync(join(root, ELIGIBLE, "diagnostics.archive.json.gz")),
    ).toBe(false);
  });

  it("prepares and verifies compression before entering the ledger fence", async () => {
    const root = fixture();
    const retention = new SolAttemptRetention(root);
    let observedPreparedArchive = false;
    await retention.apply(
      {
        ...eligibility(new Set([ELIGIBLE]), new Set()),
        withAttemptReservation: (_attemptId, operation) => {
          const names = readdirSync(join(root, ELIGIBLE));
          observedPreparedArchive = names.some(
            (name) =>
              name.startsWith(".diagnostics.archive.json.gz.") &&
              name.endsWith(".tmp"),
          );
          operation();
          return true;
        },
      },
      POLICY,
    );
    expect(observedPreparedArchive).toBe(true);
  });
});
