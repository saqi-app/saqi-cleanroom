import { describe, expect, it, vi } from "vitest";

import {
  SourceLineageMaintenanceJob,
  type SourceLineageMaintenanceLease,
  type SourceLineageMaintenanceRepository,
} from "../source-lineage-maintenance";

const IDS = [
  "00000000-0000-4000-8000-000000000001",
  "00000000-0000-4000-8000-000000000002",
] as const;
const LEASE: SourceLineageMaintenanceLease = {
  cursorPoemId: null,
  leaseEpoch: 3,
  leaseToken: "00000000-0000-4000-8000-000000000003",
  pass: 1,
};

function fixture(pages: readonly (readonly string[])[] = [IDS, []]) {
  let page = 0;
  let remaining = IDS.length;
  let adoptedTotal = 0;
  let conflictTotal = 0;
  let cursorPoemId: null | string = null;
  let pass = 1;
  let scannedTotal = 0;
  const acquire = vi.fn<SourceLineageMaintenanceRepository["acquire"]>();
  acquire.mockResolvedValue(LEASE);
  const commitPage = vi.fn<SourceLineageMaintenanceRepository["commitPage"]>();
  commitPage.mockImplementation(async (_lease, cursor, result) => {
    remaining -= result.adopted;
    adoptedTotal += result.adopted;
    conflictTotal += result.conflicts.length;
    scannedTotal += result.scanned;
    cursorPoemId = cursor;
    return true;
  });
  const fail = vi.fn<SourceLineageMaintenanceRepository["fail"]>();
  fail.mockResolvedValue(undefined);
  const finishPass = vi.fn<SourceLineageMaintenanceRepository["finishPass"]>();
  finishPass.mockImplementation(async () => {
    pass += 1;
    cursorPoemId = null;
    return true;
  });
  const readPoemIds = vi.fn<SourceLineageMaintenanceRepository["poemIds"]>();
  readPoemIds.mockImplementation(async () => pages[page++] ?? []);
  const quarantine = vi.fn<SourceLineageMaintenanceRepository["quarantine"]>();
  quarantine.mockResolvedValue(undefined);
  const remainingCount =
    vi.fn<SourceLineageMaintenanceRepository["remaining"]>();
  remainingCount.mockImplementation(async () => remaining);
  const readStatus = vi.fn<SourceLineageMaintenanceRepository["status"]>();
  readStatus.mockImplementation(async () => ({
    adoptedTotal,
    conflictTotal,
    cursorPoemId,
    lastErrorCode: null,
    pass,
    scannedTotal,
    state: "idle" as const,
    updatedAt: 1,
  }));
  const yieldLease = vi.fn<SourceLineageMaintenanceRepository["yieldLease"]>();
  yieldLease.mockResolvedValue(true);
  const repository: SourceLineageMaintenanceRepository = {
    acquire,
    commitPage,
    fail,
    finishPass,
    poemIds: readPoemIds,
    quarantine,
    remaining: remainingCount,
    status: readStatus,
    yieldLease,
  };
  const adoptLegacySourceLineage = vi
    .fn()
    .mockImplementation(async ({ poemIds }) => ({
      adopted: poemIds.length,
      conflicts: [],
      scanned: poemIds.length,
      unchanged: 0,
    }));
  return {
    adopter: { adoptLegacySourceLineage },
    adoptLegacySourceLineage,
    job: new SourceLineageMaintenanceJob({
      adopter: { adoptLegacySourceLineage },
      clock: () => 10,
      repository,
    }),
    mocks: { acquire, commitPage, fail, finishPass, quarantine, yieldLease },
  };
}

describe("source lineage maintenance", () => {
  it("leases, keyset-pages through the bounded adopter, and resets the pass", async () => {
    const value = fixture();
    const result = await value.job.run({ maxPages: 2, owner: "cron" });

    expect(value.mocks.acquire).toHaveBeenCalledWith("cron", 10, 300_000);
    expect(value.adoptLegacySourceLineage).toHaveBeenCalledWith({
      poemIds: IDS,
    });
    expect(value.mocks.commitPage).toHaveBeenCalledWith(
      LEASE,
      IDS[1],
      expect.objectContaining({ adopted: 2 }),
      10
    );
    expect(value.mocks.finishPass).toHaveBeenCalledWith(LEASE, 10);
    expect(result).toMatchObject({ pass: 2, remaining: 0 });
  });

  it("is neutral when another worker owns the lease", async () => {
    const value = fixture();
    value.mocks.acquire.mockResolvedValueOnce(null);
    await expect(value.job.run()).resolves.toMatchObject({ remaining: 2 });
    expect(value.adoptLegacySourceLineage).not.toHaveBeenCalled();
  });

  it("quarantines conflicts before advancing the cursor", async () => {
    const value = fixture([IDS]);
    value.adoptLegacySourceLineage.mockResolvedValueOnce({
      adopted: 1,
      conflicts: [{ code: "SOURCE_LINEAGE_CONFLICT", poemId: IDS[1] }],
      scanned: 2,
      unchanged: 0,
    });
    await value.job.run({ maxPages: 1 });

    expect(value.mocks.quarantine).toHaveBeenCalledWith(
      LEASE,
      [{ code: "SOURCE_LINEAGE_CONFLICT", poemId: IDS[1] }],
      10
    );
    expect(value.mocks.quarantine.mock.invocationCallOrder[0]).toBeLessThan(
      value.mocks.commitPage.mock.invocationCallOrder[0] ??
        Number.MAX_SAFE_INTEGER
    );
  });

  it("releases the lease after a bounded page budget for immediate resumption", async () => {
    const value = fixture([IDS]);
    await value.job.run({ maxPages: 1 });
    expect(value.mocks.yieldLease).toHaveBeenCalledWith(LEASE, 10);
  });

  it("fails closed when cursor CAS ownership is lost", async () => {
    const value = fixture([IDS]);
    value.mocks.commitPage.mockResolvedValueOnce(false);
    await expect(value.job.run({ maxPages: 1 })).rejects.toThrow(
      "SOURCE_LINEAGE_LEASE_LOST"
    );
    expect(value.mocks.fail).toHaveBeenCalledWith(
      LEASE,
      "SOURCE_LINEAGE_LEASE_LOST",
      10
    );
  });

  it("does not advance the cursor after a writer failure so replay is safe", async () => {
    const value = fixture([IDS]);
    value.adoptLegacySourceLineage.mockRejectedValueOnce(
      new Error("WRITER_EPOCH_MISMATCH")
    );
    await expect(value.job.run({ maxPages: 1 })).rejects.toThrow(
      "WRITER_EPOCH_MISMATCH"
    );
    expect(value.mocks.commitPage).not.toHaveBeenCalled();
    expect(value.mocks.fail).toHaveBeenCalledWith(
      LEASE,
      "WRITER_EPOCH_MISMATCH",
      10
    );
  });

  it("reports exact remaining work independently of the cursor", async () => {
    const value = fixture();
    await expect(value.job.status()).resolves.toMatchObject({
      cursorPoemId: null,
      remaining: 2,
    });
  });
});
