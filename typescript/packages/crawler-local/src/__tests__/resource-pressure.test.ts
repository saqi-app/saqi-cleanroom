import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ProviderHostAdmission,
  ResourcePressureMonitor,
  type ResourcePressurePolicy,
} from "../runtime/resource-pressure.js";

const POLICY: ResourcePressurePolicy = {
  enabled: true,
  maximumOpenFileDescriptors: 100,
  maximumProcessRssBytes: 1_000,
  minimumFreeDiskBytes: 1_000,
  minimumFreeMemoryBytes: 1_000,
  probeIntervalMs: 5_000,
  resumeHysteresisBasisPoints: 2_500,
};

describe("resource pressure admission", () => {
  afterEach(() => vi.useRealTimers());

  it("reports a hung probe without duplicating IO and recovers with fresh measurements", async () => {
    vi.useFakeTimers();
    const diskOperation = Promise.withResolvers<number>();
    const diskProbe = vi
      .fn<() => number | Promise<number>>()
      .mockImplementationOnce(() => diskOperation.promise)
      .mockReturnValue(0);
    const monitor = new ResourcePressureMonitor("/unused", POLICY, {
      availableDiskBytes: diskProbe,
      freeMemoryBytes: () => 10_000,
      now: Date.now,
      openFileDescriptors: () => 1,
      processRssBytes: () => 1,
    });
    const first = monitor.snapshot();
    await vi.advanceTimersByTimeAsync(5_000);
    await expect(first).resolves.toMatchObject({
      reasons: ["RESOURCE_PROBE_FAILED"],
      state: "resource_wait",
    });
    await vi.advanceTimersByTimeAsync(60_000);
    const waiting = await Promise.all(
      Array.from({ length: 128 }, () => monitor.snapshot(true)),
    );
    expect(
      waiting.every((snapshot) => snapshot.state === "resource_wait"),
    ).toBe(true);
    expect(diskProbe).toHaveBeenCalledOnce();
    diskOperation.resolve(10_000);
    await vi.advanceTimersByTimeAsync(0);
    // The old healthy result must not overwrite the timeout; read current disk.
    await expect(monitor.snapshot()).resolves.toMatchObject({
      reasons: ["DISK_PRESSURE"],
      state: "resource_wait",
    });
    expect(diskProbe).toHaveBeenCalledTimes(2);
    diskProbe.mockReturnValue(10_000);
    await vi.advanceTimersByTimeAsync(POLICY.probeIntervalMs);
    await expect(monitor.snapshot()).resolves.toMatchObject({
      reasons: [],
      state: "ready",
    });
    expect(diskProbe).toHaveBeenCalledTimes(3);
  });

  it("bounds aggregate provider descendants while keeping high lane ceilings selectable", () => {
    const admission = new ProviderHostAdmission({
      maximumProviderBufferedOutputBytes: 18,
      maximumProviderProcesses: 256,
      maximumProviderReservedMemoryBytes: 1_200,
      providerBufferedOutputReservationBytes: 6,
      providerProcessMemoryReservationBytes: 400,
    });
    const permits = Array.from({ length: 256 }, () => admission.acquire());
    expect(permits.filter((permit) => permit !== null)).toHaveLength(3);
    expect(admission.snapshot()).toEqual({
      activeProcesses: 3,
      available: false,
      maximumProcesses: 3,
      remainingProcesses: 0,
      reservedBufferedOutputBytes: 18,
      reservedMemoryBytes: 1_200,
    });
    permits.find((permit) => permit !== null)?.release();
    expect(admission.acquire()).not.toBeNull();
  });

  it("enters an indefinite wait and resumes only beyond hysteresis", async () => {
    let now = 0;
    let disk = 999;
    const monitor = new ResourcePressureMonitor("/unused", POLICY, {
      availableDiskBytes: () => disk,
      freeMemoryBytes: () => 10_000,
      now: () => now,
      openFileDescriptors: () => 1,
      processRssBytes: () => 1,
    });

    await expect(monitor.snapshot()).resolves.toMatchObject({
      reasons: ["DISK_PRESSURE"],
      state: "resource_wait",
    });
    disk = 1_100;
    now = 5_000;
    await expect(monitor.snapshot()).resolves.toMatchObject({
      state: "resource_wait",
    });
    disk = 1_250;
    now = 10_000;
    await expect(monitor.snapshot()).resolves.toMatchObject({
      reasons: [],
      state: "ready",
    });
  });

  it("coalesces probes and reports every active pressure source", async () => {
    let probes = 0;
    let now = 0;
    const monitor = new ResourcePressureMonitor("/unused", POLICY, {
      availableDiskBytes: () => {
        probes += 1;
        return 100;
      },
      freeMemoryBytes: () => 100,
      now: () => now,
      openFileDescriptors: () => 100,
      processRssBytes: () => 1_001,
    });

    const first = monitor.snapshot();
    now = 4_999;
    for (let lane = 0; lane < 128; lane += 1)
      expect(monitor.snapshot()).toBe(first);
    expect(probes).toBe(1);
    const snapshot = await first;
    expect(snapshot.reasons).toEqual([
      "DISK_PRESSURE",
      "MEMORY_PRESSURE",
      "PROCESS_MEMORY_PRESSURE",
      "FILE_DESCRIPTOR_PRESSURE",
    ]);
  });

  it("can be explicitly disabled without hiding metrics", async () => {
    const monitor = new ResourcePressureMonitor(
      "/unused",
      { ...POLICY, enabled: false },
      {
        availableDiskBytes: () => 0,
        freeMemoryBytes: () => 0,
        now: () => 0,
        openFileDescriptors: () => 1_000,
        processRssBytes: () => 10_000,
      },
    );
    await expect(monitor.snapshot()).resolves.toMatchObject({
      availableDiskBytes: 0,
      reasons: [],
      state: "ready",
    });
  });

  it("fails admission closed when a resource probe itself fails", async () => {
    const monitor = new ResourcePressureMonitor("/unused", POLICY, {
      availableDiskBytes: () => {
        throw new Error("synthetic statfs failure");
      },
      freeMemoryBytes: () => 10_000,
      now: () => 0,
      openFileDescriptors: () => null,
      processRssBytes: () => 1,
    });
    await expect(monitor.snapshot()).resolves.toMatchObject({
      availableDiskBytes: 0,
      reasons: ["DISK_PRESSURE", "RESOURCE_PROBE_FAILED"],
      state: "resource_wait",
    });
  });
});
