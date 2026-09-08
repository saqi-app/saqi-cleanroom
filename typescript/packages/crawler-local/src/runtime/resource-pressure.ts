import { execFile } from "node:child_process";
import { readdir, readFile, statfs } from "node:fs/promises";
import { freemem, platform, totalmem } from "node:os";
import { resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const RESOURCE_PROBE_TIMEOUT_MS = 5_000;

export interface ResourcePressurePolicy {
  readonly enabled: boolean;
  readonly maximumOpenFileDescriptors: number;
  readonly maximumProcessRssBytes: number;
  readonly minimumAvailableMemoryBasisPoints?: number;
  readonly minimumFreeDiskBytes: number;
  readonly minimumFreeMemoryBytes: number;
  readonly probeIntervalMs: number;
  readonly resumeHysteresisBasisPoints: number;
}

export interface ProviderHostAdmissionPolicy {
  readonly maximumProviderBufferedOutputBytes: number;
  readonly maximumProviderProcesses: number;
  readonly maximumProviderReservedMemoryBytes: number;
  readonly providerBufferedOutputReservationBytes: number;
  readonly providerProcessMemoryReservationBytes: number;
}

export interface ProviderHostAdmissionSnapshot {
  readonly activeProcesses: number;
  readonly available: boolean;
  readonly maximumProcesses: number;
  readonly remainingProcesses: number;
  readonly reservedBufferedOutputBytes: number;
  readonly reservedMemoryBytes: number;
}

/** A process-wide reservation fence for heavyweight provider descendants.
 *
 * Node's RSS excludes child processes, so measuring only the supervisor can
 * admit hundreds of CLIs before system-wide pressure becomes visible. This
 * conservative gate reserves descendant memory and parent-held output before
 * spawn. Configuration may still select 256; excess lanes wait and adapt as
 * reservations are released instead of failing or oversubscribing the host. */
// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This process-wide reservation owner must share one counter; policies and returned permits define the external contracts.
export class ProviderHostAdmission {
  readonly #policy: ProviderHostAdmissionPolicy;
  #activeProcesses = 0;

  constructor(policy: ProviderHostAdmissionPolicy) {
    this.#policy = policy;
  }

  acquire(): { release(): void } | null {
    if (!this.#canAdmit(this.#activeProcesses + 1)) return null;
    this.#activeProcesses += 1;
    let released = false;
    return {
      release: () => {
        if (released) throw new Error("PROVIDER_HOST_PERMIT_ALREADY_RELEASED");
        released = true;
        this.#activeProcesses -= 1;
      },
    };
  }

  snapshot(): ProviderHostAdmissionSnapshot {
    const maximumProcesses = Math.min(
      this.#policy.maximumProviderProcesses,
      Math.floor(
        this.#policy.maximumProviderBufferedOutputBytes /
          this.#policy.providerBufferedOutputReservationBytes,
      ),
      Math.floor(
        this.#policy.maximumProviderReservedMemoryBytes /
          this.#policy.providerProcessMemoryReservationBytes,
      ),
    );
    return {
      activeProcesses: this.#activeProcesses,
      available: this.#canAdmit(this.#activeProcesses + 1),
      maximumProcesses,
      remainingProcesses: Math.max(0, maximumProcesses - this.#activeProcesses),
      reservedBufferedOutputBytes:
        this.#activeProcesses *
        this.#policy.providerBufferedOutputReservationBytes,
      reservedMemoryBytes:
        this.#activeProcesses *
        this.#policy.providerProcessMemoryReservationBytes,
    };
  }

  #canAdmit(processes: number): boolean {
    return (
      processes <= this.#policy.maximumProviderProcesses &&
      processes * this.#policy.providerBufferedOutputReservationBytes <=
        this.#policy.maximumProviderBufferedOutputBytes &&
      processes * this.#policy.providerProcessMemoryReservationBytes <=
        this.#policy.maximumProviderReservedMemoryBytes
    );
  }
}

export interface ResourcePressureSnapshot {
  readonly availableDiskBytes: number;
  readonly freeMemoryBytes: number;
  readonly nextProbeAt: number;
  readonly openFileDescriptors: null | number;
  readonly processRssBytes: number;
  readonly reasons: readonly ResourcePressureReason[];
  readonly state: "ready" | "resource_wait";
}

type ResourcePressureReason =
  | "DISK_PRESSURE"
  | "FILE_DESCRIPTOR_PRESSURE"
  | "MEMORY_PRESSURE"
  | "PROCESS_MEMORY_PRESSURE"
  | "RESOURCE_PROBE_FAILED";

export interface ResourcePressureProbe {
  readonly availableDiskBytes: () => number | Promise<number>;
  readonly freeMemoryBytes: () => number | Promise<number>;
  readonly now: () => number;
  readonly openFileDescriptors: () => null | number | Promise<null | number>;
  readonly processRssBytes: () => number | Promise<number>;
}

/**
 * Fail-closed admission gate with hysteresis. It never aborts admitted work;
 * it stops new cycles until pressure has actually cleared, avoiding retry
 * storms and half-written paid operations.
 */
// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete hysteresis state machine consumes the injectable ResourcePressureProbe contract.
export class ResourcePressureMonitor {
  readonly #policy: ResourcePressurePolicy;
  readonly #probe: ResourcePressureProbe;
  #probeInFlight: null | Promise<ResourcePressureSnapshot> = null;
  #snapshot: null | ResourcePressureSnapshot = null;

  constructor(
    root: string,
    policy: ResourcePressurePolicy,
    probe: Partial<ResourcePressureProbe> = {},
  ) {
    this.#policy = policy;
    this.#probe = {
      availableDiskBytes:
        probe.availableDiskBytes ?? (() => filesystemAvailableBytes(root)),
      freeMemoryBytes: probe.freeMemoryBytes ?? availableMemoryBytes,
      now: probe.now ?? Date.now,
      openFileDescriptors:
        probe.openFileDescriptors ?? countOpenFileDescriptors,
      processRssBytes:
        probe.processRssBytes ?? (() => process.memoryUsage().rss),
    };
  }

  snapshot(force = false): Promise<ResourcePressureSnapshot> {
    const now = this.#probe.now();
    if (!force && this.#snapshot && now < this.#snapshot.nextProbeAt)
      return Promise.resolve(this.#snapshot);
    if (this.#probeInFlight) return this.#probeInFlight;
    // Filesystem probes cannot be cancelled. Keep a timed-out operation
    // coalesced until it settles, rather than accumulating blocked requests.
    let timedOut = false;
    const pending = new Promise<ResourcePressureSnapshot>((resolveSnapshot) => {
      const timer = setTimeout(() => {
        timedOut = true;
        const failed: ResourcePressureSnapshot = {
          availableDiskBytes: 0,
          freeMemoryBytes: 0,
          nextProbeAt: this.#probe.now() + this.#policy.probeIntervalMs,
          openFileDescriptors: null,
          processRssBytes: 0,
          reasons: this.#policy.enabled
            ? [
                ...(this.#snapshot?.reasons ?? []).filter(
                  (reason) => reason !== "RESOURCE_PROBE_FAILED",
                ),
                "RESOURCE_PROBE_FAILED",
              ]
            : [],
          state: this.#policy.enabled ? "resource_wait" : "ready",
        };
        this.#snapshot = failed;
        resolveSnapshot(failed);
      }, RESOURCE_PROBE_TIMEOUT_MS);
      void this.#probeSnapshot(now).then((snapshot) => {
        clearTimeout(timer);
        if (this.#probeInFlight === pending) this.#probeInFlight = null;
        if (timedOut) {
          // Do not publish stale measurements. Settlement permits a fresh
          // probe immediately, even if the failed snapshot was just cached.
          if (this.#snapshot)
            this.#snapshot = {
              ...this.#snapshot,
              nextProbeAt: this.#probe.now(),
            };
        } else {
          this.#snapshot = snapshot;
          resolveSnapshot(snapshot);
        }
      });
    });
    this.#probeInFlight = pending;
    return pending;
  }

  async #probeSnapshot(now: number): Promise<ResourcePressureSnapshot> {
    const [disk, memory, descriptors, rss] = await Promise.all([
      safeProbe(this.#probe.availableDiskBytes, 0),
      safeProbe(this.#probe.freeMemoryBytes, 0),
      safeProbe(this.#probe.openFileDescriptors, null),
      safeProbe(this.#probe.processRssBytes, Number.MAX_SAFE_INTEGER),
    ]);
    const availableDiskBytes = disk.value;
    const freeMemoryBytes = memory.value;
    const openFileDescriptors = descriptors.value;
    const processRssBytes = rss.value;
    const recovering = new Set(this.#snapshot?.reasons);
    const reasons: ResourcePressureReason[] = this.#policy.enabled
      ? pressureReasons(
          {
            availableDiskBytes,
            freeMemoryBytes,
            openFileDescriptors,
            processRssBytes,
          },
          this.#policy,
          recovering,
        )
      : [];
    if (
      this.#policy.enabled &&
      [disk, memory, descriptors, rss].some(({ failed }) => failed)
    )
      reasons.push("RESOURCE_PROBE_FAILED");
    return {
      availableDiskBytes,
      freeMemoryBytes,
      nextProbeAt: now + this.#policy.probeIntervalMs,
      openFileDescriptors,
      processRssBytes,
      reasons,
      state: reasons.length === 0 ? "ready" : "resource_wait",
    };
  }
}

async function safeProbe<T>(
  operation: () => Promise<T> | T,
  fallback: T,
): Promise<{
  readonly failed: boolean;
  readonly value: T;
}> {
  try {
    return { failed: false, value: await operation() };
  } catch {
    return { failed: true, value: fallback };
  }
}

function pressureReasons(
  metrics: Omit<ResourcePressureSnapshot, "nextProbeAt" | "reasons" | "state">,
  policy: ResourcePressurePolicy,
  recovering: ReadonlySet<ResourcePressureReason>,
): ResourcePressureReason[] {
  const resumeFactor = 1 + policy.resumeHysteresisBasisPoints / 10_000;
  const releaseFactor = 1 / resumeFactor;
  const reasons: ResourcePressureReason[] = [];
  const minimumFreeDiskBytes = recovering.has("DISK_PRESSURE")
    ? Math.ceil(policy.minimumFreeDiskBytes * resumeFactor)
    : policy.minimumFreeDiskBytes;
  const configuredMemoryFloor = Math.max(
    policy.minimumFreeMemoryBytes,
    Math.floor(
      (totalmem() * (policy.minimumAvailableMemoryBasisPoints ?? 0)) / 10_000,
    ),
  );
  const minimumFreeMemoryBytes = recovering.has("MEMORY_PRESSURE")
    ? Math.ceil(configuredMemoryFloor * resumeFactor)
    : configuredMemoryFloor;
  const maximumProcessRssBytes = recovering.has("PROCESS_MEMORY_PRESSURE")
    ? Math.floor(policy.maximumProcessRssBytes * releaseFactor)
    : policy.maximumProcessRssBytes;
  const maximumOpenFileDescriptors = recovering.has("FILE_DESCRIPTOR_PRESSURE")
    ? Math.floor(policy.maximumOpenFileDescriptors * releaseFactor)
    : policy.maximumOpenFileDescriptors;
  if (metrics.availableDiskBytes < minimumFreeDiskBytes)
    reasons.push("DISK_PRESSURE");
  if (metrics.freeMemoryBytes < minimumFreeMemoryBytes)
    reasons.push("MEMORY_PRESSURE");
  if (metrics.processRssBytes > maximumProcessRssBytes)
    reasons.push("PROCESS_MEMORY_PRESSURE");
  if (
    metrics.openFileDescriptors !== null &&
    metrics.openFileDescriptors >= maximumOpenFileDescriptors
  )
    reasons.push("FILE_DESCRIPTOR_PRESSURE");
  return reasons;
}

async function filesystemAvailableBytes(root: string): Promise<number> {
  const filesystem = await statfs(resolve(root));
  const available = filesystem.bavail * filesystem.bsize;
  return Number.isSafeInteger(available) && available >= 0 ? available : 0;
}

async function availableMemoryBytes(): Promise<number> {
  switch (platform()) {
    case "darwin": {
      const { stdout } = await execFileAsync(
        "/usr/bin/memory_pressure",
        ["-Q"],
        { encoding: "utf8", timeout: 2_000 },
      );
      const percentage = /System-wide memory free percentage:\s*(\d+)%/i.exec(
        stdout,
      )?.[1];
      if (percentage === undefined)
        throw new Error("RESOURCE_MEMORY_PRESSURE_OUTPUT_INVALID");
      return Math.floor((totalmem() * Number(percentage)) / 100);
    }
    case "linux": {
      const memoryInfo = await readFile("/proc/meminfo", "utf8");
      const kibibytes = /^MemAvailable:\s+(\d+)\s+kB$/im.exec(memoryInfo)?.[1];
      if (kibibytes === undefined)
        throw new Error("RESOURCE_MEMINFO_AVAILABLE_MISSING");
      return Number(kibibytes) * 1024;
    }
    case "aix":
    case "android":
    case "cygwin":
    case "freebsd":
    case "haiku":
    case "netbsd":
    case "openbsd":
    case "sunos":
    case "win32":
      return freemem();
  }
}

async function countOpenFileDescriptors(): Promise<null | number> {
  for (const directory of ["/dev/fd", "/proc/self/fd"]) {
    try {
      // eslint-disable-next-line no-await-in-loop -- Descriptor roots are ordered platform fallbacks; stop after the first supported path.
      const entries = await readdir(directory);
      return entries.length;
    } catch (error) {
      if (filesystemErrorCode(error) === "ENOENT") continue;
      // Unsupported or permission denied: preserve portability and rely on
      // process-memory/disk gates plus the scheduler's own concurrency cap.
    }
  }
  return null;
}

function filesystemErrorCode(error: unknown): null | string {
  if (typeof error !== "object" || error === null || !("code" in error))
    return null;
  return typeof error.code === "string" ? error.code : null;
}
