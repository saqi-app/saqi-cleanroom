import type * as ChildProcess from "node:child_process";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import { Ledger } from "../persistence/ledger.js";
import { controlLaunchdService } from "../runtime/launchd-control.js";
import {
  readServiceEnabled,
  writeServiceEnabled,
} from "../runtime/service-enabled-control.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const { execute } = vi.hoisted(() => ({ execute: vi.fn() }));
vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof ChildProcess>();
  const { promisify } = await import("node:util");
  return {
    ...original,
    execFile: Object.assign(vi.fn(), { [promisify.custom]: execute }),
  };
});

describe("stop during unacknowledged managed startup", () => {
  it("disables future recovery while refusing to signal an unverified process", async () => {
    const root = trackedMkdtempSync(join(tmpdir(), "saqi-startup-stop-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    writeServiceEnabled(root, true);
    const configPath = join(root, "config.json");
    writeFileSync(
      configPath,
      JSON.stringify({ schemaVersion: 1, stateDirectory: root }),
    );
    execute.mockResolvedValue({
      stdout: "state = running\npid = 12345\nexit timeout = 180\n",
      stderr: "",
    });
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    try {
      await expect(
        controlLaunchdService({
          action: "stop",
          configPath,
          label: "net.saqi.test",
        }),
      ).rejects.toThrow(
        "LAUNCHD_STOP_DISABLED_BUT_UNACKNOWLEDGED: SERVICE_PID_MISMATCH",
      );
      expect(readServiceEnabled(root)).toBe(false);
      expect(kill).not.toHaveBeenCalled();
      expect(execute).toHaveBeenCalledOnce();
      expect(execute).toHaveBeenCalledWith(
        "/bin/launchctl",
        ["print", expect.any(String)],
        expect.any(Object),
      );
    } finally {
      kill.mockRestore();
    }
  });
});
