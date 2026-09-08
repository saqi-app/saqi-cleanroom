import type * as ChildProcess from "node:child_process";
import { EventEmitter } from "node:events";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";

import type { PoemEnrichmentInput } from "@saqi/precedent-iso";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CodexSolRunner } from "../enrichment/sol-runner.js";
import { Ledger } from "../persistence/ledger.js";
import { importEmptyTestOperations } from "./support/import-empty-test-operations.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const MOCKS = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", async (importOriginal) => ({
  ...(await importOriginal<typeof ChildProcess>()),
  spawn: MOCKS.spawn,
}));

const LEDGERS: Ledger[] = [];
async function childFixture() {
  // eslint-disable-next-line unicorn/prefer-event-target -- Node ChildProcess exposes EventEmitter error/close semantics, including synchronous reentrant errors.
  const child = Object.assign(new EventEmitter(), {
    exitCode: null,
    signalCode: null,
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    kill: vi.fn(() => true),
  });
  MOCKS.spawn.mockReturnValue(child);
  const root = trackedMkdtempSync(join(tmpdir(), "saqi-process-error-"));
  await importEmptyTestOperations(root);
  const ledger = Ledger.open(join(root, "ledger.sqlite3"));
  LEDGERS.push(ledger);
  const runner = new CodexSolRunner({
    operations: ledger.solOperations,
    attemptRoot: join(root, "attempts"),
    command: { executable: "injected-child", killGraceMs: 20 },
    cwd: root,
    authStatusTimeoutMs: 100,
  });
  return { child, runner };
}

afterEach(() => {
  for (const ledger of LEDGERS) ledger.close();
  LEDGERS.length = 0;
  vi.useRealTimers();
  MOCKS.spawn.mockReset();
});

describe("unexpected Codex child errors", () => {
  it("quarantines a paid operation after unexpected process failure without redispatch", async () => {
    vi.useFakeTimers();
    const { child, runner } = await childFixture();
    const input: PoemEnrichmentInput = {
      authorArabic: "المتنبي",
      linesArabic: ["على قدر أهل العزم تأتي العزائم"],
      poemId: "poem-1",
      schemaId: "saqi.poem-enrichment-input",
      schemaVersion: 1,
      sourceContentSha256: "a".repeat(64),
      sourceRevisionId: "a".repeat(64),
      titleArabic: "على قدر أهل العزم",
    };
    const failure = new Error("dispatch pipe failed");
    const outcome = runner.generate(input).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    child.stdin.emit("error", failure);
    await vi.advanceTimersByTimeAsync(20);
    await expect(outcome).resolves.toBe(failure);
    await expect(runner.generate(input)).resolves.toMatchObject({
      errorCode: "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
      state: "retry_wait",
    });
    expect(MOCKS.spawn).toHaveBeenCalledOnce();
  });

  it.each(["stdin", "child"])(
    "bounds %s failure and retains first error during reentrant kill errors",
    async (source) => {
      vi.useFakeTimers();
      const { child, runner } = await childFixture();
      const firstError = Object.assign(new Error("original IO failure"), {
        code: "EIO",
      });
      const killError = Object.assign(new Error("signal denied"), {
        code: "EPERM",
      });
      child.kill.mockImplementation(() => {
        child.emit("error", killError);
        return false;
      });
      let settled = false;
      const outcome = runner.verifyChatGptLogin().catch((error: unknown) => {
        settled = true;
        return error;
      });
      await vi.advanceTimersByTimeAsync(0);
      if (source === "stdin") child.stdin.emit("error", firstError);
      else child.emit("error", firstError);
      await vi.advanceTimersByTimeAsync(19);
      expect(settled).toBe(false);
      expect(child.kill).toHaveBeenCalledExactlyOnceWith("SIGTERM");
      await vi.advanceTimersByTimeAsync(1);
      await expect(outcome).resolves.toBe(firstError);
      expect(child.kill).toHaveBeenCalledWith("SIGKILL");
      expect(child.kill.mock.calls.length).toBeLessThanOrEqual(3);
      expect(child).toMatchObject({
        stdin: { destroyed: true },
        stdout: { destroyed: true },
        stderr: { destroyed: true },
      });
      const calls = child.kill.mock.calls.length;
      await vi.advanceTimersByTimeAsync(200);
      expect(child.kill).toHaveBeenCalledTimes(calls);
    },
  );

  it("attempts teardown before rejecting an early close and preserves original error when signalling throws", async () => {
    vi.useFakeTimers();
    const { child, runner } = await childFixture();
    const failure = new Error("stdin failed");
    child.kill.mockImplementation(() => {
      throw new Error("kill failed");
    });
    const outcome = runner
      .verifyChatGptLogin()
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    child.stdin.emit("error", failure);
    child.emit("close", 0, null);
    await expect(outcome).resolves.toBe(failure);
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    await vi.advanceTimersByTimeAsync(200);
    expect(child.kill).toHaveBeenCalledTimes(2);
  });
});
