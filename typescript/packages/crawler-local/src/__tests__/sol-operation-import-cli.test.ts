import { execFile } from "node:child_process";
import { mkdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import Database from "better-sqlite3";
import { expect, test } from "vitest";
import { z } from "zod";

import { Ledger } from "../persistence/ledger.js";
import { inputHash, sha256 } from "../persistence/work-key.js";
import { acquireRunLock, readRunLock } from "../runtime/run-lock.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const execFileAsync = promisify(execFile);
const DryRunSchema = z.object({
  result: z.object({
    mode: z.literal("dry_run"),
    sourceDigest: z.string(),
    records: z.literal(0),
  }),
});

function inspectLedger(path: string): Ledger {
  const database = new Database(path, { readonly: true, fileMustExist: true });
  database.pragma("synchronous=FULL");
  return new Ledger(database, { readonly: true, path });
}

function fixture() {
  const root = trackedMkdtempSync(join(tmpdir(), "sol-import-cli-"));
  const path = join(root, "ledger.sqlite3");
  Ledger.initialize(path).close();
  const db = new Database(path);
  try {
    db.exec(`INSERT INTO runtime_control VALUES('service_enabled', 0)
      ON CONFLICT(control_key) DO UPDATE SET enabled = 0;
      INSERT INTO runtime_control VALUES('global_paused', 1), ('paid_work_paused', 1),
      ('legacy_pause_imported', 1), ('legacy_service_imported', 1)
      ON CONFLICT(control_key) DO UPDATE SET enabled = 1;`);
  } finally {
    db.close();
  }
  mkdirSync(join(root, "sol-attempts", "operation-index"), { recursive: true });
  return { root, path };
}

async function run(root: string, ...args: string[]): Promise<unknown> {
  const result = await execFileAsync(
    process.execPath,
    [
      "--import",
      "tsx",
      resolve(import.meta.dirname, "../cli.ts"),
      "import-sol-operations",
      "--state-dir",
      root,
      ...args,
    ],
    {
      timeout: 20_000,
      env: {
        ...process.env,
        SAQI_SOURCE_NAME: "incomplete-unused-source",
        SAQI_SOURCE_BASE_URL: undefined,
      },
    },
  );
  return JSON.parse(result.stdout);
}

test("CLI import requires explicit digest apply and never rearms paused budgets", async () => {
  const { root, path } = fixture();
  let ledger = inspectLedger(path);
  expect(ledger.solOperations).toBe(ledger.solOperations);
  expect(() => ledger.solOperations.assertImported()).toThrow();
  const budget = ledger.solPaidUsageBudgetStatus();
  const pauses = ledger.pauseControls.read();
  ledger.close();
  const before = sha256(readFileSync(path));
  const dryRun = DryRunSchema.parse(await run(root, "--dry-run"));
  expect(sha256(readFileSync(path))).toBe(before);
  await expect(run(root, "--apply")).rejects.toThrow(
    "--apply requires --expected-digest",
  );
  await expect(
    run(
      root,
      "--apply",
      "--dry-run",
      "--expected-digest",
      dryRun.result.sourceDigest,
    ),
  ).rejects.toThrow("mutually exclusive");
  await expect(
    run(root, "--apply", "--expected-digest", "0".repeat(64)),
  ).rejects.toThrow();
  await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
  const state = new Database(path, { readonly: true });
  try {
    expect(
      state.prepare("SELECT epoch,held,owner_kind FROM runtime_owner").get(),
    ).toEqual({ epoch: 1, held: 0, owner_kind: "maintenance" });
    expect(state.prepare("SELECT * FROM sol_operation").all()).toEqual([]);
    expect(state.prepare("SELECT * FROM sol_invocation_attempt").all()).toEqual(
      [],
    );
    expect(
      state.prepare("SELECT * FROM sol_operation_import_receipt").all(),
    ).toEqual([]);
  } finally {
    state.close();
  }
  await expect(
    run(root, "--apply", "--expected-digest", dryRun.result.sourceDigest),
  ).resolves.toMatchObject({
    result: { mode: "applied", receipt: { records: 0 } },
  });
  ledger = inspectLedger(path);
  try {
    expect(ledger.solOperations.assertImported().sourceDigest).toBe(
      dryRun.result.sourceDigest,
    );
    expect(ledger.solPaidUsageBudgetStatus()).toEqual(budget);
    expect(ledger.pauseControls.read()).toEqual(pauses);
  } finally {
    ledger.close();
  }
  await expect(run(root)).resolves.toMatchObject({
    result: { mode: "already_imported" },
  });
}, 30_000);

test("CLI import refuses enabled service without changing controls", async () => {
  const { root, path } = fixture();
  const db = new Database(path);
  db.exec(
    "UPDATE runtime_control SET enabled = 1 WHERE control_key = 'service_enabled'",
  );
  db.close();
  const before = sha256(readFileSync(path));
  await expect(run(root)).rejects.toThrow();
  expect(sha256(readFileSync(path))).toBe(before);
}, 30_000);

test("run-enrichment refuses missing import before recovering expired work or reserving budget", async () => {
  const { root, path } = fixture();
  const ledger = Ledger.open(path);
  const seeded = ledger.seed(
    {
      kind: "author-manifest",
      priority: 0,
      implementationVersion: "fixture-v1",
      schemaVersion: "fixture-v1",
      input: { author: "fixture" },
      inputHash: inputHash({ author: "fixture" }),
    },
    1,
  );
  const claim = ledger.claim("expired-fixture", 2, 1, ["author-manifest"]);
  expect(claim).not.toBeNull();
  ledger.armSolPaidUsageBudget(3);
  const beforeWork = ledger.get(seeded.workKey);
  const beforeBudget = ledger.solPaidUsageBudgetStatus();
  ledger.close();
  await expect(
    execFileAsync(
      process.execPath,
      [
        "--import",
        "tsx",
        resolve(import.meta.dirname, "../cli.ts"),
        "run-enrichment",
        "--state-dir",
        root,
        "--max",
        "1",
      ],
      { timeout: 20_000 },
    ),
  ).rejects.toThrow();
  const reopened = Ledger.open(path);
  try {
    expect(reopened.get(seeded.workKey)).toEqual(beforeWork);
    expect(reopened.solPaidUsageBudgetStatus()).toEqual(beforeBudget);
  } finally {
    reopened.close();
  }
  await expect(readRunLock(join(root, "RUN.lock"))).resolves.toBeNull();
}, 30_000);

test.each([false, true])(
  "standalone refuses existing owner (strict maintenance %s) before changing work or budget",
  async (maintenance) => {
    const { root, path } = fixture();
    const owner = await acquireRunLock(
      join(root, "RUN.lock"),
      "a".repeat(64),
      new Date(),
      { recoverStale: !maintenance },
    );
    const before = new Database(path, { readonly: true });
    const controls = before
      .prepare("SELECT * FROM runtime_control ORDER BY control_key")
      .all();
    const budget = before.prepare("SELECT * FROM sol_paid_usage_budget").all();
    before.close();
    try {
      await expect(
        execFileAsync(
          process.execPath,
          [
            "--import",
            "tsx",
            resolve(import.meta.dirname, "../cli.ts"),
            "run-enrichment",
            "--state-dir",
            root,
            "--max",
            "1",
          ],
          { timeout: 20_000 },
        ),
      ).rejects.toThrow("Runtime owner already held");
      await expect(readRunLock(join(root, "RUN.lock"))).resolves.toEqual(
        owner.record,
      );
      const after = new Database(path, { readonly: true });
      try {
        expect(
          after
            .prepare("SELECT * FROM runtime_control ORDER BY control_key")
            .all(),
        ).toEqual(controls);
        expect(
          after.prepare("SELECT * FROM sol_paid_usage_budget").all(),
        ).toEqual(budget);
      } finally {
        after.close();
      }
    } finally {
      await owner.release();
    }
  },
  30_000,
);
