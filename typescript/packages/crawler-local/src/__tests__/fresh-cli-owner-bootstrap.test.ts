import { execFile } from "node:child_process";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import Database from "better-sqlite3";
import { expect, test } from "vitest";
import { z } from "zod";

import { trackedMkdtempSync } from "./support/tracked-test-root.js";

const execute = promisify(execFile);
const PlanSchema = z.object({
  result: z.object({
    mode: z.literal("dry_run"),
    sourceDigest: z.string(),
    records: z.literal(0),
  }),
});

async function command(
  root: string,
  name: string,
  ...args: string[]
): Promise<unknown> {
  const result = await execute(
    process.execPath,
    [
      "--import",
      "tsx",
      resolve(import.meta.dirname, "../cli.ts"),
      name,
      "--state-dir",
      root,
      ...args,
    ],
    {
      timeout: 20_000,
      env: {
        ...process.env,
        PATH: `${join(root, "bin")}:${process.env["PATH"] ?? ""}`,
        SAQI_TEST_CODEX_MARKER: join(root, "codex-invoked"),
      },
    },
  );
  return JSON.parse(result.stdout);
}

test("fresh CLI initializes controls, explicitly imports empty history, and starts paused without calling Codex", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "fresh-owner-cli-"));
  mkdirSync(join(root, "bin"));
  writeFileSync(
    join(root, "bin", "codex"),
    '#!/bin/sh\nprintf invoked > "$SAQI_TEST_CODEX_MARKER"\nexit 99\n',
  );
  chmodSync(join(root, "bin", "codex"), 0o700);
  await expect(command(root, "init")).resolves.toMatchObject({
    initialized: true,
  });
  const path = join(root, "ledger.sqlite3");
  const database = new Database(path);
  try {
    expect(
      database.prepare("SELECT * FROM sol_operation_import_receipt").all(),
    ).toEqual([]);
    expect(
      database.prepare("SELECT * FROM sol_paid_usage_budget").all(),
    ).toEqual([]);
    await command(root, "pause");
    await command(root, "pause-paid");
    const controls = database
      .prepare("SELECT * FROM runtime_control ORDER BY control_key")
      .all();
    await command(root, "init");
    expect(
      database
        .prepare("SELECT * FROM runtime_control ORDER BY control_key")
        .all(),
    ).toEqual(controls);
    const plan = PlanSchema.parse(
      await command(root, "import-sol-operations", "--dry-run"),
    );
    expect(
      database.prepare("SELECT * FROM sol_operation_import_receipt").all(),
    ).toEqual([]);
    await expect(
      command(
        root,
        "import-sol-operations",
        "--apply",
        "--expected-digest",
        plan.result.sourceDigest,
      ),
    ).resolves.toMatchObject({
      result: {
        mode: "applied",
        receipt: { records: 0, sourceDigest: plan.result.sourceDigest },
      },
    });
    await expect(
      command(root, "run-enrichment", "--max", "1"),
    ).resolves.toMatchObject({ result: { claimed: 0 } });
    expect(database.prepare("SELECT held FROM runtime_owner").get()).toEqual({
      held: 0,
    });
    expect(
      database.prepare("SELECT * FROM sol_paid_usage_budget").all(),
    ).toEqual([]);
    expect(
      database.prepare("SELECT * FROM sol_paid_usage_reservation").all(),
    ).toEqual([]);
    expect(
      database
        .prepare(
          "SELECT * FROM runtime_control WHERE control_key!='sol_operation_import_complete' ORDER BY control_key",
        )
        .all(),
    ).toEqual(controls);
    expect(existsSync(join(root, "codex-invoked"))).toBe(false);
  } finally {
    database.close();
  }
}, 30_000);

test("init never creates a fabricated empty index beside existing legacy artifacts", async () => {
  const root = trackedMkdtempSync(
    join(tmpdir(), "fresh-owner-existing-attempts-"),
  );
  const attempts = join(root, "sol-attempts");
  mkdirSync(attempts);
  const evidence = join(attempts, "retained-artifact.json");
  writeFileSync(evidence, "retained original bytes");
  await expect(command(root, "init")).rejects.toThrow();
  expect(existsSync(join(attempts, "operation-index"))).toBe(false);
  expect(readFileSync(evidence, "utf8")).toBe("retained original bytes");
  const database = new Database(join(root, "ledger.sqlite3"), {
    readonly: true,
  });
  try {
    expect(
      database.prepare("SELECT * FROM sol_operation_import_receipt").all(),
    ).toEqual([]);
  } finally {
    database.close();
  }
});
