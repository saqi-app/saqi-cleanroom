import { type ChildProcess, fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import Database from "better-sqlite3";
import { ModuleKind, transpileModule } from "typescript";
import { expect, test } from "vitest";

import { RUNTIME_OWNER_MIGRATION_SQL } from "../persistence/runtime-owner-schema.js";
import { RuntimeOwnerStore } from "../persistence/runtime-owner-store.js";
import { trackedMkdtempSync } from "./support/tracked-test-root.js";

async function terminate(children: readonly ChildProcess[]): Promise<void> {
  await Promise.all(
    children.map(
      (child) =>
        new Promise<void>((resolve) => {
          if (child.exitCode !== null || child.signalCode !== null) {
            resolve();
            return;
          }
          child.once("exit", () => resolve());
          child.kill("SIGKILL");
        }),
    ),
  );
}

function outcome(child: ChildProcess): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("Child observation timed out")),
      3000,
    );
    child.once("message", (message) => {
      clearTimeout(timer);
      resolve(message);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`Child exited early: ${String(code)}`));
    });
  });
}

test("two processes cannot both claim or remove a replacement runtime owner", async () => {
  const root = trackedMkdtempSync(join(tmpdir(), "runtime-owner-process-"));
  const path = join(root, "ledger.sqlite3");
  const database = new Database(path);
  database.pragma("journal_mode=WAL");
  database.exec(
    "CREATE TABLE local_schema(singleton INTEGER PRIMARY KEY, version INTEGER); INSERT INTO local_schema VALUES(1,36)",
  );
  database.exec(RUNTIME_OWNER_MIGRATION_SQL);
  const base = import.meta.dirname;
  for (const name of [
    "baseline-schema",
    "runtime-owner-schema",
    "runtime-owner-store",
  ]) {
    const source = readFileSync(
      join(base, "../persistence", `${name}.ts`),
      "utf8",
    );
    writeFileSync(
      join(root, `${name}.js`),
      transpileModule(source, {
        compilerOptions: { module: ModuleKind.ESNext },
      }).outputText,
    );
  }
  symlinkSync(
    join(base, "../../../../node_modules"),
    join(root, "node_modules"),
  );
  writeFileSync(join(root, "package.json"), '{"type":"module"}');
  const script = join(root, "child.js");
  writeFileSync(
    script,
    `
    import Database from 'better-sqlite3';
    import { randomUUID } from 'node:crypto';
    import { RuntimeOwnerStore } from './runtime-owner-store.js';
    const database = new Database(process.argv[2], {fileMustExist:true,timeout:3000, verbose(sql) {
      if(process.argv[3]==='before-commit' && sql==='COMMIT') {
        process.send({beforeCommit:true});
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5000);
      }
    }});
    const store = new RuntimeOwnerStore(database);
    let fence;
    try { fence=store.claim({pid:process.pid,runId:randomUUID(),configDigest:'a'.repeat(64),schemaVersion:1,startedAt:new Date().toISOString()},'supervisor',true); process.send({claimed:true,pid:process.pid}); }
    catch(error) { process.send({claimed:false,name:error.name}); }
    process.on('message', () => { if(fence) store.release(fence); database.close(); process.exit(0); });
  `,
  );
  const children = [
    fork(script, [path], { silent: true }),
    fork(script, [path], { silent: true }),
  ];
  try {
    const outcomes = await Promise.all(children.map(outcome));
    expect(outcomes).toEqual(
      expect.arrayContaining([
        { claimed: true, pid: expect.any(Number) },
        { claimed: false, name: "RuntimeOwnerBusyError" },
      ]),
    );
    const current = new RuntimeOwnerStore(database).read();
    expect(current?.epoch).toBe(1);
    if (!current) throw new Error("Winning owner missing");
    await terminate(children);
    const store = new RuntimeOwnerStore(database);
    const replacement = store.claim(
      {
        pid: process.pid,
        runId: randomUUID(),
        configDigest: "b".repeat(64),
        schemaVersion: 1,
        startedAt: new Date().toISOString(),
      },
      "supervisor",
      true,
    );
    expect(replacement.epoch).toBe(2);
    expect(store.release(current)).toBe(false);
    expect(store.read()).toEqual(replacement);
    expect(store.release(replacement)).toBe(true);
    const uncommitted = fork(script, [path, "before-commit"], { silent: true });
    children.push(uncommitted);
    await expect(outcome(uncommitted)).resolves.toEqual({ beforeCommit: true });
    expect(store.read()).toBeNull();
    await terminate([uncommitted]);
    const afterCrash = store.claim(
      { ...replacement.record, runId: randomUUID() },
      "supervisor",
      true,
    );
    expect(afterCrash.epoch).toBe(3);
    expect(store.release(afterCrash)).toBe(true);
  } finally {
    await terminate(children);
    database.close();
  }
}, 10_000);
