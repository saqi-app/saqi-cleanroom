import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import { CURRENT_SCHEMA_VERSION, Ledger } from "../persistence/ledger";
import { canonicalJson, sha256 } from "../persistence/work-key";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const AUTHOR_ID = "00000000-0000-4000-8000-000000000001";
const POEM_ID = "00000000-0000-4000-8000-000000000002";

describe("production detail recovery CLI", () => {
  it("is dry-run by default and applies idempotently only when explicit", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-detail-recovery-"));
    Ledger.initialize(join(root, "ledger.sqlite3")).close();
    const authors = join(root, "authors.ndjson");
    const poems = join(root, "poems.ndjson");
    const manifest = join(root, "manifest.json");
    const authorsContent = `${JSON.stringify({ id: AUTHOR_ID, name_arabic: "المتنبي", slug: "mutanabi" })}\n`;
    const poemsContent = `${JSON.stringify({
      active_source_revision_id: null,
      author_id: AUTHOR_ID,
      content_arabic: { content: ["بيت"] },
      id: POEM_ID,
      insights: null,
      name_arabic: "قصيدة",
      slug: "work-1",
      translation: null,
      translation_gemini: null,
    })}\n`;
    writeFileSync(authors, authorsContent);
    writeFileSync(poems, poemsContent);
    const body = {
      authorsSha256: sha256(authorsContent),
      poemIds: [POEM_ID],
      poemsSha256: sha256(poemsContent),
      schemaId: "saqi.production-detail-recovery-manifest",
      schemaVersion: 2,
    } as const;
    writeFileSync(
      manifest,
      `${JSON.stringify({ ...body, manifestHash: sha256(canonicalJson(body)) })}\n`,
    );
    const run = (mode?: "--apply"): unknown => {
      const parsed: unknown = JSON.parse(
        execFileSync(
          process.execPath,
          [
            "--import",
            "tsx",
            resolve(import.meta.dirname, "../cli.ts"),
            "recover-legacy-sources",
            "--state-dir",
            root,
            "--manifest",
            manifest,
            "--authors",
            authors,
            "--poems",
            poems,
            ...(mode ? [mode] : []),
          ],
          { encoding: "utf8" },
        ),
      );
      return parsed;
    };
    const ledgerHashBeforeDryRun = sha256(
      readFileSync(join(root, "ledger.sqlite3")),
    );
    expect(run()).toMatchObject({
      command: "recover-legacy-sources",
      mode: "dry-run",
      result: { applied: false, candidateWork: 1, seeded: 0 },
    });
    expect(schemaVersion(root)).toBe(CURRENT_SCHEMA_VERSION);
    expect(sha256(readFileSync(join(root, "ledger.sqlite3")))).toBe(
      ledgerHashBeforeDryRun,
    );
    expect(readDetailCount(root)).toBe(0);
    expect(() => run("--apply")).toThrow("RECOVERY_REQUIRES_PAID_WORK_PAUSED");
    expect(readDetailCount(root)).toBe(0);
    const controls = Ledger.open(join(root, "ledger.sqlite3"));
    controls.pauseControls.set("paid", true);
    controls.close();
    writeFileSync(authors, `${authorsContent} `);
    expect(() => run("--apply")).toThrow("RECOVERY_AUTHORS_HASH_MISMATCH");
    expect(readDetailCount(root)).toBe(0);
    writeFileSync(authors, authorsContent);
    expect(run("--apply")).toMatchObject({
      mode: "apply",
      result: { applied: true, duplicateWork: 0, seeded: 1 },
    });
    expect(run("--apply")).toMatchObject({
      mode: "apply",
      result: { activeExisting: 1, applied: true, duplicateWork: 1, seeded: 0 },
    });
    expect(readDetailCount(root)).toBe(1);
    expect(schemaVersion(root)).toBe(CURRENT_SCHEMA_VERSION);
  }, 20_000);
});

function schemaVersion(root: string): number {
  const database = new Database(join(root, "ledger.sqlite3"), {
    readonly: true,
  });
  try {
    const row = database
      .prepare<[], { version: number }>(
        "SELECT version FROM local_schema WHERE singleton = 1",
      )
      .get();
    if (!row) throw new Error("Missing schema version");
    return row.version;
  } finally {
    database.close();
  }
}

function readDetailCount(root: string): number {
  const ledger = Ledger.open(join(root, "ledger.sqlite3"), { readonly: true });
  try {
    return (
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === "source_poem_detail")
        ?.total ?? 0
    );
  } finally {
    ledger.close();
  }
}
