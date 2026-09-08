import { tmpdir } from "node:os";
import { join } from "node:path";

import type { PoemEnrichmentInput } from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import {
  SOL_ENRICHMENT_WORK_KIND,
  SolEnrichmentCoordinator,
} from "../enrichment/sol-coordinator.js";
import { CodexSolRunner } from "../enrichment/sol-runner.js";
import { ArtifactStore } from "../persistence/artifact-store.js";
import { Ledger } from "../persistence/ledger.js";
import { inputHash } from "../persistence/work-key.js";
import { trackedMkdtempSync } from "./support/tracked-test-root";

const INPUT: PoemEnrichmentInput = {
  authorArabic: "المتنبي",
  linesArabic: ["على قدر أهل العزم تأتي العزائم"],
  poemId: "poem-1",
  schemaId: "saqi.poem-enrichment-input",
  schemaVersion: 1,
  sourceContentSha256: "a".repeat(64),
  sourceRevisionId: "a".repeat(64),
  titleArabic: "على قدر أهل العزم",
};

describe("Sol versioned seeding", () => {
  it.each(["running", "retry_wait", "quota_wait", "succeeded"] as const)(
    "reuses exact v2 %s work without admitting another generation",
    async (state) => {
      const fixture = await createFixture();
      try {
        const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
        const claim = fixture.ledger.claim("legacy", 2, 1_000, [
          SOL_ENRICHMENT_WORK_KIND,
        ]);
        if (!claim) throw new Error("Missing legacy claim");
        switch (state) {
          case "succeeded":
            fixture.ledger.succeed(claim, "b".repeat(64), 3);
            break;
          case "retry_wait":
            fixture.ledger.retry(claim, "TRANSIENT_FAILURE", 10, 3);
            break;
          case "quota_wait":
            fixture.ledger.quotaWait(claim, "QUOTA_WAIT", 10, 3);
            break;
          case "running":
            break;
        }

        expect(fixture.coordinator.seedMany([INPUT, INPUT])).toEqual([
          { inserted: false, workKey: legacy.workKey },
          { inserted: false, workKey: legacy.workKey },
        ]);
        expect(
          fixture.ledger.seedMany([
            {
              ...legacyDefinition(INPUT),
              implementationVersion: "sol-word-gloss-v3",
            },
          ]),
        ).toEqual([{ inserted: false, workKey: legacy.workKey }]);
        expect(fixture.ledger.get(legacy.workKey)).toMatchObject({
          attemptCount: 1,
          implementationVersion: "sol-word-gloss-v2",
          state,
        });
        expect(fixture.ledger.status().total).toBe(1);
      } finally {
        fixture.ledger.close();
      }
    },
  );

  it("does not reuse failed work or a different immutable source", async () => {
    const fixture = await createFixture();
    try {
      const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
      const claim = fixture.ledger.claim("legacy", 2, 1_000, [
        SOL_ENRICHMENT_WORK_KIND,
      ]);
      if (!claim) throw new Error("Missing legacy claim");
      fixture.ledger.deadLetter(claim, "INVALID_OUTPUT", 3);
      const results = fixture.coordinator.seedMany([
        INPUT,
        { ...INPUT, sourceRevisionId: "c".repeat(64) },
      ]);
      expect(results).toHaveLength(2);
      for (const result of results) {
        expect(result.inserted).toBe(true);
        expect(result.workKey).not.toBe(legacy.workKey);
        expect(fixture.ledger.get(result.workKey)?.implementationVersion).toBe(
          "sol-word-gloss-v3",
        );
      }
      expect(results[0]?.workKey).not.toBe(results[1]?.workKey);
    } finally {
      fixture.ledger.close();
    }
  });

  it.each([
    "CODEX_OPERATION_OUTCOME_UNKNOWN",
    "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
  ])("preserves the %s fence across a profile upgrade", async (errorCode) => {
    const fixture = await createFixture();
    try {
      const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
      const claim = fixture.ledger.claim("legacy", 2, 1_000, [
        SOL_ENRICHMENT_WORK_KIND,
      ]);
      if (!claim) throw new Error("Missing legacy claim");
      fixture.ledger.deadLetter(claim, errorCode, 3);
      expect(fixture.coordinator.seed(INPUT)).toEqual({
        inserted: false,
        workKey: legacy.workKey,
      });
      expect(fixture.ledger.get(legacy.workKey)).toMatchObject({
        lastErrorCode: errorCode,
        outputArtifactHash: null,
        state: "dead_letter",
      });
      expect(fixture.ledger.status().total).toBe(1);
    } finally {
      fixture.ledger.close();
    }
  });

  it("seeds untouched v2 work into the exact replacement used by migration", async () => {
    const fixture = await createFixture();
    try {
      const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
      const replacement = fixture.coordinator.seed(INPUT);
      expect(replacement.inserted).toBe(true);
      expect(replacement.workKey).not.toBe(legacy.workKey);
      expect(fixture.coordinator.seed(INPUT)).toEqual({
        ...replacement,
        inserted: false,
      });
      fixture.ledger.migrateUnattemptedSolV2Work(50, Date.now());
      expect(fixture.ledger.get(legacy.workKey)?.state).toBe("imported");
      expect(fixture.ledger.get(replacement.workKey)).toMatchObject({
        attemptCount: 0,
        implementationVersion: "sol-word-gloss-v3",
        state: "pending",
      });
    } finally {
      fixture.ledger.close();
    }
  });

  it("does not let a completed old source suppress a corrected source", async () => {
    const fixture = await createFixture();
    try {
      const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
      const claim = fixture.ledger.claim("legacy", 2, 1_000, [
        SOL_ENRICHMENT_WORK_KIND,
      ]);
      if (!claim) throw new Error("Missing legacy claim");
      fixture.ledger.succeed(claim, "b".repeat(64), 3);
      const result = fixture.coordinator.seed({
        ...INPUT,
        sourceRevisionId: "c".repeat(64),
      });
      expect(result.inserted).toBe(true);
      expect(result.workKey).not.toBe(legacy.workKey);
    } finally {
      fixture.ledger.close();
    }
  });
});

it("retires an unattempted v3 duplicate when a legacy quarantine is present", async () => {
  const fixture = await createFixture();
  try {
    const legacy = fixture.ledger.seed(legacyDefinition(INPUT), 1);
    const duplicate = fixture.coordinator.seed(INPUT);
    const claim = fixture.ledger.claim(
      "legacy",
      Date.now(),
      1_000,
      [SOL_ENRICHMENT_WORK_KIND],
      { implementationVersion: "sol-word-gloss-v2" },
    );
    if (!claim) throw new Error("Missing legacy claim");
    fixture.ledger.deadLetter(claim, "CODEX_OPERATION_OUTCOME_UNKNOWN");
    expect(fixture.coordinator.seed(INPUT)).toEqual({
      inserted: false,
      workKey: legacy.workKey,
    });
    expect(fixture.ledger.get(duplicate.workKey)).toMatchObject({
      attemptCount: 0,
      outputArtifactHash: null,
      state: "imported",
    });
    expect(fixture.ledger.claim("new", Date.now(), 1_000)).toBeNull();
  } finally {
    fixture.ledger.close();
  }
});

function legacyDefinition(input: PoemEnrichmentInput) {
  return {
    implementationVersion: "sol-word-gloss-v2",
    input,
    inputHash: inputHash(input),
    kind: SOL_ENRICHMENT_WORK_KIND,
    priority: 0,
    schemaVersion: `${input.schemaId}@${String(input.schemaVersion)}`,
  };
}

async function createFixture() {
  const root = trackedMkdtempSync(join(tmpdir(), "saqi-sol-version-seeding-"));
  const ledger = Ledger.open(join(root, "ledger.sqlite3"));
  const database = new Database(join(root, "ledger.sqlite3"));
  try {
    database
      .prepare("INSERT INTO sol_operation_import_receipt VALUES(1, ?, 0, 0, 0)")
      .run("a".repeat(64));
    database.exec(
      "INSERT INTO runtime_control VALUES('sol_operation_import_complete', 1)",
    );
  } finally {
    database.close();
  }
  const artifacts = new ArtifactStore(join(root, "artifacts"));
  await artifacts.temporaryCleanup;
  return {
    coordinator: new SolEnrichmentCoordinator({
      artifacts,
      ledger,
      runner: new CodexSolRunner({
        attemptRoot: join(root, "attempts"),
        cwd: root,
        operations: ledger.solOperations,
      }),
    }),
    ledger,
  };
}
