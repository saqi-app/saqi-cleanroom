import { hash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  canonicalPoemBindingIdBody,
  type CanonicalPoemBindingV1,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import Database from "better-sqlite3";
import { afterEach, describe, expect, test } from "vitest";
import { z } from "zod";

import {
  CURRENT_SCHEMA_VERSION,
  Ledger,
  LedgerIntegrityError,
  LostLeaseError,
  MAX_DURABLE_FANOUT_PRIORITY_HINTS,
  PoemIdentityConflictError,
} from "../persistence/ledger.js";
import type { WorkDefinition } from "../persistence/schema.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const LEDGERS: Ledger[] = [];
const LEGACY_SOL_INPUT: PoemEnrichmentInput = {
  authorArabic: "شاعر",
  linesArabic: ["بيت أول", "", "بيت ثان"],
  poemId: "poem-1",
  schemaId: "saqi.poem-enrichment-input",
  schemaVersion: 1,
  sourceContentSha256: "b".repeat(64),
  sourceRevisionId: "e".repeat(64),
  titleArabic: "قصيدة",
};

afterEach(() => {
  for (const ledger of LEDGERS) ledger.close();
  LEDGERS.length = 0;
});

test("tracks exact-profile Sol poem throughput without counting provider operations", () => {
  const ledger = open();
  const now = 4_000_000;
  const requirements = {
    implementationVersion: "sol-word-gloss-v2",
    schemaVersion: "saqi.poem-enrichment-input@1",
  } as const;
  const source = ledger.seed(
    {
      ...requirements,
      input: LEGACY_SOL_INPUT,
      inputHash: inputHash(LEGACY_SOL_INPUT),
      kind: "poem-enrichment-sol",
      priority: 1,
    },
    now - 10 * 60_000,
  );
  const claim = ledger.claim(
    "sol-worker",
    now - 4 * 60_000,
    60_000,
    ["poem-enrichment-sol"],
    requirements,
  );
  if (!claim) throw new Error("Expected Sol claim");
  const artifactHash = "a".repeat(64);
  ledger.succeed(claim, artifactHash, now - 4 * 60_000);

  expect(ledger.backfillSolPoemMilestones("succeeded", 10, now)).toMatchObject({
    complete: true,
  });
  expect(ledger.backfillSolPoemMilestones("imported", 10, now)).toMatchObject({
    complete: true,
  });
  expect(ledger.poemThroughput(requirements, now)).toMatchObject({
    coverage: { backfillComplete: true },
    generated: { last15m: 1, last1h: 1, last5m: 1, lastAt: now - 4 * 60_000 },
    published: { last1h: 0, lastAt: null },
    remaining: {
      active: 0,
      delayed: 0,
      endToEndPublication: 1,
      generatedAwaitingPublication: 1,
      generation: 0,
      ready: 0,
      terminalDead: 0,
    },
  });

  expect(ledger.markImported(source.workKey, artifactHash, now - 60_000)).toBe(
    "imported",
  );
  expect(ledger.poemThroughput(requirements, now)).toMatchObject({
    generated: { last5m: 1 },
    published: { last15m: 1, last1h: 1, last5m: 1, lastAt: now - 60_000 },
    remaining: {
      endToEndPublication: 0,
      generatedAwaitingPublication: 0,
    },
  });
});

function open(
  path = join(mkdtempSync(join(tmpdir(), "saqi-ledger-")), "ledger.sqlite3"),
): Ledger {
  const ledger = Ledger.open(path);
  LEDGERS.push(ledger);
  return ledger;
}

function definition(
  input: Record<string, unknown> = { authorId: "495" },
): WorkDefinition {
  return {
    implementationVersion: "crawler@1",
    input,
    inputHash: inputHash(input),
    kind: "author-manifest",
    priority: 10,
    schemaVersion: "author-manifest@1",
  };
}

function poemDefinition(
  implementationVersion = "crawler@1",
  authorHref = "https://source.invalid/writers/poet-one",
  poemHref = "https://source.invalid/works/42",
): WorkDefinition {
  const input = {
    authorHref,
    poemHref,
  };
  return {
    implementationVersion,
    input,
    inputHash: inputHash(input),
    kind: "source_poem_detail",
    priority: 10,
    schemaVersion: "source-projection-v1",
  };
}

function canonicalBinding(): CanonicalPoemBindingV1 {
  const identity = {
    authorId: "author-1",
    authorNameArabic: LEGACY_SOL_INPUT.authorArabic,
    externalPoemId: "42",
    lineNfcHash: sha256(sourceLineNfcHashBody(LEGACY_SOL_INPUT.linesArabic)),
    poemId: "poem-1",
    promptMaterialHash: sha256(sourcePromptMaterialHashBody(LEGACY_SOL_INPUT)),
    schemaId: "saqi.canonical-poem-binding",
    schemaVersion: 1,
    sourceName: "source",
    sourceRevisionId: "e".repeat(64),
  } as const;
  return {
    admissionEvidence: {
      databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
      issuedAt: "2026-09-01T12:00:00Z",
      sourcePointerVersion: 7,
    },
    bindingId: hash("sha256", canonicalPoemBindingIdBody(identity), "hex"),
    ...identity,
  };
}

test("Sol paid usage budget persists, exhausts atomically, and requires explicit rearm", () => {
  const ledger = open();
  expect(ledger.solPaidUsageBudgetStatus()).toMatchObject({
    remainingOperations: 0,
    state: "unarmed",
  });
  expect(ledger.armSolPaidUsageBudget(6)).toMatchObject({
    maximumOperations: 6,
    remainingOperations: 6,
    state: "active",
  });
  expect(ledger.armSolPaidUsageBudget(6)).toMatchObject({
    remainingOperations: 6,
    state: "active",
  });

  ledger.seed(definition({ poem: 1 }));
  ledger.seed(definition({ poem: 2 }));
  ledger.seed(definition({ poem: 3 }));
  const claimAt = Date.now() + 1;
  const first = ledger.claim("sol", claimAt, 100, ["author-manifest"]);
  const second = ledger.claim("sol", claimAt, 100, ["author-manifest"]);
  const third = ledger.claim("sol", claimAt, 100, ["author-manifest"]);
  expect(first).not.toBeNull();
  expect(second).not.toBeNull();
  expect(third).not.toBeNull();
  expect(ledger.reserveSolPaidClaim(first!, claimAt)).toBe(true);
  expect(ledger.reserveSolPaidClaim(first!, claimAt)).toBe(true);
  expect(ledger.reserveSolPaidClaim(second!, claimAt)).toBe(true);
  expect(ledger.solPaidUsageBudgetStatus()).toMatchObject({
    remainingOperations: 0,
    reservedOperations: 6,
    state: "exhausted",
  });
  expect(ledger.reserveSolPaidClaim(third!, claimAt)).toBe(false);
  expect(() => ledger.armSolPaidUsageBudget(6)).toThrow(
    "SOL_PAID_USAGE_BUDGET_REARM_REQUIRED",
  );
  expect(ledger.armSolPaidUsageBudget(3, true)).toMatchObject({
    maximumOperations: 3,
    remainingOperations: 3,
    state: "active",
  });
});

test("an active Sol budget can only be increased monotonically", () => {
  const ledger = open();
  expect(ledger.armSolPaidUsageBudget(6)).toMatchObject({
    maximumOperations: 6,
    remainingOperations: 6,
  });
  expect(ledger.armSolPaidUsageBudget(9)).toMatchObject({
    maximumOperations: 9,
    remainingOperations: 9,
  });
  expect(() => ledger.armSolPaidUsageBudget(6)).toThrow(
    "SOL_PAID_USAGE_BUDGET_CANNOT_DECREASE",
  );
  expect(() => ledger.armSolPaidUsageBudget(9, true)).toThrow(
    "SOL_PAID_USAGE_BUDGET_ALREADY_ACTIVE",
  );
  ledger.close();
});

test.each(["current", "previous", "none"])(
  "expired Sol recovery recognizes only the %s attempt reservation",
  (reservation) => {
    const ledger = open();
    const key = ledger.seed(
      { ...definition(), kind: "poem-enrichment-sol" },
      100,
    ).workKey;
    ledger.armSolPaidUsageBudget(3);
    ledger.pauseControls.read();
    let claim = ledger.claim("crashed", 100, 10);
    if (!claim) throw new Error("Expected claim");
    if (reservation !== "none")
      expect(ledger.reserveSolPaidClaim(claim, 101)).toBe(true);
    if (reservation === "previous") {
      ledger.operatorRelease(claim, "OPERATOR_RELEASED", 102);
      claim = ledger.claim("replacement", 103, 10);
      if (!claim) throw new Error("Expected replacement claim");
    }
    const beforeBudget = ledger.solPaidUsageBudgetStatus();
    const beforePause = ledger.pauseControls.read();
    expect(ledger.recoverExpired(114)).toBe(1);
    expect(ledger.get(key)).toMatchObject({
      state: "pending",
      lastErrorCode:
        reservation === "current"
          ? "CODEX_OPERATION_OUTCOME_UNKNOWN"
          : "LEASE_EXPIRED",
    });
    expect(ledger.solPaidUsageBudgetStatus()).toEqual(beforeBudget);
    expect(ledger.pauseControls.read()).toEqual(beforePause);
    const recovered = ledger.claimUnknownOperationRecovery(
      "free-recovery",
      200,
      100,
      "poem-enrichment-sol",
      200,
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
    );
    expect(recovered !== null).toBe(reservation === "current");
  },
);

test("Sol paid usage budget rejects partial claim ceilings", () => {
  const ledger = open();
  expect(() => ledger.armSolPaidUsageBudget(1)).toThrow(
    "positive multiple of 3",
  );
});

describe("work identity and fenced leases", () => {
  test.each([
    {
      schemaVersion: "saqi.poem-enrichment-input@1",
      error: "TEST_RETRY",
      ready: false,
    },
    {
      schemaVersion: "saqi.poem-enrichment-input@2",
      error: "CODEX_OPERATION_OUTCOME_UNKNOWN",
      ready: false,
    },
    {
      schemaVersion: "saqi.poem-enrichment-input@2",
      error: "TEST_RETRY",
      ready: true,
    },
  ])(
    "legacy readiness matches paid claims for $schemaVersion / $error",
    ({ schemaVersion, error, ready }) => {
      const ledger = open();
      ledger.seed(
        {
          implementationVersion: "sol-word-gloss-v2",
          input: {},
          inputHash: inputHash({}),
          kind: "poem-enrichment-sol",
          priority: 1,
          schemaVersion,
        },
        1,
      );
      const claim = ledger.claim("worker", 2, 100, ["poem-enrichment-sol"]);
      if (!claim) throw new Error("Expected claim");
      ledger.retry(claim, error, 4, 3);
      expect(ledger.hasReadyStartedSolV2Work(4)).toBe(ready);
    },
  );

  test("upgrades only unattempted v2 work and preserves started work for recovery", () => {
    const ledger = open();
    const legacyDefinition = {
      implementationVersion: "sol-word-gloss-v2",
      input: LEGACY_SOL_INPUT,
      inputHash: inputHash(LEGACY_SOL_INPUT),
      kind: "poem-enrichment-sol",
      priority: 10,
      schemaVersion: "saqi.poem-enrichment-input@1",
    };
    const started = ledger.seed(legacyDefinition, 1);
    const claim = ledger.claim("worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected claim");
    ledger.retry(claim, "TEST_RETRY", 4, 3);
    const freshInput = { ...LEGACY_SOL_INPUT, poemId: "new-poem" };
    const fresh = ledger.seed(
      {
        ...legacyDefinition,
        input: freshInput,
        inputHash: inputHash(freshInput),
        priority: 100,
      },
      1,
    );
    expect(ledger.hasReadyStartedSolV2Work(3)).toBe(false);
    expect(ledger.hasReadyStartedSolV2Work(4)).toBe(false);
    const requirements = {
      implementationVersion: "sol-word-gloss-v2",
      schemaVersion: legacyDefinition.schemaVersion,
      minimumAttemptCount: 1,
    };
    expect(
      ledger.availability([legacyDefinition.kind], 4, requirements).ready,
    ).toBe(1);
    const recovery = ledger.claim(
      "recovery",
      4,
      100,
      [legacyDefinition.kind],
      requirements,
    );
    expect(recovery?.work.workKey).toBe(started.workKey);
    if (!recovery) throw new Error("Expected recovery claim");
    ledger.retry(recovery, "TEST_RETRY", 6, 5);
    expect(ledger.migrateUnattemptedSolV2Work(1, 4)).toEqual({
      inserted: 1,
      migrated: 1,
    });
    expect(ledger.migrateUnattemptedSolV2Work(1, 4)).toEqual({
      inserted: 0,
      migrated: 0,
    });
    expect(ledger.get(fresh.workKey)?.state).toBe("imported");
    expect(ledger.get(started.workKey)).toMatchObject({
      state: "retry_wait",
      attemptCount: 2,
      implementationVersion: "sol-word-gloss-v2",
    });
    expect(
      ledger.listReadyWork(
        "poem-enrichment-sol",
        10,
        {
          implementationVersion: "sol-word-gloss-v3",
          schemaVersion: legacyDefinition.schemaVersion,
        },
        4,
      ),
    ).toMatchObject([{ input: freshInput, attemptCount: 0 }]);
  });

  test("bounds recipe migration by exact profile while preserving global priority", () => {
    const calls: string[] = [];
    const database = new Database(":memory:", {
      verbose: (sql) => {
        calls.push(String(sql));
      },
    });
    const ledger = new Ledger(database);
    LEDGERS.push(ledger);
    const candidates = [
      {
        schemaVersion: "saqi.poem-enrichment-input@1",
        priority: 10,
        createdAt: 1,
      },
      {
        schemaVersion: "saqi.poem-enrichment-input@2",
        priority: 30,
        createdAt: 2,
      },
      {
        schemaVersion: "saqi.poem-enrichment-input@1",
        priority: 30,
        createdAt: 1,
      },
      {
        schemaVersion: "saqi.poem-enrichment-input@2",
        priority: 10,
        createdAt: 1,
      },
    ].map(({ schemaVersion, priority, createdAt }, index) => {
      const input = {
        ...LEGACY_SOL_INPUT,
        poemId: `migration-${String(index)}`,
      };
      const seeded = ledger.seed(
        {
          kind: "poem-enrichment-sol",
          implementationVersion: "sol-word-gloss-v2",
          schemaVersion,
          priority,
          input,
          inputHash: inputHash(input),
        },
        createdAt,
      );
      return { ...seeded, priority, createdAt };
    });
    const expected = candidates.toSorted(
      (left, right) =>
        right.priority - left.priority ||
        left.createdAt - right.createdAt ||
        (left.workKey < right.workKey
          ? -1
          : left.workKey > right.workKey
            ? 1
            : 0),
    );
    for (const candidate of expected) {
      calls.length = 0;
      expect(ledger.migrateUnattemptedSolV2Work(1, 3)).toEqual({
        inserted: 1,
        migrated: 1,
      });
      expect(ledger.get(candidate.workKey)?.state).toBe("imported");
      const query = calls.find(
        (sql) =>
          sql.includes("INDEXED BY work_item_ready_priority") &&
          sql.includes("attempt_count = 0"),
      );
      if (!query) throw new Error("Missing profile migration query");
      const plan = z
        .array(z.object({ detail: z.string() }))
        .parse(database.prepare(`EXPLAIN QUERY PLAN ${query}`).all());
      expect(
        plan.some(({ detail }) => detail.includes("work_item_claimable")),
      ).toBe(false);
      expect(plan.some(({ detail }) => detail.includes("TEMP B-TREE"))).toBe(
        false,
      );
      expect(
        plan.filter(({ detail }) => detail.includes("SEARCH work_item")),
      ).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            detail:
              "SEARCH work_item USING INDEX work_item_ready_priority (kind=? AND implementation_version=? AND schema_version=?)",
          }),
        ]),
      );
    }
    expect(ledger.migrateUnattemptedSolV2Work(1, 3)).toEqual({
      inserted: 0,
      migrated: 0,
    });
  });

  test.each(["sol-word-gloss-v2", "sol-word-gloss-v3"])(
    "atomically attaches approved %s work without changing its success",
    (implementationVersion) => {
      const path = join(
        mkdtempSync(join(tmpdir(), "saqi-legacy-direct-publication-")),
        "ledger.sqlite3",
      );
      const ledger = open(path);
      const input = LEGACY_SOL_INPUT;
      const source = ledger.seed(
        {
          implementationVersion,
          input,
          inputHash: inputHash(input),
          kind: "poem-enrichment-sol",
          priority: 19,
          schemaVersion: "saqi.poem-enrichment-input@1",
        },
        1,
      );
      const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
      if (!claim) throw new Error("Expected Sol claim");
      ledger.succeed(claim, "a".repeat(64), 3);

      const first = ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2", priority: 19 },
        4,
      );
      const replay = ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2", priority: 19 },
        5,
      );

      expect(replay).toEqual(first);
      expect(
        ledger.approvedPublicationAttachmentForTranslation(
          source.workKey,
          "a".repeat(64),
        ),
      ).toEqual({ binding: canonicalBinding(), publicationPriority: 19 });
      expect(() =>
        ledger.approvedPublicationAttachmentForTranslation(
          source.workKey,
          "b".repeat(64),
        ),
      ).toThrow("APPROVED_PUBLICATION_ATTACHMENT_CONFLICT");
      expect(ledger.get(source.workKey)).toMatchObject({
        outputArtifactHash: "a".repeat(64),
        state: "succeeded",
      });
      expect(ledger.get(first.publicationWorkKey)).toMatchObject({
        input: {
          binding: canonicalBinding(),
          jobType: "bound-translation",
          source: { artifactHash: "a".repeat(64), workKey: source.workKey },
        },
        state: "pending",
      });
      const database = new Database(path, { readonly: true });
      try {
        expect(
          database
            .prepare(
              `SELECT event_type, COUNT(*) AS count FROM work_event
             WHERE work_key = ? GROUP BY event_type ORDER BY event_type`,
            )
            .all(source.workKey),
        ).toEqual([
          { count: 1, event_type: "claimed" },
          { count: 1, event_type: "seeded" },
          { count: 1, event_type: "succeeded" },
        ]);
        expect(
          database
            .prepare(
              `SELECT COUNT(*) AS count FROM canonical_translation_binding
             WHERE translation_work_key = ?`,
            )
            .get(source.workKey),
        ).toEqual({ count: 1 });
        expect(
          database
            .prepare(
              `SELECT COUNT(*) AS count FROM publication_derivation
             WHERE translation_work_key = ?`,
            )
            .get(source.workKey),
        ).toEqual({ count: 1 });
      } finally {
        database.close();
      }
    },
  );

  test("reuses imported Sol work only after exact publication provenance is attached", () => {
    const ledger = open();
    const input = LEGACY_SOL_INPUT;
    const artifactHash = "a".repeat(64);
    const source = ledger.seed(
      {
        implementationVersion: "sol-word-gloss-v2",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 19,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    ledger.succeed(claim, artifactHash, 3);
    expect(ledger.markImported(source.workKey, artifactHash, 4)).toBe(
      "imported",
    );
    const fanout = ledger.seed(
      {
        implementationVersion: "fanout-reconciler-v1",
        input: {
          lane: "sol",
          source: { artifactHash, workKey: source.workKey },
        },
        inputHash: inputHash({
          lane: "sol",
          source: { artifactHash, workKey: source.workKey },
        }),
        kind: "fanout-succeeded-sol",
        priority: 19,
        schemaVersion: "fanout@1",
      },
      5,
    );
    const reusable = {
      fanoutWorkKey: fanout.workKey,
      modelKey: "openai:gpt-5.6-sol",
      outputArtifactHash: artifactHash,
      promptMaterialHash: canonicalBinding().promptMaterialHash,
      translationWorkKey: source.workKey,
    };

    expect(ledger.indexReusableEnrichment(reusable)).toBe(false);
    expect(
      ledger.reusableEnrichmentForMaterial(
        reusable.modelKey,
        reusable.promptMaterialHash,
      ),
    ).toBeNull();

    ledger.attachApprovedBindingAndSeedPublication(
      source.workKey,
      artifactHash,
      canonicalBinding(),
      { implementationVersion: "direct-publication-v2", priority: 19 },
      6,
    );
    expect(ledger.indexReusableEnrichment(reusable)).toBe(true);
    expect(
      ledger.reusableEnrichmentForMaterial(
        reusable.modelKey,
        reusable.promptMaterialHash,
      ),
    ).toMatchObject({
      fanoutState: "pending",
      outputArtifactHash: artifactHash,
      translationWorkKey: source.workKey,
    });
  });

  test("rejects a legacy Sol attachment whose exact prompt material differs", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-legacy-material-mismatch-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const input = { ...LEGACY_SOL_INPUT, titleArabic: "قصيدة أخرى" };
    const source = ledger.seed(
      {
        implementationVersion: "sol-word-gloss-v2",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 19,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    ledger.succeed(claim, "a".repeat(64), 3);

    expect(() =>
      ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2", priority: 19 },
        4,
      ),
    ).toThrow("Approved legacy Sol translation material does not match");
    expect(ledger.status().total).toBe(1);
    const database = new Database(path, { readonly: true });
    try {
      expect(
        database
          .prepare(
            `SELECT
               (SELECT COUNT(*) FROM canonical_translation_binding) AS bindings,
               (SELECT COUNT(*) FROM publication_derivation) AS derivations`,
          )
          .get(),
      ).toEqual({ bindings: 0, derivations: 0 });
    } finally {
      database.close();
    }
  });

  test("rejects malformed legacy Sol input before writing provenance", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-legacy-malformed-input-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const input = { poemId: "poem-1" };
    const source = ledger.seed(
      {
        implementationVersion: "sol-word-gloss-v2",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 19,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    ledger.succeed(claim, "a".repeat(64), 3);

    expect(() =>
      ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2", priority: 19 },
        4,
      ),
    ).toThrow("Approved legacy Sol translation material does not match");
    const database = new Database(path, { readonly: true });
    try {
      expect(
        database
          .prepare(
            `SELECT
               (SELECT COUNT(*) FROM canonical_translation_binding) AS bindings,
               (SELECT COUNT(*) FROM publication_derivation) AS derivations`,
          )
          .get(),
      ).toEqual({ bindings: 0, derivations: 0 });
    } finally {
      database.close();
    }
  });

  test("rejects conflicting legacy attachment without changing exact lineage", () => {
    const ledger = open();
    const input = LEGACY_SOL_INPUT;
    const source = ledger.seed(
      {
        implementationVersion: "sol-word-gloss-v2",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 19,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    ledger.succeed(claim, "a".repeat(64), 3);
    const first = ledger.attachApprovedBindingAndSeedPublication(
      source.workKey,
      "a".repeat(64),
      canonicalBinding(),
      { implementationVersion: "direct-publication-v2", priority: 19 },
      4,
    );
    const conflictingIdentity = {
      ...canonicalBinding(),
      sourceRevisionId: "f".repeat(64),
    };
    const {
      admissionEvidence: _admissionEvidence,
      bindingId: _bindingId,
      ...identity
    } = conflictingIdentity;
    const conflict = {
      ...conflictingIdentity,
      bindingId: hash("sha256", canonicalPoemBindingIdBody(identity), "hex"),
    };

    expect(() =>
      ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        conflict,
        { implementationVersion: "direct-publication-v2", priority: 19 },
        5,
      ),
    ).toThrow("Approved publication attachment conflicts");
    expect(ledger.status().total).toBe(2);
    expect(ledger.get(first.publicationWorkKey)?.state).toBe("pending");
  });

  test("rolls back the entire legacy attachment when derivation insertion fails", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-legacy-attachment-rollback-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const input = LEGACY_SOL_INPUT;
    const source = ledger.seed(
      {
        implementationVersion: "sol-word-gloss-v2",
        input,
        inputHash: inputHash(input),
        kind: "poem-enrichment-sol",
        priority: 19,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    ledger.succeed(claim, "a".repeat(64), 3);
    const database = new Database(path);
    database.exec(`
      CREATE TRIGGER reject_legacy_derivation
      BEFORE INSERT ON publication_derivation
      BEGIN
        SELECT RAISE(ABORT, 'REJECT_TEST_DERIVATION');
      END;
    `);
    database.close();

    expect(() =>
      ledger.attachApprovedBindingAndSeedPublication(
        source.workKey,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2", priority: 19 },
        4,
      ),
    ).toThrow("REJECT_TEST_DERIVATION");
    expect(ledger.status().total).toBe(1);
    expect(ledger.get(source.workKey)).toMatchObject({
      outputArtifactHash: "a".repeat(64),
      state: "succeeded",
    });
    const readonlyDatabase = new Database(path, { readonly: true });
    try {
      expect(
        readonlyDatabase
          .prepare(
            `SELECT COUNT(*) AS count FROM canonical_translation_binding`,
          )
          .get(),
      ).toEqual({ count: 0 });
      expect(
        readonlyDatabase
          .prepare(`SELECT COUNT(*) AS count FROM publication_derivation`)
          .get(),
      ).toEqual({ count: 0 });
    } finally {
      readonlyDatabase.close();
    }
  });

  test("atomically completes an approved Sol translation and seeds its direct publication", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-direct-publication-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const source = ledger.seed(
      {
        ...definition({ poemId: "poem-1" }),
        implementationVersion: "sol-5.6",
        kind: "poem-enrichment-sol",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    const result = ledger.completeApprovedAndSeedPublication(
      claim,
      "a".repeat(64),
      canonicalBinding(),
      { implementationVersion: "direct-publication-v2", priority: 19 },
      3,
    );

    expect(result.translationWorkKey).toBe(source.workKey);
    expect(ledger.get(source.workKey)).toMatchObject({
      outputArtifactHash: "a".repeat(64),
      state: "succeeded",
    });
    expect(ledger.get(result.publicationWorkKey)).toMatchObject({
      input: {
        binding: canonicalBinding(),
        jobType: "bound-translation",
        source: { artifactHash: "a".repeat(64), workKey: source.workKey },
      },
      kind: "corpus-publication-enrichment-v2",
      priority: 19,
      state: "pending",
    });
    const database = new Database(path, { readonly: true });
    try {
      expect(
        database
          .prepare(
            `SELECT translation_work_key, binding_id, poem_id,
                    source_revision_id, line_nfc_hash, prompt_material_hash
             FROM canonical_translation_binding`,
          )
          .get(),
      ).toEqual({
        binding_id: canonicalBinding().bindingId,
        line_nfc_hash: canonicalBinding().lineNfcHash,
        poem_id: "poem-1",
        prompt_material_hash: canonicalBinding().promptMaterialHash,
        source_revision_id: "e".repeat(64),
        translation_work_key: source.workKey,
      });
      expect(
        database.prepare(`SELECT * FROM publication_derivation`).get(),
      ).toMatchObject({
        approved_artifact_hash: "a".repeat(64),
        binding_id: canonicalBinding().bindingId,
        publication_work_key: result.publicationWorkKey,
        translation_work_key: source.workKey,
      });
    } finally {
      database.close();
    }
    const publicationClaim = ledger.claim("publication-worker", 4, 100, [
      "corpus-publication-enrichment-v2",
    ]);
    if (!publicationClaim) throw new Error("Expected direct publication claim");
    expect(
      ledger.confirmDirectPublication(
        publicationClaim,
        source.workKey,
        "a".repeat(64),
        "f".repeat(64),
        5,
      ),
    ).toBe("confirmed");
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    expect(ledger.get(result.publicationWorkKey)).toMatchObject({
      outputArtifactHash: "f".repeat(64),
      state: "succeeded",
    });
    expect(
      ledger.confirmDirectPublication(
        publicationClaim,
        source.workKey,
        "a".repeat(64),
        "f".repeat(64),
        6,
      ),
    ).toBe("already_confirmed");
  });

  test("rolls back binding and publication when the translation lease is stale", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-direct-publication-stale-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const source = ledger.seed(
      {
        ...definition({ poemId: "poem-1" }),
        implementationVersion: "sol-5.6",
        kind: "poem-enrichment-sol",
      },
      1,
    );
    const claim = ledger.claim("sol-worker", 2, 10, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected Sol claim");
    expect(() =>
      ledger.completeApprovedAndSeedPublication(
        claim,
        "a".repeat(64),
        canonicalBinding(),
        { implementationVersion: "direct-publication-v2" },
        12,
      ),
    ).toThrow(LostLeaseError);
    expect(ledger.get(source.workKey)).toMatchObject({
      outputArtifactHash: null,
      state: "running",
    });
    expect(ledger.status().total).toBe(1);
    const database = new Database(path, { readonly: true });
    try {
      expect(
        database
          .prepare(
            `SELECT COUNT(*) AS count FROM canonical_translation_binding`,
          )
          .get(),
      ).toEqual({ count: 0 });
      expect(
        database
          .prepare(`SELECT COUNT(*) AS count FROM publication_derivation`)
          .get(),
      ).toEqual({ count: 0 });
    } finally {
      database.close();
    }
  });

  test("compare-and-swaps scheduler state without losing a concurrent writer", () => {
    const ledger = open();
    const first = JSON.stringify({ selectedConcurrency: 2 });
    const second = JSON.stringify({ selectedConcurrency: 4 });
    const firstDigest = hash("sha256", first, "hex");
    const secondDigest = hash("sha256", second, "hex");
    expect(
      ledger.saveSchedulerState("provider:sol", first, firstDigest, null, 1),
    ).toBe(true);
    expect(
      ledger.saveSchedulerState(
        "provider:sol",
        second,
        secondDigest,
        "f".repeat(64),
        2,
      ),
    ).toBe(false);
    expect(ledger.loadSchedulerState("provider:sol")).toEqual({
      digest: firstDigest,
      serialized: first,
    });
    expect(
      ledger.saveSchedulerState(
        "provider:sol",
        second,
        secondDigest,
        firstDigest,
        3,
      ),
    ).toBe(true);
  });

  test("keeps canonical poem ownership immutable across work versions", () => {
    const ledger = open();
    expect(ledger.seed(poemDefinition("crawler@1"), 1).inserted).toBe(true);
    expect(ledger.seed(poemDefinition("crawler@2"), 2).inserted).toBe(true);
    expect(() =>
      ledger.seed(
        poemDefinition("crawler@3", "https://source.invalid/writers/poet-two"),
        3,
      ),
    ).toThrow(PoemIdentityConflictError);
    expect(ledger.status().total).toBe(2);
  });

  test("commits nonconflicting poems from a conflicting batch atomically", () => {
    const ledger = open();
    ledger.seed(poemDefinition(), 1);
    const result = ledger.seedPoems(
      [
        poemDefinition("crawler@2", "https://source.invalid/writers/poet-two"),
        poemDefinition(
          "crawler@2",
          "https://source.invalid/writers/poet-two",
          "https://source.invalid/works/43",
        ),
      ],
      2,
    );
    expect(result).toMatchObject({
      conflicts: [
        {
          poemHref: "https://source.invalid/works/42",
        },
      ],
      results: [{ inserted: true }],
    });
    expect(ledger.status().total).toBe(2);
  });

  test("retires poem work without colliding with immutable identity", () => {
    const ledger = open();
    const old = ledger.seed(poemDefinition("crawler@1"), 1).workKey;
    expect(
      ledger.retireIncompatible(
        ["source_poem_detail"],
        {
          implementationVersion: "crawler@2",
          schemaVersion: "source-projection-v1",
        },
        2,
      ),
    ).toBe(1);
    expect(ledger.get(old)).toMatchObject({
      lastErrorCode: "WORKER_VERSION_RETIRED",
      state: "dead_letter",
    });
    expect(
      ledger.claim("current", 2, 100, ["source_poem_detail"], {
        implementationVersion: "crawler@2",
        schemaVersion: "source-projection-v1",
      })?.work.input,
    ).toEqual(poemDefinition().input);
  });

  test("protects active and young Sol attempts from retention", () => {
    const ledger = open();
    const oldId = "11111111-1111-4111-8111-111111111111";
    const activeId = "22222222-2222-4222-8222-222222222222";
    for (const [authorId, attemptId] of [
      ["old", oldId],
      ["active", activeId],
    ] as const) {
      ledger.seed(
        {
          ...definition({ authorId }),
          implementationVersion: "sol-v1",
          kind: "poem-enrichment-sol",
        },
        1,
      );
      const claim = ledger.claim("owner", 1, 100, ["poem-enrichment-sol"]);
      if (!claim) throw new Error("Expected claim");
      ledger.checkpoint(
        claim,
        {
          artifactHash: null,
          kind: "phase",
          payload: {
            attemptId,
            ...(authorId === "old" ? { duplicateAttemptId: attemptId } : {}),
          },
        },
        2,
      );
      if (authorId === "old") ledger.succeed(claim, "a".repeat(64), 3);
      else ledger.retry(claim, "WAIT", 1_000, 3);
    }
    const before = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      50,
      100,
    );
    expect(before.eligibleAttemptIds.size).toBe(0);
    expect(before.protectedAttemptIds).toEqual(new Set([activeId, oldId]));
    expect(before.nextEligibleAt).toBe(103);
    const after = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      104,
      100,
    );
    expect(after.eligibleAttemptIds).toEqual(new Set([oldId]));
    expect(after.protectedAttemptIds).toEqual(new Set([activeId]));
    expect(after.withAttemptReservation(oldId, () => undefined)).toBe(true);
    expect(after.withAttemptReservation(activeId, () => undefined)).toBe(false);
    const bounded = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      104,
      100,
      [oldId],
    );
    expect(bounded.eligibleAttemptIds).toEqual(new Set([oldId]));
    expect(bounded.protectedAttemptIds).toEqual(new Set());
  });

  test("protects ambiguous paid attempts until reconciliation is proven", () => {
    const ledger = open();
    const attemptId = "55555555-5555-4555-8555-555555555555";
    const seeded = ledger.seed(
      {
        ...definition({ authorId: "ambiguous-paid" }),
        implementationVersion: "sol-v1",
        kind: "poem-enrichment-sol",
      },
      1,
    );
    const claim = ledger.claim("owner", 1, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected claim");
    ledger.checkpoint(
      claim,
      { artifactHash: null, kind: "phase", payload: { attemptId } },
      2,
    );
    ledger.deadLetter(claim, "CODEX_OPERATION_OUTCOME_UNKNOWN", 3);
    const operationKey = "f".repeat(64);
    ledger.recordPaidOperationUnknown(
      seeded.workKey,
      operationKey,
      attemptId,
      1_000,
      4,
    );

    const ambiguous = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      104,
      100,
    );
    expect(ambiguous.eligibleAttemptIds).not.toContain(attemptId);
    expect(ambiguous.protectedAttemptIds).toContain(attemptId);
    expect(ambiguous.withAttemptReservation(attemptId, () => undefined)).toBe(
      false,
    );

    expect(ledger.recordPaidOperationReconciled(operationKey, 105)).toBe(true);
    const reconciled = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      105,
      100,
    );
    expect(reconciled.eligibleAttemptIds).toContain(attemptId);
    expect(reconciled.protectedAttemptIds).not.toContain(attemptId);
    expect(reconciled.withAttemptReservation(attemptId, () => undefined)).toBe(
      true,
    );
  });

  test("revalidates terminal attempts against current ledger state", () => {
    const ledger = open();
    const attemptId = "33333333-3333-4333-8333-333333333333";
    ledger.seed(
      {
        ...definition({ authorId: "requeued" }),
        implementationVersion: "sol-v1",
        kind: "poem-enrichment-sol",
      },
      1,
    );
    const claim = ledger.claim("owner", 1, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected claim");
    ledger.checkpoint(
      claim,
      { artifactHash: null, kind: "phase", payload: { attemptId } },
      2,
    );
    ledger.deadLetter(claim, "TEST_TERMINAL", 3);
    const eligibility = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      104,
      100,
    );
    expect(eligibility.withAttemptReservation(attemptId, () => undefined)).toBe(
      true,
    );
    expect(
      ledger.requeueDeadLetters(
        ["poem-enrichment-sol"],
        ["TEST_TERMINAL"],
        105,
      ),
    ).toBe(1);
    expect(eligibility.withAttemptReservation(attemptId, () => undefined)).toBe(
      false,
    );
  });

  test("holds a ledger write fence throughout a retention reservation", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-ledger-fence-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const attemptId = "44444444-4444-4444-8444-444444444444";
    ledger.seed(
      {
        ...definition({ authorId: "fenced" }),
        implementationVersion: "sol-v1",
        kind: "poem-enrichment-sol",
      },
      1,
    );
    const claim = ledger.claim("owner", 1, 100, ["poem-enrichment-sol"]);
    if (!claim) throw new Error("Expected claim");
    ledger.checkpoint(
      claim,
      { artifactHash: null, kind: "phase", payload: { attemptId } },
      2,
    );
    ledger.deadLetter(claim, "TEST_TERMINAL", 3);
    const retention = ledger.attemptRetentionEligibility(
      "poem-enrichment-sol",
      "sol-v1",
      104,
      100,
    );
    const competitor = new Database(path);
    competitor.pragma("busy_timeout = 0");
    try {
      expect(
        retention.withAttemptReservation(attemptId, () => {
          expect(() =>
            competitor
              .prepare(
                `UPDATE work_item SET state = 'pending', available_at = 105,
                   last_error_code = NULL, updated_at = 105
                 WHERE state = 'dead_letter'`,
              )
              .run(),
          ).toThrow(/locked/iu);
        }),
      ).toBe(true);
    } finally {
      competitor.close();
    }
    expect(
      ledger.requeueDeadLetters(
        ["poem-enrichment-sol"],
        ["TEST_TERMINAL"],
        105,
      ),
    ).toBe(1);
  });

  test("reports lane availability with exact version filters", () => {
    const ledger = open();
    ledger.seed(definition({ authorId: "current" }), 10);
    ledger.seed(
      {
        ...definition({ authorId: "future" }),
        implementationVersion: "crawler@2",
      },
      20,
    );
    const current = ledger.claim("owner", 10, 100, ["author-manifest"], {
      implementationVersion: "crawler@1",
    });
    if (!current) throw new Error("Expected current claim");
    ledger.retry(current, "WAIT", 50, 11);
    expect(
      ledger.availability(["author-manifest"], 20, {
        implementationVersion: "crawler@1",
      }),
    ).toEqual({ earliestAvailableAt: 50, ready: 0 });
    expect(
      ledger.availability(["author-manifest"], 20, {
        implementationVersion: "crawler@2",
      }),
    ).toEqual({ earliestAvailableAt: 20, ready: 1 });
  });

  test("reports bounded supervisor-ready progress and wake metadata", () => {
    const ledger = open();
    const success = ledger.seed(definition({ authorId: "success" }), 1);
    const successClaim = ledger.claim("owner", 1, 100);
    if (!successClaim) throw new Error("Expected claim");
    ledger.succeed(successClaim, "a".repeat(64), 2);

    const retry = ledger.seed(definition({ authorId: "retry" }), 3);
    const retryClaim = ledger.claim("owner", 3, 100);
    if (!retryClaim) throw new Error("Expected claim");
    ledger.retry(retryClaim, "SOURCE_TIMEOUT", 100, 4);

    const pending = ledger.seed(
      { ...definition({ poemId: "pending" }), kind: "poem-detail" },
      5,
    );
    const status = ledger.status(5);
    expect([success.workKey, retry.workKey, pending.workKey]).toHaveLength(3);
    expect(status).toMatchObject({
      affectedByErrorCode: [{ code: "SOURCE_TIMEOUT", count: 1 }],
      affectedByKindAndErrorCode: [
        { code: "SOURCE_TIMEOUT", count: 1, kind: "author-manifest" },
      ],
      earliestWorkAvailableAt: 5,
      failureEventsByCode: [{ code: "SOURCE_TIMEOUT", count: 1 }],
      failureEventWindow: 10_000,
      lastFailureAt: 4,
      lastSuccessAt: 2,
      ready: 1,
      total: 3,
      truncated: false,
    });
    expect(status.kindProgress).toEqual([
      expect.objectContaining({
        completed: 1,
        kind: "author-manifest",
        lastSuccessAt: 2,
        terminal: 1,
        total: 2,
      }),
      expect.objectContaining({
        completed: 0,
        kind: "poem-detail",
        lastSuccessAt: null,
        terminal: 0,
        total: 1,
      }),
    ]);
    expect(status.origins).toEqual([]);
  });

  test("derives ready status from state totals minus future availability", () => {
    const ledger = open();
    ledger.seed(definition({ authorId: "pending" }), 5);

    ledger.seed(definition({ authorId: "retry" }), 1);
    const retry = ledger.claim("retry-owner", 1, 100);
    if (!retry) throw new Error("Expected retry claim");
    ledger.retry(retry, "SOURCE_TIMEOUT", 10, 2);

    ledger.seed(definition({ authorId: "quota" }), 1);
    const quota = ledger.claim("quota-owner", 1, 100);
    if (!quota) throw new Error("Expected quota claim");
    ledger.quotaWait(quota, "CODEX_QUOTA", 15, 2);

    expect(ledger.status(9).ready).toBe(1);
    expect(ledger.status(10).ready).toBe(2);
    expect(ledger.status(15).ready).toBe(3);
  });

  test("rebuilds materialized status counters transactionally and idempotently", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-ledger-status-repair-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    ledger.seed(definition({ authorId: "success" }), 1);
    const claim = ledger.claim("owner", 1, 100);
    if (!claim) throw new Error("Expected claim");
    ledger.succeed(claim, "a".repeat(64), 2);
    ledger.seed(definition({ authorId: "retry" }), 3);
    const retryClaim = ledger.claim("owner", 3, 100);
    if (!retryClaim) throw new Error("Expected retry claim");
    ledger.retry(retryClaim, "SOURCE_TIMEOUT", 10, 4);
    ledger.seed(
      { ...definition({ authorId: "pending" }), kind: "poem-detail" },
      5,
    );
    const expected = ledger.status(5);

    const raw = new Database(path);
    raw.exec(`
      DELETE FROM ledger_state_count;
      DELETE FROM ledger_kind_state_count;
      DELETE FROM ledger_profile_state_count;
      DELETE FROM ledger_profile_availability_count;
      DELETE FROM ledger_error_count;
      DELETE FROM ledger_kind_error_count;
      DELETE FROM ledger_profile_error_count;
      DELETE FROM ledger_kind_success_clock;
      DELETE FROM ledger_profile_success_clock;
      UPDATE ledger_status_clock
        SET last_success_at = NULL, last_failure_at = NULL;
    `);
    raw.close();
    expect(ledger.status(5)).toMatchObject({ ready: 0, total: 0 });
    expect(
      ledger.countErrors("author-manifest", ["SOURCE_TIMEOUT"], {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      }),
    ).toBe(0);

    ledger.rebuildStatusCounters();
    expect(ledger.status(5)).toEqual(expected);
    expect(
      ledger.countErrors("author-manifest", ["SOURCE_TIMEOUT"], {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      }),
    ).toBe(1);
    ledger.rebuildStatusCounters();
    expect(ledger.status(5)).toEqual(expected);
  });

  test("reports affected errors independently for each provider work kind", () => {
    const ledger = open();
    for (const [kind, code, now] of [
      ["poem-enrichment-claude-opus-5", "CODEX_OUTPUT_INVALID", 1],
      ["poem-enrichment-sol", "CODEX_OPERATION_OUTCOME_UNKNOWN", 2],
    ] as const) {
      ledger.seed({ ...definition({ kind }), kind }, now);
      const claim = ledger.claim(`owner-${kind}`, now, 100, [kind]);
      if (!claim) throw new Error(`Expected claim for ${kind}`);
      ledger.retry(claim, code, 100, now + 1);
    }

    expect(ledger.status(10).affectedByKindAndErrorCode).toEqual([
      {
        code: "CODEX_OUTPUT_INVALID",
        count: 1,
        kind: "poem-enrichment-claude-opus-5",
      },
      {
        code: "CODEX_OPERATION_OUTCOME_UNKNOWN",
        count: 1,
        kind: "poem-enrichment-sol",
      },
    ]);
  });

  test("counts live errors only for the exact worker profile", () => {
    const ledger = open();
    for (const [authorId, implementationVersion] of [
      ["current", "crawler@1"],
      ["obsolete", "crawler@obsolete"],
    ] as const) {
      const item = { ...definition({ authorId }), implementationVersion };
      ledger.seed(item, 1);
      const claim = ledger.claim("worker", 2, 100, [item.kind], {
        implementationVersion,
        schemaVersion: item.schemaVersion,
      });
      if (!claim) throw new Error("Expected profile claim");
      ledger.operatorRelease(claim, "CODEX_OPERATION_OUTCOME_UNKNOWN", 3);
    }

    expect(
      ledger.countErrors(
        "author-manifest",
        ["CODEX_OPERATION_OUTCOME_UNKNOWN"],
        {
          implementationVersion: "crawler@1",
          schemaVersion: "author-manifest@1",
        },
      ),
    ).toBe(1);
    expect(ledger.status().affectedByErrorCode).toContainEqual({
      code: "CODEX_OPERATION_OUTCOME_UNKNOWN",
      count: 2,
    });
  });

  test("reports active progress without obsolete profile pollution", () => {
    const ledger = open();
    const currentSucceeded = definition({ authorId: "current-succeeded" });
    const currentPending = definition({ authorId: "current-pending" });
    const obsolete = {
      ...definition({ authorId: "obsolete" }),
      implementationVersion: "crawler@obsolete",
    };
    for (const item of [currentSucceeded, currentPending, obsolete]) {
      ledger.seed(item, 1);
    }
    const succeededClaim = ledger.claim(
      "current-worker",
      2,
      100,
      [currentSucceeded.kind],
      {
        implementationVersion: currentSucceeded.implementationVersion,
        schemaVersion: currentSucceeded.schemaVersion,
      },
    );
    if (!succeededClaim) throw new Error("Expected current claim");
    ledger.succeed(succeededClaim, "a".repeat(64), 3);
    const obsoleteClaim = ledger.claim(
      "obsolete-worker",
      4,
      100,
      [obsolete.kind],
      {
        implementationVersion: obsolete.implementationVersion,
        schemaVersion: obsolete.schemaVersion,
      },
    );
    if (!obsoleteClaim) throw new Error("Expected obsolete claim");
    ledger.operatorRelease(obsoleteClaim, "CODEX_OPERATION_OUTCOME_UNKNOWN", 5);

    expect(
      ledger.profileProgress("author-manifest", {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      }),
    ).toMatchObject({
      byState: { pending: 1, succeeded: 1 },
      completed: 1,
      lastSuccessAt: 3,
      terminal: 1,
      total: 2,
    });
    expect(
      ledger
        .status()
        .kindProgress.find(({ kind }) => kind === "author-manifest"),
    ).toMatchObject({
      byState: { pending: 2, succeeded: 1 },
      total: 3,
    });
  });

  test("reuses a completed input across implementation versions", () => {
    const ledger = open();
    const input = { poemHref: "https://source.invalid/works/1" };
    const first: WorkDefinition = {
      implementationVersion: "v1",
      input,
      inputHash: inputHash(input),
      kind: "poem-detail",
      priority: 0,
      schemaVersion: "projection-v1",
    };
    const seeded = ledger.seed(first, 10);
    const claim = ledger.claim("owner", 10, 100, ["poem-detail"], {
      implementationVersion: "v1",
      schemaVersion: "projection-v1",
    });
    if (!claim) throw new Error("Expected claim");
    const artifactHash = "a".repeat(64);
    ledger.succeed(claim, artifactHash, 11);
    expect(
      ledger.successfulArtifactForInput(
        "poem-detail",
        "projection-v1",
        first.inputHash,
      ),
    ).toBe(artifactHash);
    expect(
      ledger.successfulArtifactForInput(
        "poem-detail",
        "projection-v2",
        first.inputHash,
      ),
    ).toBeNull();
    expect(
      ledger.successfulArtifactsForInputs("poem-detail", "projection-v1", [
        first.inputHash,
        first.inputHash,
        "f".repeat(64),
      ]),
    ).toEqual(new Map([[first.inputHash, artifactHash]]));
    expect(ledger.get(seeded.workKey)?.state).toBe("succeeded");
  });

  test("duplicate seeding converges on one immutable work item", () => {
    const ledger = open();
    const first = ledger.seed(definition(), 100);
    const second = ledger.seed(definition(), 200);

    expect(first.inserted).toBe(true);
    expect(second).toEqual({ inserted: false, workKey: first.workKey });
    expect(ledger.status().total).toBe(1);
    expect(ledger.eventCount(first.workKey)).toBe(1);
  });

  test("duplicate seeding may raise ready-work priority without changing identity", () => {
    const ledger = open();
    const first = ledger.seed(definition(), 100);
    const raised = { ...definition(), priority: 25 };

    expect(ledger.seed(raised, 200)).toEqual({
      inserted: false,
      workKey: first.workKey,
    });
    expect(ledger.get(first.workKey)?.priority).toBe(25);
    expect(ledger.eventCount(first.workKey)).toBe(1);
  });

  test("bulk seeding is atomic, ordered, and duplicate-safe", () => {
    const ledger = open();
    const definitions = Array.from({ length: 5_000 }, (_, index) =>
      definition({ authorId: String(index) }),
    );
    const first = ledger.seedMany(definitions, 100);
    const replay = ledger.seedMany(definitions, 200);

    expect(first).toHaveLength(definitions.length);
    expect(first.every(({ inserted }) => inserted)).toBe(true);
    expect(replay.every(({ inserted }) => !inserted)).toBe(true);
    expect(replay.map(({ workKey }) => workKey)).toEqual(
      first.map(({ workKey }) => workKey),
    );
    expect(ledger.status().total).toBe(definitions.length);
    expect(ledger.eventCount(first[0]!.workKey)).toBe(1);
  });

  test("bulk validation fails before inserting any work", () => {
    const ledger = open();
    expect(() =>
      ledger.seedMany([
        definition({ authorId: "valid" }),
        { ...definition({ authorId: "invalid" }), inputHash: "0".repeat(64) },
      ]),
    ).toThrow("inputHash");
    expect(ledger.status().total).toBe(0);
  });

  test("canonical object key order produces the same work identity", () => {
    const ledger = open();
    const one = definition({ author: { id: 1, name: "x" }, page: 2 });
    const two = definition({ page: 2, author: { name: "x", id: 1 } });
    expect(ledger.seed(one).workKey).toBe(ledger.seed(two).workKey);
    expect(ledger.status().total).toBe(1);
  });

  test("canonical object key order is independent of the host locale", () => {
    expect(canonicalJson({ ä: 2, z: 1 })).toBe('{"z":1,"ä":2}');
  });

  test("rejects input whose declared hash differs from its canonical value", () => {
    const ledger = open();
    expect(() =>
      ledger.seed({ ...definition(), inputHash: "0".repeat(64) }),
    ).toThrow("inputHash");
  });

  test("rejects values outside the deterministic JSON domain", () => {
    expect(() => inputHash({ broken: NaN })).toThrow("non-finite");
    expect(() => inputHash({ broken: undefined })).toThrow("rejects undefined");
    expect(() => inputHash({ broken: new Date(0) })).toThrow("plain objects");
  });

  test("only one database connection wins a racing claim", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-race-")),
      "ledger.sqlite3",
    );
    const first = open(path);
    const second = open(path);
    first.seed(definition(), 100);

    expect(first.claim("one", 100, 1_000)).not.toBeNull();
    expect(second.claim("two", 100, 1_000)).toBeNull();
  });

  test("batch claims preserve per-item fencing across independent releases", () => {
    const ledger = open();
    const seeded = Array.from({ length: 4 }, (_, index) =>
      ledger.seed(definition({ authorId: String(index) }), 100),
    );

    const claims = ledger.claimMany(
      "batch-owner",
      100,
      1_000,
      ["author-manifest"],
      3,
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
    );
    expect(claims).toHaveLength(3);
    expect(new Set(claims.map(({ leaseToken }) => leaseToken)).size).toBe(3);
    expect(claims.every(({ work }) => work.state === "running")).toBe(true);
    expect(claims.every(({ work }) => work.attemptCount === 1)).toBe(true);

    for (const claim of claims) {
      ledger.operatorRelease(claim, "EXTERNAL_PENDING", 200, 500);
      expect(ledger.get(claim.work.workKey)).toMatchObject({
        attemptCount: 0,
        availableAt: 500,
        lastErrorCode: "EXTERNAL_PENDING",
        state: "pending",
      });
      expect(ledger.eventCount(claim.work.workKey)).toBe(3);
    }
    expect(ledger.get(seeded[3]!.workKey)?.state).toBe("pending");
  });

  test("batch claims can target an exact bounded work-key cohort", () => {
    const ledger = open();
    const seeded = Array.from({ length: 4 }, (_, index) =>
      ledger.seed(definition({ authorId: String(index) }), 100 + index),
    );
    const target = seeded[3];
    if (!target) throw new Error("Expected exact claim target");

    const claims = ledger.claimMany(
      "exact-batch-owner",
      200,
      1_000,
      ["author-manifest"],
      1,
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
      [],
      [target.workKey],
    );

    expect(claims.map(({ work }) => work.workKey)).toEqual([target.workKey]);
    expect(ledger.get(seeded[0]!.workKey)?.state).toBe("pending");
  });

  test("batch renewal reports a stale member without losing valid renewals", () => {
    const ledger = open();
    ledger.seed(definition({ authorId: "one" }), 100);
    ledger.seed(definition({ authorId: "two" }), 100);
    const claims = ledger.claimMany(
      "batch-owner",
      100,
      1_000,
      ["author-manifest"],
      2,
    );
    const first = claims[0];
    if (!first) throw new Error("Expected first claim");
    ledger.retry(first, "FIRST_DONE", 300, 200);

    expect(ledger.renewMany(claims, 250, 1_000)).toEqual({
      renewed: [claims[1]!.work.workKey],
      stale: [first.work.workKey],
    });
    expect(ledger.get(claims[1]!.work.workKey)).toMatchObject({
      leaseExpiresAt: 1_250,
      state: "running",
    });
  });

  test("batch renewal never resurrects expired leases", () => {
    const ledger = open();
    ledger.seed(definition({ authorId: "one" }), 100);
    ledger.seed(definition({ authorId: "two" }), 100);
    const claims = ledger.claimMany(
      "batch-owner",
      100,
      100,
      ["author-manifest"],
      2,
    );

    expect(ledger.renewMany(claims, 200, 1_000)).toEqual({
      renewed: [],
      stale: claims.map(({ work }) => work.workKey),
    });
    expect(
      claims.every(
        ({ work }) => ledger.get(work.workKey)?.leaseExpiresAt === 200,
      ),
    ).toBe(true);
  });

  test("same-token fanout recovery renews after host suspension without defeating a replacement claim", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-ledger-suspend-")),
      "ledger.sqlite3",
    );
    const first = open(path);
    first.seed(definition({ authorId: "sleeping" }), 100);
    const sleeping = first.claim("fanout-owner", 100, 100, ["author-manifest"]);
    if (!sleeping) throw new Error("Expected sleeping claim");

    expect(first.renewMany([sleeping], 1_100, 1_000, true)).toEqual({
      renewed: [sleeping.work.workKey],
      stale: [],
    });
    expect(first.get(sleeping.work.workKey)).toMatchObject({
      leaseExpiresAt: 2_100,
      state: "running",
    });

    const second = open(path);
    first.operatorRelease(sleeping, "SUSPEND_RETRY", 1_101, 1_101);
    const replacement = second.claim("replacement-owner", 1_102, 1_000, [
      "author-manifest",
    ]);
    if (!replacement) throw new Error("Expected replacement claim");
    expect(first.renewMany([sleeping], 1_103, 1_000, true)).toEqual({
      renewed: [],
      stale: [sleeping.work.workKey],
    });
    expect(second.get(replacement.work.workKey)).toMatchObject({
      leaseOwner: "replacement-owner",
      leaseToken: replacement.leaseToken,
      state: "running",
    });
    second.close();
  });

  test("claims only the exact worker implementation and schema", () => {
    const ledger = open();
    ledger.seed(definition(), 100);
    expect(
      ledger.claim("new-worker", 100, 1_000, ["author-manifest"], {
        implementationVersion: "crawler@2",
        schemaVersion: "author-manifest@1",
      }),
    ).toBeNull();
    expect(ledger.get(ledger.seed(definition()).workKey)?.state).toBe(
      "pending",
    );
  });

  test("retires incompatible queued work atomically and idempotently", () => {
    const ledger = open();
    const old = ledger.seed(definition(), 100).workKey;
    const currentDefinition = {
      ...definition({ authorId: "496" }),
      implementationVersion: "crawler@2",
    };
    const current = ledger.seed(currentDefinition, 100).workKey;
    const requirements = {
      implementationVersion: "crawler@2",
      schemaVersion: "author-manifest@1",
    };
    expect(
      ledger.retireIncompatible(["author-manifest"], requirements, 101),
    ).toBe(1);
    expect(
      ledger.retireIncompatible(["author-manifest"], requirements, 102),
    ).toBe(0);
    expect(ledger.get(old)).toMatchObject({
      lastErrorCode: "WORKER_VERSION_RETIRED",
      state: "dead_letter",
    });
    expect(ledger.get(current)?.state).toBe("pending");
    expect(ledger.eventCount(old)).toBe(2);
  });

  test("retirement seeds an equivalent current-version replacement", () => {
    const ledger = open();
    const old = ledger.seed(definition(), 100).workKey;
    expect(
      ledger.retireIncompatible(
        ["author-manifest"],
        {
          implementationVersion: "crawler@2",
          schemaVersion: "author-manifest@2",
        },
        101,
      ),
    ).toBe(1);
    const replacement = ledger.claim(
      "current",
      101,
      1_000,
      ["author-manifest"],
      {
        implementationVersion: "crawler@2",
        schemaVersion: "author-manifest@2",
      },
    );
    expect(replacement?.work).toMatchObject({
      implementationVersion: "crawler@2",
      input: { authorId: "495" },
      schemaVersion: "author-manifest@2",
    });
    expect(replacement?.work.workKey).not.toBe(old);
  });

  test("an expired lease is recovered and the stale owner is fenced", () => {
    const ledger = open();
    const key = ledger.seed(definition(), 100).workKey;
    const stale = ledger.claim("crashed", 100, 10);
    expect(stale).not.toBeNull();
    expect(ledger.recoverExpired(110)).toBe(1);
    const current = ledger.claim("replacement", 110, 1_000);
    expect(current?.leaseEpoch).toBe(2);
    expect(() => ledger.succeed(stale!, "a".repeat(64), 111)).toThrow(
      LostLeaseError,
    );
    ledger.succeed(current!, "b".repeat(64), 112);
    expect(ledger.get(key)?.outputArtifactHash).toBe("b".repeat(64));
  });

  test("recovers a crashed process once and preserves its attempt identity", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-crash-recovery-")),
      "ledger.sqlite3",
    );
    const crashedLedger = open(path);
    const key = crashedLedger.seed(definition(), 100).workKey;
    const crashed = crashedLedger.claim("crashed-process", 100, 10);
    if (!crashed) throw new Error("Expected crashed claim");
    crashedLedger.close();
    LEDGERS.splice(LEDGERS.indexOf(crashedLedger), 1);

    const restarted = open(path);
    expect(restarted.recoverExpired(110)).toBe(1);
    expect(restarted.recoverExpired(110)).toBe(0);
    expect(restarted.get(key)).toMatchObject({
      attemptCount: 1,
      lastErrorCode: "LEASE_EXPIRED",
      state: "pending",
    });
    const reader = new Database(path, { readonly: true });
    try {
      expect(
        reader
          .prepare(
            `SELECT attempt_id, lease_epoch FROM work_event
             WHERE work_key = ? AND event_type = 'lease_expired'`,
          )
          .get(key),
      ).toEqual({ attempt_id: crashed.attemptId, lease_epoch: 1 });
    } finally {
      reader.close();
    }
    const replacement = restarted.claim("replacement", 110, 1_000);
    expect(replacement?.leaseEpoch).toBe(2);
  });

  test("rolls back lease recovery when its audit event cannot commit", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-recovery-atomic-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const key = ledger.seed(definition(), 1).workKey;
    ledger.claim("crashed", 1, 5);
    const database = new Database(path);
    database.exec(`CREATE TRIGGER reject_expiry_event
      BEFORE INSERT ON work_event WHEN NEW.event_type = 'lease_expired'
      BEGIN SELECT RAISE(ABORT, 'synthetic expiry audit failure'); END;`);
    database.close();

    expect(() => ledger.recoverExpired(6)).toThrow(
      "synthetic expiry audit failure",
    );
    expect(ledger.get(key)).toMatchObject({
      leaseOwner: "crashed",
      state: "running",
    });
  });

  test("checkpoints are append-only and require the current lease", () => {
    const ledger = open();
    const key = ledger.seed(definition(), 10).workKey;
    const claim = ledger.claim("worker", 10, 10);
    ledger.checkpoint(
      claim!,
      { artifactHash: null, kind: "cursor", payload: { cursor: "a" } },
      11,
    );
    ledger.checkpoint(
      claim!,
      { artifactHash: null, kind: "cursor", payload: { cursor: "b" } },
      12,
    );
    expect(ledger.checkpointCount(key)).toBe(2);
    expect(ledger.latestCheckpoint(key, "cursor")).toMatchObject({
      artifactHash: null,
      attemptId: claim?.attemptId,
      createdAt: 12,
      kind: "cursor",
      leaseEpoch: claim?.leaseEpoch,
      payload: { cursor: "b" },
      sequence: 2,
      workKey: key,
    });
    expect(ledger.latestCheckpoint(key, "missing")).toBeNull();
    ledger.recoverExpired(20);
    expect(() =>
      ledger.checkpoint(
        claim!,
        { artifactHash: null, kind: "cursor", payload: {} },
        21,
      ),
    ).toThrow(LostLeaseError);
    expect(ledger.checkpointCount(key)).toBe(2);
    expect(ledger.referencedArtifactHashes()).toEqual([]);
  });

  test("enumerates unique artifacts referenced by outputs and checkpoints", () => {
    const ledger = open();
    const claim =
      ledger.claim("worker", 10, 100) ??
      (() => {
        ledger.seed(definition(), 10);
        return ledger.claim("worker", 10, 100)!;
      })();
    const checkpointHash = "a".repeat(64);
    const outputHash = "b".repeat(64);
    ledger.checkpoint(
      claim,
      { artifactHash: checkpointHash, kind: "phase", payload: {} },
      11,
    );
    ledger.succeed(claim, outputHash, 12);
    expect(ledger.referencedArtifactHashes()).toEqual([
      checkpointHash,
      outputHash,
    ]);
  });

  test("retry and quota waits preserve work identity and availability", () => {
    const ledger = open();
    const key = ledger.seed(definition(), 1).workKey;
    const first = ledger.claim("one", 1, 100)!;
    ledger.retry(first, "NETWORK_TIMEOUT", 50, 2);
    expect(ledger.claim("early", 49, 100)).toBeNull();
    const second = ledger.claim("two", 50, 100)!;
    ledger.quotaWait(second, "CODEX_QUOTA", 500, 51);
    expect(ledger.claim("early", 499, 100)).toBeNull();
    const third = ledger.claim("three", 500, 100)!;
    expect(third.work.workKey).toBe(key);
    expect(third.leaseEpoch).toBe(3);
  });

  test("import is an artifact-fenced idempotency boundary", () => {
    const ledger = open();
    const key = ledger.seed(definition(), 1).workKey;
    const claim = ledger.claim("one", 1, 100)!;
    const artifactHash = "c".repeat(64);
    ledger.succeed(claim, artifactHash, 2);
    expect(() => ledger.markImported(key, "d".repeat(64), 3)).toThrow();
    expect(ledger.markImported(key, artifactHash, 3)).toBe("imported");
    expect(ledger.get(key)?.state).toBe("imported");
    const events = ledger.eventCount(key);
    expect(ledger.markImported(key, artifactHash, 4)).toBe("already_imported");
    expect(ledger.eventCount(key)).toBe(events);
    expect(() => ledger.markImported(key, "d".repeat(64), 5)).toThrow();
  });
});

describe("persistent origin gate", () => {
  test("enforces one active request and completion-to-start spacing", () => {
    const ledger = open();
    const first = ledger.claimOrigin("https://source.invalid", 1_000, 10_000);
    expect(first.state).toBe("claimed");
    expect(
      ledger.claimOrigin("https://source.invalid", 1_001, 10_000).state,
    ).toBe("busy");
    if (first.state !== "claimed") throw new Error("expected lease");
    ledger.completeOrigin(first.lease, 2_000, 3_000);
    expect(ledger.claimOrigin("https://source.invalid", 4_999, 10_000)).toEqual(
      {
        retryAt: 5_000,
        state: "waiting",
      },
    );
    expect(
      ledger.claimOrigin("https://source.invalid", 5_000, 10_000).state,
    ).toBe("claimed");
  });

  test("expired origin owner is fenced after recovery", () => {
    const ledger = open();
    const stale = ledger.claimOrigin("https://source.invalid", 10, 10);
    expect(stale.state).toBe("claimed");
    ledger.recoverExpired(20);
    const current = ledger.claimOrigin("https://source.invalid", 20, 10);
    expect(current.state).toBe("claimed");
    if (stale.state !== "claimed") throw new Error("expected stale lease");
    expect(() => ledger.completeOrigin(stale.lease, 21, 3_000)).toThrow(
      LostLeaseError,
    );
  });

  test("renews an origin lease and fences expiry", () => {
    const ledger = open();
    const claimed = ledger.claimOrigin("https://source.invalid", 10, 10);
    if (claimed.state !== "claimed") throw new Error("expected lease");
    ledger.renewOrigin(claimed.lease, 19, 10);
    expect(ledger.claimOrigin("https://source.invalid", 20, 10).state).toBe(
      "busy",
    );
    expect(() => ledger.renewOrigin(claimed.lease, 29, 10)).toThrow(
      LostLeaseError,
    );
    expect(() => ledger.completeOrigin(claimed.lease, 29, 0)).toThrow(
      LostLeaseError,
    );
  });

  test("persists cooldowns, opens a circuit, and resets after success", () => {
    const ledger = open();
    const origin = "https://source.invalid";
    let now = 1_000;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const claimed = ledger.claimOrigin(origin, now, 10_000);
      if (claimed.state !== "claimed") throw new Error("expected lease");
      const failed = ledger.failOrigin(claimed.lease, now + 100, 3_000, {
        circuitBreakerAfter: 3,
        circuitBreakerCooldownMs: 60_000,
        retryAt: now + 100,
      });
      expect(failed.consecutiveFailures).toBe(attempt);
      expect(
        ledger.claimOrigin(origin, failed.nextAllowedAt - 1, 10_000),
      ).toEqual({
        retryAt: failed.nextAllowedAt,
        state: "waiting",
      });
      now = failed.nextAllowedAt;
    }
    const recovered = ledger.claimOrigin(origin, now, 10_000);
    if (recovered.state !== "claimed") throw new Error("expected lease");
    ledger.completeOrigin(recovered.lease, now + 100, 3_000);
    expect(ledger.claimOrigin(origin, now + 3_100, 10_000).state).toBe(
      "claimed",
    );
  });

  test("automatically retries a human-required stop after its persisted cooldown", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-origin-restart-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const origin = "https://source.invalid";
    const claimed = ledger.claimOrigin(origin, 1_000, 10_000);
    if (claimed.state !== "claimed") throw new Error("expected lease");
    ledger.failOrigin(claimed.lease, 2_000, 3_000, {
      circuitBreakerAfter: 3,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
      stopReason: "SOURCE_HUMAN_REQUIRED",
    });
    ledger.close();
    LEDGERS.splice(LEDGERS.indexOf(ledger), 1);
    const restarted = open(path);
    expect(restarted.claimOrigin(origin, 4_999, 10_000)).toEqual({
      retryAt: 5_000,
      state: "waiting",
    });
    expect(restarted.claimOrigin(origin, 5_000, 10_000).state).toBe("claimed");
  });

  test("keeps non-human origin stops fail-closed after their cooldown", () => {
    const ledger = open();
    const origin = "https://source.invalid";
    const claimed = ledger.claimOrigin(origin, 1_000, 10_000);
    if (claimed.state !== "claimed") throw new Error("expected lease");
    ledger.failOrigin(claimed.lease, 2_000, 3_000, {
      circuitBreakerAfter: 3,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
      stopReason: "COLLECTOR_HOTFIX_PENDING",
    });
    expect(ledger.claimOrigin(origin, 1_000_000, 10_000)).toEqual({
      reason: "COLLECTOR_HOTFIX_PENDING",
      state: "stopped",
    });
  });

  test("admits one origin lease when concurrent ledgers retry an elapsed human stop", () => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-origin-retry-")),
      "ledger.sqlite3",
    );
    const first = open(path);
    const second = open(path);
    const origin = "https://source.invalid";
    const initial = first.claimOrigin(origin, 1_000, 10_000);
    if (initial.state !== "claimed") throw new Error("expected lease");
    first.failOrigin(initial.lease, 2_000, 0, {
      circuitBreakerAfter: 3,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
      stopReason: "SOURCE_HUMAN_REQUIRED",
    });

    const boundaryClaims = [
      first.claimOrigin(origin, 5_000, 10_000),
      second.claimOrigin(origin, 5_000, 10_000),
    ];
    expect(
      boundaryClaims.filter(({ state }) => state === "claimed"),
    ).toHaveLength(1);
    expect(boundaryClaims.filter(({ state }) => state === "busy")).toHaveLength(
      1,
    );
  });

  test("guards work until a human-required cooldown elapses, then claims it", () => {
    const ledger = open();
    const origin = "https://source.invalid";
    ledger.seed(definition(), 1);
    const initial = ledger.claimOrigin(origin, 1_000, 10_000);
    if (initial.state !== "claimed") throw new Error("expected lease");
    ledger.failOrigin(initial.lease, 2_000, 0, {
      circuitBreakerAfter: 3,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
      stopReason: "SOURCE_HUMAN_REQUIRED",
    });

    expect(
      ledger.claimUnlessOriginStopped(origin, "worker", 4_999, 10_000),
    ).toEqual({
      reason: "SOURCE_HUMAN_REQUIRED",
      retryAt: 5_000,
      state: "waiting",
    });
    expect(
      ledger.claimUnlessOriginStopped(origin, "worker", 5_000, 10_000).state,
    ).toBe("claimed");
  });

  test("explicit recovery clears a cooldown without touching an active origin", () => {
    const ledger = open();
    const origin = "https://source.invalid";
    const claimed = ledger.claimOrigin(origin, 1_000, 10_000);
    if (claimed.state !== "claimed") throw new Error("expected lease");
    ledger.failOrigin(claimed.lease, 2_000, 3_000, {
      circuitBreakerAfter: 1,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
    });
    expect(ledger.claimOrigin(origin, 3_000, 10_000).state).toBe("waiting");
    expect(ledger.clearOriginFailures(origin, 3_000)).toBe(true);
    expect(ledger.clearOriginFailures(origin, 3_000)).toBe(false);
    expect(ledger.claimOrigin(origin, 3_000, 10_000).state).toBe("claimed");
  });

  test("guards pending work from claims while the persisted origin is waiting", () => {
    const ledger = open();
    const origin = "https://source.invalid";
    const seeded = ledger.seed(definition(), 1);
    const originClaim = ledger.claimOrigin(origin, 1_000, 10_000);
    if (originClaim.state !== "claimed") throw new Error("expected lease");
    const failure = ledger.failOrigin(originClaim.lease, 2_000, 0, {
      circuitBreakerAfter: 1,
      circuitBreakerCooldownMs: 60_000,
      retryAt: 5_000,
    });
    const eventsBefore = ledger.eventCount(seeded.workKey);

    expect(
      ledger.claimUnlessOriginStopped(
        origin,
        "worker",
        3_000,
        10_000,
        ["author-manifest"],
        {
          implementationVersion: "crawler@1",
          schemaVersion: "author-manifest@1",
        },
      ),
    ).toEqual({ retryAt: failure.nextAllowedAt, state: "waiting" });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      attemptCount: 0,
      leaseOwner: null,
      state: "pending",
    });
    expect(ledger.eventCount(seeded.workKey)).toBe(eventsBefore);
  });
});

test("doctor reports WAL, integrity, schema, and crash residue", () => {
  const ledger = open();
  ledger.seed(definition(), 1);
  ledger.claim("crashed", 1, 5);
  expect(ledger.doctor(6)).toEqual({
    coolingOrigins: 0,
    integrity: "ok",
    integrityScope: "full",
    journalMode: "wal",
    schemaVersion: CURRENT_SCHEMA_VERSION,
    staleRunning: 1,
    stoppedOrigins: 0,
  });
});

test("uses only a schema-scoped integrity scan on startup and reserves the deep scan for doctor", () => {
  const path = join(
    mkdtempSync(join(tmpdir(), "saqi-startup-integrity-")),
    "ledger.sqlite3",
  );
  const seeded = open(path);
  seeded.seed(definition(), 1);
  seeded.close();
  LEDGERS.pop();

  const calls: string[] = [];
  const database = new Database(path, {
    verbose: (statement) => {
      calls.push(String(statement));
    },
  });
  const reopened = new Ledger(database, { existing: true, path });
  LEDGERS.push(reopened);
  expect(calls).toContain("PRAGMA cell_size_check = ON");
  expect(calls).toContain("PRAGMA quick_check('sqlite_schema')");
  expect(calls).not.toContain("PRAGMA quick_check(1)");
  expect(calls).not.toContain("PRAGMA integrity_check");

  calls.length = 0;
  expect(reopened.doctor(Date.now(), "schema")).toMatchObject({
    integrity: "ok",
    integrityScope: "schema",
  });
  expect(calls).toContain("PRAGMA quick_check('sqlite_schema')");
  expect(calls).not.toContain("PRAGMA integrity_check");

  calls.length = 0;
  expect(reopened.doctor().integrity).toBe("ok");
  expect(calls).toContain("PRAGMA integrity_check");
});

test("fails closed before migration when an existing ledger is corrupt", () => {
  const path = join(
    mkdtempSync(join(tmpdir(), "saqi-corrupt-ledger-")),
    "ledger.sqlite3",
  );
  writeFileSync(path, Buffer.alloc(4_096, 0xa5));
  expect(() => Ledger.open(path)).toThrow(LedgerIntegrityError);
});

test("enforces append-only audit events and recovery checkpoints in SQL", () => {
  const path = join(
    mkdtempSync(join(tmpdir(), "saqi-append-only-")),
    "ledger.sqlite3",
  );
  const ledger = open(path);
  const key = ledger.seed(definition(), 1).workKey;
  const claim = ledger.claim("worker", 1, 100);
  if (!claim) throw new Error("Expected claim");
  ledger.checkpoint(
    claim,
    { artifactHash: null, kind: "cursor", payload: { page: 1 } },
    2,
  );

  const database = new Database(path);
  try {
    expect(() =>
      database.prepare("DELETE FROM work_event WHERE work_key = ?").run(key),
    ).toThrow("WORK_EVENT_IMMUTABLE");
    expect(() =>
      database.prepare("DELETE FROM checkpoint WHERE work_key = ?").run(key),
    ).toThrow("CHECKPOINT_IMMUTABLE");
    expect(() =>
      database
        .prepare("UPDATE checkpoint SET payload_json = '{}' WHERE work_key = ?")
        .run(key),
    ).toThrow("CHECKPOINT_IMMUTABLE");
  } finally {
    database.close();
  }
  expect(ledger.eventCount(key)).toBe(3);
  expect(ledger.checkpointCount(key)).toBe(1);
});

test("scans succeeded work with a stable bounded event cursor and exact filters", () => {
  const ledger = open();
  for (const id of [1, 2, 3]) {
    const seeded = ledger.seed(definition({ authorId: String(id) }), id);
    const claim = ledger.claim("worker", id, 1_000, ["author-manifest"]);
    if (!claim) throw new Error("claim missing");
    ledger.succeed(claim, String(id).padStart(64, "a").slice(-64), id + 10);
    expect(seeded.inserted).toBe(true);
  }
  const first = ledger.listSucceededAfter(0, ["author-manifest"], 2, {
    implementationVersion: "crawler@1",
    schemaVersion: "author-manifest@1",
  });
  expect(first.items).toHaveLength(2);
  const second = ledger.listSucceededAfter(
    first.cursor,
    ["author-manifest"],
    2,
    { implementationVersion: "crawler@1", schemaVersion: "author-manifest@1" },
  );
  expect(second.items).toHaveLength(1);
  expect(
    new Set([...first.items, ...second.items].map(({ work }) => work.workKey))
      .size,
  ).toBe(3);
  expect(
    ledger.listSucceededAfter(second.cursor, ["author-manifest"], 2).items,
  ).toEqual([]);
});

test("scans legacy Sol fallbacks only for poems without a completed preferred result", () => {
  const ledger = open();
  const kind = "poem-enrichment-sol";
  const legacy = "sol-enrichment-v1";
  const preferred = "sol-word-gloss-v2";
  const input = (poemId: string): PoemEnrichmentInput => ({
    ...LEGACY_SOL_INPUT,
    poemId,
  });
  const seed = (
    poemId: string,
    implementationVersion: string,
    now: number,
    complete: boolean,
  ) => {
    const value = input(poemId);
    const result = ledger.seed(
      {
        implementationVersion,
        input: value,
        inputHash: inputHash(value),
        kind,
        priority: 100,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      now,
    );
    if (!complete) return result;
    const claim = ledger.claim(
      `${implementationVersion}-worker`,
      now + 1,
      1_000,
      [kind],
      { implementationVersion },
    );
    if (!claim) throw new Error("Sol claim missing");
    ledger.succeed(claim, sha256(result.workKey), now + 2);
    return result;
  };

  seed("legacy-only", legacy, 1, true);
  const importedLegacy = seed("legacy-imported", legacy, 5, true);
  ledger.markImported(
    importedLegacy.workKey,
    sha256(importedLegacy.workKey),
    8,
  );
  seed("shadowed", legacy, 10, true);
  seed("shadowed", preferred, 20, true);
  seed("preferred-pending", legacy, 30, true);
  seed("different-legacy", legacy, 50, true);
  seed("different-preferred", preferred, 60, true);
  seed("preferred-imported", legacy, 70, true);
  const imported = seed("preferred-imported", preferred, 80, true);
  ledger.markImported(imported.workKey, sha256(imported.workKey), 83);
  seed("preferred-pending", preferred, 90, false);

  const page = ledger.listSucceededSolFallbackAfter(
    0,
    kind,
    100,
    legacy,
    preferred,
  );
  expect(
    page.items.map(
      ({ work }) => PoemEnrichmentInputSchema.parse(work.input).poemId,
    ),
  ).toEqual([
    "legacy-only",
    "legacy-imported",
    "preferred-pending",
    "different-legacy",
  ]);
});

test("advances the legacy Sol cursor over a suppressed bounded page", () => {
  const ledger = open();
  const kind = "poem-enrichment-sol";
  const complete = (
    poemId: string,
    implementationVersion: string,
    now: number,
  ) => {
    const input = { ...LEGACY_SOL_INPUT, poemId };
    ledger.seed(
      {
        implementationVersion,
        input,
        inputHash: inputHash(input),
        kind,
        priority: 100,
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      now,
    );
    const claim = ledger.claim("worker", now + 1, 1_000, [kind], {
      implementationVersion,
    });
    if (!claim) throw new Error("Sol claim missing");
    ledger.succeed(claim, sha256(claim.work.workKey), now + 2);
  };
  complete("shadowed-first", "sol-enrichment-v1", 1);
  complete("eligible-second", "sol-enrichment-v1", 10);
  complete("shadowed-first", "sol-word-gloss-v2", 20);

  const first = ledger.listSucceededSolFallbackAfter(
    0,
    kind,
    1,
    "sol-enrichment-v1",
    "sol-word-gloss-v2",
  );
  expect(first.items).toEqual([]);
  const second = ledger.listSucceededSolFallbackAfter(
    first.cursor,
    kind,
    1,
    "sol-enrichment-v1",
    "sol-word-gloss-v2",
  );
  expect(
    second.items.map(
      ({ work }) => PoemEnrichmentInputSchema.parse(work.input).poemId,
    ),
  ).toEqual(["eligible-second"]);
  expect(second.cursor).toBeGreaterThan(first.cursor);
});

test("scans ready work by bounded mixed-order keyset without duplicates", () => {
  const ledger = open();
  const requirements = {
    implementationVersion: "crawler@1",
    schemaVersion: "author-manifest@1",
  };
  for (const [id, priority, now] of [
    [1, 20, 1],
    [2, 20, 1],
    [3, 20, 2],
    [4, 10, 1],
    [5, 10, 2],
  ] as const) {
    ledger.seed({ ...definition({ authorId: String(id) }), priority }, now);
  }
  const expected = ledger
    .listReadyWork("author-manifest", 10, requirements, 10)
    .map(({ workKey }) => workKey);
  const first = ledger.listReadyWorkAfter(
    null,
    "author-manifest",
    2,
    requirements,
    10,
  );
  const second = ledger.listReadyWorkAfter(
    first.cursor,
    "author-manifest",
    2,
    requirements,
    10,
  );
  const third = ledger.listReadyWorkAfter(
    second.cursor,
    "author-manifest",
    2,
    requirements,
    10,
  );

  expect(first.done).toBe(false);
  expect(second.done).toBe(false);
  expect(third.done).toBe(true);
  expect(
    [...first.items, ...second.items, ...third.items].map(
      ({ workKey }) => workKey,
    ),
  ).toEqual(expected);
});

test.each(["renewed", "succeeded"] as const)(
  "rolls back the work-item mutation when the %s event cannot commit",
  (eventType) => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-ledger-atomic-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const seeded = ledger.seed(definition(), 1);
    const claim = ledger.claim("worker", 2, 1_000, ["author-manifest"]);
    if (!claim) throw new Error("claim missing");
    const database = new Database(path);
    database.exec(
      eventType === "renewed"
        ? `CREATE TRIGGER reject_test_event BEFORE INSERT ON work_event
           WHEN NEW.event_type = 'renewed' BEGIN
             SELECT RAISE(ABORT, 'synthetic event failure');
           END;`
        : `CREATE TRIGGER reject_test_event BEFORE INSERT ON work_event
           WHEN NEW.event_type = 'succeeded' BEGIN
             SELECT RAISE(ABORT, 'synthetic event failure');
           END;`,
    );
    database.close();

    if (eventType === "renewed") {
      expect(() => ledger.renew(claim, 3, 2_000)).toThrow(
        "synthetic event failure",
      );
      expect(ledger.get(seeded.workKey)).toMatchObject({
        leaseExpiresAt: 1_002,
        state: "running",
      });
    } else {
      expect(() => ledger.succeed(claim, "a".repeat(64), 3)).toThrow(
        "synthetic event failure",
      );
      expect(ledger.get(seeded.workKey)).toMatchObject({
        outputArtifactHash: null,
        state: "running",
      });
    }
  },
);

test("operator requeue is audited, bounded by code, and idempotent", () => {
  const ledger = open();
  for (const [authorId, code] of [
    ["empty", "SOURCE_POEM_CONTENT_EMPTY"],
    ["other", "PERMANENT_OTHER"],
  ] as const) {
    ledger.seed(definition({ authorId }), 1);
    const claim = ledger.claim("worker", 2, 1_000, ["author-manifest"]);
    if (!claim) throw new Error("claim missing");
    ledger.deadLetter(claim, code, 3);
  }
  expect(
    ledger.requeueDeadLetters(
      ["author-manifest"],
      ["SOURCE_POEM_CONTENT_EMPTY"],
      4,
    ),
  ).toBe(1);
  expect(
    ledger.requeueDeadLetters(
      ["author-manifest"],
      ["SOURCE_POEM_CONTENT_EMPTY"],
      5,
    ),
  ).toBe(0);
  expect(ledger.status().byState).toMatchObject({
    dead_letter: 1,
    pending: 1,
  });
});

test.each(["sol-5.6", 42])(
  "legacy Sol fanout recovery validates model key %s before mutation",
  (modelKey) => {
    const path = join(
      mkdtempSync(join(tmpdir(), "saqi-ledger-recovery-")),
      "ledger.sqlite3",
    );
    const ledger = open(path);
    const sourceArtifactHash = "a".repeat(64);
    const source = ledger.seed(
      {
        ...definition({ poemId: "legacy-sol" }),
        implementationVersion: "sol-enrichment-v1",
        kind: "poem-enrichment-sol",
        schemaVersion: "saqi.poem-enrichment-input@1",
      },
      1,
    );
    const sourceClaim = ledger.claim("sol", 2, 1_000, ["poem-enrichment-sol"]);
    if (!sourceClaim) throw new Error("source claim missing");
    ledger.succeed(sourceClaim, sourceArtifactHash, 3);

    const fanoutInput = {
      lane: "enrichment",
      modelKey,
      source: { artifactHash: sourceArtifactHash, workKey: source.workKey },
    };
    const fanout = ledger.seed(
      {
        implementationVersion: "fanout-reconciler-v1",
        input: fanoutInput,
        inputHash: inputHash(fanoutInput),
        kind: "fanout-succeeded-enrichment",
        priority: 200,
        schemaVersion: "fanout@1",
      },
      4,
    );
    const fanoutClaim = ledger.claim("fanout", 5, 1_000, [
      "fanout-succeeded-enrichment",
    ]);
    if (!fanoutClaim) throw new Error("fanout claim missing");
    ledger.succeed(fanoutClaim, "b".repeat(64), 6);

    const publicationInput = {
      actionArtifactHash: "c".repeat(64),
      actionHash: "d".repeat(64),
      lane: "enrichment",
      sources: [{ artifactHash: sourceArtifactHash, workKey: source.workKey }],
    };
    ledger.seed(
      {
        implementationVersion: "publication-lane-v1",
        input: publicationInput,
        inputHash: inputHash(publicationInput),
        kind: "corpus-publication-enrichment",
        priority: 200,
        schemaVersion: "publication-work@1",
      },
      7,
    );
    const publicationClaim = ledger.claim("publication", 8, 1_000, [
      "corpus-publication-enrichment",
    ]);
    if (!publicationClaim) throw new Error("publication claim missing");
    ledger.deadLetter(publicationClaim, "CORPUS_IMPORT_REJECTED", 9);

    if (typeof modelKey !== "string") {
      expect(() =>
        ledger.reopenRejectedLegacySolFanout([fanout.workKey], 10),
      ).toThrow("SQLITE_ROW_VALIDATION_FAILED:ledger.legacySolFanoutRecovery");
      expect(ledger.get(fanout.workKey)).toMatchObject({
        outputArtifactHash: "b".repeat(64),
        state: "succeeded",
      });
      return;
    }

    expect(ledger.reopenRejectedLegacySolFanout([fanout.workKey], 10)).toBe(
      true,
    );
    expect(ledger.get(fanout.workKey)).toMatchObject({
      availableAt: 10,
      lastErrorCode: "FANOUT_LEGACY_REBIND_REQUIRED",
      outputArtifactHash: null,
      state: "pending",
    });
    expect(ledger.reopenRejectedLegacySolFanout([fanout.workKey], 11)).toBe(
      false,
    );
    const database = new Database(path, { readonly: true });
    const event = database
      .prepare(
        `SELECT event_type, payload_json FROM work_event
       WHERE work_key = ? ORDER BY sequence DESC LIMIT 1`,
      )
      .get(fanout.workKey) as { event_type: string; payload_json: string };
    database.close();
    expect({
      eventType: event.event_type,
      payload: JSON.parse(event.payload_json),
    }).toMatchObject({
      eventType: "operator_reopened",
      payload: {
        previousArtifactHash: "b".repeat(64),
        reason: "legacy_sol_publication_rejected",
      },
    });
  },
);

test("resolution wakes are exact, bounded, idempotent, and preserve live claims", () => {
  const ledger = open();
  const fanoutKind = "fanout-succeeded-sol";
  const fanout = (authorId: string): WorkDefinition => ({
    ...definition({ authorId }),
    kind: fanoutKind,
  });
  const defer = (authorId: string, code = "FANOUT_RESOLUTION_PENDING") => {
    const seeded = ledger.seed(fanout(authorId), 1);
    const claim = ledger.claim(`worker-${authorId}`, 2, 1_000, [fanoutKind]);
    if (!claim) throw new Error(`claim missing for ${authorId}`);
    if (code === "FANOUT_RESOLUTION_PENDING") {
      ledger.operatorRelease(claim, code, 3, 100);
    } else {
      ledger.retry(claim, code, 100, 3);
    }
    return seeded.workKey;
  };

  const deferred = defer("deferred");
  const runningSeed = ledger.seed(fanout("running"), 4);
  const running = ledger.claim("running-worker", 4, 1_000, [fanoutKind]);
  if (!running) throw new Error("running claim missing");
  const terminalSeed = ledger.seed(fanout("terminal"), 4);
  const terminal = ledger.claim("terminal-worker", 4, 1_000, [fanoutKind]);
  if (!terminal) throw new Error("terminal claim missing");
  ledger.succeed(terminal, "a".repeat(64), 5);
  const wrongError = defer("wrong-error", "SOURCE_TIMEOUT");
  const wrongKind = ledger.seed(definition({ authorId: "wrong-kind" }), 1);
  const wrongKindClaim = ledger.claim("wrong-kind-worker", 2, 1_000, [
    "author-manifest",
  ]);
  if (!wrongKindClaim) throw new Error("wrong-kind claim missing");
  ledger.operatorRelease(wrongKindClaim, "FANOUT_RESOLUTION_PENDING", 3, 100);
  const alreadyReady = ledger.seed(fanout("ready"), 1);
  const missing = "f".repeat(64);
  const before = ledger.get(deferred);

  expect(
    ledger.wakeResolutionPending(
      [
        deferred,
        runningSeed.workKey,
        terminalSeed.workKey,
        wrongError,
        wrongKind.workKey,
        alreadyReady.workKey,
        missing,
        deferred,
      ],
      [fanoutKind],
      10,
    ),
  ).toEqual({
    acknowledge: [
      deferred,
      terminalSeed.workKey,
      wrongError,
      wrongKind.workKey,
      alreadyReady.workKey,
      missing,
    ],
    retry: [runningSeed.workKey],
  });
  expect(ledger.get(deferred)).toMatchObject({
    attemptCount: before?.attemptCount,
    availableAt: 10,
    lastErrorCode: "FANOUT_RESOLUTION_PENDING",
    state: "pending",
  });
  expect(
    new Set(ledger.listFanoutPriorityWorkKeys([fanoutKind], 10, 10)),
  ).toEqual(new Set([deferred, alreadyReady.workKey]));
  expect(ledger.wakeResolutionPending([deferred], [fanoutKind], 11)).toEqual({
    acknowledge: [deferred],
    retry: [],
  });

  // A signal delivered during a live claim remains unacknowledged. Once that
  // claim defers, the same durable signal wakes it without touching its lease.
  ledger.operatorRelease(running, "FANOUT_RESOLUTION_PENDING", 12, 100);
  expect(
    ledger.wakeResolutionPending([runningSeed.workKey], [fanoutKind], 13),
  ).toEqual({ acknowledge: [runningSeed.workKey], retry: [] });
  expect(ledger.get(runningSeed.workKey)).toMatchObject({
    availableAt: 13,
    state: "pending",
  });

  const changedErrorKind = "fanout-succeeded-sol-changed-error";
  const changedErrorSeed = ledger.seed(
    { ...fanout("changed-error"), kind: changedErrorKind },
    14,
  );
  const changedError = ledger.claim("changed-error-worker", 14, 1_000, [
    changedErrorKind,
  ]);
  if (!changedError) throw new Error("changed-error claim missing");
  expect(
    ledger.wakeResolutionPending(
      [changedErrorSeed.workKey],
      [changedErrorKind],
      15,
    ),
  ).toEqual({ acknowledge: [], retry: [changedErrorSeed.workKey] });
  ledger.retry(changedError, "SOURCE_TIMEOUT", 100, 16);
  expect(
    ledger.wakeResolutionPending(
      [changedErrorSeed.workKey],
      [changedErrorKind],
      17,
    ),
  ).toEqual({ acknowledge: [changedErrorSeed.workKey], retry: [] });

  const status = ledger.status(13);
  ledger.rebuildStatusCounters();
  expect(ledger.status(13)).toEqual(status);
});

test("resolution wake validates its bounded exact-key contract", () => {
  const ledger = open();
  expect(ledger.wakeResolutionPending([], ["fanout-succeeded-sol"], 0)).toEqual(
    { acknowledge: [], retry: [] },
  );
  expect(() =>
    ledger.wakeResolutionPending(["invalid"], ["fanout"], 0),
  ).toThrow("Invalid resolution wake work key");
  expect(() =>
    ledger.wakeResolutionPending(
      Array.from({ length: 501 }, (_, index) =>
        index.toString(16).padStart(64, "0"),
      ),
      ["fanout"],
      0,
    ),
  ).toThrow("Resolution wake batch must not exceed 500 work keys");
});

test("durable fanout priority is globally bounded and terminal work is pruned", () => {
  const ledger = open();
  const fanoutKind = "fanout-succeeded-sol";
  const results = ledger.seedMany(
    Array.from(
      { length: MAX_DURABLE_FANOUT_PRIORITY_HINTS + 1 },
      (_, index) => ({
        ...definition({ authorId: `durable-hint-${String(index)}` }),
        kind: fanoutKind,
      }),
    ),
    1,
  );
  const admitted = results.slice(0, MAX_DURABLE_FANOUT_PRIORITY_HINTS);
  for (let offset = 0; offset < admitted.length; offset += 500) {
    expect(
      ledger.prioritizeFanoutWork(
        admitted.slice(offset, offset + 500).map(({ workKey }) => workKey),
        [fanoutKind],
        2,
      ),
    ).toBe(Math.min(500, admitted.length - offset));
  }
  const overflow = results.at(-1)?.workKey;
  if (!overflow) throw new Error("overflow fanout work missing");
  expect(ledger.prioritizeFanoutWork([overflow], [fanoutKind], 2)).toBe(0);
  expect(ledger.wakeResolutionPending([overflow], [fanoutKind], 2)).toEqual({
    acknowledge: [],
    retry: [overflow],
  });

  const terminalKey = admitted[0]?.workKey;
  if (!terminalKey) throw new Error("terminal fanout work missing");
  const [claim] = ledger.claimMany(
    "fanout-terminal",
    3,
    1_000,
    [fanoutKind],
    1,
    {},
    [],
    [terminalKey],
  );
  if (!claim) throw new Error("terminal fanout claim missing");
  ledger.deadLetter(claim, "TERMINAL_TEST", 4);
  expect(ledger.acknowledgeFanoutPriorityWork([terminalKey])).toBe(0);
  expect(ledger.prioritizeFanoutWork([overflow], [fanoutKind], 5)).toBe(1);
});

test("durable fanout priority survives an expired lease for safe retry", () => {
  const ledger = open();
  const fanoutKind = "fanout-succeeded-sol";
  const seeded = ledger.seed(
    { ...definition({ authorId: "lost-lease-hint" }), kind: fanoutKind },
    1,
  );
  expect(ledger.prioritizeFanoutWork([seeded.workKey], [fanoutKind], 2)).toBe(
    1,
  );
  expect(ledger.claim("fanout", 2, 1, [fanoutKind])?.work.workKey).toBe(
    seeded.workKey,
  );
  expect(ledger.listFanoutPriorityWorkKeys([fanoutKind], 10, 2)).toEqual([]);
  expect(ledger.recoverExpired(4)).toBe(1);
  expect(ledger.listFanoutPriorityWorkKeys([fanoutKind], 10, 4)).toEqual([
    seeded.workKey,
  ]);
  expect(ledger.acknowledgeFanoutPriorityWork([seeded.workKey])).toBe(1);
});

test("durable fanout priority backfills only bounded ready resolution work", () => {
  const calls: string[] = [];
  const database = new Database(":memory:", {
    verbose: (statement) => {
      calls.push(String(statement));
    },
  });
  const ledger = new Ledger(database);
  LEDGERS.push(ledger);
  const fanoutKind = "fanout-succeeded-sol";
  const keys = ["first", "second", "unrelated"].map(
    (authorId) =>
      ledger.seed({ ...definition({ authorId }), kind: fanoutKind }, 1).workKey,
  );
  for (const [index, key] of keys.entries()) {
    const [claim] = ledger.claimMany(
      `fanout-${String(index)}`,
      2,
      1_000,
      [fanoutKind],
      1,
      {},
      [],
      [key],
    );
    if (!claim) throw new Error("backfill fanout claim missing");
    ledger.operatorRelease(
      claim,
      index === 2 ? "SOURCE_TIMEOUT" : "FANOUT_RESOLUTION_PENDING",
      3,
      10,
    );
  }
  calls.length = 0;
  expect(ledger.backfillFanoutPriorityWork([fanoutKind], 1, 9)).toBe(0);
  expect(
    calls.some((statement) =>
      statement.includes("INDEXED BY work_item_fanout_resolution_pending"),
    ),
  ).toBe(false);
  const readyProbe = calls.find((statement) =>
    statement.includes("INDEXED BY work_item_fanout_resolution_ready"),
  );
  if (!readyProbe) throw new Error("ready range probe missing");
  expect(database.prepare(`EXPLAIN QUERY PLAN ${readyProbe}`).all()).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        detail: expect.stringContaining(
          "SEARCH work USING COVERING INDEX work_item_fanout_resolution_ready (kind=? AND available_at<?)",
        ),
      }),
    ]),
  );
  expect(ledger.backfillFanoutPriorityWork([fanoutKind], 1, 10)).toBe(1);
  expect(ledger.backfillFanoutPriorityWork([fanoutKind], 10, 10)).toBe(1);
  calls.length = 0;
  expect(ledger.backfillFanoutPriorityWork([fanoutKind], 10, 10)).toBe(0);
  expect(
    calls.some((statement) =>
      statement.includes("INDEXED BY work_item_fanout_resolution_pending"),
    ),
  ).toBe(false);
  expect(ledger.backfillFanoutPriorityWork([fanoutKind], 10, 10)).toBe(0);
  // A new Ledger instance must discover eligible work without in-memory cursors.
  const restarted = new Ledger(database);
  const newlyReady = ledger.seed(
    { ...definition({ authorId: "after-restart" }), kind: fanoutKind },
    11,
  ).workKey;
  const [newClaim] = ledger.claimMany(
    "restart-fanout",
    12,
    1_000,
    [fanoutKind],
    1,
    {},
    [],
    [newlyReady],
  );
  if (!newClaim) throw new Error("new fanout claim missing");
  ledger.operatorRelease(newClaim, "FANOUT_RESOLUTION_PENDING", 13, 20);
  expect(restarted.backfillFanoutPriorityWork([fanoutKind], 10, 19)).toBe(0);
  expect(restarted.backfillFanoutPriorityWork([fanoutKind], 10, 20)).toBe(1);
  expect(
    calls.some((statement) =>
      statement.includes("INDEXED BY work_item_fanout_resolution_pending"),
    ),
  ).toBe(true);
  expect(
    new Set(ledger.listFanoutPriorityWorkKeys([fanoutKind], 10, 10)),
  ).toEqual(new Set(keys.slice(0, 2)));
  expect(
    calls.some(
      (statement) =>
        statement.includes("INDEXED BY fanout_priority_hint_schedule") &&
        !statement.includes("FROM work_item"),
    ),
  ).toBe(true);
});

test("resolution-pending compatibility pages are stable, bounded, and index-only", () => {
  const calls: string[] = [];
  const database = new Database(":memory:", {
    verbose: (statement) => {
      calls.push(String(statement));
    },
  });
  const ledger = new Ledger(database);
  LEDGERS.push(ledger);
  const fanoutKind = "fanout-succeeded-sol";
  const deferred = ["first", "second", "third"].map((authorId, index) => {
    const seeded = ledger.seed(
      {
        ...definition({ authorId }),
        kind: fanoutKind,
        priority: 30 - index,
      },
      index + 1,
    );
    const claim = ledger.claim(`resolution-page-${String(index)}`, 10, 1_000, [
      fanoutKind,
    ]);
    if (!claim) throw new Error("resolution page claim missing");
    ledger.operatorRelease(claim, "FANOUT_RESOLUTION_PENDING", 11, 1_000);
    return seeded.workKey;
  });
  const unrelated = ledger.seed(
    { ...definition({ authorId: "unrelated" }), kind: fanoutKind },
    4,
  );
  const unrelatedClaim = ledger.claim("unrelated", 10, 1_000, [fanoutKind]);
  if (!unrelatedClaim) throw new Error("unrelated claim missing");
  ledger.retry(unrelatedClaim, "SOURCE_TIMEOUT", 1_000, 11);

  const first = ledger.listResolutionPendingAfter(null, [fanoutKind], 1, 12);
  const second = ledger.listResolutionPendingAfter(
    first.cursor,
    [fanoutKind],
    1,
    12,
  );
  const third = ledger.listResolutionPendingAfter(
    second.cursor,
    [fanoutKind],
    1,
    12,
  );
  expect([
    first.items[0]?.workKey,
    second.items[0]?.workKey,
    third.items[0]?.workKey,
  ]).toEqual(deferred);
  expect(first.complete).toBe(false);
  expect(second.complete).toBe(false);
  expect(third.complete).toBe(true);
  expect(third.items.map(({ workKey }) => workKey)).not.toContain(
    unrelated.workKey,
  );
  expect(
    calls.some((statement) =>
      statement.includes("INDEXED BY work_item_fanout_resolution_pending"),
    ),
  ).toBe(true);
});

test("unknown recovery is aged, exact-profile, and preserves priority", () => {
  const ledger = open();
  const definitions = [
    definition({ authorId: "current" }),
    {
      ...definition({ authorId: "obsolete" }),
      implementationVersion: "crawler@obsolete",
    },
    definition({ authorId: "fresh" }),
  ];
  for (const [index, item] of definitions.entries()) {
    ledger.seed(item, 1);
    const claim = ledger.claim("worker", 2, 1_000, ["author-manifest"], {
      implementationVersion: item.implementationVersion,
      schemaVersion: item.schemaVersion,
    });
    if (!claim) throw new Error("claim missing");
    ledger.deadLetter(
      claim,
      "CODEX_OPERATION_OUTCOME_UNKNOWN",
      index === 2 ? 21 : 10,
    );
  }

  const recovered = ledger.claimUnknownOperationRecovery(
    "reconciler",
    30,
    1_000,
    "author-manifest",
    20,
    {
      implementationVersion: "crawler@1",
      schemaVersion: "author-manifest@1",
    },
  );
  expect(recovered?.work).toMatchObject({
    input: { authorId: "current" },
    lastErrorCode: "CODEX_OPERATION_OUTCOME_UNKNOWN",
    priority: 10,
    state: "running",
  });
  expect(ledger.status().byState).toMatchObject({ dead_letter: 2, running: 1 });
});

test("empty recovery probes remain no-op under 256-lane contention", () => {
  const path = join(
    mkdtempSync(join(tmpdir(), "saqi-recovery-contention-")),
    "ledger.sqlite3",
  );
  const ledger = open(path);
  const initialMtime = new Database(path, { readonly: true });
  const initialEvents = initialMtime
    .prepare("SELECT COUNT(*) AS count FROM work_event")
    .get() as { count: number };
  initialMtime.close();
  for (let lane = 0; lane < 256; lane += 1) {
    expect(
      ledger.claimUnknownOperationRecovery(
        `sol-${String(lane)}`,
        30,
        1_000,
        "author-manifest",
        20,
        {
          implementationVersion: "crawler@1",
          schemaVersion: "author-manifest@1",
        },
      ),
    ).toBeNull();
  }
  const database = new Database(path, { readonly: true });
  expect(
    database.prepare("SELECT COUNT(*) AS count FROM work_event").get(),
  ).toEqual(initialEvents);
  database.close();
});

test("ordinary workers cannot fan out a pending ambiguous operation", () => {
  const ledger = open();
  ledger.seed(definition(), 1);
  const first = ledger.claim("worker", 2, 1_000, ["author-manifest"]);
  if (!first) throw new Error("claim missing");
  ledger.operatorRelease(first, "CODEX_OPERATION_OUTCOME_UNKNOWN", 3);

  for (let lane = 0; lane < 256; lane += 1) {
    expect(
      ledger.claim(
        `ordinary-${String(lane)}`,
        10,
        1_000,
        ["author-manifest"],
        {
          implementationVersion: "crawler@1",
          schemaVersion: "author-manifest@1",
        },
        ["CODEX_OPERATION_OUTCOME_UNKNOWN"],
      ),
    ).toBeNull();
  }
  expect(
    ledger.claimUnknownOperationRecovery(
      "maintenance",
      10,
      1_000,
      "author-manifest",
      3,
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
    )?.work.lastErrorCode,
  ).toBe("CODEX_OPERATION_OUTCOME_UNKNOWN");
});

test("a crashed ambiguity recovery remains fenced after restart", () => {
  const path = join(
    mkdtempSync(join(tmpdir(), "saqi-ambiguous-recovery-crash-")),
    "ledger.sqlite3",
  );
  const beforeCrash = open(path);
  beforeCrash.seed(definition(), 1);
  const initial = beforeCrash.claim("provider", 2, 10, ["author-manifest"]);
  if (!initial) throw new Error("initial claim missing");
  beforeCrash.deadLetter(initial, "CODEX_OPERATION_OUTCOME_UNKNOWN", 3);
  const recovery = beforeCrash.claimUnknownOperationRecovery(
    "maintenance",
    10,
    10,
    "author-manifest",
    3,
    {
      implementationVersion: "crawler@1",
      schemaVersion: "author-manifest@1",
    },
  );
  if (!recovery) throw new Error("recovery claim missing");
  beforeCrash.close();
  LEDGERS.splice(LEDGERS.indexOf(beforeCrash), 1);

  const restarted = open(path);
  expect(restarted.recoverExpired(20)).toBe(1);
  expect(restarted.get(recovery.work.workKey)).toMatchObject({
    lastErrorCode: "CODEX_OPERATION_OUTCOME_UNKNOWN",
    state: "pending",
  });
  expect(
    restarted.claim(
      "ordinary",
      20,
      100,
      ["author-manifest"],
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
      ["CODEX_OPERATION_OUTCOME_UNKNOWN"],
    ),
  ).toBeNull();
  expect(
    restarted.claimUnknownOperationRecovery(
      "maintenance-after-restart",
      20,
      100,
      "author-manifest",
      20,
      {
        implementationVersion: "crawler@1",
        schemaVersion: "author-manifest@1",
      },
    )?.work.lastErrorCode,
  ).toBe("CODEX_OPERATION_OUTCOME_UNKNOWN");
});

test("operator release returns a claim without consuming its attempt budget", () => {
  const ledger = open();
  const seeded = ledger.seed(definition(), 1);
  const claim = ledger.claim("worker", 2, 1_000, ["author-manifest"]);
  if (!claim) throw new Error("claim missing");
  expect(claim.work.attemptCount).toBe(1);
  ledger.operatorRelease(claim, "COLLECTOR_OPERATOR_STOP", 3);
  expect(ledger.get(seeded.workKey)).toMatchObject({
    attemptCount: 0,
    lastErrorCode: "COLLECTOR_OPERATOR_STOP",
    state: "pending",
  });
  const resumed = ledger.claim("worker", 4, 1_000, ["author-manifest"]);
  expect(resumed?.work.attemptCount).toBe(1);
});

test("tracks ambiguous paid operations idempotently by immutable operation key", () => {
  const ledger = open();
  const seeded = ledger.seed(definition(), 1);
  const operationKey = "a".repeat(64);

  ledger.recordPaidOperationUnknown(
    seeded.workKey,
    operationKey,
    "attempt-1",
    20,
    10,
  );
  ledger.recordPaidOperationUnknown(
    seeded.workKey,
    operationKey,
    "attempt-1",
    30,
    11,
  );
  expect(ledger.paidOperationReconciliationStatus(29)).toEqual({
    due: 0,
    quarantined: 0,
    reconciled: 0,
    unknown: 1,
  });
  expect(ledger.paidOperationReconciliationStatus(30).due).toBe(1);
  expect(() =>
    ledger.recordPaidOperationUnknown(
      seeded.workKey,
      operationKey,
      "different-attempt",
      30,
      12,
    ),
  ).toThrow("PAID_OPERATION_IDENTITY_CONFLICT");

  expect(
    ledger.recordPaidOperationQuarantined(
      operationKey,
      31,
      "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
    ),
  ).toBe(true);
  expect(
    ledger.recordPaidOperationQuarantined(
      operationKey,
      32,
      "CODEX_OPERATION_UNRESOLVED_QUARANTINED",
    ),
  ).toBe(false);
  expect(ledger.paidOperationReconciliationStatus(32)).toEqual({
    due: 0,
    quarantined: 1,
    reconciled: 0,
    unknown: 0,
  });

  // A delayed duplicate observation cannot reopen a quarantined operation.
  ledger.recordPaidOperationUnknown(
    seeded.workKey,
    operationKey,
    "attempt-1",
    50,
    40,
  );
  expect(ledger.paidOperationReconciliationStatus(50)).toEqual({
    due: 0,
    quarantined: 1,
    reconciled: 0,
    unknown: 0,
  });

  expect(ledger.recordPaidOperationReconciled(operationKey, 51)).toBe(true);
  expect(ledger.recordPaidOperationReconciled(operationKey, 52)).toBe(false);

  // A delayed duplicate observation cannot downgrade a proven result.
  ledger.recordPaidOperationUnknown(
    seeded.workKey,
    operationKey,
    "attempt-1",
    60,
    53,
  );
  expect(ledger.paidOperationReconciliationStatus(60)).toEqual({
    due: 0,
    quarantined: 0,
    reconciled: 1,
    unknown: 0,
  });
});

test.each(["retry", "quotaWait"] as const)(
  "rejects invalid or past timestamps before entering %s",
  (transition) => {
    for (const retryAt of [2, 3, NaN, Infinity]) {
      const ledger = open();
      const seeded = ledger.seed(definition({ retryAt: String(retryAt) }), 1);
      const claim = ledger.claim("worker", 2, 1_000, ["author-manifest"]);
      if (!claim) throw new Error("claim missing");
      expect(() =>
        ledger[transition](claim, "TRANSIENT_FAILURE", retryAt, 3),
      ).toThrow("Work retryAt must be later than now");
      expect(ledger.get(seeded.workKey)).toMatchObject({
        state: "running",
      });
    }
  },
);
