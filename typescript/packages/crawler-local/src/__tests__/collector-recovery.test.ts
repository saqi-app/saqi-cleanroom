import { tmpdir } from "node:os";
import { join } from "node:path";

import { currentSource } from "@saqi/source-adapter";
import { describe, expect, it } from "vitest";

import {
  collectionWorkKinds,
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "../collection/collector";
import {
  COLLECTOR_RECOVERY_COHORT_SIZES,
  COLLECTOR_RECOVERY_ERROR_CODES,
  COLLECTOR_RECOVERY_STATE_KEY,
  CollectorRecoveryController,
} from "../collection/collector-recovery";
import { Ledger } from "../persistence/ledger";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const ERROR = COLLECTOR_RECOVERY_ERROR_CODES[0];
const HASH = "a".repeat(64);

function open(): Ledger {
  const root = mkdtempSync(join(tmpdir(), "collector-recovery-"));
  return Ledger.initialize(join(root, "ledger.sqlite3"));
}

function deadLetter(
  ledger: Ledger,
  id: number,
  options: {
    errorCode?: string;
    implementationVersion?: string;
    kind?: string;
    priority?: number;
    schemaVersion?: string;
  } = {},
): string {
  const sourceId = id + 1;
  const input = {
    authorHref: `https://source.invalid/writers/${String(sourceId)}`,
    poemHref: `https://source.invalid/works/${String(sourceId)}`,
  };
  const kind = options.kind ?? collectionWorkKinds().poemDetail;
  const seeded = ledger.seed(
    {
      implementationVersion:
        options.implementationVersion ?? collectorImplementationVersion(),
      input,
      inputHash: inputHash(input),
      kind,
      priority: options.priority ?? 0,
      schemaVersion: options.schemaVersion ?? collectorSchemaVersion(),
    },
    id + 1,
  );
  const claim = ledger.claim(`seed-${String(id)}`, 10_000, 10_000, [kind]);
  if (claim?.work.workKey !== seeded.workKey)
    throw new Error("fixture claim missing");
  ledger.deadLetter(claim, options.errorCode ?? ERROR, 10_001);
  return seeded.workKey;
}

function succeedReleased(
  ledger: Ledger,
  expectedKeys: readonly string[],
  now: number,
): void {
  const claims = ledger.claimMany(
    "collector",
    now,
    10_000,
    [collectionWorkKinds().poemDetail],
    expectedKeys.length,
    {
      implementationVersion: collectorImplementationVersion(),
      schemaVersion: collectorSchemaVersion(),
    },
  );
  expect(new Set(claims.map(({ work }) => work.workKey))).toEqual(
    new Set(expectedKeys),
  );
  for (const claim of claims) ledger.succeed(claim, HASH, now + 1);
}

describe("bounded collector recovery", () => {
  it("fences concurrent exact-cohort claims without admitting unrelated work", () => {
    const root = mkdtempSync(join(tmpdir(), "collector-recovery-claim-"));
    const database = join(root, "ledger.sqlite3");
    const first = Ledger.initialize(database);
    const reserved = deadLetter(first, 1);
    first.requeueDeadLetters(
      [collectionWorkKinds().poemDetail],
      [ERROR],
      20_000,
    );
    const unrelatedInput = {
      authorHref: "https://source.invalid/writers/unrelated",
      poemHref: "https://source.invalid/works/999",
    };
    const unrelated = first.seed(
      {
        implementationVersion: collectorImplementationVersion(),
        input: unrelatedInput,
        inputHash: inputHash(unrelatedInput),
        kind: collectionWorkKinds().poemDetail,
        priority: 1_000,
        schemaVersion: collectorSchemaVersion(),
      },
      20_000,
    );
    const second = Ledger.open(database);
    const requirements = {
      implementationVersion: collectorImplementationVersion(),
      schemaVersion: collectorSchemaVersion(),
    } as const;

    expect(
      first.claimUnlessOriginStopped(
        currentSource().origin,
        "first",
        20_001,
        10_000,
        [collectionWorkKinds().poemDetail],
        requirements,
        [reserved],
      ),
    ).toMatchObject({
      claim: { work: { workKey: reserved } },
      state: "claimed",
    });
    expect(
      second.claimUnlessOriginStopped(
        currentSource().origin,
        "second",
        20_001,
        10_000,
        [collectionWorkKinds().poemDetail],
        requirements,
        [reserved],
      ),
    ).toEqual({ state: "idle" });
    expect(second.get(unrelated.workKey)).toMatchObject({
      attemptCount: 0,
      state: "pending",
    });
    second.close();
    first.close();
  });

  it("atomically reserves only an exact, eligible source-detail cohort", () => {
    const ledger = open();
    const eligibleLow = deadLetter(ledger, 1);
    const eligibleHigh = deadLetter(ledger, 2, { priority: 10 });
    const author = deadLetter(ledger, 3, {
      kind: collectionWorkKinds().authorManifest,
    });
    const paid = deadLetter(ledger, 4, { kind: "poem-enrichment-sol" });
    const wrongError = deadLetter(ledger, 5, { errorCode: "OTHER_FAILURE" });
    const obsolete = deadLetter(ledger, 6, {
      implementationVersion: "source-chrome-old",
    });
    const controller = new CollectorRecoveryController({
      ledger,
      reservationId: () => "11111111-1111-4111-8111-111111111111",
    });

    controller.arm(20_000);
    const result = controller.cycle(20_000);
    expect(result.released).toEqual([eligibleHigh]);
    expect(result.state).toMatchObject({
      phase: "observing",
      reservation: { workKeys: [eligibleHigh] },
    });
    expect(ledger.get(eligibleHigh)?.state).toBe("pending");
    for (const key of [eligibleLow, author, paid, wrongError, obsolete])
      expect(ledger.get(key)?.state).toBe("dead_letter");
    ledger.close();
  });

  it("leaves both scheduler state and work untouched when its CAS is stale", () => {
    const ledger = open();
    const workKey = deadLetter(ledger, 1);
    const initial = canonicalJson({ phase: "ready" });
    expect(
      ledger.saveSchedulerState(
        COLLECTOR_RECOVERY_STATE_KEY,
        initial,
        sha256(initial),
        null,
        20_000,
      ),
    ).toBe(true);
    const reserved = canonicalJson({ phase: "observing", workKeys: [workKey] });

    expect(
      ledger.reserveDeadLetterCohort(
        {
          errorCodes: COLLECTOR_RECOVERY_ERROR_CODES,
          expectedStateDigest: "b".repeat(64),
          implementationVersion: collectorImplementationVersion(),
          kind: collectionWorkKinds().poemDetail,
          reservationId: "11111111-1111-4111-8111-111111111111",
          schedulerStateDigest: sha256(reserved),
          schedulerStateKey: COLLECTOR_RECOVERY_STATE_KEY,
          schedulerStateSerialized: reserved,
          schemaVersion: collectorSchemaVersion(),
          workKeys: [workKey],
        },
        20_001,
      ),
    ).toBe(false);
    expect(ledger.get(workKey)?.state).toBe("dead_letter");
    expect(ledger.loadSchedulerState(COLLECTOR_RECOVERY_STATE_KEY)).toEqual({
      digest: sha256(initial),
      serialized: initial,
    });
    ledger.close();
  });

  it("ramps 1, 5, 20, 100 and then repeats bounded cohorts of 100", () => {
    const ledger = open();
    for (let index = 0; index < 130; index += 1) deadLetter(ledger, index);
    let reservation = 0;
    const controller = new CollectorRecoveryController({
      ledger,
      random: () => 0,
      reservationId: () =>
        `00000000-0000-4000-8000-${String(++reservation).padStart(12, "0")}`,
    });
    let now = 20_000;
    controller.arm(now);

    for (const expected of [...COLLECTOR_RECOVERY_COHORT_SIZES, 4]) {
      const released = controller.cycle(now).released;
      expect(released).toHaveLength(expected);
      succeedReleased(ledger, released, now + 1);
      const resting = controller.cycle(now + 2).state;
      expect(resting.phase).toBe("resting");
      now = resting.nextActionAt ?? 0;
    }
    expect(controller.cycle(now).state.phase).toBe("complete");
    expect(controller.status().completed).toBe(130);
    ledger.close();
  }, 15_000);

  it("persists its chosen rest deadline and stops on a failed reservation", () => {
    const ledger = open();
    deadLetter(ledger, 1);
    deadLetter(ledger, 2);
    const first = new CollectorRecoveryController({
      ledger,
      random: () => 0,
      reservationId: () => "11111111-1111-4111-8111-111111111111",
    });
    first.arm(20_000);
    const released = first.cycle(20_000).released;
    succeedReleased(ledger, released, 20_001);
    const resting = first.cycle(20_002).state;
    expect(resting.nextActionAt).toBe(50_002);

    const restarted = new CollectorRecoveryController({
      ledger,
      random: () => 0.999,
      reservationId: () => "22222222-2222-4222-8222-222222222222",
    });
    expect(restarted.cycle(50_001).state).toEqual(resting);
    const second = restarted.cycle(50_002);
    expect(second.released).toHaveLength(1);
    const claim = ledger.claim("collector", 50_003, 10_000, [
      collectionWorkKinds().poemDetail,
    ]);
    if (!claim) throw new Error("recovery claim missing");
    ledger.deadLetter(claim, ERROR, 50_004);
    expect(restarted.cycle(50_005).state).toMatchObject({
      phase: "stopped",
      stopReason: ERROR,
    });
    expect(restarted.cycle(100_000).released).toEqual([]);
    ledger.close();
  });

  it("retains and resumes the exact reservation after an operator-rearmed source stop", () => {
    const ledger = open();
    deadLetter(ledger, 1);
    const controller = new CollectorRecoveryController({
      ledger,
      reservationId: () => "11111111-1111-4111-8111-111111111111",
    });
    controller.arm(20_000);
    const observing = controller.cycle(20_000).state;
    expect(observing.phase).toBe("observing");

    const stopped = controller.stop("SOURCE_HUMAN_REQUIRED", 20_001);
    expect(stopped).toMatchObject({
      phase: "stopped",
      reservation: observing.reservation,
      stopReason: "SOURCE_HUMAN_REQUIRED",
    });
    expect(controller.cycle(30_000).state).toEqual(stopped);
    expect(controller.arm(30_001)).toMatchObject({
      phase: "observing",
      reservation: observing.reservation,
      stopReason: null,
    });
    ledger.close();
  });
});
