import { randomUUID } from "node:crypto";

import { z } from "zod";

import type { Ledger, SchedulerStateRecord } from "../persistence/ledger.js";
import { canonicalJson, sha256 } from "../persistence/work-key.js";
import {
  collectionWorkKinds,
  collectorImplementationVersion,
  collectorSchemaVersion,
} from "./collector.js";

export const COLLECTOR_RECOVERY_STATE_KEY = "collector-recovery-budget:v1";
export const COLLECTOR_RECOVERY_COHORT_SIZES = [1, 5, 20, 100] as const;
export const COLLECTOR_RECOVERY_ERROR_CODES = [
  "SOURCE_CLASSICAL_LINE_COUNT_MISMATCH",
  "SOURCE_POEM_CONTENT_EMPTY",
  "SOURCE_POEM_TITLE_EMPTY",
] as const;
const MINIMUM_REST_MS = 30_000;
const MAXIMUM_REST_MS = 60_000;
const CollectorRecoveryPhaseSchema = z.enum([
  "complete",
  "disarmed",
  "observing",
  "ready",
  "resting",
  "stopped",
]);

const ReservationSchema = z.strictObject({
  reservationId: z.uuid(),
  workKeys: z
    .array(z.string().regex(/^[a-f\d]{64}$/))
    .min(1)
    .max(100),
});

const CollectorRecoveryStateSchema = z
  .strictObject({
    completed: z.number().int().nonnegative(),
    nextActionAt: z.number().int().nonnegative().nullable(),
    phase: CollectorRecoveryPhaseSchema,
    reservation: ReservationSchema.nullable(),
    schemaVersion: z.literal(1),
    stageIndex: z.number().int().min(0).max(3),
    stopReason: z.string().min(1).max(256).nullable(),
  })
  .superRefine((state, context) => {
    if (
      (state.phase === "observing" && state.reservation === null) ||
      (state.phase !== "observing" &&
        state.phase !== "stopped" &&
        state.reservation !== null)
    ) {
      context.addIssue({
        code: "custom",
        message:
          "Observing recovery requires a reservation and only a stopped recovery may retain it",
      });
    }
    if (
      (state.phase === "ready" || state.phase === "resting") !==
      (state.nextActionAt !== null)
    ) {
      context.addIssue({
        code: "custom",
        message: "Only ready or resting recovery state has a next action",
      });
    }
    if ((state.phase === "stopped") !== (state.stopReason !== null)) {
      context.addIssue({
        code: "custom",
        message: "Only stopped recovery state has a stop reason",
      });
    }
  });

export type CollectorRecoveryState = z.infer<
  typeof CollectorRecoveryStateSchema
>;

export interface CollectorRecoveryCycleResult {
  readonly released: readonly string[];
  readonly state: CollectorRecoveryState;
}

interface CollectorRecoveryPort {
  arm(now?: number): CollectorRecoveryState;
  cycle(now?: number): CollectorRecoveryCycleResult;
  status(): CollectorRecoveryState;
  stop(reason: string, now?: number): CollectorRecoveryState;
}

const DISARMED_STATE = {
  completed: 0,
  nextActionAt: null,
  phase: "disarmed",
  reservation: null,
  schemaVersion: 1,
  stageIndex: 0,
  stopReason: null,
} as const satisfies CollectorRecoveryState;

/** Bounded recovery only releases exact source-detail cohorts. The ordinary
 * collector remains the sole owner of browser work and source admission. */
export class CollectorRecoveryController implements CollectorRecoveryPort {
  readonly #ledger: Ledger;
  readonly #random: () => number;
  readonly #reservationId: () => string;

  constructor(options: {
    readonly ledger: Ledger;
    readonly random?: () => number;
    readonly reservationId?: () => string;
  }) {
    this.#ledger = options.ledger;
    this.#random = options.random ?? Math.random;
    this.#reservationId = options.reservationId ?? randomUUID;
  }

  arm(now = Date.now()): CollectorRecoveryState {
    const stored = this.#load();
    if (
      stored.state.phase === "ready" ||
      stored.state.phase === "resting" ||
      stored.state.phase === "observing"
    )
      return stored.state;
    const next = CollectorRecoveryStateSchema.parse(
      stored.state.reservation
        ? {
            ...stored.state,
            nextActionAt: null,
            phase: "observing",
            stopReason: null,
          }
        : { ...DISARMED_STATE, nextActionAt: now, phase: "ready" },
    );
    this.#save(next, stored.record?.digest ?? null, now);
    return next;
  }

  status(): CollectorRecoveryState {
    return this.#load().state;
  }

  stop(reason: string, now = Date.now()): CollectorRecoveryState {
    const stored = this.#load();
    if (stored.state.phase === "stopped") return stored.state;
    const next = CollectorRecoveryStateSchema.parse({
      ...stored.state,
      nextActionAt: null,
      phase: "stopped",
      stopReason: reason,
    });
    this.#save(next, stored.record?.digest ?? null, now);
    return next;
  }

  cycle(now = Date.now()): CollectorRecoveryCycleResult {
    const stored = this.#load();
    const state = stored.state;
    if (
      state.phase === "disarmed" ||
      state.phase === "stopped" ||
      state.phase === "complete"
    )
      return { released: [], state };

    if (state.phase === "observing") {
      const reservation = state.reservation;
      if (!reservation)
        throw new Error("COLLECTOR_RECOVERY_RESERVATION_MISSING");
      const works = reservation.workKeys.map((workKey) =>
        this.#ledger.get(workKey),
      );
      const failed = works.find(
        (work) => work === null || work.state === "dead_letter",
      );
      if (failed !== undefined) {
        const next = CollectorRecoveryStateSchema.parse({
          ...state,
          nextActionAt: null,
          phase: "stopped",
          reservation: null,
          stopReason:
            failed === null
              ? "COLLECTOR_RECOVERY_WORK_MISSING"
              : (failed.lastErrorCode ?? "COLLECTOR_RECOVERY_WORK_FAILED"),
        });
        this.#save(next, stored.record?.digest ?? null, now);
        return { released: [], state: next };
      }
      if (
        works.some(
          (work) => work?.state !== "succeeded" && work?.state !== "imported",
        )
      )
        return { released: [], state };

      const random = this.#random();
      if (!Number.isFinite(random) || random < 0 || random >= 1)
        throw new Error("COLLECTOR_RECOVERY_RANDOM_INVALID");
      const restMs =
        MINIMUM_REST_MS +
        Math.floor(random * (MAXIMUM_REST_MS - MINIMUM_REST_MS + 1));
      const next = CollectorRecoveryStateSchema.parse({
        ...state,
        completed: state.completed + reservation.workKeys.length,
        nextActionAt: now + restMs,
        phase: "resting",
        reservation: null,
        stageIndex: Math.min(
          state.stageIndex + 1,
          COLLECTOR_RECOVERY_COHORT_SIZES.length - 1,
        ),
      });
      this.#save(next, stored.record?.digest ?? null, now);
      return { released: [], state: next };
    }

    if (state.nextActionAt === null)
      throw new Error("COLLECTOR_RECOVERY_NEXT_ACTION_MISSING");
    if (state.nextActionAt > now) return { released: [], state };
    const limit = COLLECTOR_RECOVERY_COHORT_SIZES[state.stageIndex];
    if (limit === undefined)
      throw new Error("COLLECTOR_RECOVERY_STAGE_INVALID");
    const workKeys = this.#ledger.listRecoverableDeadLetters(
      {
        errorCodes: COLLECTOR_RECOVERY_ERROR_CODES,
        implementationVersion: collectorImplementationVersion(),
        kind: collectionWorkKinds().poemDetail,
        schemaVersion: collectorSchemaVersion(),
      },
      limit,
    );
    if (workKeys.length === 0) {
      const next = CollectorRecoveryStateSchema.parse({
        ...state,
        nextActionAt: null,
        phase: "complete",
      });
      this.#save(next, stored.record?.digest ?? null, now);
      return { released: [], state: next };
    }

    const reservation = ReservationSchema.parse({
      reservationId: this.#reservationId(),
      workKeys,
    });
    const next = CollectorRecoveryStateSchema.parse({
      ...state,
      nextActionAt: null,
      phase: "observing",
      reservation,
    });
    const serialized = canonicalJson(next);
    if (
      !this.#ledger.reserveDeadLetterCohort(
        {
          errorCodes: COLLECTOR_RECOVERY_ERROR_CODES,
          expectedStateDigest: stored.record?.digest ?? null,
          implementationVersion: collectorImplementationVersion(),
          kind: collectionWorkKinds().poemDetail,
          reservationId: reservation.reservationId,
          schedulerStateDigest: sha256(serialized),
          schedulerStateKey: COLLECTOR_RECOVERY_STATE_KEY,
          schedulerStateSerialized: serialized,
          schemaVersion: collectorSchemaVersion(),
          workKeys,
        },
        now,
      )
    )
      throw new Error("COLLECTOR_RECOVERY_RESERVATION_CONFLICT");
    return { released: workKeys, state: next };
  }

  #load(): {
    readonly record: null | SchedulerStateRecord;
    readonly state: CollectorRecoveryState;
  } {
    const record = this.#ledger.loadSchedulerState(
      COLLECTOR_RECOVERY_STATE_KEY,
    );
    if (!record) return { record: null, state: DISARMED_STATE };
    if (sha256(record.serialized) !== record.digest)
      throw new Error("COLLECTOR_RECOVERY_STATE_DIGEST_INVALID");
    return {
      record,
      state: CollectorRecoveryStateSchema.parse(JSON.parse(record.serialized)),
    };
  }

  #save(
    state: CollectorRecoveryState,
    expectedDigest: null | string,
    now: number,
  ): void {
    const serialized = canonicalJson(CollectorRecoveryStateSchema.parse(state));
    if (
      !this.#ledger.saveSchedulerState(
        COLLECTOR_RECOVERY_STATE_KEY,
        serialized,
        sha256(serialized),
        expectedDigest,
        now,
      )
    )
      throw new Error("COLLECTOR_RECOVERY_STATE_WRITE_CONFLICT");
  }
}
