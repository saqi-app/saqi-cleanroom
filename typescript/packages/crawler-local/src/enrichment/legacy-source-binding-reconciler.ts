import {
  type CanonicalPoemBindingV1,
  type PoemEnrichmentInput,
  PoemEnrichmentInputSchema,
} from "@saqi/precedent-iso";
import { z } from "zod";

import type { Ledger } from "../persistence/ledger.js";
import type { ProductionResolutionDemandCache } from "../persistence/production-resolution-demand-cache.js";
import { inputHash, sha256 } from "../persistence/work-key.js";
import { bindCollectedPoem } from "../publication/corpus-import-actions.js";
import {
  POEM_ENRICHMENT_SCHEMA_VERSION,
  POEM_ENRICHMENT_V2_SCHEMA_VERSION,
  SOL_ENRICHMENT_WORK_KIND,
} from "./sol-coordinator.js";
import {
  ENRICHMENT_PROVIDER_SPECS,
  SOL_PIPELINE_VERSION,
} from "./sol-runner.js";

const BatchSizeSchema = z.number().int().min(1).max(1_000);
const CanonicalPoemUuidSchema = z.uuid();
const CanonicalPoemHashSchema = z.string().regex(/^[a-f\d]{64}$/);
const READY_STATES: ReadonlySet<string> = new Set([
  "pending",
  "quota_wait",
  "retry_wait",
]);
const STATE_KEY = "legacy-source-binding-reconciliation-v1";
const StateSchema = z.strictObject({
  cursor: z
    .strictObject({
      createdAt: z.number().int().nonnegative(),
      priority: z.number().int().min(-1_000_000).max(1_000_000),
      workKey: z.string().regex(/^[a-f\d]{64}$/),
    })
    .nullable(),
  pass: z.number().int().nonnegative(),
  schemaVersion: z.literal(1),
});
type State = z.infer<typeof StateSchema>;

export interface LegacySourceBindingReconciliationSummary {
  readonly conflicts: number;
  readonly pendingResolution: number;
  readonly scanned: number;
  readonly seeded: number;
  readonly superseded: number;
}

interface LegacySourceBindingReconciliationPort {
  cycle(now?: number): LegacySourceBindingReconciliationSummary;
}

/** Converts legacy canonical inputs into source-bound v2 work without invoking
 * a paid model. Production resolution remains the authority for every binding. */
export class LegacySourceBindingReconciler implements LegacySourceBindingReconciliationPort {
  readonly #batchSize: number;
  readonly #cache: ProductionResolutionDemandCache;
  readonly #ledger: Ledger;

  constructor(options: {
    readonly batchSize?: number;
    readonly cache: ProductionResolutionDemandCache;
    readonly ledger: Ledger;
  }) {
    this.#batchSize = BatchSizeSchema.parse(options.batchSize ?? 50);
    this.#cache = options.cache;
    this.#ledger = options.ledger;
  }

  cycle(now = Date.now()): LegacySourceBindingReconciliationSummary {
    const summaries = [
      ...new Set(["sol-word-gloss-v2", SOL_PIPELINE_VERSION]),
    ].map((version) => this.#cycleVersion(version, now));
    return summaries.reduce(
      (total, summary) => ({
        conflicts: total.conflicts + summary.conflicts,
        pendingResolution: total.pendingResolution + summary.pendingResolution,
        scanned: total.scanned + summary.scanned,
        seeded: total.seeded + summary.seeded,
        superseded: total.superseded + summary.superseded,
      }),
      {
        conflicts: 0,
        pendingResolution: 0,
        scanned: 0,
        seeded: 0,
        superseded: 0,
      },
    );
  }

  #cycleVersion(
    implementationVersion: string,
    now: number,
  ): LegacySourceBindingReconciliationSummary {
    const stateKey =
      implementationVersion === "sol-word-gloss-v2"
        ? STATE_KEY
        : `${STATE_KEY}:${implementationVersion}`;
    const stored = this.#ledger.loadSchedulerState(stateKey);
    const state = stored
      ? StateSchema.parse(JSON.parse(stored.serialized))
      : defaultState();
    if (
      stored &&
      sha256(Buffer.from(stored.serialized, "utf8")) !== stored.digest
    ) {
      throw new Error("LEGACY_SOURCE_BINDING_STATE_DIGEST_INVALID");
    }
    const scan = this.#ledger.listReadyWorkAfter(
      state.cursor,
      SOL_ENRICHMENT_WORK_KIND,
      this.#batchSize,
      {
        implementationVersion,
        schemaVersion: POEM_ENRICHMENT_SCHEMA_VERSION,
      },
      now,
    );
    const sources = scan.items;
    let conflicts = 0;
    let pendingResolution = 0;
    let seeded = 0;
    let superseded = 0;
    for (const source of sources) {
      if (!READY_STATES.has(source.state)) continue;
      // Paid attempts may already have durable artifacts/checkpoints under this
      // work key. Bind those through approved-artifact attachment, never replay.
      if (source.attemptCount > 0) continue;
      const parsedInput = PoemEnrichmentInputSchema.safeParse(source.input);
      if (!parsedInput.success) {
        if (
          this.#ledger.classifyUnclaimedTerminal(
            source.workKey,
            "LEGACY_ENRICHMENT_INPUT_INVALID",
            now,
          )
        )
          conflicts += 1;
        continue;
      }
      const input = parsedInput.data;
      if (!isCanonicalPoemId(input.poemId)) {
        if (
          this.#ledger.classifyUnclaimedTerminal(
            source.workKey,
            "LEGACY_ENRICHMENT_CANONICAL_ID_INVALID",
            now,
          )
        )
          conflicts += 1;
        continue;
      }
      let resolution = this.#cache.resolveCanonicalEnrichment(
        input,
        ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
      );
      if (
        resolution.status === "pending" &&
        this.#cache.terminalCanonicalFailure(
          input.poemId,
          ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          input.sourceRevisionId,
        ) === "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED"
      ) {
        // A retired canonical ID does not mean its exact source disappeared.
        // Production fingerprint resolution verifies title, author and lines;
        // ownership conflicts never enter this fallback.
        resolution = this.#cache.resolveFingerprintEnrichment(
          input,
          ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
        );
        if (resolution.status === "pending") {
          const failure = this.#cache.terminalFingerprintFailure(
            input,
            ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          );
          if (failure) {
            if (
              this.#ledger.classifyUnclaimedTerminal(
                source.workKey,
                failure,
                now,
              )
            )
              conflicts += 1;
            this.#cache.retireFingerprintDemand(
              input,
              ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            );
          } else {
            this.#cache.registerFingerprintDemand(
              input,
              ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
              source.priority,
            );
            pendingResolution += 1;
          }
          this.#cache.retireCanonicalDemand(
            input.poemId,
            ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            input.sourceRevisionId,
          );
          continue;
        }
      }
      if (resolution.status === "pending") {
        const terminalFailure = this.#cache.terminalCanonicalFailure(
          input.poemId,
          ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          input.sourceRevisionId,
        );
        if (terminalFailure) {
          if (
            this.#ledger.classifyUnclaimedTerminal(
              source.workKey,
              terminalFailure,
              now,
            )
          )
            conflicts += 1;
          this.#cache.retireCanonicalDemand(
            input.poemId,
            ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
            input.sourceRevisionId,
          );
          continue;
        }
        this.#cache.registerPublicationDemand(
          input.poemId,
          ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
          input.sourceRevisionId,
          source.priority,
        );
        pendingResolution += 1;
        continue;
      }
      if (resolution.status === "conflict") {
        if (
          this.#ledger.classifyUnclaimedTerminal(
            source.workKey,
            resolution.code,
            now,
          )
        )
          conflicts += 1;
        continue;
      }
      const replacement = boundInput(input, resolution.binding);
      const result = this.#ledger.supersedeUnclaimed(
        source.workKey,
        {
          implementationVersion: source.implementationVersion,
          input: replacement,
          inputHash: inputHash(replacement),
          kind: SOL_ENRICHMENT_WORK_KIND,
          priority: source.priority,
          schemaVersion: POEM_ENRICHMENT_V2_SCHEMA_VERSION,
        },
        "SOURCE_BINDING_SUPERSEDED",
        now,
      );
      superseded += 1;
      if (result.inserted) seeded += 1;
    }
    let nextState: State;
    if (scan.done) {
      nextState = { cursor: null, pass: state.pass + 1, schemaVersion: 1 };
    } else {
      if (scan.cursor === null)
        throw new Error("LEGACY_SOURCE_BINDING_CURSOR_MISSING");
      nextState = {
        cursor: scan.cursor,
        pass: state.pass,
        schemaVersion: 1,
      };
    }
    if (sources.length > 0 || state.cursor !== null) {
      const serialized = `${JSON.stringify(nextState)}\n`;
      if (
        !this.#ledger.saveSchedulerState(
          stateKey,
          serialized,
          sha256(Buffer.from(serialized, "utf8")),
          stored?.digest ?? null,
          now,
        )
      ) {
        throw new Error("LEGACY_SOURCE_BINDING_STATE_WRITE_CONFLICT");
      }
    }
    return {
      conflicts,
      pendingResolution,
      scanned: sources.length,
      seeded,
      superseded,
    };
  }
}

function isCanonicalPoemId(value: string): boolean {
  return (
    CanonicalPoemUuidSchema.safeParse(value).success ||
    CanonicalPoemHashSchema.safeParse(value).success
  );
}

function defaultState(): State {
  return { cursor: null, pass: 0, schemaVersion: 1 };
}

function boundInput(
  input: PoemEnrichmentInput,
  canonicalBinding: CanonicalPoemBindingV1,
) {
  return bindCollectedPoem(
    {
      ...input,
      authorArabic: canonicalBinding.authorNameArabic,
      poemId: canonicalBinding.poemId,
    },
    canonicalBinding,
  );
}
