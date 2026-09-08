import { readFile } from "node:fs/promises";

import {
  APPROVED_ENRICHMENT_PROFILES,
  isProductionResolutionSourceTarget,
  MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES,
  MAX_PRODUCTION_RESOLUTION_TARGETS,
  PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
  ProductionResolutionRequestSchema,
} from "@saqi/precedent-iso";
import { z } from "zod";

import type { ProductionResolutionDemandCache } from "../persistence/production-resolution-demand-cache.js";
import { requestScopedProductionResolutionWithTransport } from "../persistence/scoped-production-resolution.js";
import type {
  AuthenticatedPublicationTransport,
  PublicationAuthPreflight,
} from "./publication-auth-client.js";

export interface DemandDrivenProductionResolutionRefresherOptions {
  readonly auth: Pick<AuthenticatedPublicationTransport, "transport">;
  readonly authorize: (
    signal?: AbortSignal,
  ) => Promise<PublicationAuthPreflight> | PublicationAuthPreflight;
  readonly bootstrapRequestPath?: null | string;
  readonly cache: ProductionResolutionDemandCache;
  readonly endpoint: string;
  readonly now?: () => number;
  readonly retryBackoffMs: number;
  readonly unresolvedRetryMs?: number;
  /**
   * Wakes exact fanout jobs after their production identity resolves. Keys are
   * acknowledged from the durable cache outbox only when the ledger confirms
   * that delivering the wake is safe.
   */
  readonly wakePublications?: (workKeys: readonly string[]) => {
    readonly acknowledge: readonly string[];
  };
}

interface ResolutionScopePruneMetrics {
  readonly models: number;
  readonly scopes: number;
  readonly targets: number;
}

const RESOLUTION_SCOPE_PRUNE_INTERVAL_MS = 5 * 60_000;
const PUBLICATION_WAKEUP_DELIVERY_LIMIT = 500;
const EMPTY_PRUNE_METRICS: ResolutionScopePruneMetrics = {
  models: 0,
  scopes: 0,
  targets: 0,
};
const ActiveBootstrapEnvelopeSchema = z.strictObject({
  schemaId: z.literal(PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID),
  schemaVersion: z.literal([1, 2, 3]),
  targets: z.array(z.unknown()).min(1).max(MAX_PRODUCTION_RESOLUTION_TARGETS),
});
const BootstrapTargetSchema = z.looseObject({
  modelKeys: z.array(z.string().trim().min(1).max(100)).min(1).max(32),
});
const ACTIVE_MODEL_KEYS: ReadonlySet<string> = new Set(
  APPROVED_ENRICHMENT_PROFILES.map(({ modelKey }) => modelKey),
);

export type DemandDrivenProductionResolutionRefreshResult =
  | {
      readonly errorCode: string;
      readonly nextWakeAt: number;
      readonly state: "auth_wait" | "retry_wait" | "split";
    }
  | {
      readonly nextWakeAt: number;
      readonly pruned: ResolutionScopePruneMetrics;
      readonly state: "idle";
    }
  | {
      readonly nextWakeAt: number;
      readonly pruned: ResolutionScopePruneMetrics;
      readonly state: "merged" | "replayed";
      readonly targets: number;
    };

// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete orchestration lane composes injected stores and transport; it has no independently selected implementation.
export class DemandDrivenProductionResolutionRefresher {
  readonly #options: DemandDrivenProductionResolutionRefresherOptions;
  readonly #now: () => number;
  #bootstrapLoaded = false;
  #nextPruneAt = 0;

  constructor(options: DemandDrivenProductionResolutionRefresherOptions) {
    this.#options = options;
    this.#now = options.now ?? Date.now;
  }

  async runOnce(
    signal?: AbortSignal,
  ): Promise<DemandDrivenProductionResolutionRefreshResult> {
    await this.#loadBootstrap();
    const wakeups = this.#drainPublicationWakeups();
    if (!this.#options.cache.hasDueWork()) return this.#idle(wakeups);
    let authorization: PublicationAuthPreflight;
    try {
      authorization = await this.#options.authorize(signal);
    } catch (error) {
      this.#pruneIfDue(wakeups.remaining);
      return {
        errorCode: errorCode(error),
        nextWakeAt: this.#now() + this.#options.retryBackoffMs,
        state: "auth_wait",
      };
    }
    if (authorization.state !== "ready") {
      const now = this.#now();
      const retryAt = authorization.status.retryAt;
      this.#pruneIfDue(wakeups.remaining);
      return {
        errorCode:
          authorization.errorCode ?? "PRODUCTION_RESOLUTION_AUTH_PAUSED",
        nextWakeAt: Math.max(now + this.#options.retryBackoffMs, retryAt ?? 0),
        state: "auth_wait",
      };
    }
    const claim = this.#options.cache.claim();
    if (!claim) return this.#idle(wakeups);
    let resolution: Awaited<
      ReturnType<typeof requestScopedProductionResolutionWithTransport>
    >;
    let state: "merged" | "replayed";
    try {
      resolution = await requestScopedProductionResolutionWithTransport({
        endpoint: this.#options.endpoint,
        now: this.#now,
        request: claim.request,
        ...(signal ? { signal } : {}),
        transport: this.#options.auth.transport,
      });
      state = this.#options.cache.complete(claim, resolution.response);
    } catch (error) {
      const code = errorCode(error);
      if (isResolutionConflict(code)) {
        const retryAt =
          this.#now() + (this.#options.unresolvedRetryMs ?? 30 * 60_000);
        const children = this.#options.cache.bisect(claim, retryAt, code);
        return {
          errorCode: code,
          // A rejected singleton is deferred in the cache, but must not put the
          // entire demand lane to sleep while unrelated work is ready.
          nextWakeAt: this.#now(),
          state: children > 0 ? "split" : "retry_wait",
        };
      }
      const retryAt = this.#now() + this.#options.retryBackoffMs;
      this.#options.cache.retry(claim, code, retryAt);
      return { errorCode: code, nextWakeAt: retryAt, state: "retry_wait" };
    }
    const delivered = this.#drainPublicationWakeups();
    const pruned = this.#pruneIfDue(delivered.remaining);
    return {
      nextWakeAt: this.#now(),
      pruned,
      state,
      targets: claim.request.targets.length,
    };
  }

  #idle(wakeups: {
    progressed: boolean;
    remaining: boolean;
  }): DemandDrivenProductionResolutionRefreshResult {
    return {
      nextWakeAt:
        wakeups.remaining && wakeups.progressed
          ? this.#now()
          : this.#now() + 30_000,
      pruned: this.#pruneIfDue(wakeups.remaining),
      state: "idle",
    };
  }

  #drainPublicationWakeups(): { progressed: boolean; remaining: boolean } {
    const wake = this.#options.wakePublications;
    if (!wake) return { progressed: false, remaining: false };
    let progressed = false;
    for (let batch = 0; batch < 8; batch += 1) {
      const publication = this.#options.cache.listPublicationWakeups(
        PUBLICATION_WAKEUP_DELIVERY_LIMIT,
      );
      const terminalCanonical =
        batch === 0
          ? this.#options.cache.listTerminalCanonicalWakeups(
              PUBLICATION_WAKEUP_DELIVERY_LIMIT,
            )
          : [];
      const fingerprint = this.#options.cache.listFingerprintWakeups(
        PUBLICATION_WAKEUP_DELIVERY_LIMIT,
      );
      const pending = new Set<string>();
      for (
        let index = 0;
        index < PUBLICATION_WAKEUP_DELIVERY_LIMIT;
        index += 1
      ) {
        const publicationKey = publication[index]?.workKey;
        if (publicationKey) pending.add(publicationKey);
        if (pending.size === PUBLICATION_WAKEUP_DELIVERY_LIMIT) break;
        const terminalCanonicalKey = terminalCanonical[index]?.workKey;
        if (terminalCanonicalKey) pending.add(terminalCanonicalKey);
        if (pending.size === PUBLICATION_WAKEUP_DELIVERY_LIMIT) break;
        const fingerprintKey = fingerprint[index]?.workKey;
        if (fingerprintKey) pending.add(fingerprintKey);
        if (pending.size === PUBLICATION_WAKEUP_DELIVERY_LIMIT) break;
      }
      if (pending.size === 0) return { progressed, remaining: false };
      const result = wake([...pending]);
      if (result.acknowledge.length === 0)
        return { progressed, remaining: true };
      const acknowledged = new Set(result.acknowledge);
      let terminalCanonicalRetired = 0;
      for (const { workKey } of terminalCanonical) {
        if (!acknowledged.has(workKey)) continue;
        this.#options.cache.retirePublicationWaiter(workKey);
        terminalCanonicalRetired += 1;
      }
      if (terminalCanonicalRetired > 0)
        this.#options.cache.retireUnboundCanonicalDemands();
      this.#options.cache.acknowledgePublicationWakeups(result.acknowledge);
      this.#options.cache.acknowledgeFingerprintWakeups(result.acknowledge);
      progressed = true;
      if (result.acknowledge.length !== pending.size)
        return { progressed, remaining: true };
      const remaining =
        this.#options.cache.listPublicationWakeups(1).length > 0 ||
        this.#options.cache.listFingerprintWakeups(1).length > 0;
      if (!remaining) return { progressed, remaining: false };
    }
    return {
      progressed,
      remaining:
        this.#options.cache.listPublicationWakeups(1).length > 0 ||
        this.#options.cache.listFingerprintWakeups(1).length > 0,
    };
  }

  #pruneIfDue(wakeupsRemaining: boolean): ResolutionScopePruneMetrics {
    const now = this.#now();
    if (wakeupsRemaining || now < this.#nextPruneAt) return EMPTY_PRUNE_METRICS;
    this.#nextPruneAt = now + RESOLUTION_SCOPE_PRUNE_INTERVAL_MS;
    return this.#options.cache.pruneExpiredScopes();
  }

  async #loadBootstrap(): Promise<void> {
    if (this.#bootstrapLoaded) return;
    const path = this.#options.bootstrapRequestPath;
    if (!path) {
      this.#bootstrapLoaded = true;
      return;
    }
    const serialized = await readFile(path, "utf8");
    if (Buffer.byteLength(serialized) > MAX_PRODUCTION_RESOLUTION_REQUEST_BYTES)
      throw new Error("PRODUCTION_RESOLUTION_REQUEST_TOO_LARGE");
    const request = activeBootstrapRequest(JSON.parse(serialized));
    if (request === null) {
      this.#bootstrapLoaded = true;
      return;
    }
    if (request.schemaVersion === 3)
      throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_CACHE_UNSUPPORTED");
    for (const target of request.targets) {
      if (isProductionResolutionSourceTarget(target))
        this.#options.cache.registerDemand({
          modelKeys: target.modelKeys,
          priority: 0,
          sourceAuthorSlug: target.sourceAuthorSlug,
          sourcePoemId: target.sourcePoemId,
        });
      else
        for (const modelKey of target.modelKeys)
          this.#options.cache.registerPublicationDemand(
            target.poemId,
            modelKey,
            target.sourceRevisionId,
            0,
          );
    }
    this.#bootstrapLoaded = true;
  }
}

/** Normalizes a stale derived bootstrap onto the currently executable models. */
export function activeBootstrapRequest(input: unknown) {
  const envelope = ActiveBootstrapEnvelopeSchema.parse(input);
  if (envelope.schemaVersion === 3)
    return ProductionResolutionRequestSchema.parse(envelope);
  const targets = envelope.targets.flatMap((target) => {
    const parsed = BootstrapTargetSchema.parse(target);
    const modelKeys: string[] = [];
    for (const modelKey of new Set(parsed.modelKeys))
      if (ACTIVE_MODEL_KEYS.has(modelKey)) modelKeys.push(modelKey);
    return modelKeys.length === 0 ? [] : [{ ...parsed, modelKeys }];
  });
  if (targets.length === 0) return null;
  return ProductionResolutionRequestSchema.parse({ ...envelope, targets });
}

function isResolutionConflict(code: string): boolean {
  return (
    code === "PRODUCTION_RESOLUTION_HTTP_409" ||
    [
      "PRODUCTION_RESOLUTION_LEGACY_OWNERSHIP_CONFLICT",
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
      "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
      "PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED",
      "PRODUCTION_RESOLUTION_MODEL_SCOPE_CONFLICT",
      "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
      "PRODUCTION_RESOLUTION_TARGET_COUNT_MISMATCH",
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
      "PRODUCTION_RESOLUTION_WRITER_CONTROL_INVALID",
    ].includes(code)
  );
}

function errorCode(error: unknown): string {
  if (!(error instanceof Error)) return "PRODUCTION_RESOLUTION_REFRESH_FAILED";
  return /^[A-Z][A-Z\d_:.-]{0,255}$/.test(error.message)
    ? error.message
    : "PRODUCTION_RESOLUTION_REFRESH_FAILED";
}
