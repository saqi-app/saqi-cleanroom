import { setTimeout as delay } from "node:timers/promises";

import {
  type AuthorInventoryPageProjection,
  AuthorInventoryPageSchema,
  canonicalAuthorUrl,
  canonicalInventoryUrl,
  certifyAuthorInventoryPages,
  currentSource,
  sourceInventoryUrl,
} from "@saqi/source-adapter";
import { z } from "zod";

import type { Artifact } from "../persistence/artifact-store.js";
import type { Ledger, OriginLease } from "../persistence/ledger.js";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key.js";
import { isNetworkFailureText } from "../runtime/network-resilience.js";
import { SourceBrowserError } from "./collection-source-browser.js";
import { seedAuthorManifests } from "./collector.js";

export const AUTHOR_INVENTORY_WORK_SCHEMA = "author-inventory-work@1";
const INVENTORY_CHECKPOINT_KIND = "author-inventory-progress-v1";

interface AuthorInventoryIdentity {
  readonly certificateVersion: string;
  readonly collectorVersion: string;
  readonly discoveryKind: string;
}

function authorInventoryIdentity(): AuthorInventoryIdentity {
  const sourceName = currentSource().name;
  return {
    certificateVersion: `${sourceName}-author-inventory-certificate-v1`,
    collectorVersion: `${sourceName}-author-inventory-collector-v1`,
    discoveryKind: `${sourceName}_author_inventory_discovery`,
  };
}

const RefreshGenerationSchema = z.string().regex(/^[\w.-]{1,64}$/);
const ProductionAuthorSchema = z
  .object({ canonical_url: z.url().max(2_048) })
  .strict();
const ProductionAuthorsSchema = z.array(ProductionAuthorSchema).max(50_000);
const RawProductionAuthorsSchema = z.array(z.unknown()).max(50_000);
type InventoryPass = 1 | 2;
const AuthorInventoryPassSchema = z.strictObject({
  pageArtifactHashes: z.array(z.string().regex(/^[\da-f]{64}$/)),
  pages: z.array(AuthorInventoryPageSchema),
  pass: z.literal([1, 2]),
  schemaId: z.literal("saqi.author-inventory-pass"),
  schemaVersion: z.literal(1),
});

export interface AuthorInventoryLaneStatus {
  readonly alreadySeededManifests: number;
  readonly baselineAuthors: number;
  readonly certificateDigest: string;
  readonly certificateVersion: string;
  readonly complete: true;
  readonly discoveredAuthors: number;
  readonly duplicateReferences: number;
  readonly existingAuthors: number;
  readonly insertedManifests: number;
  readonly kind: string;
  readonly missingBaselineAuthorIds: readonly string[];
  readonly newAuthorIds: readonly string[];
  readonly pagesPerPass: number;
  readonly refreshGeneration: string;
}

export interface CertifyAndSeedInventoryOptions {
  readonly firstPass: readonly AuthorInventoryPageProjection[];
  readonly ledger: Ledger;
  readonly priority?: number;
  readonly productionSourceAuthors: unknown;
  readonly refreshGeneration: string;
  readonly secondPass: readonly AuthorInventoryPageProjection[];
}

export async function certifyAndSeedAuthorInventory(
  options: CertifyAndSeedInventoryOptions,
): Promise<AuthorInventoryLaneStatus> {
  const identity = authorInventoryIdentity();
  const refreshGeneration = RefreshGenerationSchema.parse(
    options.refreshGeneration,
  );
  const productionRecords = ProductionAuthorsSchema.parse(
    options.productionSourceAuthors,
  );
  const productionIds = new Set<string>();
  for (const record of productionRecords) {
    const id = canonicalAuthorUrl(record.canonical_url).canonicalId;
    if (productionIds.has(id)) {
      throw new Error("SOURCE_PRODUCTION_AUTHOR_DUPLICATE");
    }
    productionIds.add(id);
  }

  const certificate = await certifyAuthorInventoryPages(
    options.firstPass,
    options.secondPass,
  );
  const discoveredIds = new Set(
    certificate.authors.map((author) => author.canonicalId),
  );
  const newAuthorIds = certificate.authors
    .map((author) => author.canonicalId)
    .filter((id) => !productionIds.has(id));
  const missingBaselineAuthorIds = [...productionIds]
    .filter((id) => !discoveredIds.has(id))
    .toSorted();
  const seeds = seedAuthorManifests(
    options.ledger,
    {
      authors: certificate.authors,
      declaredPoems: certificate.authors.reduce(
        (sum, author) => sum + (author.poemCount ?? 0),
        0,
      ),
      unknownPoemCounts: certificate.authors.filter(
        (author) => author.poemCount === null,
      ).length,
    },
    options.priority,
    refreshGeneration,
  );
  const insertedManifests = seeds.filter((seed) => seed.inserted).length;

  return {
    alreadySeededManifests: seeds.length - insertedManifests,
    baselineAuthors: productionIds.size,
    certificateDigest: certificate.digest,
    certificateVersion: identity.certificateVersion,
    complete: true,
    discoveredAuthors: certificate.authors.length,
    duplicateReferences: certificate.duplicateReferences,
    existingAuthors: certificate.authors.length - newAuthorIds.length,
    insertedManifests,
    kind: identity.discoveryKind,
    missingBaselineAuthorIds,
    newAuthorIds,
    pagesPerPass: certificate.pages,
    refreshGeneration,
  };
}

const CheckpointSchema = z.strictObject({
  authorReferences: z.int().nonnegative(),
  bytes: z.int().nonnegative(),
  firstPassArtifactHash: z
    .string()
    .regex(/^[\da-f]{64}$/)
    .nullable(),
  nextPage: z.int().positive(),
  pageArtifactHashes: z.array(z.string().regex(/^[\da-f]{64}$/)).max(10_000),
  pass: z.literal([1, 2]),
  terminal: z.boolean(),
});

export interface AuthorInventoryPageBrowser {
  collectAuthorInventoryPage(
    inventoryValue: string,
    signal: AbortSignal,
  ): Promise<AuthorInventoryPageProjection>;
}

export interface AuthorInventoryCollectionOptions {
  /** Test-only crash boundary after a durable page checkpoint. */
  readonly afterPageCheckpoint?:
    ((pass: InventoryPass, page: number) => void) | undefined;
  readonly artifacts: AuthorInventoryArtifactStore;
  readonly browser: AuthorInventoryPageBrowser;
  readonly ledger: Ledger;
  readonly maximumAuthors?: number;
  readonly maximumBytes?: number;
  readonly maximumPages?: number;
  readonly minimumOriginGapMs?: number;
  readonly now?: () => number;
  readonly owner?: string;
  readonly priority?: number;
  readonly productionSourceAuthors: unknown;
  readonly refreshGeneration: string;
}

/** The minimal durable artifact boundary required by inventory collection. */
export interface AuthorInventoryArtifactStore {
  put(value: string | Uint8Array): Promise<Artifact>;
  read(hash: string): Promise<Buffer>;
}

export type AuthorInventoryCollectionResult =
  | {
      readonly status: AuthorInventoryLaneStatus;
      readonly stopped: "succeeded";
    }
  | { readonly stopped: "aborted" | "idle" };

/** Lifecycle boundary consumed by the unified collection rig. */
export interface AuthorInventoryCollectorPort {
  run(signal: AbortSignal): Promise<AuthorInventoryCollectionResult>;
  seed(): { readonly inserted: boolean; readonly workKey: string };
}

export class AuthorInventoryCollector implements AuthorInventoryCollectorPort {
  readonly #afterPageCheckpoint:
    ((pass: InventoryPass, page: number) => void) | undefined;
  readonly #artifacts: AuthorInventoryArtifactStore;
  readonly #browser: AuthorInventoryPageBrowser;
  readonly #ledger: Ledger;
  readonly #maximumAuthors: number;
  readonly #maximumBytes: number;
  readonly #maximumPages: number;
  readonly #minimumOriginGapMs: number;
  readonly #now: () => number;
  readonly #owner: string;
  readonly #priority: number;
  readonly #productionSourceAuthors: unknown;
  readonly #productionSourceAuthorsHash: string;
  readonly #refreshGeneration: string;

  constructor(options: AuthorInventoryCollectionOptions) {
    this.#artifacts = options.artifacts;
    this.#browser = options.browser;
    this.#ledger = options.ledger;
    this.#maximumAuthors = bounded(options.maximumAuthors ?? 50_000, 1, 50_000);
    this.#maximumBytes = bounded(
      options.maximumBytes ?? 64 * 1024 * 1024,
      1_024,
      256 * 1024 * 1024,
    );
    this.#maximumPages = bounded(options.maximumPages ?? 2_000, 1, 10_000);
    this.#minimumOriginGapMs = bounded(
      options.minimumOriginGapMs ?? 13_000,
      0,
      60 * 60_000,
    );
    this.#now = options.now ?? Date.now;
    this.#owner = options.owner ?? `author-inventory-${String(process.pid)}`;
    this.#priority = options.priority ?? 0;
    this.#productionSourceAuthors = RawProductionAuthorsSchema.parse(
      options.productionSourceAuthors,
    );
    this.#productionSourceAuthorsHash = sha256(
      canonicalJson(this.#productionSourceAuthors),
    );
    this.#refreshGeneration = RefreshGenerationSchema.parse(
      options.refreshGeneration,
    );
    this.#afterPageCheckpoint = options.afterPageCheckpoint;
  }

  seed(): { readonly inserted: boolean; readonly workKey: string } {
    const identity = authorInventoryIdentity();
    const input = {
      productionSourceAuthorsHash: this.#productionSourceAuthorsHash,
      refreshGeneration: this.#refreshGeneration,
    };
    return this.#ledger.seed(
      {
        implementationVersion: identity.collectorVersion,
        input,
        inputHash: inputHash(input),
        kind: identity.discoveryKind,
        priority: this.#priority,
        schemaVersion: AUTHOR_INVENTORY_WORK_SCHEMA,
      },
      this.#now(),
    );
  }

  async run(signal: AbortSignal): Promise<AuthorInventoryCollectionResult> {
    if (signal.aborted) return { stopped: "aborted" };
    const identity = authorInventoryIdentity();
    this.#ledger.recoverExpired(this.#now());
    const claim = this.#ledger.claim(
      this.#owner,
      this.#now(),
      30 * 60_000,
      [identity.discoveryKind],
      {
        implementationVersion: identity.collectorVersion,
        schemaVersion: AUTHOR_INVENTORY_WORK_SCHEMA,
      },
    );
    if (!claim) return { stopped: "idle" };
    let leaseFailure: unknown = null;
    const leaseAbort = new AbortController();
    const operationSignal = AbortSignal.any([signal, leaseAbort.signal]);
    const heartbeat = setInterval(() => {
      try {
        this.#ledger.renew(claim, this.#now(), 30 * 60_000);
      } catch (error) {
        leaseFailure = error;
        leaseAbort.abort(error);
      }
    }, 60_000);
    heartbeat.unref();
    try {
      const status = await this.#collect(
        claim.work.workKey,
        claim,
        operationSignal,
      );
      if (leaseFailure)
        throw leaseFailure instanceof Error
          ? leaseFailure
          : new Error("AUTHOR_INVENTORY_LEASE_FAILED", {
              cause: leaseFailure,
            });
      return { status, stopped: "succeeded" };
    } catch (error) {
      if (operationSignal.aborted && !leaseFailure) {
        this.#ledger.operatorRelease(
          claim,
          "AUTHOR_INVENTORY_OPERATOR_STOP",
          this.#now(),
        );
        return { stopped: "aborted" };
      }
      this.#ledger.retry(
        claim,
        "AUTHOR_INVENTORY_COLLECTION_FAILED",
        this.#now() + 60_000,
        this.#now(),
      );
      throw error;
    } finally {
      clearInterval(heartbeat);
    }
  }

  async #collect(
    workKey: string,
    claim: NonNullable<ReturnType<Ledger["claim"]>>,
    signal: AbortSignal,
  ): Promise<AuthorInventoryLaneStatus> {
    const stored = this.#ledger.latestCheckpoint(
      workKey,
      INVENTORY_CHECKPOINT_KIND,
    );
    let progress = stored
      ? CheckpointSchema.parse(stored.payload)
      : {
          authorReferences: 0,
          bytes: 0,
          firstPassArtifactHash: null,
          nextPage: 1,
          pageArtifactHashes: [],
          pass: 1 as const,
          terminal: false,
        };
    let firstPass: null | readonly AuthorInventoryPageProjection[] = null;
    if (progress.firstPassArtifactHash)
      firstPass = await this.#readPass(progress.firstPassArtifactHash, 1);
    while (!signal.aborted) {
      if (progress.terminal) {
        // eslint-disable-next-line no-await-in-loop -- Each pass artifact must be reconstructed from the preceding durable checkpoint before state advances.
        const pages = await this.#readPages(progress.pageArtifactHashes);
        // eslint-disable-next-line no-await-in-loop -- Pass artifacts are published sequentially so a resume observes one canonical checkpoint.
        const passArtifact = await this.#artifacts.put(
          `${canonicalJson({
            pageArtifactHashes: progress.pageArtifactHashes,
            pages,
            pass: progress.pass,
            schemaId: "saqi.author-inventory-pass",
            schemaVersion: 1,
          })}\n`,
        );
        if (progress.pass === 1) {
          firstPass = pages;
          progress = {
            authorReferences: 0,
            bytes: 0,
            firstPassArtifactHash: passArtifact.hash,
            nextPage: 1,
            pageArtifactHashes: [],
            pass: 2,
            terminal: false,
          };
          this.#ledger.checkpoint(
            claim,
            {
              artifactHash: passArtifact.hash,
              kind: INVENTORY_CHECKPOINT_KIND,
              payload: progress,
            },
            this.#now(),
          );
          continue;
        }
        if (!firstPass || !progress.firstPassArtifactHash)
          throw new Error("SOURCE_AUTHOR_INVENTORY_FIRST_PASS_MISSING");
        this.#ledger.checkpoint(
          claim,
          {
            artifactHash: passArtifact.hash,
            kind: INVENTORY_CHECKPOINT_KIND,
            payload: progress,
          },
          this.#now(),
        );
        // eslint-disable-next-line no-await-in-loop -- Certification must follow the durable second-pass checkpoint.
        const status = await certifyAndSeedAuthorInventory({
          firstPass,
          ledger: this.#ledger,
          priority: this.#priority,
          productionSourceAuthors: this.#productionSourceAuthors,
          refreshGeneration: this.#refreshGeneration,
          secondPass: pages,
        });
        // eslint-disable-next-line no-await-in-loop -- Certification is durably published before the claimed work can succeed.
        const certificate = await this.#artifacts.put(
          `${canonicalJson({
            firstPassArtifactHash: progress.firstPassArtifactHash,
            schemaId: "saqi.author-inventory-collection-certificate",
            schemaVersion: 1,
            secondPassArtifactHash: passArtifact.hash,
            status,
          })}\n`,
        );
        this.#ledger.succeed(claim, certificate.hash, this.#now());
        return status;
      }
      if (progress.nextPage > this.#maximumPages)
        throw new Error("SOURCE_AUTHOR_INVENTORY_PAGE_LIMIT");
      const requested = sourceInventoryUrl(progress.nextPage);
      const projection = AuthorInventoryPageSchema.parse(
        // eslint-disable-next-line no-await-in-loop -- Pages are checkpointed in strict ordinal order for resumability.
        await this.#collectPage(requested.href, signal),
      );
      const observed = canonicalInventoryUrl(projection.sourceUrl);
      if (
        observed.page !== progress.nextPage ||
        projection.page !== progress.nextPage
      )
        throw new Error("SOURCE_AUTHOR_INVENTORY_PAGE_GAP");
      if (
        !projection.terminal &&
        canonicalInventoryUrl(projection.nextPageHref ?? "").page !==
          progress.nextPage + 1
      )
        throw new Error("SOURCE_AUTHOR_INVENTORY_NEXT_PAGE_INVALID");
      // eslint-disable-next-line no-await-in-loop -- Page artifacts are persisted in source order before the next-page cursor advances.
      const artifact = await this.#artifacts.put(
        `${canonicalJson(projection)}\n`,
      );
      const nextAuthors = progress.authorReferences + projection.authors.length;
      const nextBytes = progress.bytes + artifact.bytes;
      if (nextAuthors > this.#maximumAuthors)
        throw new Error("SOURCE_AUTHOR_INVENTORY_AUTHOR_LIMIT");
      if (nextBytes > this.#maximumBytes)
        throw new Error("SOURCE_AUTHOR_INVENTORY_BYTE_LIMIT");
      progress = {
        ...progress,
        authorReferences: nextAuthors,
        bytes: nextBytes,
        nextPage: progress.nextPage + 1,
        pageArtifactHashes: [...progress.pageArtifactHashes, artifact.hash],
        terminal: projection.terminal,
      };
      this.#ledger.checkpoint(
        claim,
        {
          artifactHash: artifact.hash,
          kind: INVENTORY_CHECKPOINT_KIND,
          payload: progress,
        },
        this.#now(),
      );
      this.#afterPageCheckpoint?.(progress.pass, projection.page);
    }
    throw new Error("AUTHOR_INVENTORY_ABORTED");
  }

  async #collectPage(
    href: string,
    signal: AbortSignal,
  ): Promise<AuthorInventoryPageProjection> {
    const lease = await this.#claimOrigin(signal);
    try {
      const projection = await this.#browser.collectAuthorInventoryPage(
        href,
        signal,
      );
      this.#ledger.completeOrigin(lease, this.#now(), this.#minimumOriginGapMs);
      return projection;
    } catch (error) {
      const now = this.#now();
      if (
        signal.aborted ||
        (error instanceof Error &&
          isNetworkFailureText(`${error.name}\n${error.message}`))
      ) {
        this.#ledger.releaseOrigin(lease, now);
      } else {
        const humanRequired =
          error instanceof SourceBrowserError &&
          error.code === "SOURCE_HUMAN_REQUIRED";
        this.#ledger.failOrigin(lease, now, this.#minimumOriginGapMs, {
          circuitBreakerAfter: 3,
          circuitBreakerCooldownMs: 15 * 60_000,
          retryAt: now + (humanRequired ? 15 * 60_000 : 60_000),
          ...(humanRequired ? { stopReason: "SOURCE_HUMAN_REQUIRED" } : {}),
        });
      }
      throw error;
    }
  }

  async #claimOrigin(signal: AbortSignal): Promise<OriginLease> {
    for (;;) {
      if (signal.aborted) throw signal.reason ?? new Error("Aborted");
      const result = this.#ledger.claimOrigin(
        currentSource().origin,
        this.#now(),
        30 * 60_000,
      );
      if (result.state === "claimed") return result.lease;
      if (result.state === "stopped") {
        throw new SourceBrowserError(
          "SOURCE_HUMAN_REQUIRED",
          `Origin is stopped pending operator action: ${result.reason}`,
        );
      }
      // eslint-disable-next-line no-await-in-loop -- Shared durable origin admission must serialize all source lanes.
      await delay(Math.max(10, result.retryAt - this.#now()), undefined, {
        signal,
      });
    }
  }

  async #readPages(
    hashes: readonly string[],
  ): Promise<readonly AuthorInventoryPageProjection[]> {
    return Promise.all(
      hashes.map(async (hash) => {
        const artifact = await this.#artifacts.read(hash);
        const input: unknown = JSON.parse(artifact.toString("utf8"));
        return AuthorInventoryPageSchema.parse(input);
      }),
    );
  }

  async #readPass(
    hash: string,
    expectedPass: 1 | 2,
  ): Promise<readonly AuthorInventoryPageProjection[]> {
    const artifact = await this.#artifacts.read(hash);
    const input: unknown = JSON.parse(artifact.toString("utf8"));
    const value = AuthorInventoryPassSchema.parse(input);
    if (value.pass !== expectedPass)
      throw new Error("SOURCE_AUTHOR_INVENTORY_PASS_ARTIFACT_MISMATCH");
    return value.pages;
  }
}

function bounded(value: number, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum)
    throw new Error("Invalid author inventory bound");
  return value;
}
