import { hash, randomUUID, timingSafeEqual } from "node:crypto";
import type { FileHandle } from "node:fs/promises";
import { mkdir, open, readFile, rename, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import {
  type AuthorInventoryPageProjection,
  AuthorInventoryPageSchema,
  type AuthorPoemManifestProjection,
  canonicalAuthorUrl,
  canonicalInventoryUrl,
  canonicalPoemUrl,
  currentSource,
  currentSourceAdapterProfile,
  LIMITS,
  type PoemDetailProjection,
  PROJECTION_SCHEMA_VERSION,
  renderSourcePath,
  sourcePathValue,
} from "@saqi/source-adapter";
import {
  type BrowserContext,
  chromium,
  type Page,
  type Request,
  type Response,
  type Route,
} from "playwright-core";
import { z } from "zod";

import type { AuthorInventoryPageBrowser } from "./author-inventory-lane.js";
import type { CollectorBrowser } from "./collector.js";

const FeedRecordSchema = z.record(z.string(), z.unknown());
const FeedHtmlSchema = z.string();

const NAVIGATION_TIMEOUT_MS = 60_000;
const POEM_OPERATION_TIMEOUT_MS = 3 * NAVIGATION_TIMEOUT_MS;
const AUTHOR_OPERATION_TIMEOUT_MS = 30 * NAVIGATION_TIMEOUT_MS;
const INVENTORY_OPERATION_TIMEOUT_MS = 3 * NAVIGATION_TIMEOUT_MS;
const OPERATION_SHUTDOWN_GRACE_MS = 10_000;
const CHALLENGE_RESOLUTION_TIMEOUT_MS = 60_000;
const CHALLENGE_POLL_INTERVAL_MS = 1_000;
const AUTHOR_DOCUMENT_MAX_BYTES = 8 * 1024 * 1024;
const POEM_DOCUMENT_MAX_BYTES = 4 * 1024 * 1024;
const INVENTORY_DOCUMENT_MAX_BYTES = 4 * 1024 * 1024;
const FEED_DOCUMENT_MAX_BYTES = 2 * 1024 * 1024;
const INLINE_SCRIPT_MAX_BYTES = 256 * 1024;
const INLINE_SCRIPT_MAX_CANDIDATES = 16;
const MAX_FEED_PAGES = 2_000;
const COLLECTOR_MARKER_HEADER = "x-saqi-collector";
const MINIMUM_SOURCE_GAP_MS = 13_000;
const PROFILE_LOCK_FILENAME = ".saqi-collector.lock";
const CLOUDFLARE_CHALLENGE_ORIGIN = "https://challenges.cloudflare.com";
const CLOUDFLARE_CHALLENGE_PATH_PREFIX = "/cdn-cgi/challenge-platform/";
const CLOUDFLARE_TURNSTILE_PATH_PREFIX = "/turnstile/";

export interface FeedConfiguration {
  readonly cursor: string;
  readonly endpoint: string;
  readonly token: string;
}

export interface FeedHttpResult {
  readonly body: string;
  readonly bytes: number;
  readonly cfMitigated: null | string;
  readonly contentType: null | string;
  readonly retryAfter: null | string;
  readonly status: number;
  readonly url: string;
}

export interface SourceFeedPage {
  readonly html: string;
  readonly nextCursor: null | string;
  readonly terminal: boolean;
}

export interface SourceAccessDiagnostic {
  readonly category: "http_access_denied" | "managed_challenge" | "turnstile";
  readonly cfMitigated: boolean;
  readonly httpStatus: null | number;
  readonly schemaVersion: 1;
  readonly surface: "feed" | "navigation" | "projection";
}

export class SourceBrowserError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  readonly retryAfterMs: null | number;
  readonly sourceAccess: null | SourceAccessDiagnostic;

  constructor(
    code: string,
    message: string,
    retryable = true,
    retryAfterMs: null | number = null,
    sourceAccess: null | SourceAccessDiagnostic = null,
  ) {
    super(message);
    this.name = "SourceBrowserError";
    this.code = code;
    this.retryable = retryable;
    this.retryAfterMs = retryAfterMs;
    this.sourceAccess =
      sourceAccess === null
        ? null
        : sourceAccessDiagnostic(
            sourceAccess.category,
            sourceAccess.surface,
            sourceAccess.httpStatus,
            sourceAccess.cfMitigated,
          );
  }
}

interface AbortableSerialQueuePort {
  run<T>(operation: () => Promise<T>, signal: AbortSignal): Promise<T>;
}

export class AbortableSerialQueue implements AbortableSerialQueuePort {
  #tail: Promise<void> = Promise.resolve();

  async run<T>(operation: () => Promise<T>, signal: AbortSignal): Promise<T> {
    const previous = this.#tail;
    const { promise: tail, resolve: release } =
      Promise.withResolvers<undefined>();
    this.#tail = tail;
    try {
      await abortable(previous, signal);
    } catch (error) {
      void previous
        .then(() => release(undefined))
        .catch(() => release(undefined));
      throw error;
    }
    try {
      return await operation();
    } finally {
      release(undefined);
    }
  }
}

export interface SourceBrowserOptions {
  readonly authorOperationTimeoutMs?: number;
  readonly executablePath?: string;
  readonly headless?: boolean;
  readonly inventoryOperationTimeoutMs?: number;
  readonly launchPersistentContext?: typeof chromium.launchPersistentContext;
  readonly minimumSourceGapMs?: number;
  readonly onManifestPass?: (certificate: ManifestPassCertificate) => void;
  readonly poemOperationTimeoutMs?: number;
  readonly profileDirectory: string;
  readonly recycleAfter?: number;
  readonly shutdownGraceMs?: number;
}

export interface ManifestPassCertificate {
  readonly count: number;
  readonly digest: string;
  readonly pass: 1 | 2;
  readonly terminalCondition: "declared_count" | "feed_exhausted";
}

export class SourceChromeCollector
  implements CollectorBrowser, AuthorInventoryPageBrowser
{
  readonly #options: Required<
    Pick<
      SourceBrowserOptions,
      | "authorOperationTimeoutMs"
      | "headless"
      | "inventoryOperationTimeoutMs"
      | "minimumSourceGapMs"
      | "poemOperationTimeoutMs"
      | "recycleAfter"
      | "shutdownGraceMs"
    >
  > &
    SourceBrowserOptions;
  #context: BrowserContext | null = null;
  readonly #feedMarker = randomUUID();
  #initializingContext: BrowserContext | null = null;
  #launchInProgress = false;
  #profileLock: { readonly handle: FileHandle; readonly token: string } | null =
    null;
  #operations = 0;
  #page: null | Page = null;
  #poisoned: Error | null = null;
  #permittedFeedToken: null | string = null;
  #permittedFeedUrl: null | string = null;
  readonly #serial = new AbortableSerialQueue();
  #lastSourceCompletedAt = 0;

  private constructor(options: SourceBrowserOptions) {
    const minimumSourceGapMs = effectiveMinimumSourceGapMs(
      options.minimumSourceGapMs,
    );
    if (
      options.recycleAfter !== undefined &&
      (!Number.isSafeInteger(options.recycleAfter) || options.recycleAfter <= 0)
    ) {
      throw new Error("recycleAfter must be a positive integer");
    }
    for (const [name, timeout] of [
      ["authorOperationTimeoutMs", options.authorOperationTimeoutMs],
      ["inventoryOperationTimeoutMs", options.inventoryOperationTimeoutMs],
      ["poemOperationTimeoutMs", options.poemOperationTimeoutMs],
      ["shutdownGraceMs", options.shutdownGraceMs],
    ] as const) {
      if (
        timeout !== undefined &&
        (!Number.isSafeInteger(timeout) || timeout <= 0)
      ) {
        throw new Error(`${name} must be a positive integer`);
      }
    }
    this.#options = {
      authorOperationTimeoutMs: AUTHOR_OPERATION_TIMEOUT_MS,
      headless: false,
      inventoryOperationTimeoutMs: INVENTORY_OPERATION_TIMEOUT_MS,
      poemOperationTimeoutMs: POEM_OPERATION_TIMEOUT_MS,
      recycleAfter: 100,
      shutdownGraceMs: OPERATION_SHUTDOWN_GRACE_MS,
      ...options,
      minimumSourceGapMs,
    };
  }

  // eslint-disable-next-line @typescript-eslint/member-ordering -- The public async factory intentionally follows the private constructor whose invariant it enforces.
  static async create(
    options: SourceBrowserOptions,
  ): Promise<SourceChromeCollector> {
    const collector = new SourceChromeCollector(options);
    await mkdir(options.profileDirectory, { mode: 0o700, recursive: true });
    await collector.#acquireProfileLock();
    return collector;
  }

  // eslint-disable-next-line @typescript-eslint/member-ordering -- Public collector lifecycle methods remain together after the async factory.
  async close(): Promise<void> {
    await this.#serial.run(
      async () => this.#closeBrowser(),
      new AbortController().signal,
    );
  }

  // eslint-disable-next-line @typescript-eslint/member-ordering -- Public collection entrypoints remain adjacent for a readable adapter surface.
  async collectAuthorManifest(
    authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    return this.#exclusive(
      async () =>
        this.#withOperationDeadline(
          signal,
          this.#options.authorOperationTimeoutMs,
          "SOURCE_AUTHOR_OPERATION_TIMEOUT",
          "Author manifest collection exceeded its work deadline; browser shutdown was verified",
          async (operationSignal) => {
            const author = canonicalAuthorUrl(authorValue);
            const page = await this.#readyPage(operationSignal);
            const passes: AuthorPoemManifestProjection[] = [];
            for (let pass = 0; pass < 2; pass += 1) {
              // eslint-disable-next-line no-await-in-loop -- Each verification pass must navigate and checkpoint serially.
              const documentHtml = await this.#navigate(
                page,
                author.href,
                AUTHOR_DOCUMENT_MAX_BYTES,
                operationSignal,
              );
              // eslint-disable-next-line no-await-in-loop -- The second pass validates the first pass against the same browser page.
              const result = await this.#collectAuthorPass(
                page,
                author.href,
                documentHtml,
                operationSignal,
              );
              passes.push(result.projection);
              this.#options.onManifestPass?.({
                count: result.projection.poems.length,
                digest: manifestDigest(result.projection),
                pass: pass === 0 ? 1 : 2,
                terminalCondition: result.terminalCondition,
              });
            }
            const [first, second] = passes;
            if (
              !first ||
              !second ||
              manifestDigest(first) !== manifestDigest(second)
            ) {
              throw new SourceBrowserError(
                "SOURCE_MANIFEST_UNSTABLE",
                "Fresh author passes produced different poem identities",
              );
            }
            return second;
          },
        ),
      signal,
    );
  }

  // eslint-disable-next-line @typescript-eslint/member-ordering -- Public collection entrypoints remain adjacent for a readable adapter surface.
  async collectAuthorInventoryPage(
    inventoryValue: string,
    signal: AbortSignal,
  ): Promise<AuthorInventoryPageProjection> {
    return this.#exclusive(
      async () =>
        this.#withOperationDeadline(
          signal,
          this.#options.inventoryOperationTimeoutMs,
          "SOURCE_INVENTORY_OPERATION_TIMEOUT",
          "Author inventory collection exceeded its work deadline; browser shutdown was verified",
          async (operationSignal) => {
            const inventory = canonicalInventoryUrl(inventoryValue);
            const page = await this.#readyPage(operationSignal);
            await this.#navigate(
              page,
              inventory.href,
              INVENTORY_DOCUMENT_MAX_BYTES,
              operationSignal,
            );
            return AuthorInventoryPageSchema.parse(
              await projectAuthorInventoryPage(page, inventory.page),
            );
          },
        ),
      signal,
    );
  }

  async #collectAuthorPass(
    page: Page,
    authorHref: string,
    documentHtml: string,
    signal: AbortSignal,
  ): Promise<{
    readonly projection: AuthorPoemManifestProjection;
    readonly terminalCondition: "declared_count" | "feed_exhausted";
  }> {
    const initial = await projectManifest(page, authorHref, false);
    const poems = new Map(initial.poems.map((poem) => [poem.href, poem]));
    const declaredCount = parseLooseCount(initial.declaredPoemCountText);
    if (declaredCount !== null && poems.size === declaredCount) {
      return {
        projection: { ...initial, terminal: true },
        terminalCondition: "declared_count",
      };
    }
    const configuration = extractFeedConfigurationFromDocument(
      documentHtml,
      authorHref,
    );
    const seenCursors = new Set<string>();
    let cursor = configuration.cursor;
    let exhausted = false;
    for (let request = 0; request < MAX_FEED_PAGES; request += 1) {
      throwIfAborted(signal);
      if (seenCursors.has(cursor)) {
        throw new SourceBrowserError(
          "SOURCE_FEED_CURSOR_LOOP",
          "Author feed repeated a cursor",
          false,
        );
      }
      seenCursors.add(cursor);
      let feed: SourceFeedPage;
      try {
        // eslint-disable-next-line no-await-in-loop -- Cursor requests are source-paced and strictly ordered.
        feed = await this.#fetchFeed(page, configuration, cursor, signal);
      } catch (error) {
        if (error instanceof SourceBrowserError) {
          throw new SourceBrowserError(
            error.code,
            `${error.message} (feed request ${String(request + 1)})`,
            error.retryable,
            error.retryAfterMs,
            error.sourceAccess,
          );
        }
        throw error;
      }
      // eslint-disable-next-line no-await-in-loop -- Feed pages are projected before advancing the cursor.
      const extracted = await projectPoemsFromHtml(page, feed.html);
      let added = 0;
      for (const poem of extracted) {
        const existing = poems.get(poem.href);
        if (!existing) {
          poems.set(poem.href, poem);
          added += 1;
        } else if (
          existing.title !== poem.title ||
          (existing.verseCountText !== null &&
            poem.verseCountText !== null &&
            parseLooseCount(existing.verseCountText) !==
              parseLooseCount(poem.verseCountText))
        ) {
          throw new SourceBrowserError(
            "SOURCE_FEED_CONFLICT",
            "Feed returned conflicting data for one poem",
            false,
          );
        } else if (
          existing.verseCountText === null &&
          poem.verseCountText !== null
        ) {
          poems.set(poem.href, poem);
        }
      }
      if (poems.size > LIMITS.poemsPerAuthor) {
        throw new SourceBrowserError(
          "SOURCE_MANIFEST_LIMIT",
          "Author manifest reached its safety limit",
          false,
        );
      }
      if (declaredCount !== null && poems.size > declaredCount) {
        throw new SourceBrowserError(
          "SOURCE_MANIFEST_COUNT_EXCEEDED",
          "Author feed exceeded the declared poem count",
          false,
        );
      }
      if (declaredCount !== null && poems.size === declaredCount) break;
      if (feed.terminal) {
        exhausted = true;
        break;
      }
      assertFeedProgress(feed, cursor, seenCursors, added);
      if (feed.nextCursor === null)
        throw new Error("Unreachable terminal feed");
      cursor = feed.nextCursor;
    }
    if (declaredCount === null ? !exhausted : poems.size !== declaredCount) {
      throw new SourceBrowserError(
        "SOURCE_MANIFEST_NONTERMINAL",
        "Author feed did not prove manifest completion",
      );
    }
    if (poems.size === 0 && declaredCount !== 0) {
      throw new SourceBrowserError(
        "SOURCE_MANIFEST_UNVERIFIED_EMPTY",
        "Empty manifest has no explicit zero count",
      );
    }
    return {
      projection: {
        ...initial,
        // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Serialized browser callbacks target runtimes without Iterator Helpers.
        poems: [...poems.values()],
        terminal: true,
      },
      terminalCondition:
        declaredCount === null ? "feed_exhausted" : "declared_count",
    };
  }

  async #fetchFeed(
    page: Page,
    configuration: FeedConfiguration,
    cursor: string,
    signal: AbortSignal,
  ): Promise<SourceFeedPage> {
    await this.#sourceGap(signal);
    const expectedFeedUrl = feedRequestUrl(configuration, cursor);
    this.#permittedFeedToken = configuration.token;
    this.#permittedFeedUrl = expectedFeedUrl;
    try {
      let result: FeedHttpResult;
      try {
        result = await abortable(
          page.evaluate(
            async ({
              collectorMarker,
              cursor: feedCursor,
              endpoint,
              feedTimeoutMs,
              maximumBytes,
              token,
            }) => {
              const url = new URL(endpoint, location.origin);
              url.searchParams.set("cursor", feedCursor);
              url.searchParams.set("token", token);
              // eslint-disable-next-line @sarj/no-raw-fetch-outside-clients -- Playwright serializes this callback into the page; it cannot import Node clients. This source client bounds timeout/bytes and validates the permitted feed URL.
              const response = await fetch(url, {
                headers: {
                  "X-Feed-Token": token,
                  "X-Requested-With": "XMLHttpRequest",
                  "X-Saqi-Collector": collectorMarker,
                },
                redirect: "error",
                signal: AbortSignal.timeout(feedTimeoutMs),
              });
              const reader = response.body?.getReader();
              const chunks: Uint8Array[] = [];
              let bytes = 0;
              if (reader) {
                // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- The loop exits only when the ordered browser stream reports done.
                while (true) {
                  // eslint-disable-next-line no-await-in-loop -- Stream reads must preserve byte order and enforce the running size limit.
                  const chunk = await reader.read();
                  if (chunk.done) break;
                  bytes += chunk.value.byteLength;
                  if (bytes > maximumBytes) {
                    // eslint-disable-next-line no-await-in-loop -- Cancellation must finish before the oversized response is rejected.
                    await reader.cancel();
                    throw new Error("SOURCE_FEED_SIZE");
                  }
                  chunks.push(chunk.value);
                }
              }
              const body = new Uint8Array(bytes);
              let offset = 0;
              for (const chunk of chunks) {
                body.set(chunk, offset);
                offset += chunk.byteLength;
              }
              return {
                body: new TextDecoder("utf-8", { fatal: true }).decode(body),
                bytes,
                cfMitigated: response.headers.get("cf-mitigated"),
                contentType: response.headers.get("content-type"),
                retryAfter: response.headers.get("retry-after"),
                status: response.status,
                url: response.url,
              } satisfies FeedHttpResult;
            },
            {
              collectorMarker: this.#feedMarker,
              cursor,
              endpoint: configuration.endpoint,
              feedTimeoutMs: NAVIGATION_TIMEOUT_MS,
              maximumBytes: FEED_DOCUMENT_MAX_BYTES,
              token: configuration.token,
            },
          ),
          signal,
          () => page.close(),
        );
      } catch (error) {
        throwIfAborted(signal);
        const message = error instanceof Error ? error.message : String(error);
        if (message.includes("SOURCE_FEED_SIZE")) {
          throw new SourceBrowserError(
            "SOURCE_FEED_SIZE",
            "Author feed body exceeded its safety limit",
            false,
          );
        }
        throw new SourceBrowserError(
          "SOURCE_FEED_REQUEST_FAILED",
          `Author feed request failed before a valid response: ${message.slice(0, 1_000)}`,
        );
      }
      return parseFeedHttpResult(result, configuration, cursor);
    } finally {
      this.#permittedFeedToken = null;
      this.#permittedFeedUrl = null;
      this.#lastSourceCompletedAt = Date.now();
      this.#operations += 1;
    }
  }

  // eslint-disable-next-line @typescript-eslint/member-ordering -- Public collection entrypoints remain adjacent to the private workflow each exposes.
  async collectPoemDetail(
    poemValue: string,
    expectedAuthorValue: string,
    signal: AbortSignal,
  ): Promise<PoemDetailProjection> {
    return this.#exclusive(
      async () =>
        this.#withOperationDeadline(
          signal,
          this.#options.poemOperationTimeoutMs,
          "SOURCE_POEM_OPERATION_TIMEOUT",
          "Poem collection exceeded its work deadline; browser shutdown was verified",
          async (operationSignal) => {
            const poem = canonicalPoemUrl(poemValue);
            const expectedAuthor = canonicalAuthorUrl(expectedAuthorValue);
            const page = await this.#readyPage(operationSignal);
            await this.#navigate(
              page,
              poem.href,
              POEM_DOCUMENT_MAX_BYTES,
              operationSignal,
            );
            const projection = await projectPoem(page, expectedAuthor.href);
            if (projection.challengeDetected) {
              throw new SourceBrowserError(
                "SOURCE_HUMAN_REQUIRED",
                "Cloudflare challenge remained in the final poem projection",
                true,
                null,
                sourceAccessDiagnostic(
                  "managed_challenge",
                  "projection",
                  null,
                  false,
                ),
              );
            }
            if (
              canonicalAuthorUrl(projection.authorHref).canonicalId !==
              expectedAuthor.canonicalId
            ) {
              throw new SourceBrowserError(
                "SOURCE_POEM_AUTHOR_MISMATCH",
                "Poem detail belongs to a different author",
                false,
              );
            }
            return projection;
          },
        ),
      signal,
    );
  }

  async #closeBrowser(): Promise<void> {
    const context = this.#context;
    if (!context) {
      if (this.#poisoned) throw this.#poisoned;
      await this.#resetBrowserState();
      return;
    }
    try {
      const browser = context.browser();
      await context.close();
      if (browser?.isConnected())
        throw new Error("SOURCE_BROWSER_DISCONNECT_UNPROVEN");
      await this.#resetBrowserState();
    } catch (error) {
      const cause = toError(error, "Browser close failed");
      this.#poisoned = new SourceBrowserError(
        "SOURCE_BROWSER_RESTART_REQUIRED",
        `Browser close could not prove process isolation: ${cause.message.slice(0, 1_000)}`,
      );
      throw this.#poisoned;
    }
  }

  async #resetBrowserState(): Promise<void> {
    this.#context = null;
    this.#page = null;
    this.#permittedFeedToken = null;
    this.#permittedFeedUrl = null;
    this.#poisoned = null;
    await this.#releaseProfileLock();
  }

  async #withOperationDeadline<T>(
    signal: AbortSignal,
    timeoutMs: number,
    timeoutCode: string,
    timeoutMessage: string,
    operation: (operationSignal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const deadline = AbortSignal.timeout(timeoutMs);
    const operationSignal = AbortSignal.any([signal, deadline]);
    const running = Promise.resolve().then(() => operation(operationSignal));
    // The race may finish on abort first; observe any later browser rejection.
    void running.catch(() => undefined);
    const aborted = Promise.withResolvers<undefined>();
    const onAbort = (): void => aborted.resolve(undefined);
    operationSignal.addEventListener("abort", onAbort, { once: true });
    if (operationSignal.aborted) onAbort();
    try {
      const outcome = await Promise.race([
        running.then((value) => ({ type: "completed" as const, value })),
        aborted.promise.then(() => ({ type: "aborted" as const })),
      ]);
      if (outcome.type === "completed") return outcome.value;

      await this.#forceCloseBrowser(
        deadline.aborted && !signal.aborted
          ? timeoutCode
          : "COLLECTOR_OPERATOR_STOP",
      );
      // Browser disconnection prevents further external work. Waiting for the
      // original promise to observe that disconnect keeps serial ownership
      // until initialization, response reads, and projections have settled.
      let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
      const settled = await Promise.race([
        // eslint-disable-next-line @sarj/no-silent-promise-catch -- Rejection is already observed above; this branch only reports settlement.
        running.then(() => true).catch(() => true),
        new Promise<false>((resolvePromise) => {
          shutdownTimer = setTimeout(
            () => resolvePromise(false),
            this.#options.shutdownGraceMs,
          );
          shutdownTimer.unref();
        }),
      ]);
      if (shutdownTimer !== undefined) clearTimeout(shutdownTimer);
      if (!settled) {
        this.#poisoned = new SourceBrowserError(
          "SOURCE_BROWSER_RESTART_REQUIRED",
          "Browser operation did not settle after forced shutdown; process restart is required to preserve profile isolation",
        );
        throw this.#poisoned;
      }
      // Initialization cleanup can discover that the browser is still alive
      // after the deadline won the race. Preserve that stronger fencing error
      // instead of reporting a verified shutdown that did not occur.
      if (this.#poisoned) throw this.#poisoned;
      if (deadline.aborted && !signal.aborted) {
        throw new SourceBrowserError(timeoutCode, timeoutMessage);
      }
      throw signal.reason ?? new Error("Aborted");
    } finally {
      operationSignal.removeEventListener("abort", onAbort);
    }
  }

  async #forceCloseBrowser(reason: string): Promise<void> {
    const context = this.#context ?? this.#initializingContext;
    if (!context) {
      if (this.#launchInProgress) return;
      await this.#resetBrowserState();
      return;
    }
    const isInitializing = context === this.#initializingContext;
    try {
      const browser = context.browser();
      if (browser) {
        await browser.close({ reason });
        if (browser.isConnected())
          throw new Error("SOURCE_BROWSER_DISCONNECT_UNPROVEN");
      } else {
        await context.close();
      }
      // Initialization still owns the profile fence and will release it only
      // after every setup promise observes the closed browser and settles.
      if (isInitializing) return;
      await this.#resetBrowserState();
    } catch (error) {
      const cause = toError(error, "Browser force-close failed");
      this.#poisoned = new SourceBrowserError(
        "SOURCE_BROWSER_RESTART_REQUIRED",
        `Browser force-close could not prove process isolation: ${cause.message.slice(0, 1_000)}`,
      );
      throw this.#poisoned;
    }
  }

  async #exclusive<T>(
    operation: () => Promise<T>,
    signal: AbortSignal,
  ): Promise<T> {
    return this.#serial.run(operation, signal);
  }

  async #navigate(
    page: Page,
    href: string,
    maximumBytes: number,
    signal: AbortSignal,
  ): Promise<string> {
    await this.#sourceGap(signal);
    try {
      let response: null | Response;
      try {
        response = await abortable(
          page.goto(href, {
            timeout: NAVIGATION_TIMEOUT_MS,
            waitUntil: "domcontentloaded",
          }),
          signal,
          () => page.close(),
        );
      } catch (error) {
        throwIfAborted(signal);
        await this.#discardFailedPage(page);
        const cause = toError(error, "Browser navigation failed");
        throw new SourceBrowserError(
          "SOURCE_NAVIGATION_FAILED",
          `Browser navigation failed: ${cause.message.slice(0, 1_000)}`,
        );
      }
      return await resolveNavigationDocument(
        page,
        response,
        href,
        maximumBytes,
        signal,
      );
    } finally {
      this.#lastSourceCompletedAt = Date.now();
      this.#operations += 1;
    }
  }

  async #discardFailedPage(page: Page): Promise<void> {
    if (this.#page === page) this.#page = null;
    if (page.isClosed()) return;
    try {
      await page.close({ reason: "SOURCE_NAVIGATION_FAILED" });
    } catch (error) {
      const browser = this.#context?.browser();
      if (browser?.isConnected()) {
        await this.#forceCloseBrowser("SOURCE_PAGE_RECOVERY_FAILED");
        return;
      }
      // A disconnected browser cannot perform more source work. Clear the
      // stale handles so the next retry launches a fresh process while
      // preserving the persistent profile and its Cloudflare state.
      this.#context = null;
      this.#page = null;
      if (!(error instanceof Error))
        throw new Error("Browser page recovery failed");
    }
  }

  async #readyPage(signal: AbortSignal): Promise<Page> {
    throwIfAborted(signal);
    // A close can fail transiently while Chrome is tearing down. Retry the
    // fenced close before rejecting unrelated source work. #closeBrowser only
    // clears the profile lock after disconnection has been proven.
    if (this.#poisoned) await this.#closeBrowser();
    if (this.#operations >= this.#options.recycleAfter)
      await this.#closeBrowser();
    if (this.#context?.browser()?.isConnected() === false)
      await this.#closeBrowser();
    await this.#acquireProfileLock();
    if (!this.#context) {
      const executable = this.#options.executablePath;
      const launchPersistentContext =
        this.#options.launchPersistentContext ??
        chromium.launchPersistentContext.bind(chromium);
      let candidate: BrowserContext | null = null;
      this.#launchInProgress = true;
      try {
        candidate = await launchPersistentContext(
          this.#options.profileDirectory,
          {
            acceptDownloads: false,
            ...(executable
              ? { executablePath: executable }
              : { channel: "chrome" }),
            headless: this.#options.headless,
            serviceWorkers: "block",
            timeout: NAVIGATION_TIMEOUT_MS,
          },
        );
        this.#initializingContext = candidate;
        candidate.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
        await candidate.route("**/*", (route, request) =>
          this.#restrictRequest(route, request),
        );
        throwIfAborted(signal);
        const activeContext = candidate;
        activeContext.on("close", () => {
          if (this.#context === activeContext) {
            this.#context = null;
            this.#page = null;
          }
        });
        this.#context = candidate;
        this.#operations = 0;
      } catch (error) {
        if (candidate) {
          try {
            const browser = candidate.browser();
            if (browser) {
              if (browser.isConnected())
                await browser.close({ reason: "BROWSER_INIT_FAILED" });
              if (browser.isConnected())
                throw new Error("SOURCE_BROWSER_DISCONNECT_UNPROVEN");
            } else await candidate.close();
          } catch (closeError) {
            this.#context = candidate;
            this.#poisoned = toError(
              closeError,
              "Browser initialization cleanup failed",
            );
            throw this.#poisoned;
          }
        }
        await this.#releaseProfileLock();
        throwIfAborted(signal);
        const cause = toError(error, "Browser launch failed");
        throw new SourceBrowserError(
          "SOURCE_BROWSER_LAUNCH_FAILED",
          `Browser launch failed: ${cause.message.slice(0, 1_000)}`,
        );
      } finally {
        if (this.#initializingContext === candidate)
          this.#initializingContext = null;
        this.#launchInProgress = false;
      }
    }
    if (!this.#page || this.#page.isClosed()) {
      const [retained, ...extras] = this.#context.pages();
      await Promise.all(extras.map((extra) => extra.close()));
      const page = retained ?? (await this.#context.newPage());
      this.#page = page;
      page.on("crash", () => {
        if (this.#page === page) this.#page = null;
      });
      page.on("popup", (popup) => void popup.close());
    }
    return this.#page;
  }

  async #acquireProfileLock(): Promise<void> {
    if (this.#profileLock) return;
    const path = join(this.#options.profileDirectory, PROFILE_LOCK_FILENAME);
    const token = randomUUID();
    const record = JSON.stringify({ pid: process.pid, token });
    const recoveryPath = `${path}.recovery`;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        // eslint-disable-next-line no-await-in-loop -- Lock acquisition retries must serialize against the same profile path.
        const handle = await open(path, "wx", 0o600);
        try {
          // eslint-disable-next-line no-await-in-loop -- Ownership bytes must be written before publishing the acquired handle.
          await handle.writeFile(`${record}\n`, { encoding: "utf8" });
        } catch (error) {
          // eslint-disable-next-line no-await-in-loop -- Failed lock publication must close its handle before quarantine.
          await handle.close();
          // eslint-disable-next-line no-await-in-loop -- Quarantine must finish before another acquisition attempt.
          await rename(path, `${path}.stale.${randomUUID()}`);
          throw error;
        }
        this.#profileLock = { handle, token };
        return;
      } catch (error) {
        if (!isFileExistsError(error)) throw error;
        if (attempt > 0) {
          throw new SourceBrowserError(
            "SOURCE_PROFILE_LOCKED",
            "Chrome profile is already owned by another collector",
            false,
          );
        }
        // eslint-disable-next-line no-await-in-loop -- Each recovery attempt must inspect the current owner after the failed exclusive create.
        let existing = await readProfileLock(path);
        if (existing?.pid !== undefined && processIsAlive(existing.pid)) {
          throw new SourceBrowserError(
            "SOURCE_PROFILE_LOCKED",
            `Chrome profile is owned by live process ${String(existing.pid)}`,
            false,
          );
        }
        const recoveryToken = randomUUID();
        // eslint-disable-next-line no-await-in-loop -- Recovery ownership must be acquired before re-reading or quarantining the primary lock.
        const recovery = await acquireProfileRecoveryLock(
          recoveryPath,
          recoveryToken,
        );
        if (recovery === null)
          throw new SourceBrowserError(
            "SOURCE_PROFILE_LOCKED",
            "Chrome profile ownership recovery is already in progress",
            false,
          );
        try {
          // eslint-disable-next-line no-await-in-loop -- Recovery revalidates ownership after acquiring its serialization lock.
          existing = await readProfileLock(path);
          if (existing?.pid !== undefined && processIsAlive(existing.pid)) {
            throw new SourceBrowserError(
              "SOURCE_PROFILE_LOCKED",
              `Chrome profile is owned by live process ${String(existing.pid)}`,
              false,
            );
          }
          if (existing === null) {
            throw new SourceBrowserError(
              "SOURCE_PROFILE_LOCK_INVALID",
              "Chrome profile lock is malformed or changed during recovery",
              false,
            );
          }
          // eslint-disable-next-line no-await-in-loop -- The stale owner must be quarantined before the next exclusive-create attempt.
          await rename(path, `${path}.stale.${randomUUID()}`);
        } finally {
          // eslint-disable-next-line no-await-in-loop -- Recovery ownership is released before its token file is inspected.
          await recovery.close();
          // eslint-disable-next-line no-await-in-loop -- Cleanup verifies the exact recovery token after closing the handle.
          const retained = await readProfileLock(recoveryPath);
          if (
            retained?.token !== undefined &&
            constantTimeEqual(retained.token, recoveryToken)
          )
            // eslint-disable-next-line no-await-in-loop -- The serialized recovery token is removed before another acquisition attempt.
            await unlink(recoveryPath);
        }
      }
    }
    throw new Error("Profile lock acquisition exhausted unexpectedly");
  }

  async #releaseProfileLock(): Promise<void> {
    const lock = this.#profileLock;
    if (!lock) return;
    this.#profileLock = null;
    await lock.handle.close();
    const path = join(this.#options.profileDirectory, PROFILE_LOCK_FILENAME);
    const existing = await readProfileLock(path);
    if (
      existing?.token !== undefined &&
      constantTimeEqual(existing.token, lock.token)
    )
      await unlink(path);
  }

  async #sourceGap(signal: AbortSignal): Promise<void> {
    const remainingDelay =
      this.#lastSourceCompletedAt +
      this.#options.minimumSourceGapMs -
      Date.now();
    if (remainingDelay > 0)
      await abortable(globalThisDelay(remainingDelay), signal);
  }

  async #restrictRequest(route: Route, request: Request): Promise<void> {
    if (["xhr", "fetch"].includes(request.resourceType())) {
      let isFeed = false;
      try {
        isFeed = isFeedUrlStructure(new URL(request.url()));
      } catch {
        // The general request policy below rejects malformed URLs.
      }
      if (isFeed) {
        const headers = request.headers();
        if (
          request.method() !== "GET" ||
          request.url() !== this.#permittedFeedUrl ||
          headers[COLLECTOR_MARKER_HEADER] !== this.#feedMarker ||
          headers["x-requested-with"] !== "XMLHttpRequest" ||
          !constantTimeEqual(
            headers["x-feed-token"] ?? "",
            this.#permittedFeedToken ?? "",
          )
        ) {
          await route.abort("blockedbyclient");
          return;
        }
        this.#permittedFeedToken = null;
        this.#permittedFeedUrl = null;
        const {
          [COLLECTOR_MARKER_HEADER]: _collectorMarker,
          ...forwardedHeaders
        } = headers;
        await route.continue({ headers: forwardedHeaders });
        return;
      }
    }
    await restrictRequest(route, request);
  }
}

export function effectiveMinimumSourceGapMs(requested?: number): number {
  if (
    requested !== undefined &&
    (!Number.isSafeInteger(requested) || requested < 0)
  ) {
    throw new Error("minimumSourceGapMs must be a non-negative integer");
  }
  return Math.max(MINIMUM_SOURCE_GAP_MS, requested ?? MINIMUM_SOURCE_GAP_MS);
}

export function isAllowedBrowserRequest(request: {
  readonly isNavigationRequest: boolean;
  readonly isSubframeNavigation?: boolean;
  readonly method?: string;
  readonly resourceType: string;
  readonly url: string;
}): boolean {
  let url: URL;
  try {
    url = new URL(request.url);
  } catch {
    return false;
  }
  if (url.username !== "" || url.password !== "" || url.hash !== "") {
    return false;
  }
  if (isAllowedCloudflareChallengeRequest(request, url)) return true;
  if (url.origin !== currentSource().origin) return false;
  if (request.isNavigationRequest) {
    if (url.search !== "") return false;
    try {
      canonicalAuthorUrl(url.href);
      return true;
    } catch {
      try {
        canonicalPoemUrl(url.href);
        return true;
      } catch {
        try {
          canonicalInventoryUrl(url.href);
          return true;
        } catch {
          return false;
        }
      }
    }
  }
  if (
    ["image", "media", "font", "websocket", "manifest", "other"].includes(
      request.resourceType,
    )
  ) {
    return false;
  }
  if (["xhr", "fetch"].includes(request.resourceType)) {
    return isFeedUrlStructure(url);
  }
  return false;
}

function isAllowedCloudflareChallengeRequest(
  request: {
    readonly isNavigationRequest: boolean;
    readonly isSubframeNavigation?: boolean;
    readonly method?: string;
    readonly resourceType: string;
  },
  url: URL,
): boolean {
  const method = request.method ?? "GET";
  if (method !== "GET" && method !== "POST") return false;
  const sourceChallenge =
    url.origin === currentSource().origin &&
    url.pathname.startsWith(CLOUDFLARE_CHALLENGE_PATH_PREFIX);
  const cloudflareChallenge =
    url.origin === CLOUDFLARE_CHALLENGE_ORIGIN &&
    (url.pathname.startsWith(CLOUDFLARE_CHALLENGE_PATH_PREFIX) ||
      url.pathname.startsWith(CLOUDFLARE_TURNSTILE_PATH_PREFIX));
  if (!sourceChallenge && !cloudflareChallenge) return false;
  if (request.isNavigationRequest) {
    return (
      request.isSubframeNavigation === true &&
      request.resourceType === "document"
    );
  }
  return ["fetch", "script", "stylesheet", "xhr"].includes(
    request.resourceType,
  );
}

function isCloudflareChallengeUrl(value: string): boolean {
  if (!URL.canParse(value)) return false;
  const url = new URL(value);
  return (
    url.origin === CLOUDFLARE_CHALLENGE_ORIGIN &&
    (url.pathname.startsWith(CLOUDFLARE_CHALLENGE_PATH_PREFIX) ||
      url.pathname.startsWith(CLOUDFLARE_TURNSTILE_PATH_PREFIX))
  );
}

function isChallengeResourceUrl(value: string): boolean {
  if (!URL.canParse(value)) return false;
  const url = new URL(value);
  return (
    (url.origin === currentSource().origin ||
      url.origin === CLOUDFLARE_CHALLENGE_ORIGIN) &&
    (url.pathname.startsWith(CLOUDFLARE_CHALLENGE_PATH_PREFIX) ||
      url.pathname.startsWith(CLOUDFLARE_TURNSTILE_PATH_PREFIX))
  );
}

async function projectAuthorInventoryPage(
  page: Page,
  expectedPage: number,
): Promise<AuthorInventoryPageProjection> {
  const profile = currentSourceAdapterProfile();
  const expectedNextPath = renderSourcePath(
    profile.routes.inventoryPath,
    "page",
    String(expectedPage + 1),
  );
  return page.evaluate(
    ({
      authorContainerSelector,
      authorLinkSelector,
      expectedNextPath: serializedExpectedNextPath,
      expectedPage: serializedExpectedPage,
      maximum,
      nextPageLinkSelector,
      poemCountLabels,
      schemaVersion,
    }) => {
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const countText = (text: string, labels: readonly string[]) => {
        const normalized = text.normalize("NFC");
        for (const label of labels) {
          const at = normalized.indexOf(label);
          if (at < 0) continue;
          const before = normalized.slice(Math.max(0, at - 64), at);
          const digits = /[٠-٩۰-۹\d][٠-٩۰-۹\d,٬\s]*$/u.exec(before)?.[0];
          if (digits) return `${digits}${label}`;
        }
        return null;
      };
      const authors = new Map<
        string,
        { href: string; name: string; poemCountText: null | string }
      >();
      const links = [
        ...document.querySelectorAll<HTMLAnchorElement>(authorLinkSelector),
      ];
      for (const link of links) {
        const rawHref = link.getAttribute("href") ?? "";
        const url = new URL(rawHref, location.origin);
        if (
          url.origin !== location.origin ||
          url.search !== "" ||
          url.hash !== ""
        )
          continue;
        const name = link.textContent.trim();
        if (!name) continue;
        const container =
          link.closest(authorContainerSelector) ?? link.parentElement;
        const text = (container?.textContent ?? "").slice(0, 2_000);
        const poemCountText = countText(text, poemCountLabels);
        const existing = authors.get(url.pathname);
        if (
          existing &&
          (existing.name !== name || existing.poemCountText !== poemCountText)
        )
          throw new Error("SOURCE_AUTHOR_INVENTORY_DUPLICATE_CONFLICT");
        authors.set(url.pathname, {
          href: url.pathname,
          name,
          poemCountText,
        });
        if (authors.size > maximum)
          throw new Error("SOURCE_AUTHOR_INVENTORY_AUTHOR_LIMIT");
      }
      const hasNext = [
        ...document.querySelectorAll<HTMLAnchorElement>(nextPageLinkSelector),
      ].some((link) => {
        try {
          return (
            new URL(link.href, location.origin).pathname ===
            serializedExpectedNextPath
          );
        } catch {
          return false;
        }
      });
      return {
        // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Serialized browser callbacks target runtimes without Iterator Helpers.
        authors: [...authors.values()],
        challengeDetected: false,
        kind: "author_inventory_page" as const,
        nextPageHref: hasNext ? serializedExpectedNextPath : null,
        page: serializedExpectedPage,
        schemaVersion,
        sourceUrl: location.href,
        terminal: !hasNext,
      };
    },
    {
      expectedPage,
      expectedNextPath,
      maximum: LIMITS.authorsPerInventory,
      authorContainerSelector: profile.dom.authorContainerSelector,
      authorLinkSelector: profile.dom.authorLinkSelector,
      nextPageLinkSelector: profile.dom.nextPageLinkSelector,
      poemCountLabels: profile.labels.poemCount,
      schemaVersion: PROJECTION_SCHEMA_VERSION,
    },
  );
}

function isFeedUrlStructure(url: URL): boolean {
  const profile = currentSourceAdapterProfile();
  const authorId = sourcePathValue(
    profile.routes.feedPath,
    "authorId",
    url.pathname,
  );
  return (
    authorId !== undefined &&
    (profile.feed.authorIdFormat === "slug" || /^[1-9]\d*$/u.test(authorId)) &&
    url.searchParams.size === 2 &&
    url.searchParams.getAll(profile.feed.cursorParameter).length === 1 &&
    url.searchParams.get(profile.feed.cursorParameter) !== "" &&
    url.searchParams.getAll(profile.feed.tokenParameter).length === 1 &&
    url.searchParams.get(profile.feed.tokenParameter) !== ""
  );
}

export function extractFeedConfigurationFromInlineScripts(
  scripts: readonly string[],
  authorValue: string,
): FeedConfiguration {
  const author = canonicalAuthorUrl(authorValue);
  const sourceUrl = new URL(author.href);
  const profile = currentSourceAdapterProfile();
  let bytes = 0;
  const bounded: string[] = [];
  for (const script of scripts) {
    bytes += new TextEncoder().encode(script).byteLength;
    if (bytes > INLINE_SCRIPT_MAX_BYTES) {
      throw new SourceBrowserError(
        "SOURCE_FEED_CONFIG_SIZE",
        "Inline feed configuration is oversized",
        false,
      );
    }
    bounded.push(script);
  }
  if (!bounded.some((script) => script.includes(profile.feed.endpointMarker))) {
    throw new SourceBrowserError(
      "SOURCE_FEED_CONFIG_MISSING",
      "Author page has no trusted inline feed configuration",
    );
  }
  const joined = bounded.join("\n");
  // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Runtime compatibility requires an ordinary array before array transforms.
  const endpoints = [...joined.matchAll(/["']([^"'\n]{1,2048})["']/gu)]
    .map((match) => match[1]?.replaceAll(String.raw`\/`, "/") ?? "")
    .filter((value) => value.includes(profile.feed.endpointMarker));
  const matchingEndpoints = endpoints.filter((value) => {
    try {
      const candidate = new URL(value, currentSource().origin);
      const authorId = sourcePathValue(
        profile.routes.feedPath,
        "authorId",
        candidate.pathname,
      );
      return (
        candidate.origin === currentSource().origin &&
        authorId !== undefined &&
        (profile.feed.authorIdFormat === "slug" ||
          /^[1-9]\d*$/u.test(authorId)) &&
        candidate.search === "" &&
        candidate.hash === ""
      );
    } catch {
      // eslint-disable-next-line @sarj/no-sentinel-return-on-catch -- Invalid endpoint candidates are expected while scanning unrelated scripts.
      return false;
    }
  });
  if (matchingEndpoints.length === 0) {
    throw new SourceBrowserError(
      "SOURCE_FEED_ENDPOINT_INVALID",
      "Inline feed endpoint does not match the author",
      false,
    );
  }
  const token = extractInlineValue(
    joined,
    new RegExp(profile.feed.tokenKeys.map(escapeRegex).join("|"), "iu"),
    "SOURCE_FEED_TOKEN_MISSING",
  );
  const cursor = extractInlineValue(
    joined,
    new RegExp(profile.feed.cursorKeys.map(escapeRegex).join("|"), "iu"),
    "SOURCE_FEED_CURSOR_MISSING",
  );
  const uniqueEndpoints = new Set(
    matchingEndpoints.map((value) => new URL(value, sourceUrl.origin).href),
  );
  if (uniqueEndpoints.size !== 1) {
    throw new SourceBrowserError(
      "SOURCE_FEED_ENDPOINT_AMBIGUOUS",
      "Author page declares multiple feed endpoints",
      false,
    );
  }
  const [endpoint] = uniqueEndpoints;
  if (!endpoint) throw new Error("Validated endpoint unexpectedly missing");
  return {
    cursor,
    endpoint,
    token,
  };
}

export function parseFeedHttpResult(
  result: FeedHttpResult,
  configuration: FeedConfiguration,
  cursor: string,
): SourceFeedPage {
  const cfMitigated = result.cfMitigated?.toLowerCase() === "challenge";
  const challengeCategory = classifyCloudflareChallengeEvidence(
    {
      html: result.body,
    },
    cfMitigated,
  );
  if (cfMitigated || challengeCategory !== null) {
    throw new SourceBrowserError(
      "SOURCE_HUMAN_REQUIRED",
      "Cloudflare requires human interaction; automatic bypass is disabled",
      true,
      null,
      sourceAccessDiagnostic(
        challengeCategory ?? "managed_challenge",
        "feed",
        result.status,
        cfMitigated,
      ),
    );
  }
  if (result.status !== 200) {
    if (result.status === 429) {
      const retryAfterMs = parseRetryAfterMs(result.retryAfter);
      throw new SourceBrowserError(
        "SOURCE_RATE_LIMITED",
        `Author feed rate-limited the collector${retryAfterMs === null ? "" : ` for ${String(retryAfterMs)}ms`}`,
        true,
        retryAfterMs,
      );
    }
    if (result.status === 401 || result.status === 403) {
      throw new SourceBrowserError(
        "SOURCE_HUMAN_REQUIRED",
        `collection source denied feed access with HTTP ${String(result.status)}; automatic bypass is disabled`,
        true,
        null,
        sourceAccessDiagnostic(
          "http_access_denied",
          "feed",
          result.status,
          false,
        ),
      );
    }
    throw new SourceBrowserError(
      "SOURCE_FEED_HTTP_STATUS",
      `Author feed returned HTTP ${String(result.status)}`,
      isRetryableHttpStatus(result.status),
    );
  }
  const contentType = result.contentType
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase();
  if (contentType !== "application/json") {
    throw new SourceBrowserError(
      "SOURCE_FEED_CONTENT_TYPE",
      "Author feed is not JSON",
      false,
    );
  }
  const actualBytes = new TextEncoder().encode(result.body).byteLength;
  if (result.bytes !== actualBytes || actualBytes > FEED_DOCUMENT_MAX_BYTES) {
    throw new SourceBrowserError(
      "SOURCE_FEED_SIZE",
      "Author feed body size is invalid",
      false,
    );
  }
  if (result.url !== feedRequestUrl(configuration, cursor)) {
    throw new SourceBrowserError(
      "SOURCE_FEED_REDIRECT",
      "Author feed redirected or changed its query",
      false,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(result.body);
  } catch {
    throw new SourceBrowserError(
      "SOURCE_FEED_JSON_INVALID",
      "Author feed returned malformed JSON",
    );
  }
  const record = FeedRecordSchema.safeParse(parsed);
  if (
    !record.success ||
    Object.keys(record.data).some(
      (key) =>
        ![
          currentSourceAdapterProfile().feed.responseHtmlKey,
          currentSourceAdapterProfile().feed.responseNextCursorKey,
        ].includes(key),
    ) ||
    !(currentSourceAdapterProfile().feed.responseHtmlKey in record.data) ||
    !(
      currentSourceAdapterProfile().feed.responseNextCursorKey in record.data
    ) ||
    typeof record.data[currentSourceAdapterProfile().feed.responseHtmlKey] !==
      "string"
  ) {
    throw new SourceBrowserError(
      "SOURCE_FEED_SCHEMA_INVALID",
      "Author feed JSON does not match the expected schema",
      false,
    );
  }
  const html = FeedHtmlSchema.parse(
    record.data[currentSourceAdapterProfile().feed.responseHtmlKey],
  );
  const nextCursor = normalizeNextCursor(
    record.data[currentSourceAdapterProfile().feed.responseNextCursorKey],
  );
  return {
    html,
    nextCursor,
    terminal: html.trim() === "" || nextCursor === null,
  };
}

function parseRetryAfterMs(value: null | string): null | number {
  if (value === null) return null;
  if (/^\d+$/.test(value)) {
    const milliseconds = Number(value) * 1_000;
    return Number.isSafeInteger(milliseconds) ? milliseconds : null;
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return null;
  return Math.max(0, timestamp - Date.now());
}

function isRetryableHttpStatus(status: number): boolean {
  return status === 408 || status === 425 || status >= 500;
}

function feedRequestUrl(
  configuration: FeedConfiguration,
  cursor: string,
): string {
  const expected = new URL(configuration.endpoint);
  const profile = currentSourceAdapterProfile();
  expected.searchParams.set(profile.feed.cursorParameter, cursor);
  expected.searchParams.set(profile.feed.tokenParameter, configuration.token);
  return expected.href;
}

export function assertFeedProgress(
  feed: SourceFeedPage,
  currentCursor: string,
  seenCursors: ReadonlySet<string>,
  addedPoems: number,
): void {
  if (feed.terminal) return;
  if (addedPoems === 0) {
    throw new SourceBrowserError(
      "SOURCE_FEED_NO_PROGRESS",
      "Nonterminal author feed did not add poems",
    );
  }
  if (
    feed.nextCursor === null ||
    feed.nextCursor === currentCursor ||
    seenCursors.has(feed.nextCursor)
  ) {
    throw new SourceBrowserError(
      "SOURCE_FEED_CURSOR_LOOP",
      "Author feed repeated a cursor",
      false,
    );
  }
}

export function extractFeedConfigurationFromDocument(
  documentHtml: string,
  authorHref: string,
): FeedConfiguration {
  const scripts: string[] = [];
  let bytes = 0;
  for (const { attributes, source } of scriptElements(documentHtml)) {
    if (
      /(?:^|\s)src\s*=/iu.test(attributes) ||
      !source.includes(currentSourceAdapterProfile().feed.endpointMarker)
    )
      continue;
    bytes += new TextEncoder().encode(source).byteLength;
    if (
      scripts.length >= INLINE_SCRIPT_MAX_CANDIDATES ||
      bytes > INLINE_SCRIPT_MAX_BYTES
    ) {
      throw new SourceBrowserError(
        "SOURCE_FEED_CONFIG_SIZE",
        "Inline feed configuration exceeded its safety limits",
        false,
      );
    }
    scripts.push(source);
  }
  return extractFeedConfigurationFromInlineScripts(scripts, authorHref);
}

function scriptElements(
  documentHtml: string,
): readonly { readonly attributes: string; readonly source: string }[] {
  const lower = documentHtml.toLowerCase();
  const elements: { attributes: string; source: string }[] = [];
  let cursor = 0;
  while (elements.length <= INLINE_SCRIPT_MAX_CANDIDATES) {
    const start = lower.indexOf("<script", cursor);
    if (start < 0) break;
    const boundary = lower[start + "<script".length];
    if (boundary !== undefined && boundary !== ">" && !/\s/u.test(boundary)) {
      cursor = start + "<script".length;
      continue;
    }
    const openEnd = lower.indexOf(">", start + "<script".length);
    if (openEnd < 0) break;
    let close = lower.indexOf("</script", openEnd + 1);
    while (close >= 0) {
      const closeBoundary = lower[close + "</script".length];
      if (
        closeBoundary === ">" ||
        (closeBoundary !== undefined && /\s/u.test(closeBoundary))
      ) {
        break;
      }
      close = lower.indexOf("</script", close + "</script".length);
    }
    if (close < 0) break;
    const closeEnd = lower.indexOf(">", close + "</script".length);
    if (closeEnd < 0) break;
    elements.push({
      attributes: documentHtml.slice(start + "<script".length, openEnd),
      source: documentHtml.slice(openEnd + 1, close),
    });
    cursor = closeEnd + 1;
  }
  return elements;
}

function extractInlineValue(
  source: string,
  key: RegExp,
  projectionErrorCode: string,
): string {
  const expression = new RegExp(
    String.raw`["']?(?:${key.source})["']?\s*[:=]\s*(?:(["'])([^"'\\\s]{1,4096})\1|(\d{1,100}))`,
    "giu",
  );
  const values = new Set(
    // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Runtime compatibility requires an ordinary array before array transforms.
    [...source.matchAll(expression)]
      .map((match) => match[2] ?? match[3] ?? "")
      .filter(Boolean),
  );
  if (values.size === 0) {
    throw new SourceBrowserError(
      projectionErrorCode,
      "Inline feed configuration is incomplete",
    );
  }
  if (values.size !== 1) {
    throw new SourceBrowserError(
      `${projectionErrorCode}_AMBIGUOUS`,
      "Inline feed configuration declares conflicting values",
      false,
    );
  }
  const [value] = values;
  if (!value) throw new Error("Validated inline value unexpectedly missing");
  return value;
}

function escapeRegex(value: string): string {
  return value.replaceAll(/[.*+?^${}()|[\]\\]/gu, String.raw`\$&`);
}

function normalizeNextCursor(value: unknown): null | string {
  if (value === null || value === false || value === "" || value === 0) {
    return null;
  }
  if (
    (typeof value !== "string" && typeof value !== "number") ||
    (typeof value === "number" && (!Number.isSafeInteger(value) || value < 0))
  ) {
    throw new SourceBrowserError(
      "SOURCE_FEED_CURSOR_INVALID",
      "Author feed returned an invalid cursor",
      false,
    );
  }
  const cursor = String(value).trim();
  if (cursor.length === 0) return null;
  if (cursor.length > 4_096) {
    throw new SourceBrowserError(
      "SOURCE_FEED_CURSOR_INVALID",
      "Author feed cursor is oversized",
      false,
    );
  }
  return cursor;
}

async function restrictRequest(route: Route, request: Request): Promise<void> {
  if (
    !isAllowedBrowserRequest({
      isNavigationRequest: request.isNavigationRequest(),
      isSubframeNavigation:
        request.isNavigationRequest() && request.frame().parentFrame() !== null,
      method: request.method(),
      resourceType: request.resourceType(),
      url: request.url(),
    })
  ) {
    await route.abort("blockedbyclient");
    return;
  }
  await route.continue();
}

export async function resolveNavigationDocument(
  page: Page,
  response: null | Response,
  expectedHref: string,
  maximumBytes: number,
  signal: AbortSignal,
  timing: ChallengeResolutionTiming = {},
): Promise<string> {
  const envelope = await readNavigationEnvelope(
    response,
    expectedHref,
    maximumBytes,
  );
  if (!response) throw new Error("Validated response unexpectedly missing");
  throwIfAborted(signal);
  const resolved = await abortable(
    resolveCloudflareChallenge(
      page,
      response,
      envelope.html,
      expectedHref,
      signal,
      timing,
    ),
    signal,
    () => page.close(),
  );
  if (envelope.status !== 200 && !resolved) {
    throw new SourceBrowserError(
      "SOURCE_HTTP_STATUS",
      `Navigation returned HTTP ${String(envelope.status)}`,
      isRetryableHttpStatus(envelope.status),
    );
  }
  return resolved
    ? readSettledDocument(page, expectedHref, maximumBytes, signal)
    : envelope.html;
}

interface NavigationEnvelope {
  readonly html: string;
  readonly status: number;
}

async function readNavigationEnvelope(
  response: null | Response,
  expectedHref: string,
  maximumBytes: number,
): Promise<NavigationEnvelope> {
  if (!response)
    throw new SourceBrowserError(
      "SOURCE_NO_RESPONSE",
      "Navigation returned no response",
    );
  const headers = await response.allHeaders();
  const status = response.status();
  if (status === 429) {
    const retryAfterMs = parseRetryAfterMs(headers["retry-after"] ?? null);
    throw new SourceBrowserError(
      "SOURCE_RATE_LIMITED",
      `Navigation rate-limited the collector${retryAfterMs === null ? "" : ` for ${String(retryAfterMs)}ms`}`,
      true,
      retryAfterMs,
    );
  }
  const headerChallenged =
    headers["cf-mitigated"]?.toLowerCase() === "challenge";
  if (status !== 200 && status !== 401 && status !== 403 && !headerChallenged) {
    throw new SourceBrowserError(
      "SOURCE_HTTP_STATUS",
      `Navigation returned HTTP ${String(status)}`,
      isRetryableHttpStatus(status),
    );
  }
  if (response.url() !== expectedHref)
    throw new SourceBrowserError(
      "SOURCE_REDIRECT",
      "Navigation redirected",
      false,
    );
  const contentType = headers["content-type"]
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase();
  if (contentType !== "text/html" && contentType !== "application/xhtml+xml") {
    throw new SourceBrowserError(
      "SOURCE_CONTENT_TYPE",
      "Document is not HTML",
      false,
    );
  }
  const length = headers["content-length"];
  if (length) {
    const parsed = Number(length);
    if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > maximumBytes) {
      throw new SourceBrowserError(
        "SOURCE_DOCUMENT_SIZE",
        "Document is oversized",
        false,
      );
    }
  }
  const body = await response.body();
  if (body.byteLength > maximumBytes) {
    throw new SourceBrowserError(
      "SOURCE_DOCUMENT_SIZE",
      "Document is oversized",
      false,
    );
  }
  let html: string;
  try {
    html = new TextDecoder("utf-8", { fatal: true }).decode(body);
  } catch {
    throw new SourceBrowserError(
      "SOURCE_DOCUMENT_ENCODING",
      "Document is not valid UTF-8",
      false,
    );
  }
  if (
    status !== 200 &&
    !headerChallenged &&
    !isCloudflareChallengeEvidence({ html })
  ) {
    if (status === 401 || status === 403) {
      throw new SourceBrowserError(
        "SOURCE_HUMAN_REQUIRED",
        `collection source denied browser access with HTTP ${String(status)}; automatic bypass is disabled`,
        true,
        null,
        sourceAccessDiagnostic(
          "http_access_denied",
          "navigation",
          status,
          false,
        ),
      );
    }
    throw new SourceBrowserError(
      "SOURCE_HTTP_STATUS",
      `Navigation returned HTTP ${String(status)}`,
      isRetryableHttpStatus(status),
    );
  }
  return { html, status };
}

interface ChallengeResolutionTiming {
  readonly now?: () => number;
  readonly pollIntervalMs?: number;
  readonly sleep?: (milliseconds: number) => Promise<void>;
  readonly timeoutMs?: number;
}

interface CloudflareChallengeEvidence {
  readonly bodyText?: string;
  readonly hasChallengeOption?: boolean;
  readonly hasManagedChallengeElement?: boolean;
  readonly hasTurnstileElement?: boolean;
  readonly html?: string;
  readonly scriptSources?: readonly string[];
  readonly title?: string;
}

function sourceAccessDiagnostic(
  category: SourceAccessDiagnostic["category"],
  surface: SourceAccessDiagnostic["surface"],
  httpStatus: null | number,
  cfMitigated: boolean,
): SourceAccessDiagnostic {
  if (
    !["http_access_denied", "managed_challenge", "turnstile"].includes(
      category,
    ) ||
    !["feed", "navigation", "projection"].includes(surface) ||
    typeof cfMitigated !== "boolean"
  ) {
    throw new Error("Source access diagnostic is invalid");
  }
  if (
    httpStatus !== null &&
    (!Number.isSafeInteger(httpStatus) || httpStatus < 100 || httpStatus > 599)
  ) {
    throw new Error("Source access HTTP status must be null or 100..599");
  }
  return Object.freeze({
    category,
    cfMitigated,
    httpStatus,
    schemaVersion: 1,
    surface,
  });
}

type ChallengePageState = "challenge" | "clear" | "transition";

export function isCloudflareChallengeEvidence(
  evidence: CloudflareChallengeEvidence,
): boolean {
  return classifyCloudflareChallengeEvidence(evidence) !== null;
}

export function classifyCloudflareChallengeEvidence(
  evidence: CloudflareChallengeEvidence,
  challengeConfirmed = false,
): "managed_challenge" | "turnstile" | null {
  const html = (evidence.html ?? "").slice(0, INLINE_SCRIPT_MAX_BYTES);
  const title = (evidence.title ?? htmlTitle(html)).toLowerCase();
  const content = [evidence.bodyText ?? "", html].join("\n").toLowerCase();
  const challengeTitle =
    title.includes("just a moment") || title.includes("attention required");
  const challengePhrase =
    content.includes("verify you are human") ||
    content.includes("performing security verification") ||
    content.includes("enable javascript and cookies to continue");
  const challengeRuntime = content.includes("_cf_chl_opt");
  const challengeResource =
    content.includes("/orchestrate/chl_page/") ||
    content.includes("/cdn-cgi/challenge-platform/") ||
    evidence.scriptSources?.some(isChallengeResourceUrl) === true;
  const turnstile =
    evidence.hasTurnstileElement === true ||
    evidence.scriptSources?.some(isCloudflareChallengeUrl) === true ||
    content.includes("cf-turnstile");
  const challenged =
    challengeConfirmed ||
    evidence.hasManagedChallengeElement === true ||
    evidence.hasChallengeOption === true ||
    challengePhrase ||
    challengeRuntime ||
    (challengeTitle && (challengeResource || turnstile));
  if (!challenged) return null;
  return turnstile ? "turnstile" : "managed_challenge";
}

function htmlTitle(documentHtml: string): string {
  const lower = documentHtml.toLowerCase();
  let start = lower.indexOf("<title");
  while (start >= 0) {
    const boundary = lower[start + "<title".length];
    if (boundary === ">" || (boundary !== undefined && /\s/u.test(boundary))) {
      const openEnd = lower.indexOf(">", start + "<title".length);
      if (openEnd < 0) return "";
      const close = lower.indexOf("</title", openEnd + 1);
      if (close < 0) return "";
      return documentHtml.slice(openEnd + 1, close).trim();
    }
    start = lower.indexOf("<title", start + "<title".length);
  }
  return "";
}

export async function resolveCloudflareChallenge(
  page: Page,
  response: Response,
  documentHtml: string,
  expectedHref: string,
  signal: AbortSignal,
  timing: ChallengeResolutionTiming = {},
): Promise<boolean> {
  const headers = await response.allHeaders();
  const initialInspection = await inspectChallengePage(page, expectedHref);
  const cfMitigated = headers["cf-mitigated"]?.toLowerCase() === "challenge";
  const documentCategory = classifyCloudflareChallengeEvidence(
    {
      html: documentHtml,
    },
    cfMitigated,
  );
  let challengeCategory =
    initialInspection.category ?? documentCategory ?? "managed_challenge";
  const challenged =
    cfMitigated ||
    initialInspection.state === "challenge" ||
    documentCategory !== null;
  if (!challenged) return false;

  const resolved = await waitForChallengeResolution(
    async () => {
      const inspection = await inspectChallengePage(page, expectedHref);
      if (inspection.category === "turnstile") challengeCategory = "turnstile";
      return inspection.state;
    },
    signal,
    timing,
  );
  if (!resolved)
    throw new SourceBrowserError(
      "SOURCE_HUMAN_REQUIRED",
      "Cloudflare requires human interaction; automatic bypass is disabled",
      true,
      null,
      sourceAccessDiagnostic(
        challengeCategory,
        "navigation",
        response.status(),
        cfMitigated,
      ),
    );
  if (page.url() !== expectedHref)
    throw new SourceBrowserError(
      "SOURCE_REDIRECT",
      "Navigation redirected while resolving Cloudflare verification",
      false,
    );
  return true;
}

async function inspectChallengePage(
  page: Page,
  expectedHref: string,
): Promise<{
  readonly category: "managed_challenge" | "turnstile" | null;
  readonly state: ChallengePageState;
}> {
  try {
    const profile = currentSourceAdapterProfile();
    const expectedPath = new URL(expectedHref).pathname;
    const targetKind =
      sourcePathValue(profile.routes.poemPath, "id", expectedPath) === undefined
        ? sourcePathValue(profile.routes.authorPath, "slug", expectedPath) ===
          undefined
          ? "other"
          : "author"
        : "poem";
    const snapshot = await page.evaluate(
      ({
        authorLinkSelector,
        challengeScriptMaximum,
        contentSelector,
        expectedHref: serializedExpectedHref,
        poemCountLabels,
        targetKind: serializedTargetKind,
      }) => {
        const bodyText = document.body.innerText.slice(0, 20_000);
        const poemContent = document.querySelector(contentSelector);
        const hasMeaningfulPoemContent =
          (poemContent?.textContent ?? "").trim().length > 0;
        const hasAuthorEvidence =
          document.querySelector(authorLinkSelector) !== null ||
          poemCountLabels.some((label) => bodyText.includes(label));
        return {
          bodyText,
          hasChallengeOption: "_cf_chl_opt" in globalThis,
          hasManagedChallengeElement:
            document.querySelector("#challenge-form, .cf-challenge-running") !==
            null,
          hasTurnstileElement:
            document.querySelector(".cf-turnstile") !== null ||
            [
              ...document.querySelectorAll<HTMLIFrameElement>("iframe[src]"),
            ].some((frame) => {
              if (!URL.canParse(frame.src)) return false;
              const source = new URL(frame.src);
              return (
                source.origin === "https://challenges.cloudflare.com" &&
                (source.pathname.startsWith("/cdn-cgi/challenge-platform/") ||
                  source.pathname.startsWith("/turnstile/"))
              );
            }),
          href: location.href,
          readyState: document.readyState,
          scriptSources: [
            ...document.querySelectorAll<HTMLScriptElement>("script[src]"),
          ]
            .slice(0, challengeScriptMaximum)
            .map((script) => script.src),
          targetReady:
            location.href !== serializedExpectedHref ||
            (serializedTargetKind === "poem"
              ? hasMeaningfulPoemContent
              : serializedTargetKind === "author"
                ? hasAuthorEvidence
                : bodyText.trim().length > 0),
          title: document.title,
        };
      },
      {
        challengeScriptMaximum: INLINE_SCRIPT_MAX_CANDIDATES,
        authorLinkSelector: profile.dom.manifestPoemLinkSelector,
        contentSelector: profile.dom.detailContentSelector,
        expectedHref,
        poemCountLabels: profile.labels.poemCount,
        targetKind,
      },
    );
    const category = classifyCloudflareChallengeEvidence(snapshot);
    if (category !== null) return { category, state: "challenge" };
    return {
      category: null,
      state:
        snapshot.readyState === "loading" || !snapshot.targetReady
          ? "transition"
          : "clear",
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (
      /execution context was destroyed|cannot find context|because of a navigation/i.test(
        message,
      )
    )
      return { category: null, state: "transition" };
    throw error;
  }
}

async function readSettledDocument(
  page: Page,
  expectedHref: string,
  maximumBytes: number,
  signal: AbortSignal,
): Promise<string> {
  const snapshot = await abortable(
    page.evaluate((serializedMaximumBytes) => {
      const html = document.documentElement.outerHTML;
      if (html.length > serializedMaximumBytes) {
        return {
          byteLength: serializedMaximumBytes + 1,
          contentType: document.contentType,
          href: location.href,
          html: null,
          readyState: document.readyState,
        };
      }
      const byteLength = new TextEncoder().encode(html).byteLength;
      return {
        byteLength,
        contentType: document.contentType,
        href: location.href,
        html: byteLength > serializedMaximumBytes ? null : html,
        readyState: document.readyState,
      };
    }, maximumBytes),
    signal,
    () => page.close(),
  );
  if (snapshot.href !== expectedHref)
    throw new SourceBrowserError(
      "SOURCE_REDIRECT",
      "Navigation redirected while reading the settled document",
      false,
    );
  if (
    snapshot.contentType !== "text/html" &&
    snapshot.contentType !== "application/xhtml+xml"
  ) {
    throw new SourceBrowserError(
      "SOURCE_CONTENT_TYPE",
      "Settled document is not HTML",
      false,
    );
  }
  if (snapshot.readyState === "loading")
    throw new SourceBrowserError(
      "SOURCE_DOCUMENT_NOT_READY",
      "Settled document is still loading",
    );
  if (snapshot.html === null || snapshot.byteLength > maximumBytes)
    throw new SourceBrowserError(
      "SOURCE_DOCUMENT_SIZE",
      "Settled document is oversized",
      false,
    );
  return snapshot.html;
}

export async function waitForChallengeResolution(
  inspect: () => Promise<ChallengePageState>,
  signal: AbortSignal,
  timing: ChallengeResolutionTiming = {},
): Promise<boolean> {
  const now = timing.now ?? Date.now;
  const sleep = timing.sleep ?? globalThisDelay;
  const timeoutMs = timing.timeoutMs ?? CHALLENGE_RESOLUTION_TIMEOUT_MS;
  const pollIntervalMs = timing.pollIntervalMs ?? CHALLENGE_POLL_INTERVAL_MS;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1)
    throw new Error("Challenge timeout must be a positive integer");
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1)
    throw new Error("Challenge poll interval must be a positive integer");
  const deadline = now() + timeoutMs;
  let consecutiveClear = 0;
  while (now() < deadline) {
    throwIfAborted(signal);
    // eslint-disable-next-line no-await-in-loop -- Challenge observations must remain sequential.
    const state = await inspect();
    consecutiveClear = state === "clear" ? consecutiveClear + 1 : 0;
    if (consecutiveClear >= 2) return true;
    const remaining = deadline - now();
    if (remaining <= 0) break;
    // eslint-disable-next-line no-await-in-loop -- Polling waits between sequential challenge observations.
    await abortable(sleep(Math.min(pollIntervalMs, remaining)), signal);
  }
  return false;
}

async function projectManifest(
  page: Page,
  authorHref: string,
  terminal: boolean,
): Promise<AuthorPoemManifestProjection> {
  const profile = currentSourceAdapterProfile();
  return page.evaluate(
    ({
      authorHref: serializedAuthorHref,
      containerSelector,
      maximum,
      poemCountLabels,
      poemLinkSelector,
      poemPathTemplate,
      schemaVersion,
      terminal: serializedTerminal,
      verseCountLabels,
    }) => {
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const pathValue = (template: string, path: string) => {
        const marker = "{id}";
        const index = template.indexOf(marker);
        const prefix = template.slice(0, index);
        const suffix = template.slice(index + marker.length);
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) return null;
        const value = path.slice(prefix.length, path.length - suffix.length);
        return /^[1-9]\d*$/u.test(value) ? value : null;
      };
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const countText = (text: string, labels: readonly string[]) => {
        const normalized = text.normalize("NFC");
        for (const label of labels) {
          const at = normalized.indexOf(label);
          if (at < 0) continue;
          const before = normalized.slice(Math.max(0, at - 64), at);
          const digits = /[٠-٩۰-۹\d][٠-٩۰-۹\d,٬\s]*$/u.exec(before)?.[0];
          if (digits) return `${digits}${label}`;
        }
        return null;
      };
      const links = [
        ...document.querySelectorAll<HTMLAnchorElement>(poemLinkSelector),
      ];
      const poems = new Map<
        string,
        { href: string; title: string; verseCountText: null | string }
      >();
      for (const link of links) {
        const href = link.getAttribute("href") ?? "";
        const url = new URL(href, location.origin);
        if (url.origin !== location.origin) continue;
        const path = url.pathname;
        if (pathValue(poemPathTemplate, path) === null) continue;
        const container = link.closest(containerSelector) ?? link.parentElement;
        const title = link.textContent.trim();
        if (!title) continue;
        const text = (container?.textContent ?? "").trim();
        const verseCountText = countText(text, verseCountLabels);
        const existing = poems.get(path);
        if (!existing) {
          poems.set(path, { href: path, title, verseCountText });
        } else {
          const existingIsCount = /^[٠-٩۰-۹\d,٬\s]+$/u.test(existing.title);
          const candidateIsCount = /^[٠-٩۰-۹\d,٬\s]+$/u.test(title);
          poems.set(path, {
            href: path,
            title:
              existingIsCount && !candidateIsCount ? title : existing.title,
            verseCountText: existing.verseCountText ?? verseCountText,
          });
        }
        if (poems.size > maximum) break;
      }
      const bodyText = document.body.innerText.slice(0, 100_000);
      const declaredPoemCountText = countText(bodyText, poemCountLabels);
      return {
        authorHref: serializedAuthorHref,
        challengeDetected: false,
        declaredPoemCountText,
        kind: "author_poem_manifest" as const,
        // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Serialized browser callbacks target runtimes without Iterator Helpers.
        poems: [...poems.values()],
        schemaVersion,
        sourceUrl: location.href,
        terminal: serializedTerminal,
      };
    },
    {
      authorHref,
      containerSelector: profile.dom.manifestContainerSelector,
      maximum: LIMITS.poemsPerAuthor,
      poemCountLabels: profile.labels.poemCount,
      poemLinkSelector: profile.dom.manifestPoemLinkSelector,
      poemPathTemplate: profile.routes.poemPath,
      schemaVersion: PROJECTION_SCHEMA_VERSION,
      terminal,
      verseCountLabels: profile.labels.verseCount,
    },
  );
}

async function projectPoemsFromHtml(
  page: Page,
  html: string,
): Promise<AuthorPoemManifestProjection["poems"]> {
  const profile = currentSourceAdapterProfile();
  const poems = await page.evaluate(
    ({
      containerSelector,
      html: serializedHtml,
      maximum,
      poemLinkSelector,
      poemPathTemplate,
      verseCountLabels,
    }) => {
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const pathValue = (template: string, path: string) => {
        const marker = "{id}";
        const index = template.indexOf(marker);
        const prefix = template.slice(0, index);
        const suffix = template.slice(index + marker.length);
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) return null;
        const value = path.slice(prefix.length, path.length - suffix.length);
        return /^[1-9]\d*$/u.test(value) ? value : null;
      };
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const countText = (text: string, labels: readonly string[]) => {
        const normalized = text.normalize("NFC");
        for (const label of labels) {
          const at = normalized.indexOf(label);
          if (at < 0) continue;
          const before = normalized.slice(Math.max(0, at - 64), at);
          const digits = /[٠-٩۰-۹\d][٠-٩۰-۹\d,٬\s]*$/u.exec(before)?.[0];
          if (digits) return `${digits}${label}`;
        }
        return null;
      };
      const parsedDocument = new DOMParser().parseFromString(
        serializedHtml,
        "text/html",
      );
      const links = [
        ...parsedDocument.querySelectorAll<HTMLAnchorElement>(poemLinkSelector),
      ];
      const results = new Map<
        string,
        { href: string; title: string; verseCountText: null | string }
      >();
      for (const link of links) {
        const href = link.getAttribute("href") ?? "";
        const url = new URL(href, location.origin);
        if (url.origin !== location.origin) continue;
        const path = url.pathname;
        if (pathValue(poemPathTemplate, path) === null) continue;
        const container = link.closest(containerSelector) ?? link.parentElement;
        const title = link.textContent.trim();
        if (!title) continue;
        const text = (container?.textContent ?? "").trim();
        const verseCountText = countText(text, verseCountLabels);
        const existing = results.get(path);
        if (!existing) {
          results.set(path, { href: path, title, verseCountText });
        } else {
          const existingIsCount = /^[٠-٩۰-۹\d,٬\s]+$/u.test(existing.title);
          const candidateIsCount = /^[٠-٩۰-۹\d,٬\s]+$/u.test(title);
          results.set(path, {
            href: path,
            title:
              existingIsCount && !candidateIsCount ? title : existing.title,
            verseCountText: existing.verseCountText ?? verseCountText,
          });
        }
        if (results.size > maximum) break;
      }
      // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Serialized browser callbacks target runtimes without Iterator Helpers.
      return [...results.values()];
    },
    {
      containerSelector: profile.dom.manifestContainerSelector,
      html,
      maximum: LIMITS.poemsPerAuthor,
      poemLinkSelector: profile.dom.manifestPoemLinkSelector,
      poemPathTemplate: profile.routes.poemPath,
      verseCountLabels: profile.labels.verseCount,
    },
  );
  if (poems.length > LIMITS.poemsPerAuthor) {
    throw new SourceBrowserError(
      "SOURCE_MANIFEST_LIMIT",
      "Feed extraction exceeded its safety limit",
      false,
    );
  }
  return poems;
}

async function projectPoem(
  page: Page,
  expectedAuthorHref: string,
): Promise<PoemDetailProjection> {
  const profile = currentSourceAdapterProfile();
  const evidence = await page.evaluate(
    ({
      authorLinkSelector,
      challengeScriptMaximum,
      classicalLineSelector,
      contentSelector,
      expectedAuthorHref: serializedExpectedAuthorHref,
      fallbackLineSelector,
      maximumLines,
      schemaVersion,
      structureLabelSelector,
      verseCountLabels,
    }) => {
      // eslint-disable-next-line unicorn/consistent-function-scoping -- Playwright serializes this callback into the browser realm, so its helpers must remain inside it.
      const countText = (text: string, labels: readonly string[]) => {
        const normalized = text.normalize("NFC");
        for (const label of labels) {
          const at = normalized.indexOf(label);
          if (at < 0) continue;
          const before = normalized.slice(Math.max(0, at - 64), at);
          const digits = /[٠-٩۰-۹\d][٠-٩۰-۹\d,٬\s]*$/u.exec(before)?.[0];
          if (digits) return `${digits}${label}`;
        }
        return null;
      };
      const content = document.querySelector(contentSelector);
      const lineNodes = content
        ? [...content.querySelectorAll<HTMLElement>(classicalLineSelector)]
        : [];
      const fallback = content
        ? [...content.querySelectorAll<HTMLElement>(fallbackLineSelector)]
        : [];
      const selected = lineNodes.length > 0 ? lineNodes : fallback;
      const lines = selected
        .flatMap((node) =>
          (node.innerText || node.textContent || "").split(/\r?\n/u),
        )
        .map((line) => line.trim());
      while (lines[0] === "") lines.shift();
      while (lines.at(-1) === "") lines.pop();
      const boundedLines = lines.slice(0, maximumLines + 1);
      const expectedAuthorPath = new URL(serializedExpectedAuthorHref).pathname;
      const author = [
        ...document.querySelectorAll<HTMLAnchorElement>(authorLinkSelector),
      ].find(
        (candidate) =>
          new URL(candidate.getAttribute("href") ?? "", location.origin)
            .pathname === expectedAuthorPath,
      );
      const metadataTitle =
        document
          .querySelector<HTMLMetaElement>('meta[property="og:title"]')
          ?.getAttribute("content") ?? document.title;
      const title = metadataTitle.split(/\s+-\s+/u, 1)[0]?.trim() ?? "";
      const bodyText = document.body.innerText.slice(0, 100_000);
      const declaredVerseCountText = countText(bodyText, verseCountLabels);
      const structureLabels = [
        ...document.querySelectorAll<HTMLElement>(structureLabelSelector),
      ]
        .slice(0, 2_000)
        .map((node) => node.textContent.trim().normalize("NFC"));
      const challengeTitle = document.title;
      const challengeScriptSources = [
        ...document.querySelectorAll<HTMLScriptElement>("script[src]"),
      ]
        .slice(0, challengeScriptMaximum)
        .map((script) => script.src);
      return {
        authorHref: author?.getAttribute("href") ?? "",
        challengeEvidence: {
          bodyText: bodyText.slice(0, 20_000),
          hasChallengeOption: "_cf_chl_opt" in globalThis,
          hasManagedChallengeElement:
            document.querySelector("#challenge-form, .cf-challenge-running") !==
            null,
          hasTurnstileElement:
            document.querySelector(".cf-turnstile") !== null ||
            [
              ...document.querySelectorAll<HTMLIFrameElement>("iframe[src]"),
            ].some((frame) => {
              if (!URL.canParse(frame.src)) return false;
              const source = new URL(frame.src);
              return (
                source.origin === "https://challenges.cloudflare.com" &&
                (source.pathname.startsWith("/cdn-cgi/challenge-platform/") ||
                  source.pathname.startsWith("/turnstile/"))
              );
            }),
          scriptSources: challengeScriptSources,
          title: challengeTitle,
        },
        declaredVerseCountText,
        kind: "poem_detail" as const,
        lines: boundedLines,
        schemaVersion,
        sourceUrl: location.href,
        structureLabels,
        classicalLineNodeCount: lineNodes.length,
        title,
      };
    },
    {
      challengeScriptMaximum: INLINE_SCRIPT_MAX_CANDIDATES,
      authorLinkSelector: profile.dom.detailAuthorLinkSelector,
      classicalLineSelector: profile.dom.detailClassicalLineSelector,
      contentSelector: profile.dom.detailContentSelector,
      expectedAuthorHref,
      fallbackLineSelector: profile.dom.detailFallbackLineSelector,
      maximumLines: LIMITS.poemLines,
      schemaVersion: PROJECTION_SCHEMA_VERSION,
      structureLabelSelector: profile.dom.detailStructureLabelSelector,
      verseCountLabels: profile.labels.verseCount,
    },
  );
  const {
    challengeEvidence,
    classicalLineNodeCount,
    structureLabels,
    ...projection
  } = evidence;
  const challengeCategory =
    classifyCloudflareChallengeEvidence(challengeEvidence);
  if (challengeCategory !== null) {
    throw new SourceBrowserError(
      "SOURCE_HUMAN_REQUIRED",
      "Cloudflare challenge remained in the final poem projection",
      true,
      null,
      sourceAccessDiagnostic(challengeCategory, "projection", null, false),
    );
  }
  const structure = classifyPoemStructureEvidence(
    structureLabels,
    classicalLineNodeCount > 0,
  );
  return {
    ...projection,
    challengeDetected: false,
    declaredVerseCountText:
      structure === "free_verse"
        ? null
        : (projection.declaredVerseCountText ??
          (structure === "classical" && classicalLineNodeCount % 2 === 0
            ? String(classicalLineNodeCount / 2)
            : null)),
    structure,
  };
}

export function classifyPoemStructureEvidence(
  markerTexts: readonly string[],
  hasClassicalLineNodes: boolean,
): PoemDetailProjection["structure"] {
  if (
    markerTexts.some((value) =>
      currentSourceAdapterProfile().labels.freeVerse.includes(
        value.trim().normalize("NFC"),
      ),
    )
  ) {
    return "free_verse";
  }
  return hasClassicalLineNodes ? "classical" : "unknown";
}

function parseLooseCount(value: null | string): null | number {
  if (!value) return null;
  const translated = Array.from(value, (character) => {
    const index = "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹".indexOf(character);
    return index < 0 ? character : String(index % 10);
  }).join("");
  const match = /\d+/.exec(translated.replaceAll(/[,٬ ]/g, ""));
  return match ? Number(match[0]) : null;
}

export function manifestDigest(manifest: AuthorPoemManifestProjection): string {
  const records = manifest.poems
    .map(({ href, title, verseCountText }) => ({
      canonicalId: canonicalPoemUrl(href).canonicalId,
      title: title.trim().normalize("NFC"),
      verses: parseLooseCount(verseCountText),
    }))
    .toSorted((left, right) =>
      left.canonicalId.localeCompare(right.canonicalId),
    );
  const semanticManifest = JSON.stringify({
    declaredPoemCount: parseLooseCount(manifest.declaredPoemCountText),
    records,
  });
  return hash("sha256", semanticManifest, "hex");
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw signal.reason ?? new Error("Aborted");
}

async function abortable<T>(
  promise: Promise<T>,
  signal: AbortSignal,
  onAbort?: () => unknown,
): Promise<T> {
  throwIfAborted(signal);
  const { promise: result, reject, resolve } = Promise.withResolvers<T>();
  let settled = false;
  const cleanup = (): void => signal.removeEventListener("abort", abort);
  const abort = (): void => {
    if (settled) return;
    settled = true;
    cleanup();
    const cleanupTasks = onAbort ? [Promise.resolve().then(onAbort)] : [];
    void Promise.allSettled(cleanupTasks).then(() =>
      reject(toError(signal.reason, "Aborted")),
    );
  };
  signal.addEventListener("abort", abort, { once: true });
  void promise
    .then((value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    })
    .catch((error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(toError(error, "Operation failed"));
    });
  return result;
}

function globalThisDelay(milliseconds: number): Promise<void> {
  return delay(milliseconds);
}

function constantTimeEqual(left: string, right: string): boolean {
  const leftDigest = hash("sha256", left, "buffer");
  const rightDigest = hash("sha256", right, "buffer");
  return timingSafeEqual(leftDigest, rightDigest);
}

function errorCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !("code" in error)) return undefined;
  return typeof error.code === "string" ? error.code : undefined;
}

function toError(value: unknown, fallbackMessage: string): Error {
  return value instanceof Error ? value : new Error(fallbackMessage);
}

function isFileExistsError(error: unknown): boolean {
  return errorCode(error) === "EEXIST";
}

async function readProfileLock(path: string): Promise<{
  readonly pid: number;
  readonly raw: string;
  readonly token: string;
} | null> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    if (errorCode(error) === "ENOENT") {
      return null;
    }
    throw error;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("pid" in parsed) ||
      !("token" in parsed) ||
      !Number.isSafeInteger(parsed.pid) ||
      Number(parsed.pid) <= 0 ||
      typeof parsed.token !== "string" ||
      !/^[a-f\d-]{36}$/i.test(parsed.token)
    ) {
      return null;
    }
    return { pid: Number(parsed.pid), raw, token: parsed.token };
  } catch {
    return null;
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(errorCode(error) === "ESRCH");
  }
}

async function acquireProfileRecoveryLock(
  path: string,
  token: string,
): Promise<FileHandle | null> {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop -- Recovery-lock attempts serialize exclusive creation for one profile path.
      const handle = await open(path, "wx", 0o600);
      try {
        // eslint-disable-next-line no-await-in-loop -- The recovery token must be durable before returning ownership.
        await handle.writeFile(
          `${JSON.stringify({ pid: process.pid, token })}\n`,
          "utf8",
        );
        return handle;
      } catch (error) {
        // eslint-disable-next-line no-await-in-loop -- Failed recovery publication closes before removing its partial token.
        await handle.close();
        // eslint-disable-next-line no-await-in-loop -- Partial recovery state must be removed before retrying.
        await unlink(path);
        throw error;
      }
    } catch (error) {
      if (!isFileExistsError(error)) throw error;
      // eslint-disable-next-line no-await-in-loop -- Each retry inspects the latest recovery owner after exclusive-create contention.
      const existing = await readProfileLock(path);
      if (existing && processIsAlive(existing.pid)) return null;
      // eslint-disable-next-line no-await-in-loop -- Malformed recovery locks are age-checked before quarantine.
      const pathStats = await stat(path);
      if (!existing && Date.now() - pathStats.mtimeMs <= 60_000) return null;
      try {
        // eslint-disable-next-line no-await-in-loop -- Stale recovery ownership is quarantined before the next exclusive-create attempt.
        await rename(path, `${path}.stale.${randomUUID()}`);
      } catch (renameError) {
        if (errorCode(renameError) !== "ENOENT") throw renameError;
      }
    }
  }
  return null;
}
