import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import type {
  AuthorPoemManifestProjection,
  PoemDetailProjection,
} from "@saqi/source-adapter";
import { configureSource, SourceProjectionError } from "@saqi/source-adapter";
import type { BrowserContext, Page, Response } from "playwright-core";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  AbortableSerialQueue,
  assertFeedProgress,
  classifyCloudflareChallengeEvidence,
  classifyPoemStructureEvidence,
  effectiveMinimumSourceGapMs,
  extractFeedConfigurationFromDocument,
  extractFeedConfigurationFromInlineScripts,
  type FeedConfiguration,
  type FeedHttpResult,
  isAllowedBrowserRequest,
  isCloudflareChallengeEvidence,
  manifestDigest,
  parseFeedHttpResult,
  resolveCloudflareChallenge,
  resolveNavigationDocument,
  SourceBrowserError,
  SourceChromeCollector,
  waitForChallengeResolution,
} from "../collection/collection-source-browser";
import {
  authorUrlFromCatalogSlug,
  collectionWorkKinds,
  type CollectorBrowser,
  CollectorCoordinator,
  collectorImplementationVersion,
  collectorSchemaVersion,
  computeHumanChallengeRetryDelayMs,
  computeRetryDelayMs,
  deriveCollectorOriginHealthState,
  seedAuthorManifest,
  seedAuthorManifests,
} from "../collection/collector";
import { parseCatalogInventory } from "../collection/inventory-reconciliation";
import {
  ArtifactStore,
  DiskPressureError,
} from "../persistence/artifact-store";
import { CURRENT_SCHEMA_VERSION, Ledger } from "../persistence/ledger";
import { inputHash } from "../persistence/work-key";
import { CAPTURED_SOURCE_PROFILE } from "./support/source-profile";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

beforeEach(() => {
  configureSource({
    name: "source",
    origin: "https://source.invalid",
    profile: CAPTURED_SOURCE_PROFILE,
  });
});

const testArtifactStore = (root: string): ArtifactStore =>
  new ArtifactStore(join(root, "artifacts"), { minimumFreeBytes: 0 });

class FakeBrowser implements CollectorBrowser {
  manifestCalls = 0;
  poemCalls = 0;

  close(): Promise<void> {
    return Promise.resolve();
  }

  async collectAuthorManifest(
    _authorValue: string,
    _signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    this.manifestCalls += 1;
    return {
      authorHref: "https://source.invalid/writers/test",
      challengeDetected: false,
      declaredPoemCountText: "2",
      kind: "author_poem_manifest",
      poems: [
        { href: "/works/2", title: "ثان", verseCountText: "1" },
        { href: "/works/1", title: "أول", verseCountText: "1" },
      ],
      schemaVersion: 1,
      sourceUrl: "https://source.invalid/writers/test",
      terminal: true,
    };
  }

  async collectPoemDetail(
    poemValue: string,
    _authorValue: string,
    _signal: AbortSignal,
  ): Promise<PoemDetailProjection> {
    this.poemCalls += 1;
    return {
      authorHref: "/writers/test",
      challengeDetected: false,
      declaredVerseCountText: "1",
      kind: "poem_detail",
      lines: [`صدر ${poemValue}`, "عجز"],
      schemaVersion: 1,
      sourceUrl: poemValue,
      structure: "classical",
      title: "قصيدة",
    };
  }
}

class SlowBrowser extends FakeBrowser {
  readonly #afterDelay: (() => void) | undefined;
  readonly #delayMs: number;

  constructor(delayMs: number, afterDelay?: () => void) {
    super();
    this.#afterDelay = afterDelay;
    this.#delayMs = delayMs;
  }

  override async collectAuthorManifest(
    _authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    await delay(this.#delayMs, undefined, { signal });
    this.#afterDelay?.();
    return super.collectAuthorManifest(_authorValue, signal);
  }
}

class LaneBrowser implements CollectorBrowser {
  readonly calls: string[] = [];
  readonly #poemsPerManifest: number;

  constructor(poemsPerManifest = 25) {
    this.#poemsPerManifest = poemsPerManifest;
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  async collectAuthorManifest(
    authorValue: string,
  ): Promise<AuthorPoemManifestProjection> {
    this.calls.push(`author:${authorValue}`);
    const base = authorValue.endsWith("/writers/one") ? 1_000 : 2_000;
    return {
      authorHref: authorValue,
      challengeDetected: false,
      declaredPoemCountText: String(this.#poemsPerManifest),
      kind: "author_poem_manifest",
      poems: Array.from({ length: this.#poemsPerManifest }, (_, index) => ({
        href: `/works/${String(base + index + 1)}`,
        title: `قصيدة ${String(index + 1)}`,
        verseCountText: "1",
      })),
      schemaVersion: 1,
      sourceUrl: authorValue,
      terminal: true,
    };
  }

  async collectPoemDetail(
    poemValue: string,
    authorValue: string,
  ): Promise<PoemDetailProjection> {
    this.calls.push(`detail:${poemValue}`);
    return {
      authorHref: authorValue,
      challengeDetected: false,
      declaredVerseCountText: "1",
      kind: "poem_detail",
      lines: ["صدر", "عجز"],
      schemaVersion: 1,
      sourceUrl: poemValue,
      structure: "classical",
      title: "قصيدة",
    };
  }
}

class HumanRequiredBrowser extends FakeBrowser {
  calls = 0;

  override async collectAuthorManifest(): Promise<AuthorPoemManifestProjection> {
    this.calls += 1;
    throw Object.assign(
      new SourceBrowserError(
        "SOURCE_HUMAN_REQUIRED",
        "operator action required",
        true,
        null,
        Object.assign(
          {
            category: "turnstile" as const,
            cfMitigated: true,
            httpStatus: 403,
            schemaVersion: 1 as const,
            surface: "navigation" as const,
          },
          { cookie: "SECRET_NESTED_COOKIE", html: "SECRET_NESTED_HTML" },
        ),
      ),
      {
        bodyText: "SECRET_BODY_TEXT",
        cookie: "SECRET_COOKIE",
        html: "SECRET_HTML",
        scriptSources: "SECRET_SCRIPT_SOURCE",
        token: "SECRET_TOKEN",
      },
    );
  }
}

class RateLimitedBrowser extends FakeBrowser {
  override async collectAuthorManifest(): Promise<AuthorPoemManifestProjection> {
    throw new SourceBrowserError(
      "SOURCE_RATE_LIMITED",
      "slow down",
      true,
      60_000,
    );
  }
}

class NetworkRecoveryBrowser extends FakeBrowser {
  calls = 0;

  override async collectAuthorManifest(
    authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    this.calls += 1;
    if (this.calls === 1) throw new Error("net::ERR_INTERNET_DISCONNECTED");
    return super.collectAuthorManifest(authorValue, signal);
  }
}

class OperationTimeoutRecoveryBrowser extends FakeBrowser {
  calls = 0;

  override async collectAuthorManifest(
    authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    this.calls += 1;
    if (this.calls === 1)
      throw new SourceBrowserError(
        "SOURCE_AUTHOR_OPERATION_TIMEOUT",
        "bounded author operation timed out",
      );
    const manifest = await super.collectAuthorManifest(authorValue, signal);
    return { ...manifest, authorHref: authorValue, sourceUrl: authorValue };
  }
}

class RestartRequiredBrowser extends FakeBrowser {
  override async collectAuthorManifest(): Promise<AuthorPoemManifestProjection> {
    throw new SourceBrowserError(
      "SOURCE_BROWSER_RESTART_REQUIRED",
      "browser isolation could not be proven",
    );
  }
}

class SelectorDriftBrowser extends FakeBrowser {
  override async collectAuthorManifest(): Promise<AuthorPoemManifestProjection> {
    throw new SourceBrowserError(
      "SOURCE_SELECTOR_DRIFT",
      "fixture selector no longer matches",
      false,
    );
  }
}

class MissingFeedConfigurationBrowser extends FakeBrowser {
  override async collectAuthorManifest(): Promise<AuthorPoemManifestProjection> {
    throw new SourceBrowserError(
      "SOURCE_FEED_CONFIG_MISSING",
      "author page has no trusted feed configuration",
    );
  }
}

class CrossAuthorDuplicateBrowser extends FakeBrowser {
  override async collectAuthorManifest(
    authorValue: string,
  ): Promise<AuthorPoemManifestProjection> {
    this.manifestCalls += 1;
    const poems = [
      { href: "/works/42", title: "قصيدة", verseCountText: "1" },
      ...(authorValue.endsWith("poet-two")
        ? [
            {
              href: "/works/43",
              title: "قصيدة أخرى",
              verseCountText: "1",
            },
          ]
        : []),
    ];
    return {
      authorHref: authorValue,
      challengeDetected: false,
      declaredPoemCountText: String(poems.length),
      kind: "author_poem_manifest",
      poems,
      schemaVersion: 1,
      sourceUrl: authorValue,
      terminal: true,
    };
  }

  override async collectPoemDetail(
    poemValue: string,
    authorValue: string,
  ): Promise<PoemDetailProjection> {
    this.poemCalls += 1;
    return {
      authorHref: authorValue,
      challengeDetected: false,
      declaredVerseCountText: "1",
      kind: "poem_detail",
      lines: ["صدر", "عجز"],
      schemaVersion: 1,
      sourceUrl: poemValue,
      structure: "classical",
      title: "قصيدة",
    };
  }
}

class TransientProjectionBrowser extends FakeBrowser {
  override async collectAuthorManifest(
    authorValue: string,
    signal: AbortSignal,
  ): Promise<AuthorPoemManifestProjection> {
    const manifest = await super.collectAuthorManifest(authorValue, signal);
    return {
      ...manifest,
      declaredPoemCountText: "1",
      poems: manifest.poems.slice(0, 1),
    };
  }

  override async collectPoemDetail(
    poemValue: string,
  ): Promise<PoemDetailProjection> {
    this.poemCalls += 1;
    if (this.poemCalls < 3)
      throw new SourceProjectionError("SOURCE_POEM_CONTENT_EMPTY");
    return {
      authorHref: "/writers/test",
      challengeDetected: false,
      declaredVerseCountText: "1",
      kind: "poem_detail",
      lines: [`صدر ${poemValue}`, "عجز"],
      schemaVersion: 1,
      sourceUrl: poemValue,
      structure: "classical",
      title: "قصيدة",
    };
  }
}

class EmptyProjectionBrowser extends FakeBrowser {
  override async collectPoemDetail(): Promise<PoemDetailProjection> {
    this.poemCalls += 1;
    throw new SourceProjectionError("SOURCE_POEM_CONTENT_EMPTY");
  }
}

class CapacityDropArtifactStore extends ArtifactStore {
  #checks = 0;

  override async assertWritableCapacity(requiredBytes = 0) {
    this.#checks += 1;
    if (this.#checks > 1) throw new DiskPressureError(1, 2);
    return {
      availableBytes: 3 + requiredBytes,
      minimumFreeBytes: 2,
      writable: true,
    };
  }
}

describe("browser request policy", () => {
  it("enforces the empirically safe source-gap floor", () => {
    expect(effectiveMinimumSourceGapMs()).toBe(13_000);
    expect(effectiveMinimumSourceGapMs(0)).toBe(13_000);
    expect(effectiveMinimumSourceGapMs(20_000)).toBe(20_000);
    expect(() => effectiveMinimumSourceGapMs(NaN)).toThrow();
  });

  it.each([
    ["https://source.invalid/writers/test", "document", true, true],
    ["https://source.invalid/works/1", "document", true, true],
    ["https://source.invalid/directory/1", "document", true, true],
    ["https://source.invalid/writers/1/feed?cursor=x", "xhr", false, false],
    [
      "https://source.invalid/writers/1/feed?cursor=x&token=y",
      "fetch",
      false,
      true,
    ],
    ["https://source.invalid/app.js", "script", false, false],
    ["https://source.invalid/ad.jpg", "image", false, false],
    ["https://evil.example/app.js", "script", false, false],
    ["https://source.invalid/works/1?x=1", "document", true, false],
  ])("classifies %s", (url, resourceType, isNavigationRequest, expected) => {
    expect(
      isAllowedBrowserRequest({ isNavigationRequest, resourceType, url }),
    ).toBe(expected);
  });

  it.each([
    [
      {
        isNavigationRequest: false,
        method: "GET",
        resourceType: "script",
        url: "https://source.invalid/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=x",
      },
      true,
    ],
    [
      {
        isNavigationRequest: false,
        method: "POST",
        resourceType: "fetch",
        url: "https://source.invalid/cdn-cgi/challenge-platform/h/g/flow/ov1/x",
      },
      true,
    ],
    [
      {
        isNavigationRequest: true,
        isSubframeNavigation: true,
        method: "GET",
        resourceType: "document",
        url: "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/g/turnstile/if/ov2/x",
      },
      true,
    ],
    [
      {
        isNavigationRequest: false,
        method: "GET",
        resourceType: "script",
        url: "https://challenges.cloudflare.com/turnstile/v0/api.js",
      },
      true,
    ],
    [
      {
        isNavigationRequest: true,
        isSubframeNavigation: false,
        method: "GET",
        resourceType: "document",
        url: "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/x",
      },
      false,
    ],
    [
      {
        isNavigationRequest: false,
        method: "GET",
        resourceType: "script",
        url: "https://challenges.cloudflare.com/unrelated/app.js",
      },
      false,
    ],
    [
      {
        isNavigationRequest: false,
        method: "GET",
        resourceType: "script",
        url: "https://source.invalid/cdn-cgi/trace",
      },
      false,
    ],
    [
      {
        isNavigationRequest: false,
        method: "DELETE",
        resourceType: "fetch",
        url: "https://source.invalid/cdn-cgi/challenge-platform/x",
      },
      false,
    ],
  ])(
    "allows only exact Cloudflare challenge request %j",
    (request, expected) => {
      expect(isAllowedBrowserRequest(request)).toBe(expected);
    },
  );
});

describe("abortable serial queue", () => {
  it("does not deadlock or overtake when a queued operation aborts", async () => {
    const queue = new AbortableSerialQueue();
    const events: string[] = [];
    const { promise: firstGate, resolve: finishFirst } =
      Promise.withResolvers<undefined>();
    const first = queue.run(async () => {
      events.push("first:start");
      await firstGate;
      events.push("first:end");
    }, new AbortController().signal);
    await delay(0);

    const secondController = new AbortController();
    const second = queue.run(async () => {
      events.push("second:unexpected");
    }, secondController.signal);
    const third = queue.run(async () => {
      events.push("third");
    }, new AbortController().signal);
    secondController.abort(new Error("cancel second"));

    await expect(second).rejects.toThrow("cancel second");
    expect(events).toEqual(["first:start"]);
    finishFirst(undefined);
    await Promise.all([first, third]);
    expect(events).toEqual(["first:start", "first:end", "third"]);
  });
});

describe("Chrome profile ownership", () => {
  it("classifies launch failures and releases the profile for automatic retry", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-launch-fail-"));
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(async () => {
        throw new Error("Chrome executable temporarily unavailable");
      }),
      profileDirectory,
    });

    await expect(
      collector.collectPoemDetail(
        "https://source.invalid/works/1",
        "https://source.invalid/writers/poet-Test",
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({
      code: "SOURCE_BROWSER_LAUNCH_FAILED",
      retryable: true,
    });
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
  });

  it("discards a failed navigation page while retaining the browser profile", async () => {
    const profileDirectory = mkdtempSync(
      join(tmpdir(), "saqi-navigation-recovery-"),
    );
    let connected = true;
    let closed = false;
    const closePage = vi.fn(async () => {
      closed = true;
    });
    const page = {
      close: closePage,
      goto: vi.fn(async () => {
        throw new Error("net::ERR_CONNECTION_RESET");
      }),
      isClosed: () => closed,
      on: vi.fn(),
    } as unknown as Page;
    const browser = {
      isConnected: () => connected,
    };
    const context = {
      browser: () => browser,
      close: vi.fn(async () => {
        connected = false;
      }),
      on: vi.fn(),
      pages: () => [page],
      route: vi.fn(async () => undefined),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(async () => context),
      profileDirectory,
    });

    await expect(
      collector.collectPoemDetail(
        "https://source.invalid/works/1",
        "https://source.invalid/writers/poet-Test",
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({
      code: "SOURCE_NAVIGATION_FAILED",
      retryable: true,
    });
    expect(closePage).toHaveBeenCalledOnce();
    expect(browser.isConnected()).toBe(true);
    await collector.close();
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
  });

  it("force-closes a context that hangs during route initialization", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-route-race-"));
    const routeSetup = Promise.withResolvers<undefined>();
    let connected = true;
    const browser = {
      close: vi.fn(async () => {
        connected = false;
        routeSetup.reject(new Error("browser closed during route setup"));
      }),
      isConnected: () => connected,
    };
    const context = {
      browser: () => browser,
      close: vi.fn(),
      on: vi.fn(),
      route: vi.fn(() => routeSetup.promise),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(async () => context),
      poemOperationTimeoutMs: 10,
      profileDirectory,
    });

    await expect(
      collector.collectPoemDetail(
        "https://source.invalid/works/1",
        "https://source.invalid/writers/poet-Test",
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ code: "SOURCE_POEM_OPERATION_TIMEOUT" });
    expect(browser.close).toHaveBeenCalled();
    expect(browser.isConnected()).toBe(false);
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
  });

  it.each([
    {
      code: "SOURCE_AUTHOR_OPERATION_TIMEOUT",
      collect: (collector: SourceChromeCollector) =>
        collector.collectAuthorManifest(
          "https://source.invalid/writers/poet-Test",
          new AbortController().signal,
        ),
      timeout: "authorOperationTimeoutMs",
    },
    {
      code: "SOURCE_INVENTORY_OPERATION_TIMEOUT",
      collect: (collector: SourceChromeCollector) =>
        collector.collectAuthorInventoryPage(
          "https://source.invalid/directory/1",
          new AbortController().signal,
        ),
      timeout: "inventoryOperationTimeoutMs",
    },
  ] as const)(
    "bounds every $timeout browser operation",
    async ({ code, collect, timeout }) => {
      const profileDirectory = mkdtempSync(
        join(tmpdir(), "saqi-source-deadline-"),
      );
      const routeSetup = Promise.withResolvers<undefined>();
      let connected = true;
      const browser = {
        close: vi.fn(async () => {
          connected = false;
          routeSetup.reject(new Error("browser closed during route setup"));
        }),
        isConnected: () => connected,
      };
      const context = {
        browser: () => browser,
        close: vi.fn(),
        on: vi.fn(),
        route: vi.fn(() => routeSetup.promise),
        setDefaultNavigationTimeout: vi.fn(),
      } as unknown as BrowserContext;
      const collector = await SourceChromeCollector.create({
        [timeout]: 10,
        launchPersistentContext: vi.fn(async () => context),
        profileDirectory,
      });

      await expect(collect(collector)).rejects.toMatchObject({ code });
      expect(browser.close).toHaveBeenCalledOnce();
      expect(browser.isConnected()).toBe(false);
      const replacement = await SourceChromeCollector.create({
        profileDirectory,
      });
      await replacement.close();
    },
  );

  it("retries a transient poisoned close and releases the quarantined profile", async () => {
    let now = 1_000_000;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const profileDirectory = mkdtempSync(
      join(tmpdir(), "saqi-close-recovery-"),
    );
    let connected = true;
    const browser = { isConnected: () => connected };
    const close = vi
      .fn<() => Promise<void>>()
      .mockRejectedValueOnce(new Error("temporary close failure"))
      .mockImplementationOnce(async () => {
        connected = false;
      });
    let pageClosed = false;
    const page = {
      close: vi.fn(async () => {
        pageClosed = true;
      }),
      goto: vi.fn(async () => {
        throw new Error("net::ERR_CONNECTION_RESET");
      }),
      isClosed: () => pageClosed,
      on: vi.fn(),
    } as unknown as Page;
    const context = {
      browser: () => browser,
      close,
      on: vi.fn(),
      pages: () => [page],
      route: vi.fn(async () => undefined),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    let replacementPageClosed = false;
    let replacementConnected = true;
    const replacementPage = {
      close: vi.fn(async () => {
        replacementPageClosed = true;
      }),
      goto: vi.fn(async () => {
        throw new Error("net::ERR_CONNECTION_RESET");
      }),
      isClosed: () => replacementPageClosed,
      on: vi.fn(),
    } as unknown as Page;
    const replacementContext = {
      browser: () => ({ isConnected: () => replacementConnected }),
      close: vi.fn(async () => {
        replacementConnected = false;
      }),
      on: vi.fn(),
      pages: () => [replacementPage],
      route: vi.fn(async () => undefined),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    const launchPersistentContext = vi
      .fn()
      .mockResolvedValueOnce(context)
      .mockResolvedValueOnce(replacementContext);
    const collector = await SourceChromeCollector.create({
      launchPersistentContext,
      profileDirectory,
    });
    await expect(
      collector.collectPoemDetail(
        "https://source.invalid/works/1",
        "https://source.invalid/writers/poet-Test",
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ code: "SOURCE_NAVIGATION_FAILED" });
    await expect(collector.close()).rejects.toThrow("temporary close failure");
    now += 13_000;
    await expect(
      collector.collectPoemDetail(
        "https://source.invalid/works/2",
        "https://source.invalid/writers/poet-Test",
        new AbortController().signal,
      ),
    ).rejects.toMatchObject({ code: "SOURCE_NAVIGATION_FAILED" });
    expect(close).toHaveBeenCalledTimes(2);
    expect(launchPersistentContext).toHaveBeenCalledTimes(2);
    await collector.close();
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
    clock.mockRestore();
  });

  it("force-closes an unpublished context when launch resolves after timeout", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-launch-race-"));
    const launch = Promise.withResolvers<BrowserContext>();
    const launchStarted = Promise.withResolvers<undefined>();
    const deadline = new AbortController();
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(deadline.signal);
    let connected = true;
    const browser = {
      close: vi.fn(async () => {
        connected = false;
      }),
      isConnected: () => connected,
    };
    const context = {
      browser: () => browser,
      close: vi.fn(),
      on: vi.fn(),
      route: vi.fn(async () => undefined),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(() => {
        launchStarted.resolve(undefined);
        return launch.promise;
      }),
      poemOperationTimeoutMs: 10,
      profileDirectory,
    });
    const collecting = collector.collectPoemDetail(
      "https://source.invalid/works/1",
      "https://source.invalid/writers/poet-Test",
      new AbortController().signal,
    );
    await launchStarted.promise;
    deadline.abort();
    await expect(
      SourceChromeCollector.create({ profileDirectory }),
    ).rejects.toMatchObject({ code: "SOURCE_PROFILE_LOCKED" });
    launch.resolve(context);

    await expect(collecting).rejects.toMatchObject({
      code: "SOURCE_POEM_OPERATION_TIMEOUT",
    });
    expect(browser.close).toHaveBeenCalled();
    expect(browser.isConnected()).toBe(false);
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
    timeout.mockRestore();
  });

  it("retains the profile fence when launch cleanup cannot prove disconnect", async () => {
    const profileDirectory = mkdtempSync(
      join(tmpdir(), "saqi-launch-unproven-"),
    );
    const launch = Promise.withResolvers<BrowserContext>();
    const launchStarted = Promise.withResolvers<undefined>();
    const deadline = new AbortController();
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(deadline.signal);
    let connected = true;
    const browser = {
      close: vi.fn(async () => undefined),
      isConnected: () => connected,
    };
    const context = {
      browser: () => browser,
      close: vi.fn(async () => undefined),
      on: vi.fn(),
      route: vi.fn(async () => undefined),
      setDefaultNavigationTimeout: vi.fn(),
    } as unknown as BrowserContext;
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(() => {
        launchStarted.resolve(undefined);
        return launch.promise;
      }),
      poemOperationTimeoutMs: 10,
      profileDirectory,
    });
    const collecting = collector.collectPoemDetail(
      "https://source.invalid/works/1",
      "https://source.invalid/writers/poet-Test",
      new AbortController().signal,
    );
    await launchStarted.promise;
    deadline.abort();
    launch.resolve(context);

    await expect(collecting).rejects.toThrow(
      "SOURCE_BROWSER_DISCONNECT_UNPROVEN",
    );
    await expect(collector.close()).rejects.toThrow(
      "SOURCE_BROWSER_DISCONNECT_UNPROVEN",
    );
    await expect(
      SourceChromeCollector.create({ profileDirectory }),
    ).rejects.toMatchObject({ code: "SOURCE_PROFILE_LOCKED" });
    connected = false;
    await collector.close();
    timeout.mockRestore();
  });

  it("bounds a launch that never settles and requires a process restart", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-launch-wedge-"));
    const launch = Promise.withResolvers<BrowserContext>();
    const launchStarted = Promise.withResolvers<undefined>();
    const deadline = new AbortController();
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(deadline.signal);
    const collector = await SourceChromeCollector.create({
      launchPersistentContext: vi.fn(() => {
        launchStarted.resolve(undefined);
        return launch.promise;
      }),
      poemOperationTimeoutMs: 10,
      profileDirectory,
      shutdownGraceMs: 10,
    });
    const collecting = collector.collectPoemDetail(
      "https://source.invalid/works/1",
      "https://source.invalid/writers/poet-Test",
      new AbortController().signal,
    );
    await launchStarted.promise;
    deadline.abort();
    await expect(collecting).rejects.toMatchObject({
      code: "SOURCE_BROWSER_RESTART_REQUIRED",
    });
    await expect(
      SourceChromeCollector.create({ profileDirectory }),
    ).rejects.toMatchObject({ code: "SOURCE_PROFILE_LOCKED" });
    // Settle the fake launch after proving the fence; a real wedged process
    // exits, whereas this test worker remains alive for the remaining suite.
    launch.reject(new Error("Test launch shutdown"));
    await expect(collector.close()).rejects.toMatchObject({
      code: "SOURCE_BROWSER_RESTART_REQUIRED",
    });
    timeout.mockRestore();
  });

  it("fails closed when a second collector uses the same profile", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-profile-lock-"));
    const first = await SourceChromeCollector.create({ profileDirectory });
    await expect(
      SourceChromeCollector.create({ profileDirectory }),
    ).rejects.toMatchObject({ code: "SOURCE_PROFILE_LOCKED" });
    await first.close();
    const replacement = await SourceChromeCollector.create({
      profileDirectory,
    });
    await replacement.close();
  });

  it("recovers a well-formed lock owned by a dead process", async () => {
    const profileDirectory = mkdtempSync(join(tmpdir(), "saqi-stale-lock-"));
    writeFileSync(
      join(profileDirectory, ".saqi-collector.lock"),
      `${JSON.stringify({
        pid: 2_147_483_647,
        token: "00000000-0000-4000-8000-000000000000",
      })}\n`,
    );
    writeFileSync(
      join(profileDirectory, ".saqi-collector.lock.recovery"),
      `${JSON.stringify({
        pid: 2_147_483_647,
        token: "11111111-1111-4111-8111-111111111111",
      })}\n`,
    );
    const collector = await SourceChromeCollector.create({ profileDirectory });
    await expect(
      SourceChromeCollector.create({ profileDirectory }),
    ).rejects.toMatchObject({ code: "SOURCE_PROFILE_LOCKED" });
    await collector.close();
  });
});

describe("Cloudflare challenge recovery", () => {
  const authorHref = "https://source.invalid/writers/435";

  function navigationResponse(options: {
    readonly body: string;
    readonly headers?: Record<string, string>;
    readonly status?: number;
    readonly url?: string;
  }): Response {
    return {
      allHeaders: vi.fn(async () => ({
        "content-type": "text/html; charset=utf-8",
        ...options.headers,
      })),
      body: vi.fn(async () => Buffer.from(options.body)),
      status: () => options.status ?? 200,
      url: () => options.url ?? authorHref,
    } as unknown as Response;
  }

  it.each([
    [{ bodyText: "Enable JavaScript and cookies to continue" }],
    [{ html: "<script>window._cf_chl_opt={}</script>" }],
    [
      {
        scriptSources: [
          "https://source.invalid/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1",
        ],
        title: "Just a moment...",
      },
    ],
    [{ hasManagedChallengeElement: true }],
  ])("detects bounded challenge evidence", (evidence) => {
    expect(isCloudflareChallengeEvidence(evidence)).toBe(true);
  });

  it("treats an ordinary embedded Turnstile widget as clear", () => {
    expect(
      isCloudflareChallengeEvidence({
        bodyText: "صفحة قصيدة عربية عادية",
        hasTurnstileElement: true,
        scriptSources: [
          "https://challenges.cloudflare.com/turnstile/v0/api.js",
        ],
        title: "عنوان القصيدة",
      }),
    ).toBe(false);
  });

  it("classifies managed and challenge-page Turnstile evidence without classifying an ordinary widget", () => {
    expect(
      classifyCloudflareChallengeEvidence({ hasChallengeOption: true }),
    ).toBe("managed_challenge");
    expect(
      classifyCloudflareChallengeEvidence({
        bodyText: "Verify you are human",
        hasTurnstileElement: true,
      }),
    ).toBe("turnstile");
    expect(
      classifyCloudflareChallengeEvidence({
        bodyText: "صفحة قصيدة عربية عادية",
        hasTurnstileElement: true,
      }),
    ).toBeNull();
    expect(
      classifyCloudflareChallengeEvidence({
        bodyText: "Verify you are human",
        scriptSources: [
          "https://challenges.cloudflare.com.evil.example/turnstile/v0/api.js",
        ],
      }),
    ).toBe("managed_challenge");
  });

  it("does not classify an ordinary poem document as a challenge", () => {
    expect(
      isCloudflareChallengeEvidence({
        bodyText: "قصيدة عربية",
        html: '<main id="poem_content"></main>',
        title: "عنوان القصيدة",
      }),
    ).toBe(false);
  });

  it("waits for two stable clear observations after a challenge", async () => {
    let now = 0;
    const states: ("challenge" | "clear" | "transition")[] = [
      "challenge",
      "transition",
      "clear",
      "challenge",
      "clear",
      "clear",
    ];
    const inspect = vi.fn(async () => states.shift() ?? "clear");
    await expect(
      waitForChallengeResolution(inspect, new AbortController().signal, {
        now: () => now,
        pollIntervalMs: 1,
        sleep: async (milliseconds) => {
          now += milliseconds;
        },
        timeoutMs: 10,
      }),
    ).resolves.toBe(true);
    expect(inspect).toHaveBeenCalledTimes(6);
  });

  it("bounds an unresolved challenge without spinning", async () => {
    let now = 0;
    const inspect = vi.fn(async () => "challenge" as const);
    await expect(
      waitForChallengeResolution(inspect, new AbortController().signal, {
        now: () => now,
        pollIntervalMs: 2,
        sleep: async (milliseconds) => {
          now += milliseconds;
        },
        timeoutMs: 5,
      }),
    ).resolves.toBe(false);
    expect(inspect).toHaveBeenCalledTimes(3);
  });

  it("converts an unresolved browser challenge into human-required", async () => {
    let now = 0;
    const page = {
      evaluate: vi.fn(async () => ({
        bodyText: "Verify you are human",
        hasManagedChallengeElement: true,
        hasChallengeOption: true,
        readyState: "complete",
        scriptSources: ["https://source.invalid/cdn-cgi/challenge-platform/"],
        targetReady: false,
        title: "Just a moment...",
      })),
      url: () => "https://source.invalid/works/1",
    } as unknown as Page;
    const response = {
      allHeaders: vi.fn(async () => ({})),
      status: () => 403,
    } as unknown as Response;
    await expect(
      resolveCloudflareChallenge(
        page,
        response,
        "<script>window._cf_chl_opt={}</script>",
        "https://source.invalid/works/1",
        new AbortController().signal,
        {
          now: () => now,
          pollIntervalMs: 2,
          sleep: async (milliseconds) => {
            now += milliseconds;
          },
          timeoutMs: 5,
        },
      ),
    ).rejects.toMatchObject({
      code: "SOURCE_HUMAN_REQUIRED",
      sourceAccess: {
        category: "managed_challenge",
        cfMitigated: false,
        httpStatus: 403,
        schemaVersion: 1,
        surface: "navigation",
      },
    });
  });

  it("continues the same URL after a challenge clears stably", async () => {
    let now = 0;
    const snapshots = [
      {
        bodyText: "Verify you are human",
        hasManagedChallengeElement: true,
        hasChallengeOption: true,
        readyState: "complete",
        scriptSources: ["https://source.invalid/cdn-cgi/challenge-platform/"],
        targetReady: false,
        title: "Just a moment...",
      },
      {
        bodyText: "قصيدة عربية",
        hasManagedChallengeElement: false,
        hasChallengeOption: false,
        readyState: "complete",
        scriptSources: [],
        targetReady: true,
        title: "عنوان القصيدة",
      },
      {
        bodyText: "قصيدة عربية",
        hasManagedChallengeElement: false,
        hasChallengeOption: false,
        readyState: "complete",
        scriptSources: [],
        targetReady: true,
        title: "عنوان القصيدة",
      },
    ];
    const evaluate = vi.fn(async (browserFunction: unknown) => {
      expect(String(browserFunction)).not.toContain(
        "INLINE_SCRIPT_MAX_CANDIDATES",
      );
      return snapshots.shift() ?? snapshots.at(-1);
    });
    const page = {
      evaluate,
      url: () => "https://source.invalid/works/1",
    } as unknown as Page;
    const response = {
      allHeaders: vi.fn(async () => ({})),
    } as unknown as Response;
    await expect(
      resolveCloudflareChallenge(
        page,
        response,
        "<script>window._cf_chl_opt={}</script>",
        "https://source.invalid/works/1",
        new AbortController().signal,
        {
          now: () => now,
          pollIntervalMs: 1,
          sleep: async (milliseconds) => {
            now += milliseconds;
          },
          timeoutMs: 10,
        },
      ),
    ).resolves.toBe(true);
    expect(evaluate).toHaveBeenCalledTimes(3);
  });

  it("resolves a bounded 403 challenge and returns settled author HTML", async () => {
    let now = 0;
    const settledHtml = `<html><body>1 قصيدة<a href="/works/1">قصيدة</a>
      <script>var poemsEndpoint="/writers/435/feed";
      var nextPoemsCursor="cursor-settled";
      const headers={"X-Feed-Token":"token-settled"};</script></body></html>`;
    const challenge = {
      bodyText: "Verify you are human",
      hasChallengeOption: true,
      hasManagedChallengeElement: true,
      hasTurnstileElement: true,
      readyState: "complete",
      scriptSources: ["https://source.invalid/cdn-cgi/challenge-platform/"],
      targetReady: false,
      title: "Just a moment...",
    };
    const ready = {
      bodyText: "1 قصيدة",
      hasChallengeOption: false,
      hasManagedChallengeElement: false,
      hasTurnstileElement: false,
      readyState: "complete",
      scriptSources: [],
      targetReady: true,
      title: "شاعر",
    };
    const page = {
      close: vi.fn(async () => undefined),
      evaluate: vi
        .fn()
        .mockResolvedValueOnce(challenge)
        .mockResolvedValueOnce(ready)
        .mockResolvedValueOnce(ready)
        .mockResolvedValueOnce({
          byteLength: new TextEncoder().encode(settledHtml).byteLength,
          contentType: "text/html",
          href: authorHref,
          html: settledHtml,
          readyState: "complete",
        }),
      url: () => authorHref,
    } as unknown as Page;
    const html = await resolveNavigationDocument(
      page,
      navigationResponse({
        body: `<html><title>Just a moment...</title>
          <script src="/cdn-cgi/challenge-platform/orchestrate/chl_page/v1"></script></html>`,
        status: 403,
      }),
      authorHref,
      64 * 1024,
      new AbortController().signal,
      {
        now: () => now,
        pollIntervalMs: 1,
        sleep: async (milliseconds) => {
          now += milliseconds;
        },
        timeoutMs: 10,
      },
    );
    expect(html).toBe(settledHtml);
    expect(extractFeedConfigurationFromDocument(html, authorHref)).toEqual({
      cursor: "cursor-settled",
      endpoint: "https://source.invalid/writers/435/feed",
      token: "token-settled",
    });
  });

  it.each([401, 403])(
    "origin-gates an unrecognized HTTP %s access block",
    async (status) => {
      const evaluate = vi.fn();
      const page = { evaluate } as unknown as Page;
      await expect(
        resolveNavigationDocument(
          page,
          navigationResponse({
            body: "<html><body>Forbidden</body></html>",
            status,
          }),
          authorHref,
          64 * 1024,
          new AbortController().signal,
        ),
      ).rejects.toMatchObject({
        code: "SOURCE_HUMAN_REQUIRED",
        sourceAccess: {
          category: "http_access_denied",
          cfMitigated: false,
          httpStatus: status,
          schemaVersion: 1,
          surface: "navigation",
        },
      });
      expect(evaluate).not.toHaveBeenCalled();
    },
  );

  it("closes the page when an in-flight challenge resolution is aborted", async () => {
    const controller = new AbortController();
    const close = vi.fn(async () => undefined);
    const evaluate = vi
      .fn()
      .mockResolvedValueOnce({
        bodyText: "Verify you are human",
        hasChallengeOption: true,
        hasManagedChallengeElement: true,
        readyState: "complete",
        scriptSources: ["https://source.invalid/cdn-cgi/challenge-platform/"],
        targetReady: false,
        title: "Just a moment...",
      })
      .mockImplementationOnce(
        async () =>
          new Promise(() => {
            controller.abort(new Error("deadline"));
          }),
      );
    const page = { close, evaluate, url: () => authorHref } as unknown as Page;
    await expect(
      resolveNavigationDocument(
        page,
        navigationResponse({
          body: "<script>window._cf_chl_opt={}</script>",
          headers: { "cf-mitigated": "challenge" },
          status: 403,
        }),
        authorHref,
        64 * 1024,
        controller.signal,
      ),
    ).rejects.toThrow("deadline");
    expect(close).toHaveBeenCalledOnce();
  });
});

describe("poem structure evidence", () => {
  it("keeps a positive free-verse marker authoritative over h3 layout", () => {
    const representativeFreeVerseFixture = {
      classicalLineNodesPresent: true,
      metadataLabels: ["general works", "free verse"],
    } as const;
    expect(
      classifyPoemStructureEvidence(
        representativeFreeVerseFixture.metadataLabels,
        representativeFreeVerseFixture.classicalLineNodesPresent,
      ),
    ).toBe("free_verse");
    expect(classifyPoemStructureEvidence(["البحر الطويل"], true)).toBe(
      "classical",
    );
    expect(classifyPoemStructureEvidence([], false)).toBe("unknown");
  });
});

describe("author manifest verification", () => {
  const base: AuthorPoemManifestProjection = {
    authorHref: "https://source.invalid/writers/test",
    challengeDetected: false,
    declaredPoemCountText: "1",
    kind: "author_poem_manifest",
    poems: [{ href: "/works/1", title: "قصيدة", verseCountText: "2" }],
    schemaVersion: 1,
    sourceUrl: "https://source.invalid/writers/test",
    terminal: true,
  };

  it("normalizes harmless formatting but detects changed semantic metadata", () => {
    expect(
      manifestDigest({
        ...base,
        poems: [
          {
            href: "https://source.invalid/works/1",
            title: " قصيدة ",
            verseCountText: "٢ بيت",
          },
        ],
      }),
    ).toBe(manifestDigest(base));

    expect(
      manifestDigest({
        ...base,
        poems: [{ ...base.poems[0]!, title: "قصيدة مصححة" }],
      }),
    ).not.toBe(manifestDigest(base));
    expect(
      manifestDigest({
        ...base,
        poems: [{ ...base.poems[0]!, verseCountText: "3" }],
      }),
    ).not.toBe(manifestDigest(base));
  });
});

describe("author feed protocol", () => {
  const configuration: FeedConfiguration = {
    cursor: "start",
    endpoint: "https://source.invalid/writers/435/feed",
    token: "signed-token",
  };

  function response(
    body: unknown,
    overrides: Partial<FeedHttpResult> = {},
  ): FeedHttpResult {
    const serialized = typeof body === "string" ? body : JSON.stringify(body);
    return {
      body: serialized,
      bytes: new TextEncoder().encode(serialized).byteLength,
      cfMitigated: null,
      contentType: "application/json; charset=utf-8",
      retryAfter: null,
      status: 200,
      url: "https://source.invalid/writers/435/feed?cursor=start&token=signed-token",
      ...overrides,
    };
  }

  it("extracts only the exact author's bounded inline configuration", () => {
    expect(
      extractFeedConfigurationFromInlineScripts(
        [
          `var poemsEndpoint = "https://source.invalid/writers/435/feed";
           var nextPoemsCursor = "cursor-1";
           headers: { "X-Feed-Token": "token-1" };`,
        ],
        "https://source.invalid/writers/poet-Mutanabi",
      ),
    ).toEqual({
      cursor: "cursor-1",
      endpoint: "https://source.invalid/writers/435/feed",
      token: "token-1",
    });
    expect(() =>
      extractFeedConfigurationFromInlineScripts(
        [
          `const endpoint = "/writers/poet-evil/feed";
           const cursor = "x"; const feedToken = "y";`,
        ],
        "https://source.invalid/writers/435",
      ),
    ).toThrow(
      expect.objectContaining({ code: "SOURCE_FEED_ENDPOINT_INVALID" }),
    );
  });

  it("extracts static config from the raw document without running scripts", () => {
    expect(
      extractFeedConfigurationFromDocument(
        `<html><head>
          <script src="https://evil.example/feed">var cursor="evil";</script>
          <script>var poemsEndpoint="/writers/435/feed";
            var nextPoemsCursor="cursor-raw";
            const headers={"X-Feed-Token":"token-raw"};</script>
        </head></html>`,
        "https://source.invalid/writers/poet-Mutanabi",
      ),
    ).toEqual({
      cursor: "cursor-raw",
      endpoint: "https://source.invalid/writers/435/feed",
      token: "token-raw",
    });
    expect(
      extractFeedConfigurationFromDocument(
        `<script>var poemsEndpoint="/writers/435/feed";
          var nextPoemsCursor="cursor-tab";
          const headers={"X-Feed-Token":"token-tab"};</script\t\n data-extra>`,
        "https://source.invalid/writers/poet-Mutanabi",
      ),
    ).toEqual({
      cursor: "cursor-tab",
      endpoint: "https://source.invalid/writers/435/feed",
      token: "token-tab",
    });
  });

  it("accepts exact feed JSON and treats only protocol exhaustion as terminal", () => {
    expect(
      parseFeedHttpResult(
        response({ html: "<a href='/works/1'>قصيدة</a>", next_cursor: "2" }),
        configuration,
        "start",
      ),
    ).toEqual({
      html: "<a href='/works/1'>قصيدة</a>",
      nextCursor: "2",
      terminal: false,
    });
    expect(
      parseFeedHttpResult(
        response({ html: "<p>last</p>", next_cursor: null }),
        configuration,
        "start",
      ).terminal,
    ).toBe(true);
    expect(
      parseFeedHttpResult(
        response({ html: " ".repeat(3), next_cursor: "unexpected" }),
        configuration,
        "start",
      ).terminal,
    ).toBe(true);
  });

  it.each([
    [
      response("<html><body>Forbidden</body></html>", {
        contentType: "text/html",
        status: 403,
      }),
      {
        category: "http_access_denied",
        cfMitigated: false,
        httpStatus: 403,
        schemaVersion: 1,
        surface: "feed",
      },
    ],
    [
      response("<html><script>window._cf_chl_opt={}</script></html>", {
        contentType: "text/html",
        status: 503,
      }),
      {
        category: "managed_challenge",
        cfMitigated: false,
        httpStatus: 503,
        schemaVersion: 1,
        surface: "feed",
      },
    ],
    [
      response("<html><div class='cf-turnstile'></div></html>", {
        cfMitigated: "challenge",
        contentType: "text/html",
        status: 403,
      }),
      {
        category: "turnstile",
        cfMitigated: true,
        httpStatus: 403,
        schemaVersion: 1,
        surface: "feed",
      },
    ],
  ])("retains only bounded feed access evidence", (value, sourceAccess) => {
    expect(() => parseFeedHttpResult(value, configuration, "start")).toThrow(
      expect.objectContaining({ sourceAccess }),
    );
  });

  it.each([
    [
      response("<html>challenge</html>", {
        cfMitigated: "challenge",
        contentType: "text/html",
        status: 403,
      }),
      "SOURCE_HUMAN_REQUIRED",
    ],
    [response({ html: "x" }), "SOURCE_FEED_SCHEMA_INVALID"],
    [
      response({ html: "x", next_cursor: "2" }, { status: 500 }),
      "SOURCE_FEED_HTTP_STATUS",
    ],
    [
      response("<html><body>Forbidden</body></html>", {
        contentType: "text/html",
        status: 403,
      }),
      "SOURCE_HUMAN_REQUIRED",
    ],
    [
      response("<html><script>window._cf_chl_opt={}</script></html>", {
        contentType: "text/html",
        status: 503,
      }),
      "SOURCE_HUMAN_REQUIRED",
    ],
    [
      response(
        { html: "x", next_cursor: "2" },
        { url: "https://source.invalid/writers/435/feed?cursor=wrong" },
      ),
      "SOURCE_FEED_REDIRECT",
    ],
  ])(
    "rejects failed, challenged, partial, or redirected feeds",
    (value, code) => {
      expect(() => parseFeedHttpResult(value, configuration, "start")).toThrow(
        expect.objectContaining({ code }),
      );
    },
  );

  it("rejects nonterminal no-progress and cursor loops", () => {
    expect(() =>
      assertFeedProgress(
        { html: "<p>x</p>", nextCursor: "2", terminal: false },
        "1",
        new Set(["1"]),
        0,
      ),
    ).toThrow(expect.objectContaining({ code: "SOURCE_FEED_NO_PROGRESS" }));
    expect(() =>
      assertFeedProgress(
        { html: "<p>x</p>", nextCursor: "1", terminal: false },
        "1",
        new Set(["1"]),
        1,
      ),
    ).toThrow(expect.objectContaining({ code: "SOURCE_FEED_CURSOR_LOOP" }));
  });

  it.each([
    [404, false],
    [408, true],
    [425, true],
    [503, true],
  ] as const)(
    "classifies feed status %i with retryable=%s",
    (status, retryable) => {
      try {
        parseFeedHttpResult(
          response({ html: "", next_cursor: null }, { status }),
          configuration,
          "start",
        );
        expect.unreachable("Non-200 feed status must fail");
      } catch (error) {
        expect(error).toMatchObject({
          code: "SOURCE_FEED_HTTP_STATUS",
          retryable,
        });
      }
    },
  );
});

describe("collector coordinator", () => {
  it("claims v28 collection rows in the externally configured namespace", async () => {
    configureSource({
      name: "archive",
      origin: "https://source.invalid",
      profile: CAPTURED_SOURCE_PROFILE,
    });
    const root = mkdtempSync(join(tmpdir(), "saqi-collector-compat-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    try {
      expect(ledger.doctor().schemaVersion).toBe(CURRENT_SCHEMA_VERSION);
      const input = {
        authorHref: "https://source.invalid/writers/test",
        poemHref: "https://source.invalid/works/1",
      };
      const seeded = ledger.seed({
        implementationVersion: "archive-chrome-v4",
        input,
        inputHash: inputHash(input),
        kind: "archive_poem_detail",
        priority: 0,
        schemaVersion: "archive-projection-v1",
      });
      const browser = new FakeBrowser();
      const coordinator = new CollectorCoordinator({
        artifacts: testArtifactStore(root),
        browser,
        ledger,
        minimumOriginGapMs: 1,
      });

      await expect(
        coordinator.run(new AbortController().signal, { maximum: 1 }),
      ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
      expect(ledger.get(seeded.workKey)).toMatchObject({ state: "succeeded" });
      expect(browser.poemCalls).toBe(1);
      const current = seedAuthorManifest(
        ledger,
        "https://source.invalid/writers/current",
      );
      expect(ledger.get(current.workKey)).toMatchObject({
        implementationVersion: collectorImplementationVersion(),
        kind: collectionWorkKinds().authorManifest,
        schemaVersion: collectorSchemaVersion(),
      });
      expect(ledger.get(current.workKey)?.kind).toBe("archive_author_manifest");

      const currentPoemInput = {
        authorHref: "https://source.invalid/writers/current",
        poemHref: "https://source.invalid/works/2",
      };
      const currentPoem = ledger.seedPoems([
        {
          implementationVersion: collectorImplementationVersion(),
          input: currentPoemInput,
          inputHash: inputHash(currentPoemInput),
          kind: collectionWorkKinds().poemDetail,
          priority: 0,
          schemaVersion: collectorSchemaVersion(),
        },
      ]);
      expect(currentPoem.conflicts).toHaveLength(0);
      expect(ledger.get(currentPoem.results[0]!.workKey)?.kind).toBe(
        "archive_poem_detail",
      );

      const conflictingInput = {
        ...currentPoemInput,
        authorHref: "https://source.invalid/writers/conflict",
      };
      expect(
        ledger.seedPoems([
          {
            implementationVersion: collectorImplementationVersion(),
            input: conflictingInput,
            inputHash: inputHash(conflictingInput),
            kind: collectionWorkKinds().poemDetail,
            priority: 0,
            schemaVersion: collectorSchemaVersion(),
          },
        ]),
      ).toMatchObject({ conflicts: [expect.any(Error)], results: [] });
      await coordinator.close();
    } finally {
      ledger.close();
      configureSource({
        name: "source",
        origin: "https://source.invalid",
        profile: CAPTURED_SOURCE_PROFILE,
      });
    }
  });

  it("claims only an explicitly included recovery cohort", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-exact-collector-cohort-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new FakeBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
    });
    const seedDetail = (poemId: number, priority: number) => {
      const input = {
        authorHref: "https://source.invalid/writers/test",
        poemHref: `https://source.invalid/works/${String(poemId)}`,
      };
      return ledger.seed({
        implementationVersion: collectorImplementationVersion(),
        input,
        inputHash: inputHash(input),
        kind: collectionWorkKinds().poemDetail,
        priority,
        schemaVersion: collectorSchemaVersion(),
      });
    };
    const reserved = seedDetail(1, 0);
    const unrelated = seedDetail(2, 1_000);

    await expect(
      coordinator.run(new AbortController().signal, {
        includedWorkKeys: [reserved.workKey],
        maximum: 2,
      }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(ledger.get(reserved.workKey)?.state).toBe("succeeded");
    expect(ledger.get(unrelated.workKey)).toMatchObject({
      attemptCount: 0,
      state: "pending",
    });
    expect(browser.poemCalls).toBe(1);
    ledger.close();
  });

  it("backs challenge canaries off from 15 minutes to a six-hour cap", () => {
    expect(computeHumanChallengeRetryDelayMs(1, () => 0)).toBe(15 * 60_000);
    expect(computeHumanChallengeRetryDelayMs(2, () => 0)).toBe(30 * 60_000);
    expect(computeHumanChallengeRetryDelayMs(99, () => 0.999)).toBe(
      6 * 60 * 60_000,
    );
    expect(() => computeHumanChallengeRetryDelayMs(0)).toThrow(
      "COLLECTOR_CHALLENGE_COUNT_INVALID",
    );
  });

  it("uses bounded, injectable exponential retry jitter", () => {
    expect(computeRetryDelayMs(1_000, 1, () => 0)).toBe(500);
    expect(computeRetryDelayMs(1_000, 2, () => 0.999)).toBe(1_999);
    expect(computeRetryDelayMs(60_000, 99, () => 0.999)).toBeLessThanOrEqual(
      6 * 60 * 60_000,
    );
    expect(() => computeRetryDelayMs(1_000, 1, () => 1)).toThrow();
  });

  it("waits indefinitely offline without consuming attempts and resumes", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-29T12:00:00Z"));
    const root = mkdtempSync(join(tmpdir(), "saqi-network-recovery-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new NetworkRecoveryBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      random: () => 0,
      retryDelayMs: 1,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );
    try {
      await expect(
        coordinator.run(new AbortController().signal),
      ).resolves.toMatchObject({
        originState: "network_wait",
        stopped: "network_wait",
      });
      expect(ledger.get(seeded.workKey)).toMatchObject({
        attemptCount: 0,
        lastErrorCode: "SOURCE_NETWORK_UNAVAILABLE",
        state: "pending",
      });
      const persistedOrigin = ledger.status().origins[0];
      expect(persistedOrigin).toMatchObject({
        consecutiveFailures: 0,
        stopReason: "SOURCE_NETWORK_UNAVAILABLE",
      });
      expect(
        deriveCollectorOriginHealthState({
          configured: true,
          consecutiveFailures: persistedOrigin!.consecutiveFailures,
          cooldownUntil: persistedOrigin!.cooldownUntil,
          nextAllowedAt: persistedOrigin!.nextAllowedAt,
          now: Date.now(),
          stopReason: persistedOrigin!.stopReason,
        }),
      ).toBe("network_wait");
      const restarted = new CollectorCoordinator({
        artifacts: testArtifactStore(root),
        browser,
        ledger,
        minimumOriginGapMs: 0,
        random: () => 0,
        retryDelayMs: 1,
      });
      await expect(
        restarted.run(new AbortController().signal),
      ).resolves.toMatchObject({
        originState: "network_wait",
        processed: 0,
      });
      await vi.advanceTimersByTimeAsync(2_501);
      await expect(
        restarted.run(new AbortController().signal, { maximum: 1 }),
      ).resolves.toMatchObject({ stopped: "maximum", succeeded: 1 });
      expect(browser.calls).toBe(2);
    } finally {
      ledger.close();
      vi.useRealTimers();
    }
  });

  it("moves past a bounded browser timeout without consuming an attempt", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-timeout-recovery-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new OperationTimeoutRecoveryBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 60_000,
    });
    const timedOut = coordinator.seedAuthor(
      "https://source.invalid/writers/first",
      1,
    );
    coordinator.seedAuthor("https://source.invalid/writers/second");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 2 }),
    ).resolves.toMatchObject({
      processed: 2,
      stopped: "maximum",
      succeeded: 1,
    });
    expect(ledger.get(timedOut.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "SOURCE_AUTHOR_OPERATION_TIMEOUT",
      state: "pending",
    });
    ledger.close();
  });

  it("requests a controlled restart without consuming the fenced item", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-browser-restart-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new RestartRequiredBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 60_000,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/first",
    );

    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      processed: 1,
      stopped: "restart_required",
      succeeded: 0,
    });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "SOURCE_BROWSER_RESTART_REQUIRED",
      state: "pending",
    });
    expect(ledger.status().origins[0]).toMatchObject({ active: false });
    ledger.close();
  });

  it("stops the run and schedules a persisted human-required retry", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-human-stop-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new HumanRequiredBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      random: () => 0,
      retryDelayMs: 1,
    });
    const challenged = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );
    const startedAt = Date.now();
    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      originRetryAt: expect.any(Number),
      originState: "challenge_wait",
      processed: 1,
      stopped: "human_required",
      succeeded: 0,
    });
    const origin = ledger.status().origins[0];
    expect(origin?.stopReason).toBe("SOURCE_HUMAN_REQUIRED");
    expect(origin?.cooldownUntil).toBeGreaterThanOrEqual(
      startedAt + 15 * 60_000,
    );
    expect(origin?.cooldownUntil).toBeLessThanOrEqual(Date.now() + 15 * 60_000);
    expect(ledger.get(challenged.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "SOURCE_HUMAN_REQUIRED",
      state: "pending",
    });
    const diagnostic = ledger.latestCheckpoint(
      challenged.workKey,
      "collector_failure",
    )?.payload;
    expect(diagnostic).toMatchObject({
      diagnosticSchemaVersion: 2,
      sourceAccess: {
        category: "turnstile",
        cfMitigated: true,
        httpStatus: 403,
        schemaVersion: 1,
        surface: "navigation",
      },
    });
    const serializedDiagnostic = JSON.stringify(diagnostic);
    expect(serializedDiagnostic).not.toMatch(
      /SECRET_BODY_TEXT|SECRET_COOKIE|SECRET_HTML|SECRET_NESTED_COOKIE|SECRET_NESTED_HTML|SECRET_SCRIPT_SOURCE|SECRET_TOKEN|bodyText|cookie|html|scriptSources|token/u,
    );
    expect(
      ledger.claimOrigin("https://source.invalid", Date.now(), 1_000),
    ).toMatchObject({ state: "waiting" });

    const untouched = coordinator.seedAuthor(
      "https://source.invalid/writers/still-pending",
    );
    const eventsBefore = ledger.eventCount(untouched.workKey);
    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      failed: 0,
      originState: "challenge_wait",
      originRetryAt: expect.any(Number),
      processed: 0,
      stopped: "idle",
      succeeded: 0,
    });
    expect(ledger.get(untouched.workKey)).toMatchObject({
      attemptCount: 0,
      state: "pending",
    });
    expect(ledger.eventCount(untouched.workKey)).toBe(eventsBefore);
    expect(browser.calls).toBe(1);

    expect(ledger.clearOriginStop("https://source.invalid", Date.now())).toBe(
      true,
    );
    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1 });
    expect(ledger.get(untouched.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "SOURCE_HUMAN_REQUIRED",
      state: "pending",
    });
    ledger.close();
  });

  it("preserves an unknown durable origin stop as blocked", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-origin-blocked-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new FakeBrowser(),
      ledger,
      minimumOriginGapMs: 0,
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");
    const now = Date.now();
    const origin = ledger.claimOrigin("https://source.invalid", now, 10_000);
    if (origin.state !== "claimed") throw new Error("expected origin lease");
    ledger.failOrigin(origin.lease, now + 1, 0, {
      circuitBreakerAfter: 1,
      circuitBreakerCooldownMs: 60_000,
      retryAt: now + 60_000,
      stopReason: "COLLECTOR_HOTFIX_PENDING",
    });

    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      originState: "blocked",
      originStopReason: "COLLECTOR_HOTFIX_PENDING",
      processed: 0,
      stopped: "blocked",
    });
    ledger.close();
  });

  it("derives restarted origin health from durable gate state", () => {
    const base = {
      configured: true,
      consecutiveFailures: 1,
      cooldownUntil: 60_000,
      nextAllowedAt: 60_000,
      now: 1_000,
    } as const;
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        stopReason: "SOURCE_HUMAN_REQUIRED",
      }),
    ).toBe("challenge_wait");
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        stopReason: "SOURCE_NETWORK_UNAVAILABLE",
      }),
    ).toBe("network_wait");
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        stopReason: "SOURCE_RATE_LIMITED",
      }),
    ).toBe("rate_wait");
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        stopReason: "COLLECTOR_DISK_PRESSURE",
      }),
    ).toBe("disk_wait");
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        stopReason: "COLLECTOR_HOTFIX_PENDING",
      }),
    ).toBe("blocked");
    expect(
      deriveCollectorOriginHealthState({
        ...base,
        configured: false,
        stopReason: "SOURCE_HUMAN_REQUIRED",
      }),
    ).toBe("disabled");
  });

  it("reports a rate cooldown separately from idle work", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-rate-wait-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new RateLimitedBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      random: () => 0,
      retryDelayMs: 1,
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");

    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      originRetryAt: expect.any(Number),
      originState: "rate_wait",
      stopped: "idle",
    });
    ledger.close();
  });

  it("reports a source stop from the final claim in a bounded batch", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-rate-stop-bounded-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new RateLimitedBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      random: () => 0,
      retryDelayMs: 1,
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({
      originState: "rate_wait",
      originStopReason: "SOURCE_RATE_LIMITED",
      processed: 1,
      stopped: "maximum",
    });
    ledger.close();
  });

  it("opens a cooldown circuit after consecutive permanent source failures", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-selector-circuit-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new SelectorDriftBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 1,
    });
    for (const slug of ["one", "two", "three"]) {
      coordinator.seedAuthor(`https://source.invalid/writers/${slug}`);
    }
    await expect(
      coordinator.run(new AbortController().signal, { maximum: 3 }),
    ).resolves.toMatchObject({ failed: 3, processed: 3, succeeded: 0 });
    expect(ledger.status().byState.dead_letter).toBe(3);
    expect(ledger.status().origins[0]).toMatchObject({
      consecutiveFailures: 3,
      stopReason: null,
    });
    expect(ledger.status().origins[0]!.cooldownUntil).toBeGreaterThan(
      Date.now(),
    );
    ledger.close();
  });

  it("isolates missing feed configuration to its author work item", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-feed-config-isolation-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new MissingFeedConfigurationBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      random: () => 0,
      retryDelayMs: 1,
    });
    coordinator.seedAuthor("https://source.invalid/writers/missing-feed");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ failed: 1, processed: 1, succeeded: 0 });
    expect(ledger.status().origins[0]).toMatchObject({
      consecutiveFailures: 0,
      cooldownUntil: 0,
      stopReason: null,
    });
    ledger.close();
  });

  it("flags and rejects one canonical poem claimed by two authors", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-cross-author-poem-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new CrossAuthorDuplicateBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
    });
    const firstInput = {
      authorHref: "https://source.invalid/writers/poet-one",
      poemHref: "https://source.invalid/works/42",
      refreshGeneration: "generation-1",
    };
    ledger.seed({
      implementationVersion: collectorImplementationVersion(),
      input: firstInput,
      inputHash: inputHash(firstInput),
      kind: collectionWorkKinds().poemDetail,
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    });
    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });

    const manifest = seedAuthorManifest(
      ledger,
      "https://source.invalid/writers/poet-two",
      0,
      "generation-2",
    );
    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ failed: 1, processed: 1, succeeded: 0 });
    expect(ledger.get(manifest.workKey)).toMatchObject({
      lastErrorCode: "SOURCE_POEM_DUPLICATE",
      state: "dead_letter",
    });
    expect(
      ledger.latestCheckpoint(manifest.workKey, "collector_failure")?.payload,
    ).toMatchObject({ code: "SOURCE_POEM_DUPLICATE" });
    expect(browser).toMatchObject({ manifestCalls: 1, poemCalls: 1 });
    expect(ledger.status().kindProgress).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: collectionWorkKinds().poemDetail,
          total: 2,
        }),
      ]),
    );
    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(browser.poemCalls).toBe(2);
    expect(ledger.status().kindProgress).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          completed: 2,
          kind: collectionWorkKinds().poemDetail,
          total: 2,
        }),
      ]),
    );
    ledger.close();
  });

  it("retries transient empty render projections before accepting the detail", async () => {
    vi.useFakeTimers();
    try {
      const root = mkdtempSync(join(tmpdir(), "saqi-projection-retry-"));
      const ledger = Ledger.open(join(root, "ledger.sqlite3"));
      const browser = new TransientProjectionBrowser();
      const coordinator = new CollectorCoordinator({
        artifacts: testArtifactStore(root),
        browser,
        ledger,
        minimumOriginGapMs: 0,
        random: () => 0,
        retryDelayMs: 1,
      });
      coordinator.seedAuthor("https://source.invalid/writers/test");
      await coordinator.run(new AbortController().signal, { maximum: 1 });
      await coordinator.run(new AbortController().signal, { maximum: 1 });
      expect(ledger.status().byState).toMatchObject({
        dead_letter: 0,
        retry_wait: 1,
      });
      expect(browser.poemCalls).toBe(1);
      expect(ledger.status().origins[0]).toMatchObject({
        consecutiveFailures: 1,
        stopReason: null,
      });
      await vi.advanceTimersByTimeAsync(5);
      await coordinator.run(new AbortController().signal, { maximum: 1 });
      expect(ledger.status().byState).toMatchObject({
        dead_letter: 0,
        retry_wait: 1,
      });
      expect(browser.poemCalls).toBe(2);
      await vi.advanceTimersByTimeAsync(5);
      await coordinator.run(new AbortController().signal, { maximum: 1 });
      expect(ledger.status().byState).toMatchObject({
        dead_letter: 0,
        retry_wait: 0,
        succeeded: 2,
      });
      expect(browser.poemCalls).toBe(3);
      ledger.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("dead-letters the third empty projection and preserves the origin cooldown", async () => {
    vi.useFakeTimers();
    try {
      const root = mkdtempSync(join(tmpdir(), "saqi-empty-circuit-"));
      const ledger = Ledger.open(join(root, "ledger.sqlite3"));
      const browser = new EmptyProjectionBrowser();
      const coordinator = new CollectorCoordinator({
        artifacts: testArtifactStore(root),
        browser,
        ledger,
        minimumOriginGapMs: 0,
        random: () => 0,
        retryDelayMs: 1,
      });
      coordinator.seedAuthor("https://source.invalid/writers/test");
      await coordinator.run(new AbortController().signal, { maximum: 1 });
      let terminalOriginRetryAt: number | undefined;
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        const result = await coordinator.run(new AbortController().signal, {
          maximum: 1,
        });
        terminalOriginRetryAt = result.originRetryAt;
        if (attempt < 3) await vi.advanceTimersByTimeAsync(5);
      }

      const status = ledger.status();
      expect(status.byState).toMatchObject({ dead_letter: 1, pending: 1 });
      expect(status.origins[0]).toMatchObject({
        consecutiveFailures: 3,
        stopReason: null,
      });
      expect(status.origins[0]!.cooldownUntil).toBeGreaterThan(Date.now());
      expect(terminalOriginRetryAt).toBe(status.origins[0]!.cooldownUntil);

      const details = ledger.listWorkDefinitionsAfter(
        null,
        collectionWorkKinds().poemDetail,
        10,
        {
          implementationVersion: collectorImplementationVersion(),
          schemaVersion: collectorSchemaVersion(),
        },
      ).items;
      const pending = details.find((item) => item.state === "pending");
      expect(pending).toMatchObject({ attemptCount: 0, state: "pending" });
      const eventsBefore = ledger.eventCount(pending!.workKey);
      await expect(
        coordinator.run(new AbortController().signal, { maximum: 1 }),
      ).resolves.toMatchObject({
        processed: 0,
        stopped: "idle",
        succeeded: 0,
      });
      expect(browser.poemCalls).toBe(3);
      expect(ledger.get(pending!.workKey)).toMatchObject({
        attemptCount: 0,
        state: "pending",
      });
      expect(ledger.eventCount(pending!.workKey)).toBe(eventsBefore);
      ledger.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("bulk bootstrap primitives canonicalize and seed without a browser", () => {
    const ledger = Ledger.open(":memory:");
    const url = authorUrlFromCatalogSlug("المعري");
    expect(url).toBe(
      "https://source.invalid/writers/%D8%A7%D9%84%D9%85%D8%B9%D8%B1%D9%8A",
    );
    expect(seedAuthorManifest(ledger, url).inserted).toBe(true);
    expect(seedAuthorManifest(ledger, url).inserted).toBe(false);
    ledger.close();
  });

  it("uses explicit refresh generations for incremental manifests", () => {
    const ledger = Ledger.open(":memory:");
    const inventory = parseCatalogInventory([{ slug: "test" }]);
    expect(seedAuthorManifests(ledger, inventory)[0]?.inserted).toBe(true);
    expect(seedAuthorManifests(ledger, inventory)[0]?.inserted).toBe(false);
    expect(
      seedAuthorManifests(ledger, inventory, 0, "2026-08")[0]?.inserted,
    ).toBe(true);
    expect(
      seedAuthorManifests(ledger, inventory, 0, "2026-08")[0]?.inserted,
    ).toBe(false);
    expect(() =>
      seedAuthorManifests(ledger, inventory, 0, "bad generation"),
    ).toThrow();
    expect(ledger.status().total).toBe(2);
    ledger.close();
  });

  it("keeps detail identity stable across inventory generations and name refreshes", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-detail-refresh-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const artifacts = testArtifactStore(root);
    const firstBrowser = new FakeBrowser();
    const first = new CollectorCoordinator({
      artifacts,
      browser: firstBrowser,
      ledger,
      minimumOriginGapMs: 0,
    });

    expect(
      seedAuthorManifest(
        ledger,
        "https://source.invalid/writers/test",
        0,
        "generation-1",
        "شاعر قديم",
      ).inserted,
    ).toBe(true);
    await expect(
      first.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(ledger.status().total).toBe(3);
    expect(
      seedAuthorManifest(
        ledger,
        "https://source.invalid/writers/test",
        0,
        "generation-1",
        "شاعر قديم",
      ).inserted,
    ).toBe(false);
    expect(ledger.status().total).toBe(3);

    const resumedBrowser = new FakeBrowser();
    const resumed = new CollectorCoordinator({
      artifacts,
      browser: resumedBrowser,
      ledger,
      minimumOriginGapMs: 0,
    });
    await expect(
      resumed.run(new AbortController().signal),
    ).resolves.toMatchObject({ processed: 2, stopped: "idle", succeeded: 2 });
    expect(resumedBrowser).toMatchObject({ manifestCalls: 0, poemCalls: 2 });

    expect(
      seedAuthorManifest(
        ledger,
        "https://source.invalid/writers/test",
        0,
        "generation-2",
        "شاعر محدث",
      ).inserted,
    ).toBe(true);
    await expect(
      resumed.run(new AbortController().signal),
    ).resolves.toMatchObject({ processed: 1, stopped: "idle", succeeded: 1 });
    expect(resumedBrowser).toMatchObject({ manifestCalls: 1, poemCalls: 2 });
    expect(
      ledger.sourceAuthorMetadata("https://source.invalid/writers/test"),
    ).toEqual({
      authorNameArabic: "شاعر محدث",
      refreshGeneration: "generation-2",
    });
    expect(ledger.status()).toMatchObject({
      byState: { succeeded: 4 },
      total: 4,
    });
    ledger.close();
  });

  it("seeds details and converges without repeated collection", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-collector-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new FakeBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 1,
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");

    const first = await coordinator.run(new AbortController().signal);
    expect(first).toMatchObject({
      processed: 3,
      stopped: "idle",
      succeeded: 3,
    });
    expect(browser).toMatchObject({ manifestCalls: 1, poemCalls: 2 });

    expect(ledger.status().byState.succeeded).toBe(3);

    const replay = await coordinator.run(new AbortController().signal);
    expect(replay).toMatchObject({
      processed: 0,
      stopped: "idle",
      succeeded: 0,
    });
    expect(browser).toMatchObject({ manifestCalls: 1, poemCalls: 2 });

    ledger.close();
  });

  it("resolves equal-timestamp author metadata deterministically", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-author-metadata-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const href = "https://source.invalid/writers/test";
    ledger.recordSourceAuthorMetadata(href, "شاعر ب", "generation-b", 42);
    ledger.recordSourceAuthorMetadata(href, "شاعر ا", "generation-a", 42);
    ledger.recordSourceAuthorMetadata(href, "شاعر ج", "generation-c", 42);
    expect(ledger.sourceAuthorMetadata(href)).toEqual({
      authorNameArabic: "شاعر ج",
      refreshGeneration: "generation-c",
    });
    ledger.close();
  });

  it("revisions author metadata only when exact durable material changes", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-author-revision-"));
    const path = join(root, "ledger.sqlite3");
    const ledger = Ledger.open(path);
    const href = "https://source.invalid/writers/test";
    expect(ledger.sourceAuthorMetadataRevision()).toBe(0);
    ledger.recordSourceAuthorMetadata(href, "شاعر", "generation-a", 42);
    expect(ledger.sourceAuthorMetadataRevision()).toBe(1);
    ledger.recordSourceAuthorMetadata(href, "شاعر", "generation-a", 43);
    expect(ledger.sourceAuthorMetadataRevision()).toBe(1);
    ledger.recordSourceAuthorMetadata(href, "شاعر محدث", "generation-b", 44);
    expect(ledger.sourceAuthorMetadataRevision()).toBe(2);
    ledger.close();
    const reopened = Ledger.open(path);
    expect(reopened.sourceAuthorMetadataRevision()).toBe(2);
    reopened.close();
  });

  it("honors a configured detail burst without starving manifests", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-lanes-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new LaneBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      detailBurst: 20,
      ledger,
      minimumOriginGapMs: 0,
    });
    coordinator.seedAuthor("https://source.invalid/writers/one", 1);
    coordinator.seedAuthor("https://source.invalid/writers/two");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 22 }),
    ).resolves.toMatchObject({ processed: 22, succeeded: 22 });
    expect(browser.calls[0]).toBe("author:https://source.invalid/writers/one");
    expect(
      browser.calls.slice(1, 21).every((call) => call.startsWith("detail:")),
    ).toBe(true);
    expect(browser.calls[21]).toBe("author:https://source.invalid/writers/two");
    ledger.close();
  });

  it("defaults to one manifest per hundred available details", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-default-lanes-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new LaneBrowser(125);
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
    });
    coordinator.seedAuthor("https://source.invalid/writers/one", 1);
    coordinator.seedAuthor("https://source.invalid/writers/two");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 102 }),
    ).resolves.toMatchObject({ processed: 102, succeeded: 102 });
    expect(browser.calls[0]).toBe("author:https://source.invalid/writers/one");
    expect(
      browser.calls.slice(1, 101).every((call) => call.startsWith("detail:")),
    ).toBe(true);
    expect(browser.calls[101]).toBe(
      "author:https://source.invalid/writers/two",
    );
    expect(coordinator.scheduleSnapshot()).toEqual({
      detailBurst: 100,
      detailsSinceManifest: 0,
      detailsUntilManifest: 100,
      preferredKind: "source_poem_detail",
    });
    ledger.close();
  }, 15_000);

  it("falls through to a manifest immediately when no detail is ready", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-empty-detail-lane-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new LaneBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      detailBurst: 10_000,
      ledger,
      minimumOriginGapMs: 0,
    });
    coordinator.seedAuthor("https://source.invalid/writers/one");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(browser.calls).toEqual([
      "author:https://source.invalid/writers/one",
    ]);
    ledger.close();
  });

  it("emits advisory completions after durable success without risking the commit", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-durable-success-hint-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const observed: {
      artifactHash: string;
      kind: string;
      state: string | undefined;
      workKey: string;
    }[] = [];
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new FakeBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      onDurableSuccess: (completion) => {
        observed.push({
          ...completion,
          state: ledger.get(completion.workKey)?.state,
        });
        if (completion.kind === collectionWorkKinds().poemDetail)
          throw new Error("synthetic advisory failure");
      },
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 2 }),
    ).resolves.toMatchObject({ failed: 0, processed: 2, succeeded: 2 });
    expect(observed.map(({ kind, state }) => ({ kind, state }))).toEqual([
      { kind: collectionWorkKinds().authorManifest, state: "succeeded" },
      { kind: collectionWorkKinds().poemDetail, state: "succeeded" },
    ]);
    for (const completion of observed) {
      expect(ledger.get(completion.workKey)?.outputArtifactHash).toBe(
        completion.artifactHash,
      );
    }
    ledger.close();
  });

  it("prefers a persisted detail backlog after coordinator restart", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-restart-lane-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const firstBrowser = new LaneBrowser();
    const first = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: firstBrowser,
      detailBurst: 100,
      ledger,
      minimumOriginGapMs: 0,
    });
    first.seedAuthor("https://source.invalid/writers/one");
    await first.run(new AbortController().signal, { maximum: 1 });
    await first.close();
    seedAuthorManifest(ledger, "https://source.invalid/writers/two");

    const restartedBrowser = new LaneBrowser();
    const restarted = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: restartedBrowser,
      detailBurst: 100,
      ledger,
      minimumOriginGapMs: 0,
    });
    await expect(
      restarted.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(restartedBrowser.calls).toHaveLength(1);
    expect(restartedBrowser.calls[0]).toMatch(/^detail:/);
    await restarted.close();
    ledger.close();
  });

  it("stops before claiming or requesting when artifact reserve is exhausted", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-disk-stop-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new FakeBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts: new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: Number.MAX_SAFE_INTEGER,
      }),
      browser,
      ledger,
      minimumOriginGapMs: 0,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );

    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      processed: 0,
      stopped: "disk_pressure",
      succeeded: 0,
    });
    expect(browser.manifestCalls).toBe(0);
    expect(ledger.get(seeded.workKey)?.state).toBe("pending");
    ledger.close();
  });

  it("stops and records the exact phase when capacity drops during collection", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-disk-drop-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const artifacts = new CapacityDropArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const coordinator = new CollectorCoordinator({
      artifacts,
      browser: new FakeBrowser(),
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 1_000,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );

    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({
      failed: 1,
      processed: 1,
      stopped: "disk_pressure",
      succeeded: 0,
    });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      lastErrorCode: "ARTIFACT_STORE_DISK_PRESSURE",
      state: "retry_wait",
    });
    expect(
      ledger.latestCheckpoint(seeded.workKey, "collector_failure"),
    ).toMatchObject({
      payload: {
        code: "ARTIFACT_STORE_DISK_PRESSURE",
        diagnosticSchemaVersion: 2,
        errorMessage:
          "ARTIFACT_STORE_DISK_PRESSURE: 1 bytes available; 2 bytes reserved",
        errorName: "DiskPressureError",
        phase: "store_projection",
      },
    });
    ledger.close();
  });

  it("does not refetch a valid detail completed by an older implementation", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-reconcile-detail-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const artifacts = testArtifactStore(root);
    const poemInput = {
      authorHref: "https://source.invalid/writers/test",
      poemHref: "https://source.invalid/works/1",
    };
    ledger.seed({
      implementationVersion: "source-chrome-prior",
      input: poemInput,
      inputHash: inputHash(poemInput),
      kind: "source_poem_detail",
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    });
    const priorClaim = ledger.claim("prior", Date.now(), 60_000, [
      "source_poem_detail",
    ]);
    if (!priorClaim) throw new Error("Expected prior detail claim");
    const priorArtifact = await artifacts.put("{}");
    ledger.succeed(priorClaim, priorArtifact.hash);

    const browser = new FakeBrowser();
    const coordinator = new CollectorCoordinator({
      artifacts,
      browser,
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 1,
    });
    coordinator.seedAuthor("https://source.invalid/writers/test");
    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({ processed: 2, succeeded: 2 });
    expect(browser).toMatchObject({ manifestCalls: 1, poemCalls: 1 });

    ledger.close();
  });

  it("supersedes pending v3 work instead of executing it as v4", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-supersede-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const browser = new FakeBrowser();
    const input = { authorHref: "https://source.invalid/writers/test" };
    const obsolete = ledger.seed({
      implementationVersion: "source-chrome-v3",
      input,
      inputHash: inputHash(input),
      kind: collectionWorkKinds().authorManifest,
      priority: 0,
      schemaVersion: collectorSchemaVersion(),
    });
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      retryDelayMs: 1,
    });
    await expect(
      coordinator.run(new AbortController().signal),
    ).resolves.toMatchObject({ processed: 3, succeeded: 3 });
    expect(ledger.get(obsolete.workKey)).toMatchObject({
      lastErrorCode: "WORKER_VERSION_RETIRED",
      state: "dead_letter",
    });
    expect(browser.manifestCalls).toBe(1);
    ledger.close();
  });

  it("heartbeats work and origin leases during a slow browser request", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-heartbeat-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new SlowBrowser(150),
      leaseDurationMs: 1_000,
      leaseHeartbeatMs: 20,
      ledger,
      minimumOriginGapMs: 100,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 1 });
    expect(ledger.get(seeded.workKey)?.state).toBe("succeeded");
    expect(ledger.eventCount(seeded.workKey)).toBeGreaterThan(5);
    expect(
      ledger.claimOrigin("https://source.invalid", Date.now(), 30).state,
    ).toBe("waiting");
    ledger.close();
  });

  it("stops safely when another worker takes an expired lease", async () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-takeover-"));
    const path = join(root, "ledger.sqlite3");
    const ledger = Ledger.open(path);
    const replacementLedger = Ledger.open(path);
    const takeover = (): void => {
      const future = Date.now() + 10_000;
      replacementLedger.recoverExpired(future);
      replacementLedger.claim("replacement", future, 30_000);
    };
    const coordinator = new CollectorCoordinator({
      artifacts: testArtifactStore(root),
      browser: new SlowBrowser(15, takeover),
      leaseDurationMs: 100,
      leaseHeartbeatMs: 5,
      ledger,
      minimumOriginGapMs: 0,
    });
    const seeded = coordinator.seedAuthor(
      "https://source.invalid/writers/test",
    );

    await expect(
      coordinator.run(new AbortController().signal, { maximum: 1 }),
    ).resolves.toMatchObject({ processed: 1, succeeded: 0 });
    expect(ledger.get(seeded.workKey)).toMatchObject({
      leaseOwner: "replacement",
      state: "running",
    });
    replacementLedger.close();
    ledger.close();
  });
});
