import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  type AuthorInventoryPageProjection,
  AuthorInventoryPageSchema,
  canonicalInventoryUrl,
  configureSource,
} from "@saqi/source-adapter";
import { describe, expect, it } from "vitest";

import {
  AuthorInventoryCollector,
  type AuthorInventoryPageBrowser,
} from "../collection/author-inventory-lane.js";
import { ArtifactStore } from "../persistence/artifact-store.js";
import { Ledger } from "../persistence/ledger.js";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root.js";

const FIXTURE = AuthorInventoryPageSchema.parse(
  JSON.parse(
    readFileSync(
      join(import.meta.dirname, "fixtures/author-inventory-page-1.json"),
      "utf8",
    ),
  ) as unknown,
);

const PAGE_2 = AuthorInventoryPageSchema.parse({
  authors: [{ href: "/writers/b", name: "بدر", poemCountText: "٣ قصائد" }],
  challengeDetected: false,
  kind: "author_inventory_page",
  nextPageHref: null,
  page: 2,
  schemaVersion: 1,
  sourceUrl: "https://source.invalid/directory/2",
  terminal: true,
});

class FakeInventoryBrowser implements AuthorInventoryPageBrowser {
  readonly calls: number[] = [];
  readonly #mutateSecondPass: boolean;

  constructor(mutateSecondPass = false) {
    this.#mutateSecondPass = mutateSecondPass;
  }

  collectAuthorInventoryPage(
    value: string,
  ): Promise<AuthorInventoryPageProjection> {
    const page = canonicalInventoryUrl(value).page;
    this.calls.push(page);
    const pass = Math.ceil(this.calls.length / 2);
    if (page === 1) return Promise.resolve(FIXTURE);
    return Promise.resolve(
      this.#mutateSecondPass && pass === 2
        ? { ...PAGE_2, authors: [{ ...PAGE_2.authors[0]!, name: "مختلف" }] }
        : PAGE_2,
    );
  }
}

function runtime(browser: FakeInventoryBrowser, now: () => number) {
  const root = mkdtempSync(join(tmpdir(), "saqi-inventory-collector-"));
  const ledger = Ledger.open(join(root, "ledger.sqlite3"));
  const artifacts = new ArtifactStore(join(root, "artifacts"), {
    minimumFreeBytes: 0,
  });
  const create = (afterPageCheckpoint?: (pass: 1 | 2, page: number) => void) =>
    new AuthorInventoryCollector({
      ...(afterPageCheckpoint ? { afterPageCheckpoint } : {}),
      artifacts,
      browser,
      ledger,
      minimumOriginGapMs: 0,
      now,
      productionSourceAuthors: [],
      refreshGeneration: "generation-1",
    });
  return { artifacts, create, ledger };
}

describe("author inventory browser collection", () => {
  it("uses the configured source namespace for durable discovery identity", () => {
    configureSource({ name: "archive", origin: "https://example.test" });
    const browser = new FakeInventoryBrowser();
    const root = mkdtempSync(join(tmpdir(), "saqi-inventory-namespace-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    try {
      const collector = new AuthorInventoryCollector({
        artifacts: new ArtifactStore(join(root, "artifacts"), {
          minimumFreeBytes: 0,
        }),
        browser,
        ledger,
        productionSourceAuthors: [],
        refreshGeneration: "configured-namespace",
      });
      collector.seed();
      expect(ledger.status().kindProgress).toEqual([
        expect.objectContaining({
          byState: expect.objectContaining({ pending: 1 }),
          kind: "archive_author_inventory_discovery",
        }),
      ]);
    } finally {
      ledger.close();
      configureSource({ name: "source", origin: "https://source.invalid" });
    }
  });

  it("shares the durable Source origin lease with other collection lanes", async () => {
    const browser = new FakeInventoryBrowser();
    const root = mkdtempSync(join(tmpdir(), "saqi-inventory-origin-gate-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const collector = new AuthorInventoryCollector({
      artifacts: new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      }),
      browser,
      ledger,
      minimumOriginGapMs: 0,
      productionSourceAuthors: [],
      refreshGeneration: "origin-gate",
    });
    collector.seed();
    expect(
      ledger.claimOrigin("https://source.invalid", Date.now(), 5_000).state,
    ).toBe("claimed");
    const controller = new AbortController();
    setTimeout(() => controller.abort(new Error("test stop")), 20);
    await expect(collector.run(controller.signal)).resolves.toEqual({
      stopped: "aborted",
    });
    expect(browser.calls).toEqual([]);
    expect(ledger.status().byState.pending).toBe(1);
    ledger.close();
  });

  it("resumes after a page checkpoint without duplicate requests and certifies two passes", async () => {
    const browser = new FakeInventoryBrowser();
    let now = 0;
    const { artifacts, create, ledger } = runtime(browser, () => now);
    let crash = true;
    const first = create((pass, page) => {
      if (crash && pass === 1 && page === 1) {
        crash = false;
        throw new Error("synthetic crash");
      }
    });
    first.seed();
    await expect(first.run(new AbortController().signal)).rejects.toThrow(
      "synthetic crash",
    );
    expect(browser.calls).toEqual([1]);
    now = 60_000;
    const resumed = create();
    resumed.seed();
    await expect(
      resumed.run(new AbortController().signal),
    ).resolves.toMatchObject({
      status: { discoveredAuthors: 2, insertedManifests: 2, pagesPerPass: 2 },
      stopped: "succeeded",
    });
    expect(browser.calls).toEqual([1, 2, 1, 2]);
    expect(ledger.status()).toMatchObject({
      byState: expect.objectContaining({ succeeded: 1 }),
    });
    for (const hash of ledger.referencedArtifactHashes()) {
      const verification = await artifacts.verify(hash);
      expect(verification.ok).toBe(true);
    }
    ledger.close();
  });

  it("fails closed when the two fresh passes differ", async () => {
    const browser = new FakeInventoryBrowser(true);
    const { create, ledger } = runtime(browser, () => 0);
    const collector = create();
    collector.seed();
    await expect(collector.run(new AbortController().signal)).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_UNSTABLE",
    );
    expect(ledger.status().byState.retry_wait).toBe(1);
    expect(ledger.status().kindProgress).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: "source_author_inventory_discovery" }),
      ]),
    );
    ledger.close();
  });

  it("enforces the configured page bound before another browser request", async () => {
    const browser = new FakeInventoryBrowser();
    const root = mkdtempSync(join(tmpdir(), "saqi-inventory-bound-"));
    const ledger = Ledger.open(join(root, "ledger.sqlite3"));
    const collector = new AuthorInventoryCollector({
      artifacts: new ArtifactStore(join(root, "artifacts"), {
        minimumFreeBytes: 0,
      }),
      browser,
      ledger,
      maximumPages: 1,
      minimumOriginGapMs: 0,
      now: () => 0,
      productionSourceAuthors: [],
      refreshGeneration: "bounded",
    });
    collector.seed();
    await expect(collector.run(new AbortController().signal)).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_PAGE_LIMIT",
    );
    expect(browser.calls).toEqual([1]);
    ledger.close();
  });
});
