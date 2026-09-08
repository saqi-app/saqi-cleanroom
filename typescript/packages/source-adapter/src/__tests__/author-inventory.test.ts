import { describe, expect, it } from "vitest";

import {
  type AuthorInventoryPageProjection,
  certifyAuthorInventoryPages,
} from "../author-inventory";

function pass(): AuthorInventoryPageProjection[] {
  return [
    {
      authors: [
        { href: "/writers/b", name: "باء", poemCountText: "٢" },
        { href: "/writers/a", name: "ألف", poemCountText: null },
      ],
      challengeDetected: false,
      kind: "author_inventory_page",
      nextPageHref: "/directory/2",
      page: 1,
      schemaVersion: 1,
      sourceUrl: "/directory/1",
      terminal: false,
    },
    {
      authors: [
        { href: "/writers/b", name: "باء", poemCountText: "2" },
        { href: "/writers/c", name: "جيم", poemCountText: "١" },
      ],
      challengeDetected: false,
      kind: "author_inventory_page",
      nextPageHref: null,
      page: 2,
      schemaVersion: 1,
      sourceUrl: "/directory/2",
      terminal: true,
    },
  ];
}

describe("author inventory page certificate", () => {
  it("certifies two stable passes and canonically deduplicates boundaries", async () => {
    const equivalent = pass();
    equivalent[0] = {
      ...equivalent[0]!,
      nextPageHref: "https://source.invalid/directory/2",
      sourceUrl: "https://source.invalid/directory/1",
    };
    const certificate = await certifyAuthorInventoryPages(pass(), equivalent);
    expect(certificate.authors.map(({ canonicalId }) => canonicalId)).toEqual([
      "source:author:a",
      "source:author:b",
      "source:author:c",
    ]);
    expect(certificate).toMatchObject({
      duplicateReferences: 1,
      pages: 2,
    });
    expect(certificate.digest).toMatch(/^[a-f\d]{64}$/);
  });

  it("rejects unstable, discontinuous, challenged, and conflicting passes", async () => {
    const unstable = pass();
    unstable[1]!.authors[1] = {
      href: "/writers/d",
      name: "دال",
      poemCountText: "1",
    };
    await expect(certifyAuthorInventoryPages(pass(), unstable)).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_UNSTABLE",
    );

    const gap = pass();
    gap[1] = { ...gap[1]!, page: 3, sourceUrl: "/directory/3" };
    await expect(certifyAuthorInventoryPages(gap, pass())).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_PAGE_GAP",
    );

    const challenged = pass();
    challenged[0] = { ...challenged[0]!, challengeDetected: true };
    await expect(
      certifyAuthorInventoryPages(challenged, pass()),
    ).rejects.toThrow("SOURCE_CHALLENGE");

    const conflict = pass();
    conflict[1]!.authors[0] = {
      href: "/writers/b",
      name: "اسم مختلف",
      poemCountText: "2",
    };
    await expect(
      certifyAuthorInventoryPages(conflict, conflict),
    ).rejects.toThrow("SOURCE_AUTHOR_INVENTORY_DUPLICATE_CONFLICT");
  });

  it("requires exact next-page and terminal semantics", async () => {
    const skipped = pass();
    skipped[0] = { ...skipped[0]!, nextPageHref: "/directory/3" };
    await expect(certifyAuthorInventoryPages(skipped, skipped)).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_NEXT_PAGE_INVALID",
    );

    const early = pass();
    early[0] = { ...early[0]!, nextPageHref: null, terminal: true };
    await expect(certifyAuthorInventoryPages(early, early)).rejects.toThrow(
      "SOURCE_AUTHOR_INVENTORY_TERMINAL_INVALID",
    );
  });
});
