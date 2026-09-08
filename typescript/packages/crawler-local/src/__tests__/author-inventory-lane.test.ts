import {
  type AuthorInventoryPageProjection,
  configureSource,
} from "@saqi/source-adapter";
import { beforeEach, describe, expect, it } from "vitest";

import { certifyAndSeedAuthorInventory } from "../collection/author-inventory-lane";
import { Ledger } from "../persistence/ledger";
import { CAPTURED_SOURCE_PROFILE } from "./support/source-profile";

beforeEach(() => {
  configureSource({
    name: "source",
    origin: "https://source.invalid",
    profile: CAPTURED_SOURCE_PROFILE,
  });
});

const PAGES: AuthorInventoryPageProjection[] = [
  {
    authors: [
      { href: "/writers/existing", name: "قديم", poemCountText: "2" },
      { href: "/writers/new", name: "جديد", poemCountText: null },
    ],
    challengeDetected: false,
    kind: "author_inventory_page",
    nextPageHref: null,
    page: 1,
    schemaVersion: 1,
    sourceUrl: "/directory/1",
    terminal: true,
  },
];

describe("author inventory discovery lane", () => {
  it("reconciles a certificate and atomically seeds generation-bound manifests", async () => {
    const ledger = Ledger.open(":memory:");
    try {
      const first = await certifyAndSeedAuthorInventory({
        firstPass: PAGES,
        ledger,
        productionSourceAuthors: [
          { canonical_url: "https://source.invalid/writers/existing" },
          { canonical_url: "https://source.invalid/writers/missing" },
        ],
        refreshGeneration: "2026-08-25T22.00Z",
        secondPass: PAGES,
      });
      expect(first).toMatchObject({
        alreadySeededManifests: 0,
        baselineAuthors: 2,
        complete: true,
        discoveredAuthors: 2,
        existingAuthors: 1,
        insertedManifests: 2,
        kind: "source_author_inventory_discovery",
        missingBaselineAuthorIds: ["source:author:missing"],
        newAuthorIds: ["source:author:new"],
        pagesPerPass: 1,
        refreshGeneration: "2026-08-25T22.00Z",
      });
      expect(first.certificateDigest).toMatch(/^[a-f\d]{64}$/);
      const claimedNames = new Set<string>();
      const inspectionClaims = [];
      for (let index = 0; index < 2; index += 1) {
        const claim = ledger.claim(
          "inventory-inspection",
          Date.now() + 1_000,
          10_000,
          ["source_author_manifest"],
        );
        if (!claim) throw new Error("manifest claim missing");
        claimedNames.add(String(claim.work.input["authorNameArabic"]));
        inspectionClaims.push(claim);
      }
      for (const claim of inspectionClaims)
        ledger.operatorRelease(claim, "TEST_INSPECTION", Date.now() + 1_000);
      expect(claimedNames).toEqual(new Set(["قديم", "جديد"]));

      const replay = await certifyAndSeedAuthorInventory({
        firstPass: PAGES,
        ledger,
        productionSourceAuthors: [
          { canonical_url: "https://source.invalid/writers/existing" },
          { canonical_url: "https://source.invalid/writers/missing" },
        ],
        refreshGeneration: "2026-08-25T22.00Z",
        secondPass: PAGES,
      });
      expect(replay).toMatchObject({
        alreadySeededManifests: 2,
        insertedManifests: 0,
      });

      const refresh = await certifyAndSeedAuthorInventory({
        firstPass: PAGES,
        ledger,
        productionSourceAuthors: [
          { canonical_url: "https://source.invalid/writers/existing" },
          { canonical_url: "https://source.invalid/writers/missing" },
        ],
        refreshGeneration: "2026-08-26T22.00Z",
        secondPass: PAGES,
      });
      expect(refresh.insertedManifests).toBe(2);
      expect(ledger.status().kindProgress).toEqual([
        expect.objectContaining({ kind: "source_author_manifest", total: 4 }),
      ]);
    } finally {
      ledger.close();
    }
  });

  it("rejects invalid generations and duplicate production identities before seeding", async () => {
    const ledger = Ledger.open(":memory:");
    try {
      await expect(
        certifyAndSeedAuthorInventory({
          firstPass: PAGES,
          ledger,
          productionSourceAuthors: [],
          refreshGeneration: "bad generation",
          secondPass: PAGES,
        }),
      ).rejects.toThrow();
      await expect(
        certifyAndSeedAuthorInventory({
          firstPass: PAGES,
          ledger,
          productionSourceAuthors: [
            { canonical_url: "https://source.invalid/writers/same" },
            { canonical_url: "https://source.invalid/writers/same" },
          ],
          refreshGeneration: "generation-1",
          secondPass: PAGES,
        }),
      ).rejects.toThrow("SOURCE_PRODUCTION_AUTHOR_DUPLICATE");
      expect(ledger.status().total).toBe(0);
    } finally {
      ledger.close();
    }
  });
});
