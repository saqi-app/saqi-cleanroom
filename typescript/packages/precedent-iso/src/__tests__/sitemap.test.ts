import { describe, expect, it } from "vitest";

import { SITEMAP_SHARD_COUNT, sitemapShardForId } from "../sitemap.js";

describe("sitemapShardForId", () => {
  it("matches the migration's deterministic Java-style hash", () => {
    expect(sitemapShardForId("p-valid")).toBe(13);
    expect(sitemapShardForId("🌹-poem")).toBe(2);
  });

  it.each(["", "a", "poem-123", "قصيدة"])(
    "keeps %j within the configured shard range",
    (id) => {
      expect(sitemapShardForId(id)).toBeGreaterThanOrEqual(0);
      expect(sitemapShardForId(id)).toBeLessThan(SITEMAP_SHARD_COUNT);
    },
  );
});
