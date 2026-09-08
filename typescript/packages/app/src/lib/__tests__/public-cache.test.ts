import { describe, expect, it, vi } from "vitest";

import {
  publicCacheConfig,
  PublicCacheInvalidationError,
  publishedPoemUrl,
  purgePublishedPoem,
} from "../public-cache";

const ZONE_ID = "a".repeat(32);

describe("public cache invalidation", () => {
  it.each([undefined, "1"])(
    "cancels a multi-chunk overflow with content-length %s",
    async (contentLength) => {
      const cancel = vi.fn();
      const sizes = [32_768, 32_768, 1];
      let pulls = 0;
      const body = new ReadableStream<Uint8Array>(
        {
          cancel,
          pull(controller) {
            const size = sizes[pulls++];
            if (size === undefined) throw new Error("Read beyond overflow");
            controller.enqueue(new Uint8Array(size));
          },
        },
        { highWaterMark: 0 }
      );
      const response = new Response(body, {
        headers: contentLength ? { "content-length": contentLength } : {},
      });
      await expect(
        purgePublishedPoem(
          {
            apiToken: "secret",
            publicOrigin: "https://saqi.app",
            zoneId: ZONE_ID,
          },
          { authorSlug: "a", poemId: "p" },
          () => Promise.resolve(response)
        )
      ).rejects.toThrow("PUBLIC_CACHE_PURGE_INVALID_RESPONSE");
      expect(pulls).toBe(3);
      expect(cancel).toHaveBeenCalledOnce();
      expect(body.locked).toBe(false);
    }
  );

  it("disables purge only when both environment credentials are absent", () => {
    expect(
      publicCacheConfig({
        CF_CACHE_PURGE_TOKEN: undefined,
        CF_ZONE_ID: undefined,
        SAQI_PUBLIC_ORIGIN: "https://saqi.app",
      })
    ).toEqual({ state: "disabled" });
  });

  it("rejects partial or malformed environment configuration", () => {
    expect(() =>
      publicCacheConfig({
        CF_CACHE_PURGE_TOKEN: undefined,
        CF_ZONE_ID: ZONE_ID,
        SAQI_PUBLIC_ORIGIN: "https://saqi.app",
      })
    ).toThrow(new PublicCacheInvalidationError("PUBLIC_CACHE_CONFIG_INVALID"));
    expect(() =>
      publicCacheConfig({
        CF_CACHE_PURGE_TOKEN: "secret",
        CF_ZONE_ID: undefined,
        SAQI_PUBLIC_ORIGIN: "https://saqi.app",
      })
    ).toThrow("PUBLIC_CACHE_CONFIG_INVALID");
    expect(() =>
      publicCacheConfig({
        CF_CACHE_PURGE_TOKEN: "secret",
        CF_ZONE_ID: ZONE_ID,
        SAQI_PUBLIC_ORIGIN: "https://saqi.app/not-an-origin",
      })
    ).toThrow("PUBLIC_CACHE_CONFIG_INVALID");
  });

  it("constructs one canonical, percent-encoded poem URL", () => {
    expect(
      publishedPoemUrl("https://saqi.app", {
        authorSlug: "شاعر / poet",
        poemId: "id/with space",
      })
    ).toBe(
      "https://saqi.app/author/%D8%B4%D8%A7%D8%B9%D8%B1%20%2F%20poet/poem/id%2Fwith%20space"
    );
  });

  it("purges only that exact URL and never exposes the token in failures", async () => {
    const transport = vi
      .fn()
      .mockResolvedValue(
        Response.json({ errors: [], messages: [], result: {}, success: true })
      );
    const config = {
      apiToken: "do-not-log-this-token",
      publicOrigin: "https://saqi.app",
      zoneId: ZONE_ID,
    };

    await expect(
      purgePublishedPoem(
        config,
        { authorSlug: "author-1", poemId: "poem-1" },
        transport
      )
    ).resolves.toBe("https://saqi.app/author/author-1/poem/poem-1");
    expect(transport).toHaveBeenCalledExactlyOnceWith(
      `https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/purge_cache`,
      expect.objectContaining({
        body: JSON.stringify({
          files: ["https://saqi.app/author/author-1/poem/poem-1"],
        }),
        method: "POST",
      })
    );

    const rejected = vi.fn().mockResolvedValue(
      Response.json({
        errors: [{ message: "do-not-log-this-token" }],
        success: false,
      })
    );
    await expect(
      purgePublishedPoem(
        config,
        { authorSlug: "author-1", poemId: "poem-1" },
        rejected
      )
    ).rejects.toThrow("PUBLIC_CACHE_PURGE_REJECTED");
    await expect(
      purgePublishedPoem(
        config,
        { authorSlug: "author-1", poemId: "poem-1" },
        rejected
      )
    ).rejects.not.toThrow("do-not-log-this-token");
  });

  it("rejects oversized and malformed API responses", async () => {
    const config = {
      apiToken: "secret",
      publicOrigin: "https://saqi.app",
      zoneId: ZONE_ID,
    };
    await expect(
      purgePublishedPoem(config, { authorSlug: "a", poemId: "p" }, () =>
        Promise.resolve(
          new Response("x", { headers: { "content-length": "65537" } })
        )
      )
    ).rejects.toThrow("PUBLIC_CACHE_PURGE_INVALID_RESPONSE");
    await expect(
      purgePublishedPoem(config, { authorSlug: "a", poemId: "p" }, () =>
        Promise.resolve(new Response("not json"))
      )
    ).rejects.toThrow("PUBLIC_CACHE_PURGE_INVALID_RESPONSE");
  });
});
