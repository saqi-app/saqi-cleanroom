import type { APIRoute } from "astro";
import { env } from "cloudflare:workers";

import { CatalogRepository } from "../../lib/catalog";
import { poemPath, sitemapShard } from "../../lib/routes";
import { sitemap, xmlResponse } from "../../lib/xml";

export const GET: APIRoute = async ({ params, site }) => {
  const shard = sitemapShard(params.shard);
  if (!shard) return new Response("Not found", { status: 404 });
  const poems = await CatalogRepository.fromD1(env.DB).listSitemapPoems(shard);
  if (poems.length > 50_000) {
    return new Response("Sitemap capacity exceeded", { status: 503 });
  }
  const urls = poems.map(
    ({ author, poem }) => new URL(poemPath(author, poem), site).href,
  );
  return xmlResponse(sitemap(urls));
};
