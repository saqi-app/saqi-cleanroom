import type { APIRoute } from "astro";

import { SITEMAP_SHARD_COUNT } from "../lib/routes";
import { escapeXml, xmlResponse } from "../lib/xml";

export const GET: APIRoute = ({ site }) => {
  const paths = [
    "/sitemaps/authors-1.xml",
    ...Array.from(
      { length: SITEMAP_SHARD_COUNT },
      (_, index) => `/sitemaps/poems-${String(index + 1)}.xml`,
    ),
  ];
  const entries = paths
    .map(
      (path) =>
        `<sitemap><loc>${escapeXml(new URL(path, site).href)}</loc></sitemap>`,
    )
    .join("");
  return xmlResponse(
    `<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${entries}</sitemapindex>`,
  );
};
