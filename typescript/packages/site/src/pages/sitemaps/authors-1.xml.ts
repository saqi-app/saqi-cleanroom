import type { APIRoute } from "astro";
import { env } from "cloudflare:workers";

import { AUTHOR_PAGE_SIZE, CatalogRepository } from "../../lib/catalog";
import { authorPagePath } from "../../lib/routes";
import { sitemap, xmlResponse } from "../../lib/xml";

export const GET: APIRoute = async ({ site }) => {
  const authors = await CatalogRepository.fromD1(env.DB).listAuthors();
  const urls = [
    new URL("/", site).href,
    ...authors.flatMap(({ author, poemCount }) =>
      Array.from(
        { length: Math.ceil(poemCount / AUTHOR_PAGE_SIZE) },
        (_, index) => new URL(authorPagePath(author, index + 1), site).href,
      ),
    ),
  ];
  return xmlResponse(sitemap(urls));
};
