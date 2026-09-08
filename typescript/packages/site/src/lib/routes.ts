import { SITEMAP_SHARD_COUNT } from "@saqi/precedent-iso";

import type { Author, Poem } from "./snapshot-contract";

export { SITEMAP_SHARD_COUNT } from "@saqi/precedent-iso";
const SITEMAP_SHARD = /^(?:[1-9]|1[0-6])$/u;

export function sitemapShard(value: string | undefined) {
  if (!value || !SITEMAP_SHARD.test(value)) return undefined;
  const shard = Number(value);
  return shard <= SITEMAP_SHARD_COUNT ? shard : undefined;
}

export function authorPath(author: Pick<Author, "slug">) {
  return `/author/${encodeURIComponent(author.slug)}`;
}

export function englishAuthorLabel(author: Author) {
  const explicitName = author.nameEnglish?.trim();
  if (explicitName) return explicitName;

  const slug = author.slug.replace(/^poet-/iu, "");
  if (!/^[a-z0-9]+(?:[ _-][a-z0-9]+)*$/iu.test(slug)) return author.nameArabic;
  return slug
    .split(/[ _-]+/u)
    .map(
      (part) => `${part.charAt(0).toUpperCase()}${part.slice(1).toLowerCase()}`,
    )
    .join(" ");
}

export function authorPagePath(author: Author, pageNumber: number) {
  if (!Number.isSafeInteger(pageNumber) || pageNumber < 1) {
    throw new RangeError("Author page number must be a positive integer");
  }
  return pageNumber === 1
    ? authorPath(author)
    : `${authorPath(author)}/page/${String(pageNumber)}`;
}

export function poemPath(
  author: Pick<Author, "id" | "slug">,
  poem: Pick<Poem, "authorId" | "id">,
) {
  if (poem.authorId !== author.id) {
    throw new Error(`Poem ${poem.id} does not belong to author ${author.id}`);
  }
  return `${authorPath(author)}/poem/${encodeURIComponent(poem.id)}`;
}
