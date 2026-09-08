import {
  currentSource,
  currentSourceAdapterProfile,
  LIMITS,
} from "./constants.js";
import { renderSourcePath, sourcePathValue } from "./profile.js";

export interface CanonicalAuthorUrl {
  canonicalId: string;
  href: string;
  path: string;
  slug: string;
}

export interface CanonicalPoemUrl {
  canonicalId: string;
  href: string;
  numericId: string;
  path: string;
  slug: string;
}

export interface CanonicalInventoryUrl {
  href: string;
  page: number;
  path: string;
}

function exactSourceUrl(value: string): URL {
  if (value.length === 0 || value.length > LIMITS.url) {
    throw new Error("SOURCE_URL_LENGTH");
  }
  let url: URL;
  try {
    url = new URL(value, currentSource().origin);
  } catch {
    throw new Error("SOURCE_URL_INVALID");
  }
  const configuredOrigin = new URL(currentSource().origin);
  if (
    url.origin !== configuredOrigin.origin ||
    url.protocol !== "https:" ||
    url.hostname !== configuredOrigin.hostname ||
    url.port !== "" ||
    url.username !== "" ||
    url.password !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new Error("SOURCE_URL_FORBIDDEN");
  }
  return url;
}

export function canonicalAuthorUrl(value: string): CanonicalAuthorUrl {
  const url = exactSourceUrl(value);
  const encodedSlug = sourcePathValue(
    currentSourceAdapterProfile().routes.authorPath,
    "slug",
    url.pathname,
  );
  if (!encodedSlug) throw new Error("SOURCE_AUTHOR_PATH_INVALID");

  let slug: string;
  try {
    slug = decodeURIComponent(encodedSlug).normalize("NFC");
  } catch {
    throw new Error("SOURCE_AUTHOR_ENCODING_INVALID");
  }
  if (slug.length === 0 || slug.length > LIMITS.authorSlug) {
    throw new Error("SOURCE_AUTHOR_SLUG_LENGTH");
  }
  if (!/^[\p{L}\p{N}_.~-]+(?: [\p{L}\p{N}_.~-]+)*$/u.test(slug)) {
    throw new Error("SOURCE_AUTHOR_SLUG_INVALID");
  }

  const path = renderSourcePath(
    currentSourceAdapterProfile().routes.authorPath,
    "slug",
    encodeURIComponent(slug),
  );
  const source = currentSource();
  const href = `${source.origin}${path}`;
  return { canonicalId: `${source.name}:author:${slug}`, href, path, slug };
}

export function canonicalPoemUrl(value: string): CanonicalPoemUrl {
  const url = exactSourceUrl(value);
  const numericId = sourcePathValue(
    currentSourceAdapterProfile().routes.poemPath,
    "id",
    url.pathname,
  );
  if (!numericId || !/^[1-9]\d*$/u.test(numericId))
    throw new Error("SOURCE_POEM_PATH_INVALID");
  const parsedId = Number(numericId);
  if (!Number.isSafeInteger(parsedId)) throw new Error("SOURCE_POEM_ID_RANGE");
  const path = renderSourcePath(
    currentSourceAdapterProfile().routes.poemPath,
    "id",
    numericId,
  );
  return {
    canonicalId: `${currentSource().name}:poem:${numericId}`,
    href: `${currentSource().origin}${path}`,
    numericId,
    path,
    slug: renderSourcePath(
      currentSourceAdapterProfile().routes.poemSlug,
      "id",
      numericId,
    ),
  };
}

export function canonicalInventoryUrl(value: string): CanonicalInventoryUrl {
  const url = exactSourceUrl(value);
  const rawPage = sourcePathValue(
    currentSourceAdapterProfile().routes.inventoryPath,
    "page",
    url.pathname,
  );
  if (!rawPage || !/^[1-9]\d*$/u.test(rawPage))
    throw new Error("SOURCE_INVENTORY_PATH_INVALID");
  const page = Number(rawPage);
  if (!Number.isSafeInteger(page))
    throw new Error("SOURCE_INVENTORY_PAGE_RANGE");
  return { href: url.href, page, path: url.pathname };
}

export function sourceAuthorUrl(slug: string): CanonicalAuthorUrl {
  return canonicalAuthorUrl(
    renderSourcePath(
      currentSourceAdapterProfile().routes.authorPath,
      "slug",
      encodeURIComponent(slug.normalize("NFC")),
    ),
  );
}

export function sourceInventoryUrl(page: number): CanonicalInventoryUrl {
  if (!Number.isSafeInteger(page) || page < 1)
    throw new Error("SOURCE_INVENTORY_PAGE_RANGE");
  return canonicalInventoryUrl(
    renderSourcePath(
      currentSourceAdapterProfile().routes.inventoryPath,
      "page",
      String(page),
    ),
  );
}

export function sourcePoemUrl(numericId: string): CanonicalPoemUrl {
  if (!/^[1-9]\d*$/u.test(numericId))
    throw new Error("SOURCE_POEM_PATH_INVALID");
  return canonicalPoemUrl(
    renderSourcePath(
      currentSourceAdapterProfile().routes.poemPath,
      "id",
      numericId,
    ),
  );
}

export function sourcePoemIdFromSlug(slug: string): string {
  const value = sourcePathValue(
    currentSourceAdapterProfile().routes.poemSlug,
    "id",
    slug,
  );
  if (!value || !/^[1-9]\d*$/u.test(value))
    throw new Error("SOURCE_POEM_SLUG_INVALID");
  return value;
}
