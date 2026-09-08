import { LIMITS } from "./constants.js";
import {
  AuthorInventoryProjectionSchema,
  AuthorPoemManifestProjectionSchema,
  PoemDetailProjectionSchema,
} from "./projections.js";
import {
  canonicalAuthorUrl,
  canonicalInventoryUrl,
  canonicalPoemUrl,
} from "./url.js";

export interface AuthorRecord {
  canonicalId: string;
  href: string;
  name: string;
  poemCount: null | number;
  slug: string;
}

export class SourceProjectionError extends Error {
  readonly code: string;
  readonly retryable: boolean;

  constructor(
    code: string,
    message = code,
    retryable = false,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "SourceProjectionError";
    this.code = code;
    this.retryable = retryable;
  }
}

export interface AuthorInventory {
  authors: AuthorRecord[];
  complete: true;
}

export interface PoemManifestRecord {
  canonicalId: string;
  href: string;
  numericId: string;
  slug: string;
  title: string;
  verses: null | number;
}

export interface AuthorPoemManifest {
  author: ReturnType<typeof canonicalAuthorUrl>;
  complete: true;
  declaredPoemCount: null | number;
  poems: PoemManifestRecord[];
}

export type PoemStructure = "classical" | "free_verse";

export interface PoemDetail {
  author: ReturnType<typeof canonicalAuthorUrl>;
  canonicalId: string;
  href: string;
  lines: string[];
  numericId: string;
  slug: string;
  structure: PoemStructure;
  title: string;
  verses: null | number;
}

const DIGITS: ReadonlyMap<string, string> = new Map([
  ["٠", "0"],
  ["١", "1"],
  ["٢", "2"],
  ["٣", "3"],
  ["٤", "4"],
  ["٥", "5"],
  ["٦", "6"],
  ["٧", "7"],
  ["٨", "8"],
  ["٩", "9"],
  ["۰", "0"],
  ["۱", "1"],
  ["۲", "2"],
  ["۳", "3"],
  ["۴", "4"],
  ["۵", "5"],
  ["۶", "6"],
  ["۷", "7"],
  ["۸", "8"],
  ["۹", "9"],
]);

export function parseLocalizedCount(
  value: null | string,
  maximum = Number.MAX_SAFE_INTEGER,
): null | number {
  if (value === null) return null;
  const normalized = Array.from(
    value,
    (character) => DIGITS.get(character) ?? character,
  )
    .join("")
    .replaceAll(
      /(?<!\d)\d{1,3}(?:[,٬ \u{A0}\u{202F}]\d{3})+(?!\d)/gu,
      (group) => group.replaceAll(/[,٬ \u{A0}\u{202F}]/gu, ""),
    );
  const groups = normalized.match(/\d+/g);
  if (groups?.length !== 1) projectionFailure("SOURCE_COUNT_INVALID");
  const count = Number(groups[0]);
  if (!Number.isSafeInteger(count) || count < 0 || count > maximum) {
    projectionFailure("SOURCE_COUNT_RANGE");
  }
  return count;
}

function requiredText(value: string, code: string): string {
  const normalized = value.trim().normalize("NFC");
  if (normalized.length === 0) projectionFailure(code);
  return normalized;
}

function assertUsableProjection(projection: {
  challengeDetected: boolean;
  terminal?: boolean;
}): void {
  if (projection.challengeDetected) projectionFailure("SOURCE_CHALLENGE", true);
  if (projection.terminal === false)
    projectionFailure("SOURCE_PROJECTION_PARTIAL", true);
}

export function parseAuthorInventory(input: unknown): AuthorInventory {
  const projection = parseSchema(AuthorInventoryProjectionSchema, input);
  assertUsableProjection(projection);
  canonicalProjectionUrl(() => canonicalInventoryUrl(projection.sourceUrl));
  const seen = new Set<string>();
  const authors = projection.authors.map((raw) => {
    const url = canonicalProjectionUrl(() => canonicalAuthorUrl(raw.href));
    if (seen.has(url.canonicalId)) projectionFailure("SOURCE_AUTHOR_DUPLICATE");
    seen.add(url.canonicalId);
    return {
      canonicalId: url.canonicalId,
      href: url.href,
      name: requiredText(raw.name, "SOURCE_AUTHOR_NAME_EMPTY"),
      poemCount: parseLocalizedCount(raw.poemCountText, LIMITS.poemsPerAuthor),
      slug: url.slug,
    };
  });
  authors.sort((left, right) =>
    left.canonicalId.localeCompare(right.canonicalId),
  );
  return { authors, complete: true };
}

export function parseAuthorPoemManifest(input: unknown): AuthorPoemManifest {
  const projection = parseSchema(AuthorPoemManifestProjectionSchema, input);
  assertUsableProjection(projection);
  const author = canonicalProjectionUrl(() =>
    canonicalAuthorUrl(projection.authorHref),
  );
  if (
    canonicalProjectionUrl(() => canonicalAuthorUrl(projection.sourceUrl))
      .canonicalId !== author.canonicalId
  ) {
    projectionFailure("SOURCE_MANIFEST_AUTHOR_MISMATCH");
  }
  const declaredPoemCount = parseLocalizedCount(
    projection.declaredPoemCountText,
    LIMITS.poemsPerAuthor,
  );
  const seen = new Set<string>();
  const poems = projection.poems.map((raw) => {
    const url = canonicalProjectionUrl(() => canonicalPoemUrl(raw.href));
    if (seen.has(url.canonicalId)) projectionFailure("SOURCE_POEM_DUPLICATE");
    seen.add(url.canonicalId);
    return {
      canonicalId: url.canonicalId,
      href: url.href,
      numericId: url.numericId,
      slug: url.slug,
      title: requiredText(raw.title, "SOURCE_POEM_TITLE_EMPTY"),
      verses: parseLocalizedCount(raw.verseCountText, LIMITS.verses),
    };
  });
  poems.sort((left, right) => Number(left.numericId) - Number(right.numericId));
  if (poems.length === 0 && declaredPoemCount !== 0) {
    projectionFailure("SOURCE_MANIFEST_UNVERIFIED_EMPTY");
  }
  if (declaredPoemCount !== null && declaredPoemCount !== poems.length) {
    projectionFailure("SOURCE_MANIFEST_COUNT_MISMATCH");
  }
  return { author, complete: true, declaredPoemCount, poems };
}

export function parsePoemDetail(input: unknown): PoemDetail {
  const projection = parseSchema(PoemDetailProjectionSchema, input);
  assertUsableProjection(projection);
  const url = canonicalProjectionUrl(() =>
    canonicalPoemUrl(projection.sourceUrl),
  );
  const author = canonicalProjectionUrl(() =>
    canonicalAuthorUrl(projection.authorHref),
  );
  const lines = projection.lines.map((line) =>
    (line ?? "").trim().normalize("NFC"),
  );
  if (lines.every((line) => line.length === 0)) {
    projectionFailure("SOURCE_POEM_CONTENT_EMPTY");
  }
  if (
    new TextEncoder().encode(JSON.stringify(lines)).byteLength >
    LIMITS.poemTextBytes
  ) {
    projectionFailure("SOURCE_POEM_CONTENT_BYTES");
  }
  const verses = parseLocalizedCount(
    projection.declaredVerseCountText,
    LIMITS.verses,
  );
  let structure: PoemStructure;
  if (projection.structure === "classical") {
    if (verses === null || verses === 0 || lines.length !== verses * 2) {
      projectionFailure("SOURCE_CLASSICAL_LINE_COUNT_MISMATCH");
    }
    structure = "classical";
  } else if (projection.structure === "free_verse") {
    structure = "free_verse";
  } else if (verses !== null && verses > 0 && lines.length === verses * 2) {
    structure = "classical";
  } else {
    structure = "free_verse";
  }
  return {
    author,
    canonicalId: url.canonicalId,
    href: url.href,
    lines,
    numericId: url.numericId,
    slug: url.slug,
    structure,
    title: requiredText(projection.title, "SOURCE_POEM_TITLE_EMPTY"),
    verses,
  };
}

function projectionFailure(code: string, retryable = false): never {
  throw new SourceProjectionError(code, code, retryable);
}

function parseSchema<T>(
  schema: {
    safeParse(value: unknown): { data: T; success: true } | { success: false };
  },
  input: unknown,
): T {
  const result = schema.safeParse(input);
  if (!result.success) projectionFailure("SOURCE_PROJECTION_SCHEMA_INVALID");
  return result.data;
}

function canonicalProjectionUrl<T>(operation: () => T): T {
  try {
    return operation();
  } catch (error) {
    throw new SourceProjectionError(
      "SOURCE_PROJECTION_URL_INVALID",
      "SOURCE_PROJECTION_URL_INVALID",
      false,
      { cause: error },
    );
  }
}
