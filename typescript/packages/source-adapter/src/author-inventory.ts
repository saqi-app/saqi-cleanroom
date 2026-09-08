import { z } from "zod";

import { LIMITS, PROJECTION_SCHEMA_VERSION } from "./constants.js";
import { sha256Canonical } from "./hash.js";
import { parseLocalizedCount } from "./parse.js";
import { canonicalAuthorUrl, canonicalInventoryUrl } from "./url.js";

export const AuthorInventoryPageSchema = z
  .object({
    authors: z
      .array(
        z
          .object({
            href: z.string().max(LIMITS.url),
            name: z.string().max(LIMITS.authorName),
            poemCountText: z.string().max(128).nullable(),
          })
          .strict(),
      )
      .max(LIMITS.authorsPerInventory),
    challengeDetected: z.boolean(),
    kind: z.literal("author_inventory_page"),
    nextPageHref: z.string().max(LIMITS.url).nullable(),
    page: z.int().positive().max(100_000),
    schemaVersion: z.literal(PROJECTION_SCHEMA_VERSION),
    sourceUrl: z.string().max(LIMITS.url),
    terminal: z.boolean(),
  })
  .strict();

export type AuthorInventoryPageProjection = z.infer<
  typeof AuthorInventoryPageSchema
>;

const AuthorInventoryPagesSchema = z
  .array(AuthorInventoryPageSchema)
  .min(1)
  .max(100_000);

export interface CertifiedInventoryAuthor {
  readonly canonicalId: string;
  readonly href: string;
  readonly name: string;
  readonly poemCount: null | number;
  readonly slug: string;
}

export interface CertifiedAuthorInventory {
  readonly authors: readonly CertifiedInventoryAuthor[];
  readonly digest: string;
  readonly duplicateReferences: number;
  readonly pages: number;
}

interface CertifiedInventoryPass extends CertifiedAuthorInventory {
  readonly pageDigests: readonly string[];
}

export async function certifyAuthorInventoryPages(
  firstPass: unknown,
  secondPass: unknown,
): Promise<CertifiedAuthorInventory> {
  const [first, second] = await Promise.all([
    certifyPass(firstPass),
    certifyPass(secondPass),
  ]);
  if (
    first.digest !== second.digest ||
    first.pages !== second.pages ||
    first.pageDigests.join("\n") !== second.pageDigests.join("\n")
  ) {
    throw new Error("SOURCE_AUTHOR_INVENTORY_UNSTABLE");
  }
  return {
    authors: second.authors,
    digest: second.digest,
    duplicateReferences: second.duplicateReferences,
    pages: second.pages,
  };
}

async function certifyPass(input: unknown): Promise<CertifiedInventoryPass> {
  const projections = AuthorInventoryPagesSchema.parse(input);
  const authors = new Map<string, CertifiedInventoryAuthor>();
  const pages: Readonly<Record<string, unknown>>[] = [];
  let duplicateReferences = 0;

  for (const [index, projection] of projections.entries()) {
    const expectedPage = index + 1;
    const source = canonicalInventoryUrl(projection.sourceUrl);
    if (projection.challengeDetected) throw new Error("SOURCE_CHALLENGE");
    if (projection.page !== expectedPage || source.page !== expectedPage) {
      throw new Error("SOURCE_AUTHOR_INVENTORY_PAGE_GAP");
    }
    const finalPage = index === projections.length - 1;
    if (projection.terminal !== finalPage) {
      throw new Error("SOURCE_AUTHOR_INVENTORY_TERMINAL_INVALID");
    }
    let canonicalNextPageHref: null | string = null;
    if (projection.terminal) {
      if (projection.nextPageHref !== null) {
        throw new Error("SOURCE_AUTHOR_INVENTORY_NEXT_PAGE_INVALID");
      }
    } else {
      if (projection.authors.length === 0 || projection.nextPageHref === null) {
        throw new Error("SOURCE_AUTHOR_INVENTORY_NEXT_PAGE_INVALID");
      }
      const next = canonicalInventoryUrl(projection.nextPageHref);
      if (next.page !== expectedPage + 1) {
        throw new Error("SOURCE_AUTHOR_INVENTORY_NEXT_PAGE_INVALID");
      }
      canonicalNextPageHref = next.href;
    }

    const normalizedAuthors = projection.authors
      .map((raw) => {
        const canonical = canonicalAuthorUrl(raw.href);
        const author = {
          canonicalId: canonical.canonicalId,
          href: canonical.href,
          name: requiredName(raw.name),
          poemCount: parseLocalizedCount(
            raw.poemCountText,
            LIMITS.poemsPerAuthor,
          ),
          slug: canonical.slug,
        } satisfies CertifiedInventoryAuthor;
        const previous = authors.get(author.canonicalId);
        if (previous) {
          if (
            previous.href !== author.href ||
            previous.name !== author.name ||
            previous.poemCount !== author.poemCount
          ) {
            throw new Error("SOURCE_AUTHOR_INVENTORY_DUPLICATE_CONFLICT");
          }
          duplicateReferences += 1;
        } else {
          authors.set(author.canonicalId, author);
        }
        return author;
      })
      .toSorted((left, right) =>
        left.canonicalId.localeCompare(right.canonicalId),
      );
    pages.push({
      authors: normalizedAuthors,
      nextPageHref: canonicalNextPageHref,
      page: projection.page,
      sourceUrl: source.href,
      terminal: projection.terminal,
    });
  }

  // eslint-disable-next-line unicorn/prefer-iterator-to-array -- Iterator helpers are not in this package's configured runtime library.
  const sortedAuthors = [...authors.values()];
  sortedAuthors.sort((left, right) =>
    left.canonicalId.localeCompare(right.canonicalId),
  );
  const pageDigests = await Promise.all(
    pages.map((page) => sha256Canonical(page)),
  );
  const digest = await sha256Canonical({
    authors: sortedAuthors,
    duplicateReferences,
    pageDigests,
    pages: pages.length,
  });
  return {
    authors: sortedAuthors,
    digest,
    duplicateReferences,
    pageDigests,
    pages: pages.length,
  };
}

function requiredName(value: string): string {
  const normalized = value.trim().normalize("NFC");
  if (normalized.length === 0) throw new Error("SOURCE_AUTHOR_NAME_EMPTY");
  return normalized;
}
