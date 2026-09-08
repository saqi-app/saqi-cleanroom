import { z } from "zod";

import { LIMITS, PROJECTION_SCHEMA_VERSION } from "./constants.js";

const boundedText = (maximum: number) => z.string().max(maximum);
const PoemStructureSchema = z.enum(["classical", "free_verse", "unknown"]);
const SOURCE_ENVELOPE = {
  challengeDetected: z.boolean(),
  schemaVersion: z.literal(PROJECTION_SCHEMA_VERSION),
  sourceUrl: boundedText(LIMITS.url),
} as const;

export const AuthorInventoryProjectionSchema = z
  .object({
    ...SOURCE_ENVELOPE,
    authors: z
      .array(
        z
          .object({
            href: boundedText(LIMITS.url),
            name: boundedText(LIMITS.authorName),
            poemCountText: boundedText(128).nullable(),
          })
          .strict(),
      )
      .max(LIMITS.authorsPerInventory),
    kind: z.literal("author_inventory"),
    terminal: z.boolean(),
  })
  .strict();

export const AuthorPoemManifestProjectionSchema = z
  .object({
    ...SOURCE_ENVELOPE,
    authorHref: boundedText(LIMITS.url),
    declaredPoemCountText: boundedText(128).nullable(),
    kind: z.literal("author_poem_manifest"),
    poems: z
      .array(
        z
          .object({
            href: boundedText(LIMITS.url),
            title: boundedText(LIMITS.poemTitle),
            verseCountText: boundedText(128).nullable(),
          })
          .strict(),
      )
      .max(LIMITS.poemsPerAuthor),
    terminal: z.boolean(),
  })
  .strict();

export const PoemDetailProjectionSchema = z
  .object({
    ...SOURCE_ENVELOPE,
    authorHref: boundedText(LIMITS.url),
    declaredVerseCountText: boundedText(128).nullable(),
    kind: z.literal("poem_detail"),
    lines: z
      .array(boundedText(LIMITS.poemLine).nullable())
      .max(LIMITS.poemLines),
    structure: PoemStructureSchema,
    title: boundedText(LIMITS.poemTitle),
  })
  .strict();

export type AuthorInventoryProjection = z.infer<
  typeof AuthorInventoryProjectionSchema
>;
export type AuthorPoemManifestProjection = z.infer<
  typeof AuthorPoemManifestProjectionSchema
>;
export type PoemDetailProjection = z.infer<typeof PoemDetailProjectionSchema>;
