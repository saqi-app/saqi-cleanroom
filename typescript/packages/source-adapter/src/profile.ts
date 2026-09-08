import { z } from "zod";

const MAXIMUM_TEMPLATE_LENGTH = 256;
const MAXIMUM_SELECTOR_LENGTH = 512;
const MAXIMUM_LEXEMES = 16;

const BoundedTextSchema = z.string().trim().min(1).max(MAXIMUM_SELECTOR_LENGTH);
const LexemesSchema = z
  .array(z.string().trim().min(1).max(64))
  .min(1)
  .max(MAXIMUM_LEXEMES);
const FeedAuthorIdFormatSchema = z.enum(["positive_integer", "slug"]);

function pathTemplate(placeholder: string) {
  const marker = `{${placeholder}}`;
  return z
    .string()
    .min(1)
    .max(MAXIMUM_TEMPLATE_LENGTH)
    .refine(
      (value) =>
        value.startsWith("/") &&
        !value.startsWith("//") &&
        value.slice(0, value.indexOf(marker)).length > 1 &&
        !/[\\%\s\u{0}-\u{1F}\u{7F}]/u.test(value) &&
        !value.includes("?") &&
        !value.includes("#") &&
        !value
          .split("/")
          .some((segment) => segment === "." || segment === "..") &&
        value.indexOf(marker) === value.lastIndexOf(marker) &&
        value.includes(marker) &&
        !/\{[^}]*\}/u.test(value.replace(marker, "")),
      `Path template must contain exactly one ${marker} placeholder`,
    );
}

export const SourceAdapterProfileV1Schema = z
  .object({
    dom: z
      .object({
        authorContainerSelector: BoundedTextSchema,
        authorLinkSelector: BoundedTextSchema,
        detailAuthorLinkSelector: BoundedTextSchema,
        detailClassicalLineSelector: BoundedTextSchema,
        detailContentSelector: BoundedTextSchema,
        detailFallbackLineSelector: BoundedTextSchema,
        detailStructureLabelSelector: BoundedTextSchema,
        manifestContainerSelector: BoundedTextSchema,
        manifestPoemLinkSelector: BoundedTextSchema,
        nextPageLinkSelector: BoundedTextSchema,
      })
      .strict(),
    feed: z
      .object({
        authorIdFormat: FeedAuthorIdFormatSchema,
        cursorKeys: LexemesSchema,
        cursorParameter: z.string().regex(/^[A-Za-z][\w-]{0,63}$/u),
        endpointMarker: z.string().trim().min(1).max(128),
        responseHtmlKey: z.string().regex(/^[A-Za-z][\w-]{0,63}$/u),
        responseNextCursorKey: z.string().regex(/^[A-Za-z][\w-]{0,63}$/u),
        tokenKeys: LexemesSchema,
        tokenParameter: z.string().regex(/^[A-Za-z][\w-]{0,63}$/u),
      })
      .strict(),
    labels: z
      .object({
        freeVerse: LexemesSchema,
        poemCount: LexemesSchema,
        verseCount: LexemesSchema,
      })
      .strict(),
    routes: z
      .object({
        authorPath: pathTemplate("slug"),
        feedPath: pathTemplate("authorId"),
        inventoryPath: pathTemplate("page"),
        poemPath: pathTemplate("id"),
        poemSlug: z
          .string()
          .min(4)
          .max(128)
          .refine(
            (value) =>
              !value.includes("/") &&
              !/[\\%\s\u{0}-\u{1F}\u{7F}]/u.test(value) &&
              value.indexOf("{id}") > 0 &&
              value.indexOf("{id}") === value.lastIndexOf("{id}") &&
              value.includes("{id}"),
            "Poem slug template must contain exactly one {id} placeholder",
          ),
      })
      .strict(),
    schemaVersion: z.literal(1),
  })
  .strict();

export type SourceAdapterProfileV1 = z.infer<
  typeof SourceAdapterProfileV1Schema
>;

export const DEFAULT_SOURCE_ADAPTER_PROFILE: SourceAdapterProfileV1 =
  Object.freeze(
    SourceAdapterProfileV1Schema.parse({
      dom: {
        authorContainerSelector: "article, li, [data-author], .row > div",
        authorLinkSelector: 'a[href*="/writers/"]',
        detailAuthorLinkSelector: 'a[href*="/writers/"]',
        detailClassicalLineSelector: ":scope > h3",
        detailContentSelector: "[data-poem-content]",
        detailFallbackLineSelector: ":scope > p, :scope > div",
        detailStructureLabelSelector:
          "main a, main span, [data-poem] a, [data-poem] span",
        manifestContainerSelector: "article, li, [data-poem], .row > div",
        manifestPoemLinkSelector: 'a[href*="/works/"]',
        nextPageLinkSelector: "a[rel='next'], .pagination a[href]",
      },
      feed: {
        authorIdFormat: "slug",
        cursorKeys: ["initial_cursor", "next_cursor", "cursor"],
        cursorParameter: "cursor",
        endpointMarker: "/feed",
        responseHtmlKey: "content",
        responseNextCursorKey: "nextCursor",
        tokenKeys: ["feed_token", "token"],
        tokenParameter: "token",
      },
      labels: {
        freeVerse: ["free verse"],
        poemCount: ["poem", "poems"],
        verseCount: ["verse", "verses"],
      },
      routes: {
        authorPath: "/writers/{slug}",
        feedPath: "/writers/{authorId}/feed",
        inventoryPath: "/directory/{page}",
        poemPath: "/works/{id}",
        poemSlug: "work-{id}",
      },
      schemaVersion: 1,
    }),
  );

export function renderSourcePath(
  template: string,
  placeholder: "authorId" | "id" | "page" | "slug",
  value: string,
): string {
  const marker = `{${placeholder}}`;
  const index = template.indexOf(marker);
  return `${template.slice(0, index)}${value}${template.slice(index + marker.length)}`;
}

export function sourcePathValue(
  template: string,
  placeholder: "authorId" | "id" | "page" | "slug",
  path: string,
): string | undefined {
  const marker = `{${placeholder}}`;
  const index = template.indexOf(marker);
  const prefix = template.slice(0, index);
  const suffix = template.slice(index + marker.length);
  if (!path.startsWith(prefix) || !path.endsWith(suffix)) return undefined;
  const value = path.slice(prefix.length, path.length - suffix.length);
  return value.length > 0 && !value.includes("/") ? value : undefined;
}
