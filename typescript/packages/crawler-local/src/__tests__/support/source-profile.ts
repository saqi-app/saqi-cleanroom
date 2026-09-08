import {
  DEFAULT_SOURCE_ADAPTER_PROFILE,
  type SourceAdapterProfileV1,
} from "@saqi/source-adapter";

/** Compatibility fixture for projections captured before source profiles existed. */
export const CAPTURED_SOURCE_PROFILE: SourceAdapterProfileV1 = {
  ...DEFAULT_SOURCE_ADAPTER_PROFILE,
  dom: {
    ...DEFAULT_SOURCE_ADAPTER_PROFILE.dom,
    authorContainerSelector:
      "article, li, .card, [class*='author'], .row > div",
    authorLinkSelector: 'a[href*="/writers/"]',
    detailAuthorLinkSelector: 'a[href*="/writers/"]',
    detailContentSelector: "[data-poem-content]",
    detailStructureLabelSelector:
      "main a, main span, [class*='poem'] a, [class*='poem'] span",
    manifestContainerSelector: "article, li, [class*='poem'], .row > div",
    manifestPoemLinkSelector: 'a[href*="/works/"]',
  },
  feed: {
    authorIdFormat: "positive_integer",
    cursorKeys: [
      "initial_cursor",
      "next_poems_cursor",
      "next_poem_cursor",
      "next_cursor",
      "feed_cursor",
      "cursor",
    ],
    cursorParameter: "cursor",
    endpointMarker: "feed",
    responseHtmlKey: "html",
    responseNextCursorKey: "next_cursor",
    tokenKeys: ["x-feed-token", "feed_token", "feed-token"],
    tokenParameter: "token",
  },
  labels: {
    freeVerse: ["free verse"],
    poemCount: ["قصيدة", "قصائد"],
    verseCount: ["بيت", "أبيات"],
  },
  routes: {
    authorPath: "/writers/{slug}",
    feedPath: "/writers/{authorId}/feed",
    inventoryPath: "/directory/{page}",
    poemPath: "/works/{id}",
    poemSlug: "work-{id}",
  },
};
