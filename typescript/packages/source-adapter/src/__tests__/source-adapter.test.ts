import { beforeEach, describe, expect, it } from "vitest";

import { configureSource, currentSource } from "../constants.js";
import { canonicalJson, sha256Canonical } from "../hash.js";
import {
  parseAuthorInventory,
  parseAuthorPoemManifest,
  parseLocalizedCount,
  parsePoemDetail,
  SourceProjectionError,
} from "../parse.js";
import {
  canonicalAuthorUrl,
  canonicalInventoryUrl,
  canonicalPoemUrl,
} from "../url.js";

const PROJECTION_ENVELOPE = {
  challengeDetected: false,
  schemaVersion: 1 as const,
};

beforeEach(() => {
  configureSource({ name: "source", origin: "https://source.invalid" });
});

function manifest(overrides: Record<string, unknown> = {}) {
  return {
    ...PROJECTION_ENVELOPE,
    authorHref: "/writers/poet-almaarri",
    declaredPoemCountText: "٢",
    kind: "author_poem_manifest",
    poems: [
      { href: "/works/20", title: "الثاني", verseCountText: "۱۲ بيت" },
      { href: "/works/10", title: "الأول", verseCountText: "٣" },
    ],
    sourceUrl: "https://source.invalid/writers/poet-almaarri",
    terminal: true,
    ...overrides,
  };
}

function detail(overrides: Record<string, unknown> = {}) {
  return {
    ...PROJECTION_ENVELOPE,
    authorHref: "/writers/poet-almaarri",
    declaredVerseCountText: "٢",
    kind: "poem_detail",
    lines: [" صدرٌ ", "عجزٌ", "ثانٍ", "تامٌ"],
    sourceUrl: "https://source.invalid/works/101680",
    structure: "classical",
    title: " قصيدةٌ ",
    ...overrides,
  };
}

describe("localized counts", () => {
  it.each([
    ["١٬٢٣٤ بيت", 1234],
    ["١ ٢٣٤ ٥٦٧ بيت", 1_234_567],
    ["1\u{202F}234 قصيدة", 1234],
    ["۱۲۳", 123],
    ["42 قصيدة", 42],
    ["لا توجد ٠ قصائد", 0],
  ])("parses %s", (raw, expected) => {
    expect(parseLocalizedCount(raw)).toBe(expected);
  });

  it("rejects ambiguous and out-of-range counts", () => {
    expect(() => parseLocalizedCount("12 من 20")).toThrow(
      "SOURCE_COUNT_INVALID",
    );
    expect(() => parseLocalizedCount("٢٠", 10)).toThrow("SOURCE_COUNT_RANGE");
    expect(() => parseLocalizedCount("12,20")).toThrow("SOURCE_COUNT_INVALID");
  });
});

describe("canonical source URLs", () => {
  it("uses an explicitly configured source identity", () => {
    configureSource({ name: "archive", origin: "https://example.test" });
    expect(currentSource()).toEqual({
      name: "archive",
      origin: "https://example.test",
    });
    expect(canonicalPoemUrl("/works/1")).toMatchObject({
      canonicalId: "archive:poem:1",
      href: "https://example.test/works/1",
    });
  });

  it("rejects malformed source configuration", () => {
    expect(() =>
      configureSource({ name: "Source Name", origin: "https://example.test" }),
    ).toThrow("SOURCE_NAME_INVALID");
    expect(() =>
      // eslint-disable-next-line unicorn/prefer-https -- Intentional insecure-origin rejection fixture.
      configureSource({ name: "source", origin: "http://example.test" }),
    ).toThrow("SOURCE_ORIGIN_INVALID");
    expect(() =>
      configureSource({ name: "source.name", origin: "https://example.test" }),
    ).toThrow("SOURCE_NAME_INVALID");
    expect(() =>
      configureSource({ name: "source", origin: "https://example.test/path" }),
    ).toThrow("SOURCE_ORIGIN_INVALID");
  });

  it("produces stable canonical IDs", () => {
    expect(canonicalPoemUrl("/works/1").canonicalId).toBe("source:poem:1");
    expect(canonicalInventoryUrl("/directory/72").page).toBe(72);
  });

  it("normalizes encoded author Unicode deterministically", () => {
    const raw = canonicalAuthorUrl(
      "/writers/%D8%A7%D9%84%D9%85%D8%B9%D8%B1%D9%8A",
    );
    const unicode = canonicalAuthorUrl("/writers/المعري");
    expect(raw).toEqual(unicode);
    expect(raw.canonicalId).toBe("source:author:المعري");
  });

  it("canonically encodes source slugs containing internal spaces", () => {
    const author = canonicalAuthorUrl("/writers/poet-Yazid bin Al-Hakam");
    expect(author).toMatchObject({
      slug: "poet-Yazid bin Al-Hakam",
      href: "https://source.invalid/writers/poet-Yazid%20bin%20Al-Hakam",
    });
  });

  it.each([
    // eslint-disable-next-line unicorn/prefer-https -- Intentional insecure-URL rejection fixture.
    "http://source.invalid/works/1",
    "https://source.invalid.evil.example/works/1",
    "https://example.invalid/works/1",
    "https://source.invalid/works/1?x=1",
    "//evil.example/works/1",
    "/other1.html",
    "/works/0",
    "/works/01",
    "/writers/../admin",
  ])("rejects %s", (url) => {
    expect(() => canonicalPoemUrl(url)).toThrow();
    expect(() => canonicalAuthorUrl(url)).toThrow();
  });
});

describe("author inventory", () => {
  it("validates, sorts, and canonicalizes authors", () => {
    const result = parseAuthorInventory({
      ...PROJECTION_ENVELOPE,
      authors: [
        { href: "/writers/z", name: " زيد ", poemCountText: "۲" },
        { href: "/writers/a", name: "أحمد", poemCountText: null },
      ],
      kind: "author_inventory",
      sourceUrl: "https://source.invalid/directory/1",
      terminal: true,
    });
    expect(result.authors.map(({ canonicalId }) => canonicalId)).toEqual([
      "source:author:a",
      "source:author:z",
    ]);
    expect(result.authors[1]?.name).toBe("زيد");
  });

  it("rejects duplicate canonical IDs and challenge projections", () => {
    const base = {
      ...PROJECTION_ENVELOPE,
      authors: [
        {
          href: "/writers/%D8%A7%D9%84%D9%85%D8%B9%D8%B1%D9%8A",
          name: "أ",
          poemCountText: null,
        },
        { href: "/writers/المعري", name: "ب", poemCountText: null },
      ],
      kind: "author_inventory",
      sourceUrl: "https://source.invalid/directory/1",
      terminal: true,
    };
    expect(() => parseAuthorInventory(base)).toThrow("SOURCE_AUTHOR_DUPLICATE");
    expect(() =>
      parseAuthorInventory({ ...base, authors: [], challengeDetected: true }),
    ).toThrow("SOURCE_CHALLENGE");
  });

  it("rejects malformed and partial projections", () => {
    expect(() => parseAuthorInventory({})).toThrow();
    expect(() =>
      parseAuthorInventory({
        ...PROJECTION_ENVELOPE,
        authors: [],
        kind: "author_inventory",
        sourceUrl: "https://source.invalid/directory/1",
        terminal: false,
      }),
    ).toThrow("SOURCE_PROJECTION_PARTIAL");
  });
});

describe("author poem manifests", () => {
  it("parses Unicode counts and sorts poems by numeric identity", () => {
    const parsed = parseAuthorPoemManifest(manifest());
    expect(parsed.declaredPoemCount).toBe(2);
    expect(parsed.poems.map(({ numericId }) => numericId)).toEqual([
      "10",
      "20",
    ]);
    expect(parsed.poems[1]?.verses).toBe(12);
  });

  it("rejects duplicate poem IDs", () => {
    const duplicate = manifest({
      poems: [
        { href: "/works/10", title: "أ", verseCountText: "1" },
        {
          href: "https://source.invalid/works/10",
          title: "ب",
          verseCountText: "1",
        },
      ],
    });
    expect(() => parseAuthorPoemManifest(duplicate)).toThrow(
      "SOURCE_POEM_DUPLICATE",
    );
  });

  it("rejects count and author-source mismatches", () => {
    expect(() =>
      parseAuthorPoemManifest(manifest({ declaredPoemCountText: "٣" })),
    ).toThrow("SOURCE_MANIFEST_COUNT_MISMATCH");
    expect(() =>
      parseAuthorPoemManifest(
        manifest({ sourceUrl: "https://source.invalid/writers/other" }),
      ),
    ).toThrow("SOURCE_MANIFEST_AUTHOR_MISMATCH");
  });

  it("is invariant to projection order while retaining canonical identity", () => {
    const forward = parseAuthorPoemManifest(manifest());
    const reverse = parseAuthorPoemManifest(
      manifest({ poems: manifest().poems.toReversed() }),
    );
    expect(reverse).toEqual(forward);
  });

  it("accepts only explicitly certified empty manifests", () => {
    expect(() =>
      parseAuthorPoemManifest(
        manifest({ declaredPoemCountText: null, poems: [] }),
      ),
    ).toThrow("SOURCE_MANIFEST_UNVERIFIED_EMPTY");
    expect(
      parseAuthorPoemManifest(
        manifest({ declaredPoemCountText: "٠", poems: [] }),
      ).poems,
    ).toEqual([]);
  });
});

describe("poem details", () => {
  it("preserves exact slots and validates classical hemistich pairs", () => {
    const poem = parsePoemDetail(detail({ lines: ["أ", null, "ب", "ج"] }));
    expect(poem.lines).toEqual(["أ", "", "ب", "ج"]);
    expect(poem.structure).toBe("classical");
    expect(poem.canonicalId).toBe("source:poem:101680");
  });

  it("rejects a classical declared-count mismatch", () => {
    expect(() => parsePoemDetail(detail({ lines: ["أ", "ب", "ج"] }))).toThrow(
      "SOURCE_CLASSICAL_LINE_COUNT_MISMATCH",
    );
  });

  it("accepts free verse independently from declared stanza metadata", () => {
    const poem = parsePoemDetail(
      detail({
        declaredVerseCountText: "۲۰",
        lines: ["سطر أول", "سطر ثان", "سطر ثالث"],
        structure: "free_verse",
      }),
    );
    expect(poem.structure).toBe("free_verse");
    expect(poem.lines).toHaveLength(3);
  });

  it("deterministically infers unknown structure", () => {
    expect(parsePoemDetail(detail({ structure: "unknown" })).structure).toBe(
      "classical",
    );
    expect(
      parsePoemDetail(detail({ lines: ["أ", "ب", "ج"], structure: "unknown" }))
        .structure,
    ).toBe("free_verse");
  });

  it("rejects challenge and empty-content projections", () => {
    expect(() => parsePoemDetail(detail({ challengeDetected: true }))).toThrow(
      "SOURCE_CHALLENGE",
    );
    expect(() => parsePoemDetail(detail({ lines: [null, "  "] }))).toThrow(
      "SOURCE_POEM_CONTENT_EMPTY",
    );
  });
});

describe("canonical hashes", () => {
  it("sorts object keys while preserving array order", async () => {
    expect(canonicalJson({ b: 2, a: { d: 4, c: 3 } })).toBe(
      '{"a":{"c":3,"d":4},"b":2}',
    );
    await expect(sha256Canonical({ b: 2, a: 1 })).resolves.toBe(
      await sha256Canonical({ a: 1, b: 2 }),
    );
    const forwardHash = await sha256Canonical([1, 2]);
    const reverseHash = await sha256Canonical([2, 1]);
    expect(forwardHash).not.toBe(reverseHash);
  });
});

describe("projection error taxonomy", () => {
  it.each([
    [() => parseAuthorPoemManifest(manifest({ terminal: false })), true],
    [() => parsePoemDetail(detail({ challengeDetected: true })), true],
    [() => parseAuthorPoemManifest(manifest({ extra: true })), false],
    [
      () =>
        parseAuthorPoemManifest(
          manifest({
            poems: [{ href: "/bad", title: "x", verseCountText: null }],
          }),
        ),
      false,
    ],
    [() => parsePoemDetail(detail({ lines: [] })), false],
  ])(
    "classifies deterministic and source-state failures",
    (operation, retryable) => {
      try {
        operation();
        throw new Error("expected failure");
      } catch (error) {
        expect(error).toBeInstanceOf(SourceProjectionError);
        expect((error as SourceProjectionError).retryable).toBe(retryable);
      }
    },
  );
});
