import assert from "node:assert/strict";
import test from "node:test";

import {
  authorPagePath,
  authorPath,
  englishAuthorLabel,
  poemPath,
  sitemapShard,
} from "../src/lib/routes";

const AUTHOR = {
  id: "author-id",
  slug: "poet عربي",
  nameArabic: "شاعر",
};

void test("route paths encode database identity exactly once", () => {
  const path = authorPath(AUTHOR);
  assert.equal(path, "/author/poet%20%D8%B9%D8%B1%D8%A8%D9%8A");
  assert.equal(
    poemPath(AUTHOR, { authorId: AUTHOR.id, id: "poem%20/id" }),
    `${path}/poem/poem%2520%2Fid`,
  );
});

void test("author pagination uses a single canonical URL for page one", () => {
  assert.equal(authorPagePath(AUTHOR, 1), authorPath(AUTHOR));
  assert.equal(authorPagePath(AUTHOR, 2), `${authorPath(AUTHOR)}/page/2`);
  assert.throws(() => authorPagePath(AUTHOR, 0), RangeError);
  assert.throws(() => authorPagePath(AUTHOR, 1.5), RangeError);
});

void test("sitemap shards accept only the sixteen stable partitions", () => {
  assert.equal(sitemapShard("1"), 1);
  assert.equal(sitemapShard("16"), 16);
  for (const value of [undefined, "", "0", "01", "+1", "1e2", "17"]) {
    assert.equal(sitemapShard(value), undefined);
  }
});

void test("breadcrumb labels use names, safe slugs, and Arabic fallbacks", () => {
  assert.equal(
    englishAuthorLabel({ ...AUTHOR, nameEnglish: "  Al Mutanabbi  " }),
    "Al Mutanabbi",
  );
  assert.equal(
    englishAuthorLabel({ ...AUTHOR, slug: "poet-amna-bnt-otaiba" }),
    "Amna Bnt Otaiba",
  );
  assert.equal(englishAuthorLabel(AUTHOR), AUTHOR.nameArabic);
});
