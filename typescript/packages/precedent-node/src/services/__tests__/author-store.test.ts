import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { D1AuthorStore } from "../author-store";
import { AUTHOR_TABLE as author } from "../schema";

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS author (
  id TEXT PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  name_arabic TEXT NOT NULL,
  sort_name_arabic TEXT NOT NULL DEFAULT '',
  name TEXT,
  status TEXT NOT NULL DEFAULT 'init',
  poem_count INTEGER DEFAULT 0,
  gemini_translation_count INTEGER DEFAULT 0,
  hidden INTEGER NOT NULL DEFAULT 0,
  public_poem_count INTEGER NOT NULL DEFAULT 0
);

`;

describe("D1AuthorStore", () => {
  let sqlite: InstanceType<typeof Database>;
  let db: ReturnType<typeof drizzle>;
  let store: D1AuthorStore;
  let testAuthorId: string;

  beforeEach(() => {
    sqlite = new Database(":memory:");
    sqlite.exec(SCHEMA_SQL);

    db = drizzle(sqlite);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument -- better-sqlite3 and D1 use compatible Drizzle APIs but distinct driver types.
    store = new D1AuthorStore(db as any);

    testAuthorId = crypto.randomUUID();

    db.insert(author)
      .values({
        id: testAuthorId,
        slug: "test-author",
        nameArabic: "مؤلف اختبار",
        name: "Test Author",
        status: "init",
        poemCount: 0,
      })
      .run();
  });

  afterEach(() => {
    sqlite.close();
  });

  describe("getById", () => {
    it("returns author by id", async () => {
      const result = await store.getById(testAuthorId);

      expect(result.id).toBe(testAuthorId);
      expect(result.name).toBe("مؤلف اختبار");
      expect(result.nameEnglish).toBe("Test Author");
    });

    it("throws error for non-existent author", async () => {
      await expect(store.getById("non-existent")).rejects.toThrow(
        "Author not found",
      );
    });
  });

  describe("getBySlug", () => {
    it("returns author by slug", async () => {
      const result = await store.getBySlug("test-author");

      expect(result.id).toBe(testAuthorId);
      expect(result.slug).toBe("test-author");
    });

    it("throws error for non-existent slug", async () => {
      await expect(store.getBySlug("non-existent")).rejects.toThrow(
        "Author not found",
      );
    });
  });

  describe("listAll", () => {
    it("returns all authors", async () => {
      const result = await store.listAll();
      expect(result.length).toBe(1);
      expect(result[0]?.id).toBe(testAuthorId);
    });
  });

  describe("authorsToScrape", () => {
    it("returns authors with init status", async () => {
      const completedAuthorId = crypto.randomUUID();
      db.insert(author)
        .values({
          id: completedAuthorId,
          slug: "completed-author",
          nameArabic: "مؤلف مكتمل",
          status: "completed-scrape",
        })
        .run();

      const result = await store.authorsToScrape();

      expect(result.length).toBe(1);
      expect(result[0]?.status).toBe("init");
    });
  });

  describe("upsertMany", () => {
    it("inserts new authors", async () => {
      const result = await store.upsertMany([
        { slug: "new-author-1", name: "مؤلف جديد 1" },
        { slug: "new-author-2", name: "مؤلف جديد 2" },
      ]);

      expect(result.length).toBe(2);
      expect(result[0]?.slug).toBe("new-author-1");
      expect(result[1]?.slug).toBe("new-author-2");
    });

    it("updates existing author on conflict", async () => {
      const result = await store.upsertMany([
        { slug: "test-author", name: "اسم محدث" },
      ]);

      expect(result.length).toBe(1);
      expect(result[0]?.name).toBe("اسم محدث");
    });

    it("returns empty array for empty input", async () => {
      const result = await store.upsertMany([]);
      expect(result).toEqual([]);
    });

    it("chunks large inserts below D1's bind limit", async () => {
      const authors = Array.from({ length: 34 }, (_, index) => ({
        slug: `bulk-author-${String(index)}`,
        name: `مؤلف ${String(index)}`,
      }));
      const result = await store.upsertMany(authors);
      expect(result).toHaveLength(34);
    });

    it("normalizes percent-escaped slugs before storage", async () => {
      const [created] = await store.upsertMany([
        { slug: "poet-encoded%20name", name: "اسم" },
      ]);
      expect(created.slug).toBe("poet-encoded name");
      await expect(store.getBySlug("poet-encoded name")).resolves.toMatchObject(
        {
          id: created.id,
        },
      );
    });
  });

  describe("setStatusCompleted", () => {
    it("updates author status to completed-scrape", async () => {
      const result = await store.setStatusCompleted(testAuthorId);

      expect(result.status).toBe("completed-scrape");
    });

    it("throws error for non-existent author", async () => {
      await expect(store.setStatusCompleted("non-existent")).rejects.toThrow(
        "Author not found",
      );
    });
  });
});
