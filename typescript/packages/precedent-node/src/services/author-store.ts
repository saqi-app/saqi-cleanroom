import {
  type Author,
  AuthorSchema,
  AuthorStatusSchema,
  IdentityTextSchema,
  RouteSegmentSchema,
} from "@saqi/precedent-iso";
import { eq, sql } from "drizzle-orm";

import type { Database } from "./db-types";
import { AUTHOR_TABLE as author } from "./schema";

export interface InsertAuthor {
  name: string;
  slug: string;
}

export interface AuthorStore {
  authorsToScrape(): Promise<Author[]>;
  getById(id: string): Promise<Author>;
  getByIdOrNull(id: string): Promise<Author | null>;
  getBySlug(slug: string): Promise<Author>;
  getBySlugOrNull(slug: string): Promise<Author | null>;
  listAll(): Promise<Author[]>;
  setStatusCompleted(authorId: string): Promise<Author>;
  upsertMany(authors: InsertAuthor[]): Promise<Author[]>;
}

function rowToAuthor(row: typeof author.$inferSelect): Author {
  const result = {
    id: row.id,
    slug: row.slug,
    name: row.nameArabic,
    nameEnglish: row.name ?? undefined,
    status: AuthorStatusSchema.parse(row.status),
    poemCount: row.poemCount ?? 0,
    geminiTranslationCount: row.geminiTranslationCount ?? 0,
  };
  return AuthorSchema.parse(result);
}

function normalizeAuthorSlug(slug: string): string {
  try {
    return decodeURIComponent(slug);
  } catch {
    throw new Error("Author slug contains invalid percent encoding");
  }
}

export class D1AuthorStore implements AuthorStore {
  readonly #db: Database;
  constructor(db: Database) {
    this.#db = db;
  }

  async getById(id: string): Promise<Author> {
    const found = await this.getByIdOrNull(id);
    if (!found) {
      throw new Error(`Author not found: ${id}`);
    }
    return found;
  }

  async getByIdOrNull(id: string): Promise<Author | null> {
    const row = await this.#db
      .select()
      .from(author)
      .where(eq(author.id, id))
      .get();
    return row ? rowToAuthor(row) : null;
  }

  async getBySlug(slug: string): Promise<Author> {
    const found = await this.getBySlugOrNull(slug);
    if (!found) {
      throw new Error(`Author not found: ${slug}`);
    }
    return found;
  }

  async getBySlugOrNull(slug: string): Promise<Author | null> {
    const row = await this.#db
      .select()
      .from(author)
      .where(eq(author.slug, slug))
      .get();

    return row ? rowToAuthor(row) : null;
  }

  async listAll(): Promise<Author[]> {
    const rows = await this.#db
      .select()
      .from(author)
      .orderBy(author.nameArabic)
      .all();

    return rows.map(rowToAuthor);
  }

  async authorsToScrape(): Promise<Author[]> {
    const rows = await this.#db
      .select()
      .from(author)
      .where(eq(author.status, "init"))
      .all();

    return rows.map(rowToAuthor);
  }

  async upsertMany(authors: InsertAuthor[]): Promise<Author[]> {
    if (authors.length === 0) {
      return [];
    }

    const result: Author[] = [];
    for (let index = 0; index < authors.length; index += 30) {
      const values = authors.slice(index, index + 30).map(({ slug, name }) => ({
        id: crypto.randomUUID(),
        slug: RouteSegmentSchema.parse(normalizeAuthorSlug(slug)),
        nameArabic: IdentityTextSchema.parse(name),
      }));
      // eslint-disable-next-line no-await-in-loop -- D1 bind limits require bounded serial chunks.
      const rows = await this.#db
        .insert(author)
        .values(values)
        .onConflictDoUpdate({
          target: author.slug,
          set: {
            nameArabic: sql`excluded.name_arabic`,
          },
        })
        .returning();
      result.push(...rows.map(rowToAuthor));
    }

    return result;
  }

  async setStatusCompleted(authorId: string): Promise<Author> {
    const rows = await this.#db
      .update(author)
      .set({ status: "completed-scrape" })
      .where(eq(author.id, authorId))
      .returning();

    const row = rows[0];
    if (!row) {
      throw new Error(`Author not found: ${authorId}`);
    }

    return rowToAuthor(row);
  }
}
