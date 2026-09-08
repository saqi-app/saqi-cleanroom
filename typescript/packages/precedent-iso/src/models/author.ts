import { z } from "zod";

import { IdentityTextSchema, RouteSegmentSchema } from "./text.js";

export const AuthorStatusSchema = z.enum(["init", "completed-scrape"]);

export const AuthorSchema = z.object({
  id: RouteSegmentSchema,
  slug: RouteSegmentSchema,
  name: IdentityTextSchema,
  nameEnglish: IdentityTextSchema.optional(),
  status: AuthorStatusSchema,
  poemCount: z.coerce.number(),
  geminiTranslationCount: z.coerce.number(),
});

export type Author = z.infer<typeof AuthorSchema>;
