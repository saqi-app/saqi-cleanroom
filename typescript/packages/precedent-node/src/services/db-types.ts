import type { drizzle } from "drizzle-orm/d1";

export type Database = ReturnType<typeof drizzle>;
