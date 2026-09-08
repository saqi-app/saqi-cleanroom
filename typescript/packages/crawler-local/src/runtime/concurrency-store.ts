// eslint-disable-next-line @sarj/prefer-node-fs-promises -- A synchronous existence check precedes an existing-file-only SQLite open; it never creates replacement state.
import { statSync } from "node:fs";
import { join } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

const VersionSchema = z.strictObject({ version: z.int().nonnegative() });
const MarkerSchema = z.strictObject({ enabled: z.literal(1) });
const ConcurrencySchema = z
  .strictObject({
    concurrency: z.int().min(1).max(256),
    initialConcurrency: z.int().min(1).max(256),
    revision: z.int().nonnegative(),
  })
  .refine((value) => value.initialConcurrency <= value.concurrency);
type ConcurrencyState = z.infer<typeof ConcurrencySchema>;
type ConcurrencyValues = Pick<
  ConcurrencyState,
  "concurrency" | "initialConcurrency"
>;
const READ = `SELECT target AS concurrency, initial AS initialConcurrency, revision
  FROM runtime_provider_concurrency WHERE provider = 'sol'`;

interface ConcurrencyStorePort {
  initialize(values: ConcurrencyValues): ConcurrencyState;
  read(): ConcurrencyState | null;
  update(revision: number, values: ConcurrencyValues): void;
}

/** Existing-file-only control storage; read-only inspection never imports legacy JSON. */
export class ConcurrencyStore implements ConcurrencyStorePort {
  readonly #database: Database.Database;

  constructor(database: Database.Database, readonly: boolean) {
    this.#database = database;
    this.#database.pragma("busy_timeout = 5000");
    if (!readonly) this.#database.pragma("synchronous = FULL");
  }

  static inspect(root: string): ConcurrencyState | null {
    const path = join(root, "ledger.sqlite3");
    if (!statSync(path, { throwIfNoEntry: false })) return null;
    return this.withDatabase(root, true, (store) => store.read());
  }

  static withDatabase<T>(
    root: string,
    readonly: boolean,
    operation: (store: ConcurrencyStorePort) => T,
  ): T {
    const database = new Database(join(root, "ledger.sqlite3"), {
      fileMustExist: true,
      readonly,
    });
    try {
      const store = new ConcurrencyStore(database, readonly);
      return readonly
        ? store.#inspectConsistently(operation)
        : operation(store);
    } finally {
      database.close();
    }
  }

  read(): ConcurrencyState | null {
    const { version } = VersionSchema.parse(
      this.#database
        .prepare("SELECT version FROM local_schema WHERE singleton = 1")
        .get(),
    );
    if (version < 33) return null;
    const marker = this.#database
      .prepare(
        "SELECT enabled FROM runtime_control WHERE control_key = 'legacy_concurrency_imported'",
      )
      .get();
    const row = this.#database.prepare(READ).get();
    if (marker === undefined && row === undefined) return null;
    MarkerSchema.parse(marker);
    return ConcurrencySchema.parse(row);
  }

  initialize(values: ConcurrencyValues): ConcurrencyState {
    return this.#database
      .transaction(() => {
        const existing = this.read();
        if (existing) return existing;
        const parsed = ConcurrencySchema.parse({
          concurrency: values.concurrency,
          initialConcurrency: values.initialConcurrency,
          revision: 0,
        });
        this.#database
          .prepare(
            "INSERT INTO runtime_provider_concurrency(provider, target, initial, revision) VALUES('sol', ?, ?, 0)",
          )
          .run(parsed.concurrency, parsed.initialConcurrency);
        this.#database
          .prepare(
            "INSERT INTO runtime_control(control_key, enabled) VALUES('legacy_concurrency_imported', 1)",
          )
          .run();
        return parsed;
      })
      .immediate();
  }

  update(revision: number, values: ConcurrencyValues): void {
    const parsed = ConcurrencySchema.parse({ ...values, revision });
    this.#database
      .transaction(() => {
        if (this.read()?.revision !== revision)
          throw new Error("CONCURRENCY_CONFIG_CHANGED");
        const result = this.#database
          .prepare(
            "UPDATE runtime_provider_concurrency SET target = ?, initial = ?, revision = revision + 1 WHERE provider = 'sol' AND revision = ?",
          )
          .run(parsed.concurrency, parsed.initialConcurrency, revision);
        if (result.changes !== 1) throw new Error("CONCURRENCY_CONFIG_CHANGED");
      })
      .immediate();
  }

  #inspectConsistently<T>(operation: (store: ConcurrencyStorePort) => T): T {
    return this.#database.transaction(() => operation(this))();
  }
}
