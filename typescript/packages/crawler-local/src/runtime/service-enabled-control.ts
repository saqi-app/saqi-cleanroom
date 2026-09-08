// eslint-disable-next-line @sarj/prefer-node-fs-promises -- The one-time legacy read is inside a synchronous SQLite transaction; awaiting would release its atomic import boundary.
import { statSync } from "node:fs";
import { join } from "node:path";

import Database from "better-sqlite3";
import { z } from "zod";

const EnabledSchema = z.strictObject({
  enabled: z.literal([0, 1]),
});
const READ_ENABLED = `SELECT
  (SELECT enabled FROM runtime_control WHERE control_key = 'service_enabled') AS enabled
  FROM runtime_control WHERE control_key = 'legacy_service_imported'`;

/** Opens only an existing ledger; failures never authorize managed work. */
export function readServiceEnabled(root: string): boolean {
  return ServiceControlStore.withDatabase(root, (store) => store.read());
}

export function writeServiceEnabled(root: string, enabled: boolean): void {
  ServiceControlStore.withDatabase(root, (store) => store.write(enabled));
}

interface ServiceControlPort {
  read(): boolean;
  write(enabled: boolean): void;
}

class ServiceControlStore implements ServiceControlPort {
  readonly #database: Database.Database;
  readonly #root: string;

  constructor(database: Database.Database, root: string) {
    this.#database = database;
    this.#root = root;
    this.#database.pragma("busy_timeout = 5000");
    this.#database.pragma("synchronous = FULL");
  }

  static withDatabase<T>(
    root: string,
    operation: (store: ServiceControlPort) => T,
  ): T {
    const database = new Database(join(root, "ledger.sqlite3"), {
      fileMustExist: true,
    });
    try {
      return operation(new ServiceControlStore(database, root));
    } finally {
      database.close();
    }
  }

  write(enabled: boolean): void {
    this.read();
    this.#database
      .prepare(
        "UPDATE runtime_control SET enabled = ? WHERE control_key = 'service_enabled'",
      )
      .run(Number(enabled));
  }

  read(): boolean {
    const row = this.#database.prepare(READ_ENABLED).get();
    if (row !== undefined) return EnabledSchema.parse(row).enabled === 1;
    return this.#database
      .transaction(() => {
        const insert = this.#database.prepare(
          "INSERT OR IGNORE INTO runtime_control(control_key, enabled) VALUES(?, ?)",
        );
        insert.run(
          "service_enabled",
          Number(
            statSync(join(this.#root, "SERVICE_ENABLED"), {
              throwIfNoEntry: false,
            }) !== undefined,
          ),
        );
        insert.run("legacy_service_imported", 1);
        return (
          EnabledSchema.parse(this.#database.prepare(READ_ENABLED).get())
            .enabled === 1
        );
      })
      .immediate();
  }
}
