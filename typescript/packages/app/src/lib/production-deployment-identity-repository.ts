import type { D1Database } from "@cloudflare/workers-types";
import { PublicationIdentitySchema } from "@saqi/precedent-iso";
import { z } from "zod";

interface ProductionDeploymentIdentityReader {
  matchesProduction(): Promise<boolean>;
}

const ProductionDeploymentIdentityRowSchema = z.object({
  databaseId: z.string(),
});

export class ProductionDeploymentIdentityRepository implements ProductionDeploymentIdentityReader {
  readonly #database: D1Database;

  constructor(database: D1Database) {
    this.#database = database;
  }

  async matchesProduction(): Promise<boolean> {
    try {
      const row = await this.#database
        .prepare(
          "SELECT database_id AS databaseId FROM production_deployment_identity WHERE scope = 'production'"
        )
        .first<unknown>();
      const identity = ProductionDeploymentIdentityRowSchema.parse(row);
      return PublicationIdentitySchema.safeParse({
        databaseId: identity.databaseId,
        schemaId: "saqi.publication-identity",
        schemaVersion: 1,
        service: "saqi-production",
      }).success;
    } catch {
      return false;
    }
  }
}
