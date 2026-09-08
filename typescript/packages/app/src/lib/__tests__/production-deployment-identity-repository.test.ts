import type {
  D1Database,
  D1PreparedStatement,
} from "@cloudflare/workers-types";
import { SAQI_PRODUCTION_DATABASE_ID } from "@saqi/precedent-iso";
import { describe, expect, it, vi } from "vitest";

import { ProductionDeploymentIdentityRepository } from "../production-deployment-identity-repository";

function identityReader() {
  const first = vi.fn<D1PreparedStatement["first"]>();
  function unexpected(): never {
    throw new Error("Unexpected database operation");
  }
  const statement: D1PreparedStatement = {
    all: unexpected,
    bind: unexpected,
    first,
    raw: unexpected,
    run: unexpected,
  };
  const prepare = vi.fn<D1Database["prepare"]>().mockReturnValue(statement);
  const database: D1Database = {
    batch: unexpected,
    dump: unexpected,
    exec: unexpected,
    prepare,
    withSession: unexpected,
  };
  return {
    first,
    prepare,
    repository: new ProductionDeploymentIdentityRepository(database),
  };
}

describe("production deployment identity", () => {
  it.each([
    ["missing row", null],
    ["missing identity", {}],
    ["malformed identity", { databaseId: 42 }],
    ["wrong database", { databaseId: "another-database" }],
    ["wrong alias", { database_id: SAQI_PRODUCTION_DATABASE_ID }],
  ])("fails closed for %s", async (_label, row) => {
    const { first, repository } = identityReader();
    first.mockResolvedValue(row);
    await expect(repository.matchesProduction()).resolves.toBe(false);
  });

  it("fails closed when the identity query fails", async () => {
    const { first, repository } = identityReader();
    first.mockRejectedValue(new Error("D1 unavailable"));
    await expect(repository.matchesProduction()).resolves.toBe(false);
  });

  it("accepts only the pinned production database from the scoped query", async () => {
    const { first, prepare, repository } = identityReader();
    first.mockResolvedValue({ databaseId: SAQI_PRODUCTION_DATABASE_ID });
    await expect(repository.matchesProduction()).resolves.toBe(true);
    expect(prepare).toHaveBeenCalledExactlyOnceWith(
      "SELECT database_id AS databaseId FROM production_deployment_identity WHERE scope = 'production'"
    );
    expect(first).toHaveBeenCalledExactlyOnceWith();
  });
});
