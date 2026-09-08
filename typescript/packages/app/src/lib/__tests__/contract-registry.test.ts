import { readdirSync, readFileSync } from "node:fs";

import {
  CLOUDFLARE_WORKER_CONTRACTS,
  generateContractCatalog,
  HTTP_CONTRACTS,
} from "@saqi/precedent-iso";
import ts from "typescript";
import { describe, expect, it } from "vitest";

function routeFileCount(directory: URL): number {
  return readdirSync(directory, { recursive: true, encoding: "utf8" }).filter(
    (path) =>
      /(?:\.astro|\.txt\.ts|\.xml\.ts|route\.ts|page\.tsx|not-found\.tsx)$/u.test(
        path
      )
  ).length;
}

function wranglerBindingNames(source: string): string[] {
  const parsed = ts.parseJsonText("wrangler.jsonc", source);
  if (ts.parseConfigFileTextToJson("wrangler.jsonc", source).error) {
    throw new Error("Invalid Wrangler JSONC fixture");
  }
  const names: string[] = [];
  function visit(node: ts.Node): void {
    if (
      ts.isPropertyAssignment(node) &&
      ts.isStringLiteral(node.name) &&
      node.name.text === "binding"
    ) {
      if (!ts.isStringLiteral(node.initializer)) {
        throw new Error("Wrangler binding must be a string");
      }
      names.push(node.initializer.text);
    }
    ts.forEachChild(node, visit);
  }
  visit(parsed);
  return names.toSorted();
}

describe("contract registry", () => {
  it("documents every public and operations route", () => {
    expect(HTTP_CONTRACTS.map(({ id }) => id)).toEqual([
      "public.author-index",
      "public.legacy-author-page",
      "public.author",
      "public.author-page",
      "public.poem",
      "public.not-found",
      "public.server-error",
      "public.robots",
      "public.sitemap-index",
      "public.author-sitemap",
      "public.poem-sitemap",
      "operations.home",
      "operations.tasks",
      "operations.not-found",
      "operations.legacy-source-lineage-adoption",
      "operations.source-lineage-maintenance-status",
      "operations.source-lineage-maintenance-run",
      "operations.corpus-fingerprint-backfill",
      "operations.corpus-import",
      "operations.corpus-resolution",
      "operations.source-admissions-v2",
      "operations.enrichment-publications-v2",
    ]);
    expect(generateContractCatalog().queues).toEqual([]);
  });

  it("covers every deployed Worker boundary", () => {
    expect(CLOUDFLARE_WORKER_CONTRACTS).toEqual([
      expect.objectContaining({
        bindings: ["ASSETS", "DB"],
        id: "operations",
      }),
      expect.objectContaining({
        bindings: ["ASSETS", "DB"],
        id: "public-site",
      }),
      expect.objectContaining({ bindings: [], id: "www-redirect" }),
    ]);
    expect(generateContractCatalog().actions).toEqual([]);
  });

  it("keeps route and Wrangler binding inventories mechanically complete", () => {
    expect(
      HTTP_CONTRACTS.filter(({ service }) => service === "public-site")
    ).toHaveLength(
      routeFileCount(new URL("../../../../site/src/pages/", import.meta.url))
    );
    expect(
      HTTP_CONTRACTS.filter(({ service }) => service === "operations")
    ).toHaveLength(
      routeFileCount(new URL("../../../src/app/", import.meta.url))
    );
  });

  it.each([
    ["operations", "../../../../app/wrangler.jsonc"],
    ["public-site", "../../../../site/wrangler.production.jsonc"],
    ["www-redirect", "../../../../site/wrangler.www.jsonc"],
  ] as const)(
    "keeps %s Wrangler bindings mechanically complete",
    (workerId, configPath) => {
      const source = readFileSync(new URL(configPath, import.meta.url), "utf8");
      const actual = wranglerBindingNames(source);
      const expected = CLOUDFLARE_WORKER_CONTRACTS.find(
        ({ id }) => id === workerId
      );
      expect(actual).toEqual(expected?.bindings.toSorted());
    }
  );

  it("ignores commented bindings and accepts JSONC trailing commas", () => {
    expect(
      wranglerBindingNames(`{
      // "binding": "FAKE",
      "assets": { "binding": "ASSETS", },
    }`)
    ).toEqual(["ASSETS"]);
  });

  it.each([
    ["malformed JSONC", '{"assets":'],
    ["non-string binding", '{"assets":{"binding":42}}'],
  ])(
    "rejects %s rather than accepting a partial inventory",
    (_label, source) => {
      expect(() => wranglerBindingNames(source)).toThrow();
    }
  );

  it("derives portable JSON Schema from the runtime validators", () => {
    const catalog = generateContractCatalog();
    expect(JSON.stringify(catalog)).not.toContain("[object Object]");
  });
});
