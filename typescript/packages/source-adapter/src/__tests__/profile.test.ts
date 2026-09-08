import { describe, expect, it } from "vitest";

import {
  DEFAULT_SOURCE_ADAPTER_PROFILE,
  SourceAdapterProfileV1Schema,
  sourcePathValue,
} from "../profile.js";

describe("source adapter profile", () => {
  it("accepts the bounded declarative profile and extracts exact path values", () => {
    expect(
      SourceAdapterProfileV1Schema.parse(DEFAULT_SOURCE_ADAPTER_PROFILE),
    ).toEqual(DEFAULT_SOURCE_ADAPTER_PROFILE);
    expect(sourcePathValue("/works/{id}", "id", "/works/42")).toBe("42");
    expect(
      sourcePathValue("/works/{id}", "id", "/works/42/extra"),
    ).toBeUndefined();
  });

  it.each([
    {
      routes: { ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes, poemPath: "/works" },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: "/works/{id}?leak=true",
      },
    },
    {
      dom: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.dom,
        detailContentSelector: "x".repeat(513),
      },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: "//host/{id}",
      },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: "/../works/{id}",
      },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: "/%2e%2e/{id}",
      },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: String.raw`/work\{id}`,
      },
    },
    {
      routes: {
        ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes,
        poemPath: "/work /{id}",
      },
    },
    { routes: { ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes, poemPath: "/{id}" } },
    { routes: { ...DEFAULT_SOURCE_ADAPTER_PROFILE.routes, poemSlug: "{id}" } },
  ])("rejects malformed or oversized declarative input", (override) => {
    expect(
      SourceAdapterProfileV1Schema.safeParse({
        ...DEFAULT_SOURCE_ADAPTER_PROFILE,
        ...override,
      }).success,
    ).toBe(false);
  });
});
