import { describe, expect, it } from "vitest";
import { json, z } from "zod";

import { schedulerStateDigest } from "../enrichment/sol-lane-scheduler.js";
import { inputHash } from "../persistence/work-key.js";

const OriginalIdentitySchema = z.json();

describe("scheduler identity schema compatibility", () => {
  it("uses the same Zod JSON factory through its named export", () => {
    expect(json).toBe(z.json);
  });

  it.each([
    { value: null },
    { value: true },
    { value: 12.5 },
    { value: "identity" },
    { value: [null, false, 3, "nested"] },
    { value: { provider: "sol", tuning: { ceiling: 8 }, previous: null } },
  ])("preserves the original digest for JSON identity $value", ({ value }) => {
    expect(schedulerStateDigest(value)).toBe(
      inputHash({
        identity: OriginalIdentitySchema.parse(value),
        schemaId: "saqi.provider-scheduler-state",
        schemaVersion: 1,
      }),
    );
  });

  it.each([
    { value: undefined },
    { value: NaN },
    { value: Infinity },
    { value: 1n },
    { value: () => null },
    { value: new Date(0) },
    { value: { missing: undefined } },
  ])("continues rejecting non-JSON identity $value", ({ value }) => {
    expect(() => OriginalIdentitySchema.parse(value)).toThrow();
    expect(() => schedulerStateDigest(value)).toThrow();
  });
});
