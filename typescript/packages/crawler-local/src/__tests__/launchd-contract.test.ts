import { describe, expect, it } from "vitest";

import {
  LaunchdServiceLabelSchema,
  minimumLaunchdExitTimeoutSeconds,
} from "../runtime/launchd-contract.js";

describe("launchd service contract", () => {
  it("shares the default label and shutdown cleanup margin", () => {
    expect(LaunchdServiceLabelSchema.parse(undefined)).toBe("net.saqi.crawler");
    expect(minimumLaunchdExitTimeoutSeconds(180_000)).toBe(195);
    expect(minimumLaunchdExitTimeoutSeconds(180_001)).toBe(196);
  });

  it("rejects labels that launchctl cannot safely address", () => {
    expect(() =>
      LaunchdServiceLabelSchema.parse("no spaces allowed"),
    ).toThrow();
  });
});
