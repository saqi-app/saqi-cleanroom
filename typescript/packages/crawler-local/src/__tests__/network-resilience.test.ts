import { describe, expect, it } from "vitest";

import {
  isNetworkFailureText,
  isPredispatchNetworkFailureText,
  networkProbeDelayMs,
} from "../runtime/network-resilience.js";

describe("network resilience", () => {
  it.each([
    "getaddrinfo ENOTFOUND chatgpt.com",
    "connect ENETUNREACH 2606:4700::",
    "net::ERR_INTERNET_DISCONNECTED",
    "connection reset by peer",
    "socket hang up",
  ])("recognizes transport failure %s", (message) => {
    expect(isNetworkFailureText(message)).toBe(true);
  });

  it.each([
    ["getaddrinfo ENOTFOUND host", true],
    ["connection reset by peer", false],
    ["request ETIMEDOUT", false],
  ] as const)(
    "classifies pre-dispatch transport evidence %s",
    (message, expected) => {
      expect(isPredispatchNetworkFailureText(message)).toBe(expected);
    },
  );

  it("uses capped exponential probe backoff with injectable jitter", () => {
    expect(networkProbeDelayMs(1, () => 0, 1_000, 8_000)).toBe(500);
    expect(networkProbeDelayMs(2, () => 0.999, 1_000, 8_000)).toBe(1_999);
    expect(
      networkProbeDelayMs(99, () => 0.999, 1_000, 8_000),
    ).toBeLessThanOrEqual(8_000);
    expect(() => networkProbeDelayMs(0, () => 0)).toThrow(
      "NETWORK_FAILURE_COUNT_INVALID",
    );
  });
});
