import { describe, expect, it } from "vitest";

import {
  accessIdentityFromClaims,
  accessIssuer,
  verifyAccessIdentity,
} from "../access";

describe("accessIssuer", () => {
  it.each([
    // eslint-disable-next-line unicorn/prefer-https -- Negative security fixture: insecure Access issuers must be rejected.
    "http://team.cloudflareaccess.com",
    "https://cloudflareaccess.com",
    "https://team.cloudflareaccess.com.evil.example",
    "https://team.cloudflareaccess.com:8443",
    "https://team.cloudflareaccess.com/path",
    "https://user@team.cloudflareaccess.com",
  ])("rejects an untrusted issuer: %s", (value) => {
    expect(accessIssuer(value)).toBeNull();
  });

  it("normalizes an exact HTTPS Access team origin", () => {
    expect(accessIssuer("https://team.cloudflareaccess.com/")).toBe(
      "https://team.cloudflareaccess.com"
    );
  });
});

describe("verifyAccessIdentity", () => {
  it("fails closed when Access configuration is missing", async () => {
    const request = new Request("https://ops.saqi.app/tasks");

    await expect(verifyAccessIdentity(request, {})).resolves.toBeNull();
  });
});

describe("accessIdentityFromClaims", () => {
  const validClaims = {
    email: "operator@example.com",
    sub: "operator-1",
    type: "app",
  };

  it("accepts an identity application token", () => {
    expect(accessIdentityFromClaims(validClaims)).toEqual({
      email: "operator@example.com",
      subject: "operator-1",
    });
  });

  it("accepts a service token already authorized by the Access policy", () => {
    const serviceName = `${"a".repeat(32)}.access`;
    const claims = {
      common_name: serviceName,
      sub: "",
      type: "app",
    };
    expect(accessIdentityFromClaims(claims)).toEqual({
      email: `${serviceName}@service-token.invalid`,
      subject: `service:${serviceName}`,
    });
  });

  it.each([
    { ...validClaims, type: "org" },
    { ...validClaims, sub: "" },
    { ...validClaims, email: "not-an-email" },
    { common_name: "service-token", sub: "", type: "app" },
  ])("rejects a non-identity application claim set", (claims) => {
    expect(accessIdentityFromClaims(claims)).toBeNull();
  });
});
