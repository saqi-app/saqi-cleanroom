import { describe, expect, it } from "vitest";

import { prepareCollectedPoem } from "../publication/corpus-import-actions.js";
import {
  createAuthenticatedPublicationTransport,
  type PublicationAuthFetcher,
  redactPublicationAuth,
} from "../publication/publication-auth-client.js";
import { PublicationClient } from "../publication/publication-client.js";

const ORIGIN = "https://publish.example.test";

function action() {
  return prepareCollectedPoem(
    {
      artifactSchemaVersion: 1,
      collectedBy: "fixture",
      source: {
        author: {
          canonicalId: "source:author:test",
          href: "https://source.invalid/writers/test",
          path: "/writers/test",
          slug: "test",
        },
        canonicalId: "source:poem:1",
        href: "https://source.invalid/works/1",
        lines: ["صدر", "عجز"],
        numericId: "1",
        slug: "work-1",
        structure: "classical",
        title: "قصيدة",
        verses: 1,
      },
      sourceHash: "a".repeat(64),
      workKey: "fixture",
    },
    {
      authorId: "author-1",
      authorNameArabic: "شاعر",
      poemId: "poem-1",
      sourceAuthorSlug: "test",
      sourcePoemId: "1",
    },
    "2026-08-25T12:00:00.000Z",
    1,
  ).stageAction;
}

function jwt(iat: unknown, exp: unknown): string {
  const encode = (value: unknown) =>
    Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "RS256", kid: "fixture-key" })}.${encode({
    aud: ["publication-application"],
    exp,
    iat,
    iss: "https://fixture.cloudflareaccess.com",
    sub: "publication-user",
  })}.signature`;
}

describe("authenticated publication transport", () => {
  it("reads service credentials only from env and sends exact Access headers", async () => {
    const seen: Parameters<PublicationAuthFetcher>[0][] = [];
    const environment = {
      CF_ACCESS_CLIENT_ID: "client-id-secret",
      CF_ACCESS_CLIENT_SECRET: "client-secret-secret",
    };
    const config = {
      allowedOrigins: [ORIGIN],
      mode: "service_token",
    };
    const authenticated = createAuthenticatedPublicationTransport({
      config,
      environment,
      fetcher: async (request) => {
        seen.push(request);
        return { body: '{"ok":true,"result":{}}', status: 200 };
      },
    });
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    expect(seen).toEqual([
      expect.objectContaining({
        headers: {
          "CF-Access-Client-Id": "client-id-secret",
          "CF-Access-Client-Secret": "client-secret-secret",
          "content-type": "application/json",
          origin: ORIGIN,
          "sec-fetch-mode": "same-origin",
          "sec-fetch-site": "same-origin",
        },
        method: "POST",
      }),
    ]);
    expect(JSON.stringify(authenticated.status())).not.toContain("secret");
    expect(JSON.stringify(config)).not.toContain("client-id-secret");
    expect(
      redactPublicationAuth(
        "id=client-id-secret secret=client-secret-secret",
        environment,
        config,
      ),
    ).toBe("id=[REDACTED] secret=[REDACTED]");
  });

  it("enforces HTTPS exact-origin allowlisting before invoking fetch", async () => {
    let calls = 0;
    const authenticated = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "service_token" },
      environment: {
        CF_ACCESS_CLIENT_ID: "id",
        CF_ACCESS_CLIENT_SECRET: "secret",
      },
      fetcher: async () => {
        calls += 1;
        return { body: "", status: 200 };
      },
    });
    await expect(
      authenticated.transport({
        body: "{}",
        url: "https://publish.example.test.evil.test/import",
      }),
    ).rejects.toThrow("PUBLICATION_ORIGIN_NOT_ALLOWED");
    await expect(
      authenticated.transport({
        body: "{}",
        url: "https://publish.example.test:444/import",
      }),
    ).rejects.toThrow("PUBLICATION_ORIGIN_NOT_ALLOWED");
    expect(calls).toBe(0);
  });

  it("preflights identity, classifies rejection, and pauses only this transport", async () => {
    const seen: Parameters<PublicationAuthFetcher>[0][] = [];
    const authenticated = createAuthenticatedPublicationTransport({
      config: {
        allowedOrigins: [ORIGIN],
        identityEndpoint: `${ORIGIN}/cdn-cgi/access/get-identity`,
        mode: "service_token",
      },
      environment: {
        CF_ACCESS_CLIENT_ID: "id",
        CF_ACCESS_CLIENT_SECRET: "secret",
      },
      fetcher: async (request) => {
        seen.push(request);
        return { body: "forbidden", status: 403 };
      },
    });
    await expect(authenticated.preflight()).resolves.toMatchObject({
      state: "paused",
      status: { pauseReason: "rejected", paused: true },
    });
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ method: "GET" });
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    expect(seen).toHaveLength(1);

    const client = new PublicationClient({
      endpoint: `${ORIGIN}/import`,
      transport: authenticated.transport,
    });
    await expect(client.send(action())).resolves.toEqual({
      errorCode: "PUBLICATION_AUTH_REJECTED",
      retryAt: expect.any(Number),
      state: "auth_wait",
    });
  });

  it("periodically recovers Access with GET while every POST remains fail-closed", async () => {
    let now = 1_000;
    let accessState: "healthy" | "mismatch" | "rejected" = "rejected";
    const seen: Parameters<PublicationAuthFetcher>[0][] = [];
    const authenticated = createAuthenticatedPublicationTransport({
      config: {
        allowedOrigins: [ORIGIN],
        identityEndpoint: `${ORIGIN}/api/corpus-import`,
        mode: "service_token",
        rejectionProbeIntervalMs: 1_000,
      },
      environment: {
        CF_ACCESS_CLIENT_ID: "id",
        CF_ACCESS_CLIENT_SECRET: "secret",
      },
      fetcher: async (request) => {
        seen.push(request);
        return accessState === "healthy"
          ? {
              body: JSON.stringify({
                databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
                schemaId: "saqi.publication-identity",
                schemaVersion: 1,
                service: "saqi-production",
              }),
              status: 200,
            }
          : accessState === "mismatch"
            ? { body: "{}", status: 200 }
            : { body: "forbidden", status: 403 };
      },
      now: () => now,
    });

    await expect(authenticated.preflight()).resolves.toMatchObject({
      state: "paused",
      status: {
        consecutiveRejections: 1,
        paused: true,
        retryAt: 2_000,
      },
    });
    accessState = "healthy";
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    await expect(authenticated.preflight()).resolves.toMatchObject({
      errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
      state: "retry_wait",
      status: { paused: true, retryAt: 2_000 },
    });
    expect(seen.map(({ method }) => method)).toEqual(["GET"]);

    now = 2_000;
    accessState = "mismatch";
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    expect(seen.map(({ method }) => method)).toEqual(["GET"]);
    await expect(authenticated.preflight()).resolves.toMatchObject({
      errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_MISMATCH",
      state: "retry_wait",
      status: { paused: true },
    });
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    expect(seen.map(({ method }) => method)).toEqual(["GET", "GET"]);

    accessState = "healthy";
    await expect(authenticated.preflight()).resolves.toMatchObject({
      state: "ready",
      status: {
        consecutiveRejections: 0,
        paused: false,
        retryAt: null,
      },
    });
    expect(seen.map(({ method }) => method)).toEqual(["GET", "GET", "GET"]);
  });

  it("fails closed when the identity canary is absent or has the wrong service", async () => {
    const environment = {
      CF_ACCESS_CLIENT_ID: "id",
      CF_ACCESS_CLIENT_SECRET: "secret",
    };
    const missing = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "service_token" },
      environment,
      fetcher: () => {
        throw new Error("identity fetch must not run without an endpoint");
      },
    });
    await expect(missing.preflight()).resolves.toMatchObject({
      errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_REQUIRED",
      state: "paused",
      status: { paused: true },
    });

    const mismatch = createAuthenticatedPublicationTransport({
      config: {
        allowedOrigins: [ORIGIN],
        identityEndpoint: `${ORIGIN}/api/corpus-import`,
        mode: "service_token",
      },
      environment,
      fetcher: () =>
        Promise.resolve({
          body: JSON.stringify({
            databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
            schemaId: "saqi.publication-identity",
            schemaVersion: 1,
            service: "different-production",
          }),
          status: 200,
        }),
    });
    await expect(mismatch.preflight()).resolves.toMatchObject({
      errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_MISMATCH",
      state: "retry_wait",
      status: { paused: false },
    });

    await expect(mismatch.preflight()).resolves.toMatchObject({
      errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_MISMATCH",
      state: "retry_wait",
      status: { paused: false },
    });
  });

  it("accepts standard JWT claims and reconstructs expiry after restart", async () => {
    let now = 2_000_000;
    const token = jwt(1_000, 2_001);
    const headers: Readonly<Record<string, string>>[] = [];
    const make = () =>
      createAuthenticatedPublicationTransport({
        config: { allowedOrigins: [ORIGIN], mode: "access_jwt" },
        environment: { CF_ACCESS_TOKEN: token },
        fetcher: async (request) => {
          headers.push(request.headers);
          return { body: "ok", status: 200 };
        },
        now: () => now,
      });
    const first = make();
    expect(first.status()).toMatchObject({ paused: false });
    await first.transport({ body: "{}", url: `${ORIGIN}/import` });
    expect(headers).toEqual([
      {
        "cf-access-token": token,
        "content-type": "application/json",
        origin: ORIGIN,
        "sec-fetch-mode": "same-origin",
        "sec-fetch-site": "same-origin",
      },
    ]);
    now = 2_001_000;
    await expect(
      first.transport({ body: "{}", url: `${ORIGIN}/import` }),
    ).resolves.toMatchObject({ authFailure: "expired", status: 401 });
    expect(first.status()).toMatchObject({
      pauseReason: "expired",
      paused: true,
    });
    expect(make().status()).toMatchObject({
      pauseReason: "expired",
      paused: true,
    });
  });

  it.each([
    [undefined, 2_001],
    [1_000, undefined],
    ["1000", 2_001],
    [1_000, "2001"],
    [1_000.5, 2_001],
    [1_000, 2_001.5],
    [2_001, 2_001],
    [2_002, 2_001],
    [1_000, 87_401],
  ])("rejects invalid JWT timing claims iat=%s exp=%s", async (iat, exp) => {
    const authenticated = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "access_jwt" },
      environment: { CF_ACCESS_TOKEN: jwt(iat, exp) },
      fetcher: () => {
        throw new Error("invalid credentials must not reach fetch");
      },
      now: () => 2_000_000,
    });
    await expect(
      authenticated.transport({ body: "{}", url: `${ORIGIN}/import` }),
    ).resolves.toMatchObject({ authFailure: "rejected", status: 401 });
    expect(authenticated.status()).toMatchObject({
      expiresAt: null,
      mode: "access_jwt",
      pauseReason: "rejected",
      paused: true,
    });
  });

  it("never propagates credential-bearing transport errors", async () => {
    const authenticated = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "service_token" },
      environment: {
        CF_ACCESS_CLIENT_ID: "sensitive-id",
        CF_ACCESS_CLIENT_SECRET: "sensitive-secret",
      },
      fetcher: () => Promise.reject(new Error("sensitive-id sensitive-secret")),
    });
    let message = "";
    try {
      await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("PUBLICATION_AUTH_NETWORK_ERROR");
    expect(message).not.toContain("sensitive");
  });

  it("bounds a shared authenticated request even when no caller signal is supplied", async () => {
    const authenticated = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "service_token" },
      environment: {
        CF_ACCESS_CLIENT_ID: "id",
        CF_ACCESS_CLIENT_SECRET: "secret",
      },
      fetcher: (request) =>
        new Promise((_, reject) => {
          request.signal?.addEventListener(
            "abort",
            () => reject(new Error("synthetic request abort")),
            { once: true },
          );
        }),
      requestTimeoutMs: 10,
    });

    await expect(
      authenticated.transport({ body: "{}", url: `${ORIGIN}/import` }),
    ).rejects.toThrow("PUBLICATION_AUTH_NETWORK_ERROR");
  });

  it("recovers a rejected transport after service credentials rotate", async () => {
    const environment: Record<string, string | undefined> = {
      CF_ACCESS_CLIENT_ID: "first-id",
      CF_ACCESS_CLIENT_SECRET: "first-secret",
    };
    const authenticated = createAuthenticatedPublicationTransport({
      config: {
        allowedOrigins: [ORIGIN],
        identityEndpoint: `${ORIGIN}/identity`,
        mode: "service_token",
      },
      environment: () => environment,
      fetcher: async (request) => ({
        body: JSON.stringify({
          databaseId: "ffaae610-4dae-4d7e-bf86-8232f46ca2b5",
          schemaId: "saqi.publication-identity",
          schemaVersion: 1,
          service: "saqi-production",
        }),
        status:
          request.headers["CF-Access-Client-Id"] === "second-id" ? 200 : 403,
      }),
    });

    await expect(authenticated.preflight()).resolves.toMatchObject({
      state: "paused",
    });
    environment["CF_ACCESS_CLIENT_ID"] = "second-id";
    environment["CF_ACCESS_CLIENT_SECRET"] = "second-secret";
    await expect(authenticated.preflight()).resolves.toMatchObject({
      state: "ready",
      status: { paused: false },
    });
  });

  it("stops using stale credentials when the configured secret disappears", async () => {
    const environment: Record<string, string | undefined> = {
      CF_ACCESS_CLIENT_ID: "id",
      CF_ACCESS_CLIENT_SECRET: "secret",
    };
    let calls = 0;
    const authenticated = createAuthenticatedPublicationTransport({
      config: { allowedOrigins: [ORIGIN], mode: "service_token" },
      environment: () => environment,
      fetcher: async () => {
        calls += 1;
        return { body: "", status: 200 };
      },
    });
    await authenticated.transport({ body: "{}", url: `${ORIGIN}/import` });
    environment["CF_ACCESS_CLIENT_SECRET"] = undefined;
    await expect(
      authenticated.transport({ body: "{}", url: `${ORIGIN}/import` }),
    ).resolves.toMatchObject({ authFailure: "rejected", status: 401 });
    expect(calls).toBe(1);
  });
});
