import { hash } from "node:crypto";

import { PublicationIdentitySchema } from "@saqi/precedent-iso";
import { z } from "zod";

import { RUNTIME_ENVIRONMENT } from "../runtime/runtime-environment.js";
import type {
  PublicationRequest,
  PublicationTransport,
  PublicationTransportResponse,
} from "./publication-client.js";
import { PublicationRequestTimeoutMsSchema } from "./publication-client.js";

const EnvironmentNameSchema = z.string().regex(/^[A-Z_][A-Z0-9_]{0,127}$/);
const HttpsOriginSchema = z.url().transform((value, context) => {
  const url = new URL(value);
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    context.addIssue({
      code: "custom",
      message: "Expected an exact HTTPS origin",
    });
    return z.NEVER;
  }
  return url.origin;
});
const IdentityEndpointSchema = z.url().nullable().default(null);
const COMMON = {
  allowedOrigins: z.array(HttpsOriginSchema).min(1).max(10),
  identityEndpoint: IdentityEndpointSchema,
  rejectionProbeIntervalMs: z
    .int()
    .min(1_000)
    .max(30 * 60_000)
    .default(60_000),
} as const;
const JwtClaimsSchema = z.object({
  exp: z.int(),
  iat: z.int(),
});
const DEFAULT_REQUEST_TIMEOUT_MS = 2 * 60_000;

export const PublicationAuthConfigSchema = z.discriminatedUnion("mode", [
  z.strictObject({
    ...COMMON,
    maximumLifetimeSeconds: z.int().min(300).max(86_400).default(86_400),
    mode: z.literal("access_jwt"),
    tokenEnvironment: EnvironmentNameSchema.default("CF_ACCESS_TOKEN"),
  }),
  z.strictObject({
    ...COMMON,
    clientIdEnvironment: EnvironmentNameSchema.default("CF_ACCESS_CLIENT_ID"),
    clientSecretEnvironment: EnvironmentNameSchema.default(
      "CF_ACCESS_CLIENT_SECRET",
    ),
    mode: z.literal("service_token"),
  }),
]);

export type PublicationAuthConfig = z.infer<typeof PublicationAuthConfigSchema>;
export type PublicationAuthPauseReason = "expired" | "rejected";
export interface PublicationAuthStatus {
  readonly consecutiveRejections: number;
  readonly expiresAt: null | number;
  readonly mode: PublicationAuthConfig["mode"];
  readonly paused: boolean;
  readonly pauseReason: null | PublicationAuthPauseReason;
  readonly retryAt: null | number;
}

export interface PublicationAuthPreflight {
  readonly errorCode?: string;
  readonly state: "paused" | "ready" | "retry_wait";
  readonly status: PublicationAuthStatus;
}

export interface AuthenticatedPublicationTransport {
  preflight(signal?: AbortSignal): Promise<PublicationAuthPreflight>;
  status(): PublicationAuthStatus;
  readonly transport: PublicationTransport;
}

interface AuthFetchRequest {
  readonly body?: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly method: "GET" | "POST";
  readonly signal?: AbortSignal;
  readonly url: string;
}

export type PublicationAuthFetcher = (
  request: AuthFetchRequest,
) => Promise<PublicationTransportResponse>;

export interface AuthenticatedPublicationTransportOptions {
  readonly config: unknown;
  readonly environment?:
    | (() => Readonly<Record<string, string | undefined>>)
    | Readonly<Record<string, string | undefined>>;
  readonly fetcher?: PublicationAuthFetcher;
  readonly now?: () => number;
  readonly requestTimeoutMs?: number;
}

export function createAuthenticatedPublicationTransport(
  options: AuthenticatedPublicationTransportOptions,
): AuthenticatedPublicationTransport {
  const config = PublicationAuthConfigSchema.parse(options.config);
  const environment = environmentReader(
    options.environment ?? RUNTIME_ENVIRONMENT,
  );
  const now = options.now ?? Date.now;
  const requestTimeoutMs = PublicationRequestTimeoutMsSchema.parse(
    options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
  );
  const fetcher = options.fetcher ?? fetchPublicationAuth;
  const origins = new Set(config.allowedOrigins);
  let credentials: null | PublicationCredentials = null;
  let pauseReason: null | PublicationAuthPauseReason = null;
  let consecutiveRejections = 0;
  let retryAt: null | number = null;
  const refreshCredentials = (): void => {
    let next: PublicationCredentials;
    try {
      next = readCredentials(config, environment(), now());
    } catch {
      credentials = null;
      pauseReason = "rejected";
      retryAt ??= now() + config.rejectionProbeIntervalMs;
      return;
    }
    if (next.generation !== credentials?.generation) {
      consecutiveRejections = 0;
      pauseReason = null;
      retryAt = null;
    }
    credentials = next;
    if (next.pauseReason !== null) pauseReason = next.pauseReason;
  };
  refreshCredentials();
  const status = (): PublicationAuthStatus => {
    refreshCredentials();
    return {
      consecutiveRejections,
      expiresAt: credentials?.expiresAt ?? null,
      mode: config.mode,
      pauseReason,
      paused: pauseReason !== null,
      retryAt,
    };
  };
  const request = async (
    url: string,
    method: "GET" | "POST",
    body?: string,
    signal?: AbortSignal,
    allowRejectedProbe = false,
  ): Promise<PublicationTransportResponse> => {
    requireAllowedUrl(url, origins);
    refreshCredentials();
    if (
      pauseReason !== null &&
      !(allowRejectedProbe && pauseReason === "rejected" && method === "GET")
    )
      return { authFailure: pauseReason, body: "", status: 401 };
    if (credentials === null)
      return { authFailure: "rejected", body: "", status: 401 };
    if (credentials.expiresAt !== null && credentials.expiresAt <= now()) {
      pauseReason = "expired";
      return { authFailure: "expired", body: "", status: 401 };
    }
    try {
      const timeoutSignal = AbortSignal.timeout(requestTimeoutMs);
      const requestSignal = signal
        ? AbortSignal.any([signal, timeoutSignal])
        : timeoutSignal;
      const parsedUrl = new URL(url);
      const headers =
        method === "POST"
          ? {
              ...credentials.headers,
              origin: parsedUrl.origin,
              "sec-fetch-mode": "same-origin",
              "sec-fetch-site": "same-origin",
            }
          : credentials.headers;
      const response = await fetcher({
        ...(body === undefined ? {} : { body }),
        headers,
        method,
        signal: requestSignal,
        url,
      });
      if (
        response.status === 401 ||
        response.status === 403 ||
        (response.status >= 300 && response.status < 400)
      ) {
        pauseReason =
          credentials.expiresAt !== null && credentials.expiresAt <= now()
            ? "expired"
            : "rejected";
        if (pauseReason === "rejected") {
          consecutiveRejections += 1;
          retryAt = now() + config.rejectionProbeIntervalMs;
        }
        return { ...response, authFailure: pauseReason };
      }
      return response;
    } catch {
      throw new Error("PUBLICATION_AUTH_NETWORK_ERROR");
    }
  };
  return {
    status,
    transport: (publicationRequest: PublicationRequest) =>
      request(
        publicationRequest.url,
        "POST",
        publicationRequest.body,
        publicationRequest.signal,
      ),
    async preflight(signal?: AbortSignal) {
      refreshCredentials();
      if (pauseReason === "expired")
        return { state: "paused", status: status() };
      if (pauseReason === "rejected" && retryAt !== null && retryAt > now())
        return {
          errorCode: "PUBLICATION_AUTH_REPROBE_WAIT",
          state: "retry_wait",
          status: status(),
        };
      if (config.identityEndpoint === null) {
        pauseReason = "rejected";
        return {
          errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_REQUIRED",
          state: "paused",
          status: status(),
        };
      }
      let response: PublicationTransportResponse;
      try {
        response = await request(
          config.identityEndpoint,
          "GET",
          undefined,
          signal,
          pauseReason === "rejected",
        );
      } catch {
        return {
          errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_NETWORK",
          state: "retry_wait",
          status: status(),
        };
      }
      if (response.authFailure) return { state: "paused", status: status() };
      if (response.status < 200 || response.status >= 300)
        return {
          errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_HTTP",
          state: "retry_wait",
          status: status(),
        };
      const identity = PublicationIdentitySchema.safeParse(
        safeJson(response.body),
      );
      if (!identity.success) {
        return {
          errorCode: "PUBLICATION_IDENTITY_PREFLIGHT_MISMATCH",
          state: "retry_wait",
          status: status(),
        };
      }
      consecutiveRejections = 0;
      pauseReason = null;
      retryAt = null;
      return { state: "ready", status: status() };
    },
  };
}

function safeJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

export function redactPublicationAuth(
  value: string,
  environment: Readonly<Record<string, string | undefined>>,
  configInput: unknown,
): string {
  const config = PublicationAuthConfigSchema.parse(configInput);
  const names =
    config.mode === "access_jwt"
      ? [config.tokenEnvironment]
      : [config.clientIdEnvironment, config.clientSecretEnvironment];
  return names.reduce((redacted, name) => {
    const secret = environment[name];
    return secret && secret.length > 0
      ? redacted.replaceAll(secret, "[REDACTED]")
      : redacted;
  }, value);
}

interface PublicationCredentials {
  readonly expiresAt: null | number;
  readonly generation: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly pauseReason: null | PublicationAuthPauseReason;
}

function readCredentials(
  config: PublicationAuthConfig,
  environment: Readonly<Record<string, string | undefined>>,
  now: number,
): PublicationCredentials {
  if (config.mode === "service_token") {
    const clientId = requireEnvironment(
      environment,
      config.clientIdEnvironment,
    );
    const clientSecret = requireEnvironment(
      environment,
      config.clientSecretEnvironment,
    );
    return {
      expiresAt: null,
      generation: credentialGeneration(config.mode, clientId, clientSecret),
      headers: {
        "CF-Access-Client-Id": clientId,
        "CF-Access-Client-Secret": clientSecret,
        "content-type": "application/json",
      },
      pauseReason: null,
    };
  }
  const token = requireEnvironment(environment, config.tokenEnvironment);
  const claims = parseJwtClaims(token);
  const expiresAt = claims.exp * 1_000;
  const issuedAt = claims.iat * 1_000;
  if (
    expiresAt <= issuedAt ||
    expiresAt - issuedAt > config.maximumLifetimeSeconds * 1_000
  )
    throw new Error("PUBLICATION_ACCESS_JWT_NOT_SHORT_LIVED");
  return {
    expiresAt,
    generation: credentialGeneration(config.mode, token),
    headers: { "cf-access-token": token, "content-type": "application/json" },
    pauseReason: expiresAt <= now ? "expired" : null,
  };
}

function credentialGeneration(mode: string, ...values: readonly string[]) {
  return hash("sha256", JSON.stringify([mode, ...values]), "hex");
}

function environmentReader(
  environment:
    | (() => Readonly<Record<string, string | undefined>>)
    | Readonly<Record<string, string | undefined>>,
): () => Readonly<Record<string, string | undefined>> {
  return typeof environment === "function" ? environment : () => environment;
}

function parseJwtClaims(token: string): { exp: number; iat: number } {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("PUBLICATION_ACCESS_JWT_INVALID");
  try {
    const payload = parts.at(1);
    if (!payload) throw new Error("PUBLICATION_ACCESS_JWT_PAYLOAD_MISSING");
    const input: unknown = JSON.parse(
      Buffer.from(payload, "base64url").toString("utf8"),
    );
    return JwtClaimsSchema.parse(input);
  } catch {
    throw new Error("PUBLICATION_ACCESS_JWT_INVALID");
  }
}

function requireAllowedUrl(
  urlValue: string,
  allowed: ReadonlySet<string>,
): void {
  const url = new URL(urlValue);
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.hash ||
    !allowed.has(url.origin)
  )
    throw new Error("PUBLICATION_ORIGIN_NOT_ALLOWED");
}

function requireEnvironment(
  environment: Readonly<Record<string, string | undefined>>,
  name: string,
): string {
  const value = environment[name];
  if (!value || value.trim().length === 0)
    throw new Error(`PUBLICATION_AUTH_ENV_MISSING:${name}`);
  return value;
}

async function fetchPublicationAuth(
  request: AuthFetchRequest,
): Promise<PublicationTransportResponse> {
  const response = await fetch(request.url, {
    ...(request.body === undefined ? {} : { body: request.body }),
    headers: request.headers,
    method: request.method,
    redirect: "manual",
    ...(request.signal ? { signal: request.signal } : {}),
  });
  const retryAfter = response.headers.get("retry-after");
  return {
    body: await response.text(),
    ...(retryAfter === null ? {} : { retryAfter }),
    status: response.status,
  };
}
