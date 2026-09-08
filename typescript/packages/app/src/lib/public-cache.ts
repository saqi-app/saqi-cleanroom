import { z } from "zod";

import type { CloudflareEnv } from "./cloudflare";

const MAXIMUM_PURGE_RESPONSE_BYTES = 64 * 1_024;
const PURGE_TIMEOUT_MS = 10_000;
const ZoneIdSchema = z.string().regex(/^[a-f\d]{32}$/i);
const PurgeResponseSchema = z.object({ success: z.literal(true) });

export interface PublicCacheConfig {
  readonly apiToken: string;
  readonly publicOrigin: string;
  readonly zoneId: string;
}

export type PublicCacheConfiguration =
  | { readonly config: PublicCacheConfig; readonly state: "enabled" }
  | { readonly state: "disabled" };

type PublicCacheEnvironment = Pick<
  CloudflareEnv,
  "CF_CACHE_PURGE_TOKEN" | "CF_ZONE_ID" | "SAQI_PUBLIC_ORIGIN"
>;

export interface PublishedPoemRoute {
  readonly authorSlug: string;
  readonly poemId: string;
}

export class PublicCacheInvalidationError extends Error {
  constructor(code: string) {
    super(code);
    this.name = "PublicCacheInvalidationError";
  }
}

export function publicCacheConfig(
  env: PublicCacheEnvironment
): PublicCacheConfiguration {
  const apiToken = env.CF_CACHE_PURGE_TOKEN;
  const rawZoneId = env.CF_ZONE_ID;
  if (!apiToken && !rawZoneId) return { state: "disabled" };
  const zoneId = ZoneIdSchema.safeParse(env.CF_ZONE_ID);
  let origin: URL;
  try {
    origin = new URL(env.SAQI_PUBLIC_ORIGIN ?? "");
  } catch {
    throw new PublicCacheInvalidationError("PUBLIC_CACHE_CONFIG_INVALID");
  }
  if (
    !apiToken?.trim() ||
    !zoneId.success ||
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  ) {
    throw new PublicCacheInvalidationError("PUBLIC_CACHE_CONFIG_INVALID");
  }
  return {
    config: {
      apiToken,
      publicOrigin: origin.origin,
      zoneId: zoneId.data,
    },
    state: "enabled",
  };
}

export function publishedPoemUrl(
  publicOrigin: string,
  route: PublishedPoemRoute
): string {
  if (!route.authorSlug || !route.poemId) {
    throw new PublicCacheInvalidationError("PUBLIC_CACHE_ROUTE_INVALID");
  }
  const path = `/author/${encodeURIComponent(route.authorSlug)}/poem/${encodeURIComponent(route.poemId)}`;
  return new URL(path, publicOrigin).href;
}

export async function purgePublishedPoem(
  config: PublicCacheConfig,
  route: PublishedPoemRoute,
  transport: typeof fetch = fetch
): Promise<string> {
  const url = publishedPoemUrl(config.publicOrigin, route);
  let response: Response;
  try {
    response = await transport(
      `https://api.cloudflare.com/client/v4/zones/${config.zoneId}/purge_cache`,
      {
        body: JSON.stringify({ files: [url] }),
        headers: {
          authorization: `Bearer ${config.apiToken}`,
          "content-type": "application/json",
        },
        method: "POST",
        signal: AbortSignal.timeout(PURGE_TIMEOUT_MS),
      }
    );
  } catch {
    throw new PublicCacheInvalidationError("PUBLIC_CACHE_PURGE_UNAVAILABLE");
  }

  let payload: unknown;
  try {
    payload = await readBoundedJson(response, MAXIMUM_PURGE_RESPONSE_BYTES);
  } catch {
    throw new PublicCacheInvalidationError(
      "PUBLIC_CACHE_PURGE_INVALID_RESPONSE"
    );
  }
  if (!response.ok || !PurgeResponseSchema.safeParse(payload).success) {
    throw new PublicCacheInvalidationError("PUBLIC_CACHE_PURGE_REJECTED");
  }
  return url;
}

async function readBoundedJson(
  response: Response,
  maximumBytes: number
): Promise<unknown> {
  const contentLength = response.headers.get("content-length");
  if (contentLength && Number(contentLength) > maximumBytes) {
    throw new Error("Response too large");
  }
  if (!response.body) throw new Error("Missing response body");

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    for (;;) {
      // eslint-disable-next-line no-await-in-loop -- Stream chunks must be read sequentially to enforce the byte limit before requesting more data.
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maximumBytes) {
        // eslint-disable-next-line no-await-in-loop -- Cancel the owned reader before releasing its lock after exceeding the byte limit.
        await reader.cancel();
        throw new Error("Response too large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
}
