export const OPS_ORIGIN = "https://ops.saqi.app";

const RESPONSE_HEADERS = {
  "cache-control": "private, no-store, max-age=0",
  "content-security-policy":
    "base-uri 'none'; connect-src 'self'; default-src 'self'; font-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; manifest-src 'self'; media-src 'none'; object-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; worker-src 'self' blob:",
  "cross-origin-opener-policy": "same-origin",
  "cross-origin-resource-policy": "same-origin",
  expires: "0",
  "permissions-policy":
    "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
  pragma: "no-cache",
  "referrer-policy": "no-referrer",
  "strict-transport-security": "max-age=31536000; includeSubDomains",
  "x-content-type-options": "nosniff",
  "x-frame-options": "DENY",
  "x-robots-tag": "noindex, nofollow, noarchive",
} as const;

export function isImmutableNextAsset(
  request: Request,
  response: Response
): boolean {
  return (
    (request.method === "GET" || request.method === "HEAD") &&
    response.ok &&
    new URL(request.url).pathname.startsWith("/_next/static/")
  );
}

export function secureOperationsResponse(
  response: Response,
  request?: Request
): Response {
  const secured = new Response(response.body, response);
  for (const [name, value] of Object.entries(RESPONSE_HEADERS)) {
    secured.headers.set(name, value);
  }
  if (request && isImmutableNextAsset(request, response)) {
    secured.headers.set(
      "cache-control",
      "private, max-age=31536000, immutable"
    );
    secured.headers.delete("expires");
    secured.headers.delete("pragma");
  }
  return secured;
}

export const MAX_JSON_BODY_BYTES = 16_384;

export function isTrustedMutationRequest(request: Request): boolean {
  const requestOrigin = expectedOrigin(request);
  if (!requestOrigin) return false;

  const url = new URL(request.url);
  return (
    request.method === "POST" &&
    request.headers.get("host") === url.host &&
    request.headers.get("origin") === requestOrigin &&
    request.headers.get("sec-fetch-site") === "same-origin" &&
    ["cors", "same-origin"].includes(
      request.headers.get("sec-fetch-mode") ?? ""
    )
  );
}

function expectedOrigin(request: Request): null | string {
  const url = new URL(request.url);
  if (isLocalDevelopmentRequest(request, url)) return url.origin;
  return url.origin === OPS_ORIGIN ? OPS_ORIGIN : null;
}

function isLocalDevelopmentRequest(request: Request, url: URL): boolean {
  return (
    !("cf" in request) &&
    (url.hostname === "localhost" || url.hostname === "127.0.0.1")
  );
}

export function hasJsonContentType(request: Request): boolean {
  const contentType = request.headers.get("content-type") ?? "";
  return (
    contentType.split(";", 1)[0]?.trim().toLowerCase() === "application/json"
  );
}

export async function readBoundedJson(
  request: Request,
  maximumBytes = MAX_JSON_BODY_BYTES
): Promise<unknown> {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1) {
    throw new TypeError("maximumBytes must be a positive safe integer");
  }
  const contentLength = request.headers.get("content-length");
  if (contentLength) {
    const bytes = Number(contentLength);
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > maximumBytes) {
      throw new Error("Invalid content length");
    }
  }
  const contentEncoding = request.headers.get("content-encoding");
  if (contentEncoding && contentEncoding.toLowerCase() !== "identity") {
    throw new Error("Unsupported content encoding");
  }
  if (!request.body) throw new Error("Missing request body");

  const reader = request.body.getReader();
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
        throw new Error("Request body too large");
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
