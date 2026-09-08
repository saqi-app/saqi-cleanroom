import { isReadMethod } from "./canonical-request";

const CLIENT_CACHE_CONTROL =
  "public, max-age=60, stale-while-revalidate=300, stale-if-error=86400";
const EDGE_CACHE_CONTROL =
  "public, max-age=300, stale-while-revalidate=60, stale-if-error=86400";
const ERROR_EDGE_CACHE_CONTROL = "public, max-age=15";

const SECURITY_HEADERS = {
  "Content-Security-Policy":
    "default-src 'none'; base-uri 'none'; connect-src 'none'; font-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; manifest-src 'self'; media-src 'none'; object-src 'none'; script-src 'self'; style-src 'self'; worker-src 'none'; require-trusted-types-for 'script'; upgrade-insecure-requests",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Resource-Policy": "same-origin",
  "Origin-Agent-Cluster": "?1",
  "Permissions-Policy":
    "accelerometer=(), autoplay=(), camera=(), display-capture=(), encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), midi=(), payment=(), publickey-credentials-get=(), screen-wake-lock=(), serial=(), usb=(), xr-spatial-tracking=()",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "X-Permitted-Cross-Domain-Policies": "none",
} as const;

export function withResponseHeaders(
  response: Response,
  method: string,
  production: boolean,
) {
  const result = new Response(response.body, response);
  for (const [name, value] of Object.entries(SECURITY_HEADERS)) {
    result.headers.set(name, value);
  }
  if (!production) {
    result.headers.set("X-Robots-Tag", "noindex, nofollow, noarchive");
  }

  const readRequest = isReadMethod(method);
  if (
    production &&
    readRequest &&
    (result.status === 404 || result.status === 410)
  ) {
    result.headers.set("Cache-Control", "no-store");
    result.headers.set(
      "Cloudflare-CDN-Cache-Control",
      ERROR_EDGE_CACHE_CONTROL,
    );
  } else if (!readRequest || result.status >= 400) {
    result.headers.set("Cache-Control", "no-store");
    result.headers.set("Cloudflare-CDN-Cache-Control", "no-store");
  } else {
    result.headers.set("Cache-Control", CLIENT_CACHE_CONTROL);
    result.headers.set("Cloudflare-CDN-Cache-Control", EDGE_CACHE_CONTROL);
    result.headers.set("Cache-Tag", "saqi-corpus");
  }
  return result;
}
