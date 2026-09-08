import { defineMiddleware } from "astro:middleware";

import {
  canonicalRedirectUrl,
  isReadMethod,
} from "./lib/canonical-request";
import { withResponseHeaders } from "./lib/with-response-headers";

const CANONICAL_HOST = "saqi.app";

export const onRequest = defineMiddleware(async (context, next) => {
  const url = new URL(context.request.url);
  const production = url.hostname === CANONICAL_HOST;
  if (!isReadMethod(context.request.method)) {
    return withResponseHeaders(
      new Response("Method Not Allowed", {
        headers: { Allow: "GET, HEAD" },
        status: 405,
      }),
      context.request.method,
      production,
    );
  }
  const redirectUrl = canonicalRedirectUrl(url.href, context.request.method);
  if (redirectUrl) {
    const response = withResponseHeaders(
      Response.redirect(redirectUrl, 308),
      context.request.method,
      redirectUrl.hostname === CANONICAL_HOST,
    );
    if (url.search) {
      response.headers.set("Cache-Control", "no-store");
      response.headers.set("Cloudflare-CDN-Cache-Control", "no-store");
    }
    return response;
  }

  return withResponseHeaders(
    await next(),
    context.request.method,
    production,
  );
});
