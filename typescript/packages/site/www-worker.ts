const CANONICAL_HOST = "saqi.app";

export default {
  fetch(request: Request) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method Not Allowed", {
        headers: {
          Allow: "GET, HEAD",
          "Cache-Control": "no-store",
          "Cloudflare-CDN-Cache-Control": "no-store",
          "X-Content-Type-Options": "nosniff",
        },
        status: 405,
      });
    }
    const url = new URL(request.url);
    url.hostname = CANONICAL_HOST;
    url.protocol = "https:";
    return new Response(null, {
      headers: {
        "Cache-Control": "public, max-age=3600",
        "Cloudflare-CDN-Cache-Control": "public, max-age=86400",
        Location: url.href,
        "X-Content-Type-Options": "nosniff",
      },
      status: 308,
    });
  },
} satisfies ExportedHandler;
