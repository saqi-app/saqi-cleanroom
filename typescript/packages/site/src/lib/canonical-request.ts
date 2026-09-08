const CANONICAL_HOST = "saqi.app";
const UNRESERVED = /^[A-Za-z0-9._~-]$/u;

export function isReadMethod(method: string) {
  return method === "GET" || method === "HEAD";
}

export function canonicalRedirectUrl(requestUrl: string, method: string) {
  const url = new URL(requestUrl);
  let redirect = false;
  if (url.hostname === `www.${CANONICAL_HOST}`) {
    url.hostname = CANONICAL_HOST;
    redirect = true;
  }
  const pathname = canonicalPathname(url.pathname);
  if (pathname !== url.pathname) {
    url.pathname = pathname;
    redirect = true;
  }
  if (url.search && isReadMethod(method)) {
    url.search = "";
    redirect = true;
  }
  return redirect ? url : undefined;
}

function canonicalPathname(pathname: string) {
  return pathname
    .replaceAll(/\/{2,}/gu, "/")
    .replaceAll(/%[\dA-Fa-f]{2}/gu, (escape) => {
      const character = String.fromCodePoint(
        Number.parseInt(escape.slice(1), 16),
      );
      return UNRESERVED.test(character) ? character : escape.toUpperCase();
    });
}
