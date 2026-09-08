import type {
  ExecutionContext,
  ScheduledController,
} from "@cloudflare/workers-types";

import openNextHandler from "./.open-next/worker.js";
import { type AccessEnv, verifyAccessIdentity } from "./src/lib/access";
import {
  isTrustedMutationRequest,
  OPS_ORIGIN,
  secureOperationsResponse,
} from "./src/lib/operations-boundary";

interface AccessWorker {
  readonly fetch: (
    request: Request,
    env: AccessEnv,
    context: ExecutionContext
  ) => Promise<Response>;
  readonly scheduled: (
    controller: ScheduledController,
    env: AccessEnv,
    context: ExecutionContext
  ) => void;
}

const FORWARDED_IDENTITY_HEADERS: ReadonlySet<string> = new Set([
  "authorization",
  "cf-access-authenticated-user-email",
  "cf-access-jwt-assertion",
  "cf-access-user",
  "cookie",
  "remote-user",
  "x-forwarded-email",
  "x-forwarded-user",
  "x-user-email",
  "x-user-id",
]);

function isAllowedHost(request: Request): boolean {
  const url = new URL(request.url);
  const hostMatches = request.headers.get("host") === url.host;
  if (url.origin === OPS_ORIGIN) return hostMatches;

  const isLocalRequest =
    !("cf" in request) &&
    (url.hostname === "localhost" || url.hostname === "127.0.0.1");
  return isLocalRequest && hostMatches;
}

function sanitizeRequest(request: Request): Request {
  const headers = new Headers(request.headers);
  for (const name of request.headers.keys()) {
    if (
      FORWARDED_IDENTITY_HEADERS.has(name) ||
      name.startsWith("cf-access-") ||
      name.startsWith("x-auth-request-")
    ) {
      headers.delete(name);
    }
  }
  return new Request(request, { headers });
}

function errorResponse(status: 403 | 405): Response {
  return secureOperationsResponse(
    new Response(status === 403 ? "Forbidden" : "Method Not Allowed", {
      status,
      headers: { "content-type": "text/plain; charset=utf-8" },
    })
  );
}

const WORKER: AccessWorker = Object.freeze({
  async fetch(request: Request, env: AccessEnv, context: ExecutionContext) {
    if (!isAllowedHost(request)) return errorResponse(403);
    if (!["GET", "HEAD", "POST"].includes(request.method)) {
      return errorResponse(405);
    }
    if (request.method === "POST" && !isTrustedMutationRequest(request)) {
      return errorResponse(403);
    }
    if (!(await verifyAccessIdentity(request, env))) return errorResponse(403);

    const response = await openNextHandler.fetch(
      sanitizeRequest(request),
      env,
      context
    );
    return secureOperationsResponse(response, request);
  },
  scheduled(
    _controller: ScheduledController,
    env: AccessEnv,
    context: ExecutionContext
  ) {
    context.waitUntil(runSourceLineageMaintenance(env, context));
  },
});

async function runSourceLineageMaintenance(
  env: AccessEnv,
  context: ExecutionContext
): Promise<void> {
  const request = new Request(`${OPS_ORIGIN}/api/source-lineage-maintenance`, {
    body: JSON.stringify({ maxPages: 2 }),
    headers: {
      "content-type": "application/json",
      host: new URL(OPS_ORIGIN).host,
      origin: OPS_ORIGIN,
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin",
    },
    method: "POST",
  });
  const response = await openNextHandler.fetch(request, env, context);
  if (!response.ok) {
    throw new Error(
      `SOURCE_LINEAGE_MAINTENANCE_HTTP_${String(response.status)}`
    );
  }
}

export default WORKER;
