declare module "*.open-next/worker.js" {
  import type { ExecutionContext } from "@cloudflare/workers-types";

  interface OpenNextHandler {
    fetch(
      request: Request,
      env: object,
      context: ExecutionContext
    ): Promise<Response>;
  }

  const handler: OpenNextHandler;
  export default handler;
}
