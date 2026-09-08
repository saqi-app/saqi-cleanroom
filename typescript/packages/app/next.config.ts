import { resolve } from "node:path";

import { initOpenNextCloudflareForDev } from "@opennextjs/cloudflare";
import type { NextConfig } from "next";

if (process.env.NODE_ENV === "development") {
  void initOpenNextCloudflareForDev();
}

const NEXT_CONFIG: NextConfig = {
  experimental: {
    serverActions: { bodySizeLimit: "64kb" },
  },
  outputFileTracingRoot: resolve(import.meta.dirname, "../.."),
  poweredByHeader: false,
  reactStrictMode: true,
};

export default NEXT_CONFIG;
