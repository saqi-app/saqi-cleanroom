import cloudflare from "@astrojs/cloudflare";
import { defineConfig } from "astro/config";

export default defineConfig({
  adapter: cloudflare({
    configPath: process.env.SAQI_WRANGLER_CONFIG ?? "wrangler.jsonc",
    imageService: "passthrough",
  }),
  build: {
    inlineStylesheets: "never",
  },
  output: "server",
  session: false,
  site: "https://saqi.app",
  trailingSlash: "never",
  vite: {
    build: {
      assetsInlineLimit: 0,
    },
  },
});
