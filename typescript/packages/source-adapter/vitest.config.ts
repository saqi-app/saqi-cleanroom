import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    name: "collection source",
    include: ["src/**/__tests__/**/*.test.ts"],
    fileParallelism: false,
  },
});
