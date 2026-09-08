import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    name: "Crawler Local",
    include: ["src/**/__tests__/**/*.test.ts"],
    fileParallelism: true,
    maxWorkers: 2,
    sequence: { concurrent: false },
  },
});
