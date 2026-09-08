import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    name: "Precedent Node",
    include: ["src/**/__tests__/**/*.test.ts"],
    testTimeout: 30000,
    sequence: {
      concurrent: false,
    },
  },
});
