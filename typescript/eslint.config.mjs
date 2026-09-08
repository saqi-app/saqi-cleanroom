// Flat config entrypoint. `eslint.strict.mjs` next to this file is SYNCED --
// `sarj-standards setup` overwrites it, and `setup --dry-run` fails CI if
// you edit it. Put every repo-specific decision HERE instead, in the override
// block below: later entries win, so you can relax a rule, add a framework
// exemption, or scope one to a directory without forking the canonical file.
import { createConfig } from "./eslint.strict.mjs";
import nextPlugin from "@next/eslint-plugin-next";
import tseslint from "typescript-eslint";

export default [
  ...createConfig({
    projectService: {
      allowDefaultProject: [
        "packages/source-adapter/vitest.config.ts",
        "packages/crawler-local/vitest.config.ts",
        "packages/precedent-node/src/services/__tests__/author-store.test.ts",
        "packages/precedent-node/src/services/__tests__/corpus-import-coordinator.test.ts",
        "packages/precedent-node/src/services/__tests__/corpus-revision-store.test.ts",
        "packages/precedent-node/src/services/__tests__/migration-compatibility.test.ts",
        "packages/precedent-node/src/services/__tests__/model-profile-governance.test.ts",
        "packages/precedent-node/src/services/__tests__/poem-store.test.ts",
        "packages/precedent-node/src/services/__tests__/production-resolution-reader.test.ts",
        "packages/precedent-node/src/services/__tests__/task-integrity.test.ts",
        "packages/precedent-node/vitest.config.ts",
      ],
      maximumDefaultProjectFileMatchCount_THIS_WILL_SLOW_DOWN_LINTING: 12,
    },
  }),
  {
    ignores: [
      // These directories contain authored source, not compiled library output.
      "!packages/*/src/lib/",
      "!packages/*/src/lib/**",
      ".yarn/**",
      "packages/app/.open-next/**",
      "packages/app/cloudflare-env.d.ts",
      "packages/site/cloudflare-env.d.ts",
    ],
  },
  // {
  //   files: ["src/routes/**/*.tsx"],
  //   rules: {
  //     "unicorn/filename-case": ["error", {
  //       cases: { kebabCase: true },
  //       ignore: [String.raw`^\[`],
  //     }],
  //   },
  // },
  {
    files: ["packages/app/**/*.{js,jsx,ts,tsx}"],
    plugins: { "@next/next": nextPlugin },
    rules: {
      ...nextPlugin.configs.recommended.rules,
      ...nextPlugin.configs["core-web-vitals"].rules,
      "@next/next/no-html-link-for-pages": "off",
    },
  },
  {
    files: ["**/*.{js,mjs,cjs}"],
    ...tseslint.configs.disableTypeChecked,
  },
  {
    files: ["**/*.{ts,tsx}"],
    rules: {
      "@sarj/prefer-module-level-schema": ["error", { minProperties: 1 }],
    },
  },
  {
    files: ["packages/site/src/pages/**/*.ts"],
    rules: {
      "unicorn/filename-case": [
        "error",
        { cases: { kebabCase: true }, ignore: [String.raw`\[.+\]`] },
      ],
    },
  },
];
