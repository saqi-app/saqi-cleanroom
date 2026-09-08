import { join } from "node:path";

import {
  createCompilerHost,
  createProgram,
  createSourceFile,
  getPreEmitDiagnostics,
  ModuleKind,
  ModuleResolutionKind,
  ScriptTarget,
} from "typescript";
import { expect, test } from "vitest";

test("owner action contract rejects async callbacks at compile time", () => {
  const path = join(import.meta.dirname, "runtime-owner-callback.virtual.ts");
  const source = `
    import type { RuntimeOwnerStore } from '../persistence/runtime-owner-store.js';
    declare const store: RuntimeOwnerStore;
    store.withCurrentOwner(1, 'fixture', () => undefined);
    store.withCurrentOwner(1, 'fixture', async () => undefined);
  `;
  const options = {
    strict: true,
    skipLibCheck: true,
    noEmit: true,
    target: ScriptTarget.ESNext,
    module: ModuleKind.NodeNext,
    moduleResolution: ModuleResolutionKind.NodeNext,
  };
  const host = createCompilerHost(options);
  const readSource = host.getSourceFile.bind(host);
  host.getSourceFile = (filename, languageVersion, onError, fresh) =>
    filename === path
      ? createSourceFile(filename, source, languageVersion)
      : readSource(filename, languageVersion, onError, fresh);
  const diagnostics = getPreEmitDiagnostics(
    createProgram([path], options, host),
  );
  expect(
    diagnostics.map((diagnostic) => ({
      code: diagnostic.code,
      path: diagnostic.file?.fileName,
    })),
  ).toEqual([{ code: 2322, path }]);
});
