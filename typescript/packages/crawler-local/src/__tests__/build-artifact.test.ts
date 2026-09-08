import { execFileSync } from "node:child_process";
import {
  accessSync,
  chmodSync,
  constants,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

describe("crawler build artifact", () => {
  it("makes the generated shebang CLI executable", () => {
    const root = mkdtempSync(join(tmpdir(), "saqi-build-artifact-"));
    const cli = join(root, "cli.js");
    writeFileSync(cli, "#!/usr/bin/env node\nprocess.exit(0);\n", {
      mode: 0o644,
    });
    chmodSync(cli, 0o644);

    execFileSync(process.execPath, [
      resolve(import.meta.dirname, "../../scripts/finalize-build.mjs"),
      cli,
    ]);

    expect(statSync(cli).mode & 0o111).not.toBe(0);
    expect(() =>
      accessSync(cli, constants.R_OK | constants.X_OK),
    ).not.toThrow();
    expect(() => execFileSync(cli, ["--help"])).not.toThrow();
  });
});
