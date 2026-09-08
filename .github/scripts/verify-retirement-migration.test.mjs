import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const script = fileURLToPath(
  new URL("./verify-retirement-migration.mjs", import.meta.url),
);
for (const [name, input, passes] of [
  ["applied", [{ success: true, results: [{ applied: 1 }] }], true],
  ["pending", [{ success: true, results: [{ applied: 0 }] }], false],
  ["query failed", [{ success: false, results: [{ applied: 1 }] }], false],
  ["missing result", [], false],
  [
    "duplicate result",
    [{ success: true, results: [{ applied: 1 }, { applied: 1 }] }],
    false,
  ],
]) {
  test(name, () => {
    const result = spawnSync(process.execPath, [script], {
      input: JSON.stringify(input),
      encoding: "utf8",
    });
    assert.equal(result.status === 0, passes, result.stderr);
  });
}
