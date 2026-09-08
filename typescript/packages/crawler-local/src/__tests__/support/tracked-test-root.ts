import {
  chmodSync,
  lstatSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";

import { afterAll } from "vitest";

const TRACKED_ROOTS = new Set<string>();

/**
 * Creates a unique test directory and removes only that exact tracked root
 * after the importing test file. Callers retain the native mkdtemp prefix API so
 * existing fixtures cannot accidentally broaden cleanup to a shared folder.
 */
export function trackedMkdtempSync(prefix: string): string {
  if (resolve(dirname(prefix)) !== resolve(tmpdir()))
    throw new Error("Tracked test roots must be direct children of tmpdir");
  const root = mkdtempSync(prefix);
  TRACKED_ROOTS.add(root);
  return root;
}

afterAll(async () => {
  // Some fixtures complete filesystem initialization in a queued promise.
  // Let those callbacks settle before removing their owning test roots.
  await new Promise<void>((resolvePromise) => setImmediate(resolvePromise));
  for (const root of TRACKED_ROOTS) {
    const metadata = lstatSync(root, { throwIfNoEntry: false });
    if (metadata !== undefined) {
      if (!metadata.isDirectory() || metadata.isSymbolicLink())
        throw new Error(`Refusing to clean unsafe tracked test root: ${root}`);
      makeTreeRemovable(root);
      rmSync(root, {
        force: true,
        maxRetries: 10,
        recursive: true,
        retryDelay: 50,
      });
    }
    TRACKED_ROOTS.delete(root);
  }
});

function makeTreeRemovable(path: string): void {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (metadata === undefined || metadata.isSymbolicLink()) return;
  if (!metadata.isDirectory()) return;
  chmodSync(path, 0o700);
  for (const entry of readdirSync(path))
    makeTreeRemovable(resolve(path, entry));
}
