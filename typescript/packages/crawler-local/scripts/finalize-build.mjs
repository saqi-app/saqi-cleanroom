#!/usr/bin/env node

import { constants } from "node:fs";
import { access, chmod, readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";

const cliPath = resolve(process.argv[2] ?? "dist/cli.js");
const cliSource = await readFile(cliPath, "utf8");
const firstLine = cliSource.split("\n", 1)[0];
if (firstLine !== "#!/usr/bin/env node") {
  throw new Error(`Built crawler CLI has an invalid shebang: ${cliPath}`);
}

await chmod(cliPath, 0o755);
await access(cliPath, constants.R_OK | constants.X_OK);
const cliStat = await stat(cliPath);
if ((cliStat.mode & 0o111) === 0) {
  throw new Error(`Built crawler CLI is not executable: ${cliPath}`);
}
