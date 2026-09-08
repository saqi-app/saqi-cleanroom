const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const executions = JSON.parse(Buffer.concat(chunks).toString("utf8"));
if (
  !Array.isArray(executions) ||
  executions.length !== 1 ||
  executions[0]?.success !== true
) {
  throw new Error("RETIREMENT_MIGRATION_QUERY_FAILED");
}
const rows = executions[0].results;
if (!Array.isArray(rows) || rows.length !== 1 || rows[0]?.applied !== 1) {
  throw new Error(
    "RETIREMENT_MIGRATION_MISSING: apply the preserved production 0037 migration before using this workflow. See typescript/packages/app/migrations/REBASE_COMPATIBILITY.md.",
  );
}
