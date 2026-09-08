const chunks = [];
process.stdin.setEncoding("utf8");
for await (const chunk of process.stdin) chunks.push(chunk);
const executions = JSON.parse(chunks.join(""));
const rows = Array.isArray(executions)
  ? executions.flatMap((execution) => execution.results ?? [])
  : [];
const [row] = rows;
if (
  rows.length !== 1 ||
  row?.algorithm !== "sha256-canonical-nfc-v1" ||
  !/^[a-f\d]{64}$/.test(row.lineNfcHash ?? "") ||
  !/^[a-f\d]{64}$/.test(row.promptMaterialHash ?? "")
) {
  throw new Error("PRODUCTION_RESOLUTION_FINGERPRINT_CANARY_UNAVAILABLE");
}
process.stdout.write(
  JSON.stringify({
    schemaId: "saqi.production-resolution-request",
    schemaVersion: 3,
    targets: [
      {
        fingerprintAlgorithm: row.algorithm,
        lineNfcHash: row.lineNfcHash,
        modelKeys: ["sol-5.6"],
        promptMaterialHash: row.promptMaterialHash,
      },
    ],
  }),
);
