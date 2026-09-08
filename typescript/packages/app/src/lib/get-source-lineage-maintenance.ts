import { getServices } from "@/backend/get-services";

import { getCloudflareEnv } from "./cloudflare";
import {
  D1SourceLineageMaintenanceRepository,
  SourceLineageMaintenanceJob,
} from "./source-lineage-maintenance";

export function getSourceLineageMaintenance(): SourceLineageMaintenanceJob {
  const { DB, SAQI_SOURCE_ADAPTER_CONFIG, SAQI_SOURCE_NAME } =
    getCloudflareEnv();
  return new SourceLineageMaintenanceJob({
    adopter: getServices().corpusRevision,
    repository: new D1SourceLineageMaintenanceRepository(DB, {
      sourceName: SAQI_SOURCE_NAME,
      sourceProfile: SAQI_SOURCE_ADAPTER_CONFIG,
    }),
  });
}
