import {
  CorpusImportCoordinator,
  D1AuthorStore,
  D1CorpusRevisionStore,
  D1PoemStore,
  D1ProductionResolutionStore,
} from "@saqi/precedent-node";
import { drizzle } from "drizzle-orm/d1";

import { getCloudflareEnv } from "../lib/cloudflare";

export function getServices() {
  const {
    DB,
    SAQI_SOURCE_ADAPTER_CONFIG,
    SAQI_SOURCE_BASE_URL,
    SAQI_SOURCE_NAME,
  } = getCloudflareEnv();
  const db = drizzle(DB);

  const authorStore = new D1AuthorStore(db);
  const poemStore = new D1PoemStore(db);
  const productionResolution = new D1ProductionResolutionStore(db, {
    sourceName: SAQI_SOURCE_NAME,
    sourceProfile: SAQI_SOURCE_ADAPTER_CONFIG,
  });
  const corpusRevision = new D1CorpusRevisionStore(db, {
    sourceBaseUrl: SAQI_SOURCE_BASE_URL,
    sourceName: SAQI_SOURCE_NAME,
    sourceProfile: SAQI_SOURCE_ADAPTER_CONFIG,
  });
  const corpusImport = new CorpusImportCoordinator(corpusRevision);
  return {
    authorStore,
    corpusImport,
    corpusRevision,
    poemStore,
    productionResolution,
  };
}
