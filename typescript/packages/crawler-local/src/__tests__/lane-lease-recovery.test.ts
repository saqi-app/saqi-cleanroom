import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, it } from "vitest";

import { LocalEnrichmentFanout } from "../enrichment/local-enrichment-fanout";
import { SOL_ENRICHMENT_WORK_KIND } from "../enrichment/sol-coordinator";
import { ENRICHMENT_PROVIDER_SPECS } from "../enrichment/sol-runner";
import { ArtifactStore } from "../persistence/artifact-store";
import { Ledger } from "../persistence/ledger";
import { inputHash } from "../persistence/work-key";
import { FanoutReconciler } from "../publication/fanout-reconciler";
import { PublicationClient } from "../publication/publication-client";
import { PublicationLane } from "../publication/publication-lane";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

it.each(["publication", "fanout", "local"] as const)(
  "%s standalone lane recovers expired work by default",
  async (kind) => {
    const root = mkdtempSync(join(tmpdir(), "lane-recovery-"));
    const ledger = Ledger.initialize(join(root, "ledger.sqlite3"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const key = ledger.seed(
      {
        implementationVersion: "test",
        input: {},
        inputHash: inputHash({}),
        kind: "unrelated",
        priority: 0,
        schemaVersion: "test",
      },
      1,
    ).workKey;
    const stale = ledger.claim("crashed", 1, 10);
    const publication = new PublicationLane({
      artifacts,
      ledger,
      client: new PublicationClient({
        endpoint: "https://example.test/api/corpus-import",
        transport: async () => {
          throw new Error("Unexpected network request");
        },
      }),
    });
    try {
      if (kind === "publication")
        await publication.run(undefined, { now: () => 20 });
      if (kind === "fanout") {
        const fanout = new FanoutReconciler({
          enrichment: [
            {
              implementationVersion:
                ENRICHMENT_PROVIDER_SPECS.sol.pipelineVersion,
              modelKey: ENRICHMENT_PROVIDER_SPECS.sol.modelKey,
              seed: () => {
                throw new Error("Unexpected paid work");
              },
              workKind: SOL_ENRICHMENT_WORK_KIND,
            },
          ],
          artifacts,
          ledger,
          publication,
          resolvers: { collected: () => null, enrichment: () => null },
        });
        try {
          await fanout.cycle({ now: () => 20 });
        } finally {
          await fanout.close();
        }
      }
      if (kind === "local")
        await new LocalEnrichmentFanout({
          artifacts,
          ledger,
          profile: {
            modelKey: "sol-5.6",
            pipelineVersion: "test",
            seed: () => {
              throw new Error("Unexpected paid work");
            },
          },
          resolver: { resolve: () => null },
        }).cycle({ now: () => 20 });
      expect(ledger.get(key)).toMatchObject({
        state: "pending",
        lastErrorCode: "LEASE_EXPIRED",
      });
      expect(() => ledger.succeed(stale!, "a".repeat(64), 20)).toThrow();
    } finally {
      ledger.close();
    }
  },
);
