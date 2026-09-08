import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { ArtifactStore } from "../persistence/artifact-store";
import { Ledger } from "../persistence/ledger";
import { canonicalJson, inputHash, sha256 } from "../persistence/work-key";
import {
  PublicationClient,
  type PublicationTransport,
} from "../publication/publication-client";
import {
  prepareEnrichmentPublication,
  PublicationLane,
} from "../publication/publication-lane";
import { trackedMkdtempSync as mkdtempSync } from "./support/tracked-test-root";

const HASH = "a".repeat(64);

function enrichmentArtifact() {
  const output = {
    schemaId: "saqi.poem-enrichment-output",
    schemaVersion: 2,
    translation: { lines: ["line"] },
    wordGlosses: {
      lines: [
        {
          lineIndex: 0,
          segments: [
            {
              kind: "word",
              meaning: "opening hemistich",
              surface: "صدر",
              tokenIndex: 0,
            },
          ],
        },
      ],
      tokenizerVersion: "saqi-orthographic-v1",
    },
  };
  return {
    generationAttemptId: "generation",
    input: {
      authorArabic: "شاعر",
      linesArabic: ["صدر"],
      poemId: "poem-1",
      schemaId: "saqi.poem-enrichment-input",
      schemaVersion: 1,
      sourceContentSha256: sha256(canonicalJson(["صدر"])),
      sourceRevisionId: HASH,
      titleArabic: "قصيدة",
    },
    output,
    outputHash: sha256(canonicalJson(output)),
    pipelineVersion: "sol-word-gloss-v2",
    reviewAttemptIds: ["review-1", "review-2"],
    reviews: [
      { fidelityScore: 100, findings: [], insightScore: 100, verdict: "pass" },
      { fidelityScore: 99, findings: [], insightScore: 99, verdict: "pass" },
    ],
  } as const;
}

function succeededSource(ledger: Ledger, artifactHash: string) {
  const input = { source: "fixture" };
  const seeded = ledger.seed(
    {
      implementationVersion: "fixture-v1",
      input,
      inputHash: inputHash(input),
      kind: "fixture-source",
      priority: 0,
      schemaVersion: "fixture@1",
    },
    0,
  );
  const claim = ledger.claim("fixture", 0, 1_000, ["fixture-source"]);
  if (!claim) throw new Error("fixture source claim missing");
  ledger.succeed(claim, artifactHash, 1);
  return seeded.workKey;
}

describe("publication resilience", () => {
  it("keeps auth failures attempt-neutral and resumes after credentials recover", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-auth-wait-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = enrichmentArtifact();
    const storedArtifact = await artifacts.put(`${canonicalJson(artifact)}\n`);
    const artifactHash = storedArtifact.hash;
    const source = {
      artifactHash,
      workKey: succeededSource(ledger, artifactHash),
    };
    const action = prepareEnrichmentPublication(artifact, {
      expectedPointerVersion: null,
      source,
      taskKey: source.workKey,
      writerEpoch: 1,
    });
    let authenticated = false;
    let now = Date.now() + 1_000;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://ops.saqi.app/api/corpus-import",
        now: () => now,
        random: () => 0,
        transport: async () =>
          authenticated
            ? {
                body: JSON.stringify({
                  ok: true,
                  result: {
                    authorSlug: "author-1",
                    poemId: "poem-1",
                    pointerVersion: 1,
                    state: "published",
                  },
                }),
                status: 200,
              }
            : {
                authFailure: "expired",
                body: "temporary Access login redirect",
                status: 302,
              },
      }),
      ledger,
      owner: "test",
    });
    const work = await lane.seedEnrichment(action, source);

    for (let index = 0; index < 20; index += 1) {
      const summary = await lane.run(undefined, {
        maximum: 10,
        now: () => now,
      });
      expect(summary).toMatchObject({
        authWait: 1,
        claimed: 1,
        deadLettered: 0,
        stopped: "auth_wait",
      });
      expect(ledger.get(work.workKey)).toMatchObject({
        attemptCount: 0,
        lastErrorCode: "PUBLICATION_AUTH_EXPIRED",
        state: "pending",
      });
      now = (summary.earliestWakeAt ?? now) + 1;
    }

    authenticated = true;
    await expect(
      lane.run(undefined, { maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("releases a half-open request without exhausting the work item", async () => {
    const client = new PublicationClient({
      endpoint: "https://ops.saqi.app/api/corpus-import",
      now: () => 1_000,
      random: () => 0,
      requestTimeoutMs: 10,
      transport: ({ signal }) =>
        new Promise((_, reject) => {
          signal?.addEventListener(
            "abort",
            () => reject(new Error("PUBLICATION_TEST_TIMEOUT")),
            { once: true },
          );
        }),
    });
    const action = {
      action: "promote",
      bundleId: "bundle",
      expectedPlanHash: HASH,
      writerEpoch: 1,
    } as const;

    await expect(client.send(action)).resolves.toEqual({
      errorCode: "PUBLICATION_NETWORK_UNAVAILABLE",
      retryAt: 3_500,
      state: "network_wait",
    });
  });

  it("backs off proven network outages exponentially and resets after recovery", async () => {
    let calls = 0;
    const transport: PublicationTransport = async () => {
      calls += 1;
      if (calls <= 2 || calls === 4)
        throw new Error("getaddrinfo ENOTFOUND ops.saqi.app");
      return {
        body: JSON.stringify({ ok: true, result: { accepted: true } }),
        status: 200,
      };
    };
    const client = new PublicationClient({
      endpoint: "https://ops.saqi.app/api/corpus-import",
      now: () => 1_000,
      random: () => 0,
      transport,
    });
    const action = {
      action: "promote",
      bundleId: "bundle",
      expectedPlanHash: HASH,
      writerEpoch: 1,
    } as const;

    await expect(client.send(action)).resolves.toMatchObject({
      retryAt: 3_500,
      state: "network_wait",
    });
    await expect(client.send(action)).resolves.toMatchObject({
      retryAt: 6_000,
      state: "network_wait",
    });
    await expect(client.send(action)).resolves.toMatchObject({
      state: "confirmed",
    });
    await expect(client.send(action)).resolves.toMatchObject({
      retryAt: 3_500,
      state: "network_wait",
    });
  });

  it("does not exhaust work attempts while offline and resumes the same item", async () => {
    const root = mkdtempSync(join(tmpdir(), "publication-network-"));
    const artifacts = new ArtifactStore(join(root, "artifacts"), {
      minimumFreeBytes: 0,
    });
    const ledger = Ledger.initialize(join(root, "ledger.sqlite"));
    const artifact = enrichmentArtifact();
    const storedArtifact = await artifacts.put(`${canonicalJson(artifact)}\n`);
    const artifactHash = storedArtifact.hash;
    const source = {
      artifactHash,
      workKey: succeededSource(ledger, artifactHash),
    };
    const action = prepareEnrichmentPublication(artifact, {
      expectedPointerVersion: null,
      source,
      taskKey: source.workKey,
      writerEpoch: 1,
    });
    let remote: "network" | "online" | "service" = "network";
    let requests = 0;
    let now = Date.now() + 1_000;
    const lane = new PublicationLane({
      artifacts,
      client: new PublicationClient({
        endpoint: "https://ops.saqi.app/api/corpus-import",
        now: () => now,
        random: () => 0,
        transport: async () => {
          requests += 1;
          if (remote === "network")
            throw new Error("connect ENETUNREACH ops.saqi.app");
          if (remote === "service")
            return { body: "upstream unavailable", status: 503 };
          return {
            body: JSON.stringify({
              ok: true,
              result: {
                authorSlug: "author-1",
                poemId: "poem-1",
                pointerVersion: 1,
                state: "published",
              },
            }),
            status: 200,
          };
        },
      }),
      ledger,
      owner: "test",
    });
    const work = await lane.seedEnrichment(action, source);
    const queued = await lane.seedEnrichment(
      prepareEnrichmentPublication(artifact, {
        expectedPointerVersion: null,
        source,
        taskKey: source.workKey,
        writerEpoch: 2,
      }),
      source,
      -1,
    );

    for (let index = 0; index < 20; index += 1) {
      const summary = await lane.run(undefined, {
        maximum: 10,
        now: () => now,
      });
      expect(summary).toMatchObject({
        claimed: 1,
        deadLettered: 0,
        networkWait: 1,
        stopped: "network_wait",
      });
      expect(ledger.get(work.workKey)).toMatchObject({
        attemptCount: 0,
        lastErrorCode: "PUBLICATION_NETWORK_UNAVAILABLE",
        state: "pending",
      });
      const requestsBeforeProbe = requests;
      await expect(
        lane.run(undefined, { maximum: 10, now: () => now }),
      ).resolves.toMatchObject({
        claimed: 0,
        ready: 1,
        retryAt: summary.retryAt,
        stopped: "network_wait",
      });
      expect(requests).toBe(requestsBeforeProbe);
      expect(ledger.get(queued.workKey)?.attemptCount).toBe(0);
      now = (summary.retryAt ?? now) + 1;
    }

    remote = "service";
    const serviceWait = await lane.run(undefined, {
      maximum: 10,
      now: () => now,
    });
    expect(serviceWait).toMatchObject({
      claimed: 1,
      deadLettered: 0,
      serviceWait: 1,
      stopped: "service_wait",
    });
    expect(ledger.get(work.workKey)).toMatchObject({
      attemptCount: 0,
      lastErrorCode: "PUBLICATION_SERVICE_UNAVAILABLE_HTTP_503",
      state: "pending",
    });
    expect(ledger.status(now).affectedByKindAndErrorCode).toContainEqual({
      code: "PUBLICATION_SERVICE_UNAVAILABLE_HTTP_503",
      count: 1,
      kind: "corpus-publication-enrichment",
    });
    const requestsBeforeServiceProbe = requests;
    await expect(
      lane.run(undefined, { maximum: 10, now: () => now }),
    ).resolves.toMatchObject({
      claimed: 0,
      ready: 1,
      retryAt: serviceWait.retryAt,
      stopped: "service_wait",
    });
    expect(requests).toBe(requestsBeforeServiceProbe);
    now = (serviceWait.retryAt ?? now) + 1;

    remote = "online";
    await expect(
      lane.run(undefined, { maximum: 1, now: () => now }),
    ).resolves.toMatchObject({ confirmed: 1 });
    expect(ledger.get(source.workKey)?.state).toBe("imported");
    ledger.close();
  });

  it("honors Cloudflare retry hints without dead-lettering rate limits", async () => {
    let now = 1_000;
    const client = new PublicationClient({
      endpoint: "https://ops.saqi.app/api/corpus-import",
      now: () => now,
      transport: async () => ({ body: "busy", retryAfter: "120", status: 429 }),
    });
    const action = {
      action: "promote",
      bundleId: "bundle",
      expectedPlanHash: HASH,
      writerEpoch: 1,
    } as const;
    await expect(client.send(action)).resolves.toEqual({
      errorCode: "PUBLICATION_RATE_LIMITED",
      retryAt: 121_000,
      state: "retry_wait",
    });
    now = 200_000;
    await expect(client.send(action)).resolves.toMatchObject({
      errorCode: "PUBLICATION_RATE_LIMITED",
      retryAt: 320_000,
    });
  });
});
