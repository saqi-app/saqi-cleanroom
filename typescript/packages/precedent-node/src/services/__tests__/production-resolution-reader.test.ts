import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
  PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
  PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
  PRODUCTION_RESOLUTION_SCHEMA_VERSION,
} from "@saqi/precedent-iso";
import { DEFAULT_SOURCE_ADAPTER_PROFILE } from "@saqi/source-adapter";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  D1ProductionResolutionStore,
  ProductionResolutionConflictError,
} from "../production-resolution-reader";

const MIGRATIONS_DIRECTORY = fileURLToPath(
  new URL("../../../../app/migrations/", import.meta.url),
);
const MIGRATION_FILES = readdirSync(MIGRATIONS_DIRECTORY)
  .filter((fileName) => fileName.endsWith(".sql"))
  .toSorted();
const AUTHOR_ID = "11111111-1111-4111-8111-111111111111";
const POEM_ID = "22222222-2222-4222-8222-222222222222";
const GENERATED_AUTHOR_ID = "a".repeat(64);
const GENERATED_POEM_ID = "b".repeat(64);
const LINE_NFC_HASH = "1".repeat(64);
const PROMPT_MATERIAL_HASH = "2".repeat(64);
const SOURCE_NAME = "primary-source";

describe("D1ProductionResolutionStore", () => {
  let database: InstanceType<typeof Database>;
  let reader: D1ProductionResolutionStore;

  beforeEach(() => {
    database = new Database(":memory:");
    database.pragma("foreign_keys = ON");
    for (const fileName of MIGRATION_FILES) {
      database.exec(
        readFileSync(`${MIGRATIONS_DIRECTORY}/${fileName}`, "utf8"),
      );
    }
    database
      .prepare(
        "INSERT INTO author (id, slug, name_arabic, status) VALUES (?, ?, ?, ?)",
      )
      .run(AUTHOR_ID, "poet-test", "شاعر", "init");
    database
      .prepare(
        "INSERT INTO poem (id, author_id, slug, verses, name_arabic, content_arabic) VALUES (?, ?, ?, ?, ?, ?)",
      )
      .run(POEM_ID, AUTHOR_ID, "work-82737", 1, "قصيدة", '{"content":["بيت"]}');
    const db = drizzle(database);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument -- better-sqlite3 and D1 expose compatible Drizzle query APIs for store tests.
    reader = new D1ProductionResolutionStore(db as any, {
      now: () => Date.parse("2026-08-28T12:00:00.000Z"),
      sourceName: SOURCE_NAME,
      sourceProfile: DEFAULT_SOURCE_ADAPTER_PROFILE,
    });
  });

  afterEach(() => database.close());

  it("resolves an unmapped legacy poem to its existing production UUID", async () => {
    const result = await reader.resolve(request());

    expect(result).toMatchObject({
      expiresAt: "2026-08-28T12:15:00.000Z",
      observedAt: "2026-08-28T12:00:00.000Z",
      targets: [
        {
          authorId: AUTHOR_ID,
          currentSourceRevisionId: null,
          modelPointers: [],
          poemId: POEM_ID,
          sourceAuthorSlug: "poet-test",
          sourcePoemId: "82737",
          sourcePointerVersion: null,
        },
      ],
      writerEpoch: 1,
    });
    expect(result.scopeHash).toMatch(/^[a-f\d]{64}$/);
    expect(result.manifestHash).toMatch(/^[a-f\d]{64}$/);
  });

  it("rejects author mismatch and authorless legacy adoption", async () => {
    await expect(
      reader.resolve(request({ sourceAuthorSlug: "poet-other" })),
    ).rejects.toBeInstanceOf(ProductionResolutionConflictError);

    database
      .prepare("UPDATE poem SET author_id = NULL WHERE id = ?")
      .run(POEM_ID);
    await expect(reader.resolve(request())).rejects.toThrow(
      "PRODUCTION_RESOLUTION_TARGET_UNRESOLVED",
    );
  });

  it("rejects partial batches instead of returning a partial scope", async () => {
    const input = request();
    await expect(
      reader.resolve({
        ...input,
        targets: [
          ...input.targets,
          {
            modelKeys: ["sol-5.6"],
            sourceAuthorSlug: "poet-test",
            sourcePoemId: "99999",
          },
        ],
      }),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_TARGET_UNRESOLVED");
  });

  it("returns exact active lineage after adoption and rejects tombstones", async () => {
    adoptPoem(database);

    await expect(reader.resolve(request())).resolves.toMatchObject({
      targets: [
        {
          currentSourceNfcSha256: expect.stringMatching(/^[a-f\d]{64}$/),
          currentSourceRevisionId: "e".repeat(64),
          poemId: POEM_ID,
          sourcePointerVersion: 1,
        },
      ],
    });

    database.prepare("UPDATE source_poem_identity SET tombstoned_at = 1").run();
    await expect(reader.resolve(request())).rejects.toThrow(
      "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
    );
  });

  it("resolves a newly adopted poem with deterministic SHA-256 IDs", async () => {
    database.prepare("DELETE FROM poem").run();
    database.prepare("DELETE FROM author").run();
    database
      .prepare(
        "INSERT INTO author (id, slug, name_arabic, status) VALUES (?, ?, ?, ?)",
      )
      .run(GENERATED_AUTHOR_ID, "poet-test", "شاعر", "init");
    database
      .prepare(
        "INSERT INTO poem (id, author_id, slug, verses, name_arabic, content_arabic) VALUES (?, ?, ?, ?, ?, ?)",
      )
      .run(
        GENERATED_POEM_ID,
        GENERATED_AUTHOR_ID,
        "work-82737",
        1,
        "قصيدة",
        '{"content":["بيت"]}',
      );
    adoptPoem(database, {
      authorId: GENERATED_AUTHOR_ID,
      poemId: GENERATED_POEM_ID,
    });

    await expect(reader.resolve(request())).resolves.toMatchObject({
      targets: [
        {
          authorId: GENERATED_AUTHOR_ID,
          poemId: GENERATED_POEM_ID,
        },
      ],
    });
  });

  it("resolves canonical poem and revision identity without source IDs", async () => {
    adoptPoem(database);

    await expect(reader.resolve(canonicalRequest())).resolves.toMatchObject({
      targets: [
        {
          currentSourceRevisionId: "e".repeat(64),
          poemId: POEM_ID,
          sourceAuthorSlug: "poet-test",
          sourcePoemId: "82737",
          sourcePointerVersion: 1,
        },
      ],
    });
  });

  it("returns current lineage for a stale canonical revision and rejects tombstones", async () => {
    adoptPoem(database);
    await expect(
      reader.resolve(canonicalRequest({ sourceRevisionId: "f".repeat(64) })),
    ).resolves.toMatchObject({
      targets: [
        {
          currentSourceNfcSha256: expect.stringMatching(/^[a-f\d]{64}$/),
          currentSourceRevisionId: "e".repeat(64),
          poemId: POEM_ID,
        },
      ],
    });

    database.prepare("UPDATE source_poem_identity SET tombstoned_at = 1").run();
    await expect(reader.resolve(canonicalRequest())).rejects.toThrow(
      "PRODUCTION_RESOLUTION_SOURCE_LINEAGE_CONFLICT",
    );
  });

  it("binds canonical lookup through the requested revision when a poem has multiple identities", async () => {
    adoptPoem(database);
    database
      .prepare(
        `INSERT INTO source_poem_identity VALUES (
          'source-poem-other', 'primary-source', '99999', 'source-author',
          'https://source.invalid/works/99999', ?, 1, 1, NULL
        )`,
      )
      .run(POEM_ID);
    await expect(reader.resolve(canonicalRequest())).resolves.toMatchObject({
      targets: [{ sourcePoemId: "82737" }],
    });
  });

  it("resolves an exact two-hash fingerprint to one authoritative active lineage", async () => {
    adoptPoem(database);

    const result = await reader.resolve(fingerprintRequest());

    expect(result).toMatchObject({
      schemaVersion: PRODUCTION_RESOLUTION_RESPONSE_V2_SCHEMA_VERSION,
      targets: [
        {
          activeSourceFingerprint: {
            algorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
            lineNfcHash: LINE_NFC_HASH,
            promptMaterialHash: PROMPT_MATERIAL_HASH,
          },
          authorId: AUTHOR_ID,
          currentSourceNfcSha256: LINE_NFC_HASH,
          currentSourceRevisionId: "e".repeat(64),
          poemId: POEM_ID,
          sourceAuthorSlug: "poet-test",
          sourcePoemId: "82737",
          sourcePointerVersion: 1,
        },
      ],
      writerEpoch: 1,
    });
  });

  it("rejects unknown and inactive fingerprints terminally", async () => {
    adoptPoem(database);
    await expect(
      reader.resolve(
        fingerprintRequest({ promptMaterialHash: "3".repeat(64) }),
      ),
    ).rejects.toThrow("PRODUCTION_RESOLUTION_FINGERPRINT_UNRESOLVED");

    database
      .prepare("UPDATE poem SET active_source_revision_id = NULL WHERE id = ?")
      .run(POEM_ID);
    await expect(reader.resolve(fingerprintRequest())).rejects.toThrow(
      "PRODUCTION_RESOLUTION_FINGERPRINT_NOT_ACTIVE",
    );
  });

  it("rejects exact-pair collisions instead of choosing a lineage", async () => {
    adoptPoem(database);
    database
      .prepare(
        `INSERT INTO crawl_import_bundle (
           id, schema_version, manifest_hash, expected_record_count,
           status, writer_epoch, created_at
         ) VALUES ('bundle-collision', 2, ?, 1, 'open', 1, 2)`,
      )
      .run("4".repeat(64));
    database
      .prepare(
        `INSERT INTO crawl_import_record (
           bundle_id, ordinal, record_hash, source_name, source_author_id,
           source_author_url, author_name_arabic, canonical_author_id,
           source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
           content_arabic, content_hash, observed_at
         ) VALUES (
           'bundle-collision', 0, ?, 'primary-source', 'poet-test',
           'https://source.invalid/writers/poet-test', 'شاعر', ?,
           '82737', 'https://source.invalid/works/82737', ?, 'قصيدة قديمة',
           '{"content":["بيت"],"titleArabic":"قصيدة قديمة"}', ?, 2
         )`,
      )
      .run("5".repeat(64), AUTHOR_ID, POEM_ID, "3".repeat(64));
    database
      .prepare(
        `UPDATE crawl_import_bundle
         SET root_hash = ?, status = 'sealed', sealed_at = 2
         WHERE id = 'bundle-collision'`,
      )
      .run("6".repeat(64));
    database
      .prepare(
        `INSERT INTO poem_source_revision VALUES (
           ?, 'source-poem', 2, ?, 'قصيدة قديمة',
           '{"content":["بيت"],"titleArabic":"قصيدة قديمة"}',
           2, 1, 'bundle-collision', 0
         )`,
      )
      .run("f".repeat(64), "3".repeat(64));
    database
      .prepare(
        `INSERT INTO source_revision_fingerprint (
           source_revision_id, line_nfc_hash, prompt_material_hash,
           algorithm, created_at
         ) VALUES (?, ?, ?, ?, 1)`,
      )
      .run(
        "f".repeat(64),
        LINE_NFC_HASH,
        PROMPT_MATERIAL_HASH,
        PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
      );

    await expect(reader.resolve(fingerprintRequest())).rejects.toThrow(
      "PRODUCTION_RESOLUTION_FINGERPRINT_AMBIGUOUS",
    );
  });
});

function adoptPoem(
  database: InstanceType<typeof Database>,
  ids: { authorId: string; poemId: string } = {
    authorId: AUTHOR_ID,
    poemId: POEM_ID,
  },
) {
  database
    .prepare(
      `INSERT INTO crawl_import_bundle (
         id, schema_version, manifest_hash, expected_record_count,
         status, writer_epoch, created_at
       ) VALUES ('bundle', 2, ?, 1, 'open', 1, 1)`,
    )
    .run("a".repeat(64));
  database
    .prepare(
      `INSERT INTO crawl_import_record (
         bundle_id, ordinal, record_hash, source_name, source_author_id,
         source_author_url, author_name_arabic, canonical_author_id,
         source_poem_id, source_poem_url, canonical_poem_id, title_arabic,
         content_arabic, content_hash, observed_at
       ) VALUES (
         'bundle', 0, ?, 'primary-source', 'poet-test',
         'https://source.invalid/writers/poet-test', 'شاعر', ?,
         '82737', 'https://source.invalid/works/82737', ?, 'قصيدة',
         '{"content":["بيت"],"titleArabic":"قصيدة"}', ?, 1
       )`,
    )
    .run("c".repeat(64), ids.authorId, ids.poemId, "d".repeat(64));
  database
    .prepare(
      "UPDATE crawl_import_bundle SET root_hash = ?, status = 'sealed', sealed_at = 1 WHERE id = 'bundle'",
    )
    .run("b".repeat(64));
  database
    .prepare(
      `INSERT INTO source_author_identity VALUES (
         'source-author', 'primary-source', 'poet-test',
         'https://source.invalid/writers/poet-test', 'شاعر', ?, 1, 1
       )`,
    )
    .run(ids.authorId);
  database
    .prepare(
      `INSERT INTO source_poem_identity VALUES (
         'source-poem', 'primary-source', '82737', 'source-author',
         'https://source.invalid/works/82737', ?, 1, 1, NULL
       )`,
    )
    .run(ids.poemId);
  database
    .prepare(
      `INSERT INTO poem_source_revision VALUES (
         ?, 'source-poem', 2, ?, 'قصيدة',
         '{"content":["بيت"],"titleArabic":"قصيدة"}', 1, 1, 'bundle', 0
       )`,
    )
    .run("e".repeat(64), "d".repeat(64));
  database
    .prepare(
      "INSERT INTO poem_source_pointer VALUES ('source-poem', ?, 1, 1, 1)",
    )
    .run("e".repeat(64));
  database
    .prepare("UPDATE poem SET active_source_revision_id = ? WHERE id = ?")
    .run("e".repeat(64), ids.poemId);
  database
    .prepare(
      `INSERT INTO source_revision_fingerprint (
         source_revision_id, line_nfc_hash, prompt_material_hash,
         algorithm, created_at
       ) VALUES (?, ?, ?, ?, 1)`,
    )
    .run(
      "e".repeat(64),
      LINE_NFC_HASH,
      PROMPT_MATERIAL_HASH,
      PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
    );
}

function request(
  change: Partial<{
    modelKeys: "sol-5.6"[];
    sourceAuthorSlug: string;
    sourcePoemId: string;
  }> = {},
) {
  return {
    schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
    schemaVersion: PRODUCTION_RESOLUTION_SCHEMA_VERSION,
    targets: [
      {
        modelKeys: ["sol-5.6" as const],
        sourceAuthorSlug: "poet-test",
        sourcePoemId: "82737",
        ...change,
      },
    ],
  };
}

function canonicalRequest(
  change: Partial<{ poemId: string; sourceRevisionId: string }> = {},
) {
  return {
    schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
    schemaVersion: 2 as const,
    targets: [
      {
        modelKeys: ["sol-5.6" as const],
        poemId: POEM_ID,
        sourceRevisionId: "e".repeat(64),
        ...change,
      },
    ],
  };
}

function fingerprintRequest(
  change: Partial<{
    lineNfcHash: string;
    promptMaterialHash: string;
  }> = {},
) {
  return {
    schemaId: PRODUCTION_RESOLUTION_REQUEST_SCHEMA_ID,
    schemaVersion: PRODUCTION_RESOLUTION_REQUEST_V3_SCHEMA_VERSION,
    targets: [
      {
        fingerprintAlgorithm: PRODUCTION_RESOLUTION_FINGERPRINT_ALGORITHM,
        lineNfcHash: LINE_NFC_HASH,
        modelKeys: ["sol-5.6" as const],
        promptMaterialHash: PROMPT_MATERIAL_HASH,
        ...change,
      },
    ],
  };
}
