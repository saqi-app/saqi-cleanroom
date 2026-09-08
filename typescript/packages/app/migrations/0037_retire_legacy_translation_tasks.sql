-- Fresh-install schema baseline after the verified production 0037 cutover.

-- Keep this filename: existing databases with the real Wrangler receipt skip it.

-- Only empty databases may execute this SQL. Existing pre-0037 databases must

-- upgrade with the preserved pre-cutover release first; never fabricate receipts.

-- The first CREATE TABLE intentionally has no IF NOT EXISTS: fail before changes

-- on an unsupported existing catalog. Wrangler owns transaction/receipt handling.



CREATE TABLE author ( -- sarj-noqa: SARJ102 — Deliberate first-statement guard: reject an older catalog before changing its schema.
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  name_arabic TEXT NOT NULL CHECK (length(name_arabic) <= 10000),
  name TEXT,
  status TEXT NOT NULL DEFAULT 'init',
  poem_count INTEGER NOT NULL DEFAULT 0 CHECK (poem_count >= 0),
  gemini_translation_count INTEGER NOT NULL DEFAULT 0
    CHECK (gemini_translation_count >= 0),
  hidden INTEGER NOT NULL DEFAULT 0 CHECK (hidden IN (0, 1)),
  public_poem_count INTEGER NOT NULL DEFAULT 0 CHECK (public_poem_count >= 0)
, sort_name_arabic TEXT NOT NULL DEFAULT '');

CREATE TABLE IF NOT EXISTS poem (
  id TEXT PRIMARY KEY,
  author_id TEXT REFERENCES author(id) ON DELETE SET NULL,
  slug TEXT NOT NULL UNIQUE CHECK (length(slug) <= 1000),
  verses INTEGER NOT NULL CHECK (verses > 0),
  name_arabic TEXT NOT NULL CHECK (length(name_arabic) <= 10000),
  name_english TEXT CHECK (length(name_english) <= 10000),
  content_arabic TEXT NOT NULL,
  translation TEXT,
  translation_gemini TEXT,
  insights TEXT,
  english_name_original_translation TEXT,
  poem_title_first_line TEXT,
  hidden INTEGER NOT NULL DEFAULT 0 CHECK (hidden IN (0, 1)),
  publishable INTEGER NOT NULL DEFAULT 0 CHECK (publishable IN (0, 1))
, sort_name_arabic TEXT NOT NULL DEFAULT '', has_english INTEGER NOT NULL DEFAULT 0 CHECK (has_english IN (0, 1)), has_insights INTEGER NOT NULL DEFAULT 0 CHECK (has_insights IN (0, 1)), sitemap_shard INTEGER NOT NULL DEFAULT 0 -- sarj-noqa: SARJ102 — SQLite does not implement ADD COLUMN IF NOT EXISTS; Wrangler records the migration atomically.
  CHECK (sitemap_shard BETWEEN 0 AND 15), active_source_revision_id TEXT -- sarj-noqa: SARJ102 — SQLite lacks ADD COLUMN IF NOT EXISTS; Wrangler records the migration atomically.
  REFERENCES poem_source_revision(id), active_enrichment_artifact_id TEXT -- sarj-noqa: SARJ102 — SQLite lacks ADD COLUMN IF NOT EXISTS; Wrangler records the migration atomically.
  REFERENCES enrichment_artifact(id), translation_sol TEXT, insights_sol TEXT);

CREATE TABLE IF NOT EXISTS task (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  poem_id TEXT REFERENCES poem(id),
  author_id TEXT REFERENCES author(id),
  result TEXT,
  error_message TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER,
  work_key TEXT,
  queue_state TEXT NOT NULL DEFAULT 'pending',
  queue_attempt_count INTEGER NOT NULL DEFAULT 0
    CHECK (queue_attempt_count >= 0),
  queue_last_attempt_at INTEGER,
  claim_token TEXT,
  lease_expires_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)
);

CREATE TABLE IF NOT EXISTS scraper_writer_control (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  writer_id TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crawl_import_bundle (
  id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  manifest_hash TEXT NOT NULL UNIQUE CHECK (length(manifest_hash) = 64),
  root_hash TEXT CHECK (root_hash IS NULL OR length(root_hash) = 64),
  plan_hash TEXT CHECK (plan_hash IS NULL OR length(plan_hash) = 64),
  promotion_plan TEXT CHECK (
    promotion_plan IS NULL OR (
      json_valid(promotion_plan)
      AND length(CAST(promotion_plan AS BLOB)) <= 2097151
    )
  ),
  expected_record_count INTEGER NOT NULL CHECK (expected_record_count >= 0),
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'sealed', 'promoted')),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  created_at INTEGER NOT NULL,
  sealed_at INTEGER,
  promoted_at INTEGER,
  CHECK (
    (status = 'open' AND root_hash IS NULL AND sealed_at IS NULL)
    OR (status IN ('sealed', 'promoted') AND root_hash IS NOT NULL AND sealed_at IS NOT NULL)
  ),
  CHECK ((plan_hash IS NULL) = (promotion_plan IS NULL))
);

CREATE TABLE IF NOT EXISTS crawl_import_record (
  bundle_id TEXT NOT NULL REFERENCES crawl_import_bundle(id),
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  record_hash TEXT NOT NULL CHECK (length(record_hash) = 64),
  source_name TEXT NOT NULL,
  source_author_id TEXT NOT NULL,
  source_author_url TEXT NOT NULL,
  author_name_arabic TEXT NOT NULL,
  canonical_author_id TEXT NOT NULL REFERENCES author(id),
  source_poem_id TEXT NOT NULL,
  source_poem_url TEXT NOT NULL,
  canonical_poem_id TEXT REFERENCES poem(id),
  title_arabic TEXT NOT NULL,
  content_arabic TEXT NOT NULL CHECK (
    json_valid(content_arabic)
    AND length(CAST(content_arabic AS BLOB)) <= 2097151
  ),
  content_hash TEXT NOT NULL CHECK (length(content_hash) = 64),
  observed_at INTEGER NOT NULL,
  PRIMARY KEY (bundle_id, ordinal),
  UNIQUE (bundle_id, record_hash),
  UNIQUE (bundle_id, source_name, source_poem_id)
);

CREATE TABLE IF NOT EXISTS source_author_identity (
  id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  name_arabic TEXT NOT NULL,
  canonical_author_id TEXT NOT NULL REFERENCES author(id),
  first_observed_at INTEGER NOT NULL,
  last_observed_at INTEGER NOT NULL,
  UNIQUE (source_name, external_id),
  UNIQUE (source_name, canonical_url)
);

CREATE TABLE IF NOT EXISTS source_author_alias (
  source_author_id TEXT NOT NULL REFERENCES source_author_identity(id),
  alias_url TEXT NOT NULL,
  first_observed_at INTEGER NOT NULL,
  last_observed_at INTEGER NOT NULL,
  PRIMARY KEY (source_author_id, alias_url)
);

CREATE TABLE IF NOT EXISTS source_poem_identity (
  id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL,
  source_author_id TEXT NOT NULL REFERENCES source_author_identity(id),
  canonical_url TEXT NOT NULL,
  canonical_poem_id TEXT REFERENCES poem(id),
  first_observed_at INTEGER NOT NULL,
  last_observed_at INTEGER NOT NULL,
  tombstoned_at INTEGER,
  UNIQUE (source_name, external_id),
  UNIQUE (source_name, canonical_url)
);

CREATE TABLE IF NOT EXISTS poem_source_revision (
  id TEXT PRIMARY KEY,
  source_poem_id TEXT NOT NULL REFERENCES source_poem_identity(id),
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  content_hash TEXT NOT NULL CHECK (length(content_hash) = 64),
  title_arabic TEXT NOT NULL,
  content_arabic TEXT NOT NULL CHECK (
    json_valid(content_arabic)
    AND length(CAST(content_arabic AS BLOB)) <= 2097151
  ),
  observed_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  import_bundle_id TEXT NOT NULL REFERENCES crawl_import_bundle(id),
  import_ordinal INTEGER NOT NULL,
  UNIQUE (source_poem_id, schema_version, content_hash),
  FOREIGN KEY (import_bundle_id, import_ordinal)
    REFERENCES crawl_import_record(bundle_id, ordinal)
);

CREATE TABLE IF NOT EXISTS poem_source_pointer (
  source_poem_id TEXT PRIMARY KEY REFERENCES source_poem_identity(id),
  revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  pointer_version INTEGER NOT NULL CHECK (pointer_version >= 1),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS enrichment_artifact (
  id TEXT PRIMARY KEY,
  source_revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  task_key TEXT NOT NULL,
  variant INTEGER NOT NULL CHECK (variant >= 0),
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  reasoning_effort TEXT NOT NULL,
  payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
  payload TEXT NOT NULL CHECK (
    json_valid(payload) AND length(CAST(payload AS BLOB)) <= 2097151
  ),
  created_at INTEGER NOT NULL,
  UNIQUE (task_key, variant),
  UNIQUE (source_revision_id, payload_hash)
);

CREATE TABLE IF NOT EXISTS enrichment_validation (
  id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL REFERENCES enrichment_artifact(id),
  validator_key TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  attempt INTEGER NOT NULL CHECK (attempt >= 0),
  outcome TEXT NOT NULL CHECK (outcome IN ('pass', 'fail')),
  highest_severity TEXT NOT NULL
    CHECK (highest_severity IN ('none', 'minor', 'major', 'critical')),
  report_hash TEXT NOT NULL CHECK (length(report_hash) = 64),
  report TEXT NOT NULL CHECK (
    json_valid(report) AND length(CAST(report AS BLOB)) <= 2097151
  ),
  created_at INTEGER NOT NULL,
  UNIQUE (artifact_id, validator_key, validator_version, attempt)
);

CREATE TABLE IF NOT EXISTS poem_publication_pointer (
  poem_id TEXT PRIMARY KEY REFERENCES poem(id),
  source_revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  enrichment_artifact_id TEXT REFERENCES enrichment_artifact(id),
  pointer_version INTEGER NOT NULL CHECK (pointer_version >= 1),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crawl_import_receipt (
  bundle_id TEXT PRIMARY KEY REFERENCES crawl_import_bundle(id),
  plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 64),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  inserted_revisions INTEGER NOT NULL CHECK (inserted_revisions >= 0),
  reused_revisions INTEGER NOT NULL CHECK (reused_revisions >= 0),
  advanced_pointers INTEGER NOT NULL CHECK (advanced_pointers >= 0),
  unchanged_pointers INTEGER NOT NULL CHECK (unchanged_pointers >= 0),
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS model_enrichment_artifact (
  id TEXT PRIMARY KEY,
  source_revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  task_key TEXT NOT NULL,
  variant INTEGER NOT NULL CHECK (variant >= 0),
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  model_key TEXT NOT NULL,
  reasoning_effort TEXT NOT NULL,
  payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
  payload TEXT NOT NULL CHECK (
    json_valid(payload) AND length(CAST(payload AS BLOB)) <= 2097151
  ),
  created_at INTEGER NOT NULL,
  UNIQUE (task_key, variant, model_key),
  UNIQUE (source_revision_id, payload_hash, model_key)
);

CREATE TABLE IF NOT EXISTS model_enrichment_validation (
  id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL REFERENCES model_enrichment_artifact(id),
  validator_key TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  attempt INTEGER NOT NULL CHECK (attempt >= 0),
  outcome TEXT NOT NULL CHECK (outcome IN ('pass', 'fail')),
  highest_severity TEXT NOT NULL
    CHECK (highest_severity IN ('none', 'minor', 'major', 'critical')),
  report_hash TEXT NOT NULL CHECK (length(report_hash) = 64),
  report TEXT NOT NULL CHECK (
    json_valid(report) AND length(CAST(report AS BLOB)) <= 2097151
  ),
  created_at INTEGER NOT NULL,
  UNIQUE (artifact_id, validator_key, validator_version, attempt)
);

CREATE TABLE IF NOT EXISTS poem_model_publication_pointer (
  poem_id TEXT NOT NULL REFERENCES poem(id),
  model_key TEXT NOT NULL,
  source_revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  enrichment_artifact_id TEXT NOT NULL REFERENCES model_enrichment_artifact(id),
  pointer_version INTEGER NOT NULL CHECK (pointer_version >= 1),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (poem_id, model_key),
  UNIQUE (enrichment_artifact_id)
);

CREATE TABLE IF NOT EXISTS ai_vendor (
  vendor_key TEXT PRIMARY KEY CHECK (length(trim(vendor_key)) BETWEEN 1 AND 100),
  display_name TEXT NOT NULL UNIQUE CHECK (length(trim(display_name)) BETWEEN 1 AND 200),
  created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS ai_model (
  model_key TEXT PRIMARY KEY CHECK (length(trim(model_key)) BETWEEN 1 AND 200),
  vendor_key TEXT NOT NULL REFERENCES ai_vendor(vendor_key),
  family_key TEXT NOT NULL CHECK (length(trim(family_key)) BETWEEN 1 AND 100),
  version_label TEXT NOT NULL CHECK (length(trim(version_label)) BETWEEN 1 AND 100),
  display_name TEXT NOT NULL CHECK (length(trim(display_name)) BETWEEN 1 AND 200),
  created_at INTEGER NOT NULL,
  UNIQUE (vendor_key, family_key, version_label)
) STRICT;

CREATE TABLE IF NOT EXISTS inference_backend (
  backend_key TEXT PRIMARY KEY CHECK (length(trim(backend_key)) BETWEEN 1 AND 100),
  display_name TEXT NOT NULL UNIQUE CHECK (length(trim(display_name)) BETWEEN 1 AND 200),
  created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS enrichment_profile (
  profile_key TEXT PRIMARY KEY CHECK (length(trim(profile_key)) BETWEEN 1 AND 300),
  public_track_key TEXT NOT NULL CHECK (length(trim(public_track_key)) BETWEEN 1 AND 200),
  model_key TEXT NOT NULL REFERENCES ai_model(model_key),
  backend_key TEXT NOT NULL REFERENCES inference_backend(backend_key),
  runtime_model_id TEXT NOT NULL CHECK (length(trim(runtime_model_id)) BETWEEN 1 AND 200),
  prompt_version TEXT NOT NULL CHECK (length(trim(prompt_version)) BETWEEN 1 AND 200),
  reasoning_effort TEXT NOT NULL CHECK (length(trim(reasoning_effort)) BETWEEN 1 AND 100),
  input_schema_version INTEGER NOT NULL CHECK (input_schema_version >= 1),
  output_schema_version INTEGER NOT NULL CHECK (output_schema_version >= 1),
  created_at INTEGER NOT NULL,
  UNIQUE (
    public_track_key, runtime_model_id, backend_key, prompt_version,
    reasoning_effort, input_schema_version, output_schema_version
  ),
  UNIQUE (
    public_track_key, runtime_model_id, prompt_version, reasoning_effort,
    input_schema_version, output_schema_version
  )
) STRICT;

CREATE TABLE IF NOT EXISTS model_enrichment_artifact_profile (
  artifact_id TEXT PRIMARY KEY REFERENCES model_enrichment_artifact(id),
  profile_key TEXT NOT NULL REFERENCES enrichment_profile(profile_key),
  bound_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS legacy_model_attribution (
  attribution_key TEXT PRIMARY KEY CHECK (length(trim(attribution_key)) BETWEEN 1 AND 200),
  vendor_key TEXT NOT NULL REFERENCES ai_vendor(vendor_key),
  family_key TEXT NOT NULL CHECK (length(trim(family_key)) BETWEEN 1 AND 100),
  minimum_version TEXT,
  maximum_version TEXT,
  certainty TEXT NOT NULL CHECK (certainty IN ('exact', 'inferred_range', 'unknown')),
  display_name TEXT NOT NULL CHECK (length(trim(display_name)) BETWEEN 1 AND 200),
  rationale TEXT NOT NULL CHECK (length(trim(rationale)) BETWEEN 1 AND 2000),
  created_at INTEGER NOT NULL,
  CHECK (
    certainty = 'unknown'
    OR (minimum_version IS NOT NULL AND maximum_version IS NOT NULL)
  )
) STRICT;

CREATE TABLE IF NOT EXISTS poem_legacy_payload_attribution (
  poem_id TEXT NOT NULL REFERENCES poem(id),
  legacy_field TEXT NOT NULL
    CHECK (legacy_field IN ('translation', 'translation_gemini', 'insights')),
  source_payload_hash TEXT NOT NULL CHECK (
    length(source_payload_hash) = 64
    AND source_payload_hash NOT GLOB '*[^0-9a-f]*'
  ),
  hash_algorithm TEXT NOT NULL DEFAULT 'sha256-utf8-exact-v1'
    CHECK (hash_algorithm = 'sha256-utf8-exact-v1'),
  attribution_key TEXT NOT NULL REFERENCES legacy_model_attribution(attribution_key),
  attributed_at INTEGER NOT NULL,
  PRIMARY KEY (poem_id, legacy_field, source_payload_hash)
) STRICT;

CREATE TABLE IF NOT EXISTS production_deployment_identity (
  scope TEXT PRIMARY KEY CHECK (scope = 'production'),
  database_id TEXT NOT NULL UNIQUE CHECK (
    length(database_id) = 36
    AND database_id GLOB '[0-9a-f]*-[0-9a-f]*-[0-9a-f]*-[0-9a-f]*-[0-9a-f]*'
  )
) STRICT;

CREATE TABLE IF NOT EXISTS source_revision_fingerprint (
  source_revision_id TEXT PRIMARY KEY
    REFERENCES poem_source_revision(id),
  line_nfc_hash TEXT NOT NULL CHECK (length(line_nfc_hash) = 64),
  prompt_material_hash TEXT NOT NULL CHECK (length(prompt_material_hash) = 64),
  algorithm TEXT NOT NULL CHECK (algorithm = 'sha256-canonical-nfc-v1'),
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_admission_clock (
  admission_id TEXT PRIMARY KEY CHECK (length(admission_id) = 64),
  issued_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS model_publication_receipt (
  intent_id TEXT PRIMARY KEY,
  action_hash TEXT NOT NULL UNIQUE CHECK (length(action_hash) = 64),
  poem_id TEXT NOT NULL REFERENCES poem(id),
  model_key TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  source_revision_id TEXT NOT NULL REFERENCES poem_source_revision(id),
  enrichment_artifact_id TEXT NOT NULL
    REFERENCES model_enrichment_artifact(id),
  expected_pointer_version INTEGER CHECK (expected_pointer_version >= 1),
  pointer_version INTEGER NOT NULL CHECK (pointer_version >= 1),
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 1),
  outcome TEXT NOT NULL CHECK (outcome IN ('published', 'already-published')),
  committed_at INTEGER NOT NULL,
  UNIQUE (poem_id, model_key, pointer_version),
  UNIQUE (enrichment_artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_author_status ON author(status);

CREATE INDEX IF NOT EXISTS idx_author_public_slug
  ON author(slug, id)
  WHERE hidden = 0;

CREATE INDEX IF NOT EXISTS idx_poem_author_id ON poem(author_id);

CREATE INDEX IF NOT EXISTS idx_task_status ON task(status);

CREATE INDEX IF NOT EXISTS idx_task_type ON task(type);

CREATE INDEX IF NOT EXISTS idx_task_created_at ON task(created_at);

CREATE INDEX IF NOT EXISTS idx_task_poem_id ON task(poem_id); -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.

CREATE INDEX IF NOT EXISTS idx_task_author_id ON task(author_id); -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_active_work_key -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
  ON task(work_key)
  WHERE work_key IS NOT NULL
    AND status IN ('pending', 'in_progress', 'failed');

CREATE INDEX IF NOT EXISTS idx_task_outbox -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
  ON task(queue_state, status, queue_attempt_count, created_at);

CREATE INDEX IF NOT EXISTS idx_task_lease ON task(status, lease_expires_at); -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.

CREATE INDEX IF NOT EXISTS idx_author_public_catalog -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
  ON author(sort_name_arabic, id)
  WHERE hidden = 0 AND public_poem_count > 0;

CREATE INDEX IF NOT EXISTS idx_poem_public_author_title -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
  ON poem(author_id, sort_name_arabic, id)
  WHERE hidden = 0 AND publishable = 1;

CREATE INDEX IF NOT EXISTS idx_poem_public_sitemap -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON poem(sitemap_shard, id, author_id)
WHERE hidden = 0 AND publishable = 1;

CREATE INDEX IF NOT EXISTS idx_crawl_import_bundle_status -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON crawl_import_bundle(status, created_at);

CREATE INDEX IF NOT EXISTS idx_crawl_import_record_source_poem -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON crawl_import_record(source_name, source_poem_id);

CREATE INDEX IF NOT EXISTS idx_source_poem_author -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON source_poem_identity(source_author_id, external_id);

CREATE INDEX IF NOT EXISTS idx_source_revision_poem_created -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON poem_source_revision(source_poem_id, created_at);

CREATE INDEX IF NOT EXISTS idx_enrichment_artifact_revision -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON enrichment_artifact(source_revision_id, created_at);

CREATE INDEX IF NOT EXISTS idx_enrichment_validation_artifact -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON enrichment_validation(artifact_id, outcome, highest_severity);

CREATE INDEX IF NOT EXISTS idx_crawl_import_record_author -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON crawl_import_record(canonical_author_id);

CREATE INDEX IF NOT EXISTS idx_crawl_import_record_poem -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON crawl_import_record(canonical_poem_id);

CREATE INDEX IF NOT EXISTS idx_source_author_canonical -- sarj-noqa: SARJ116 — NO ACTION FK canonical_author_id -> author(id); local parent-check plan uses covering SEARCH, otherwise SCAN source_author_identity.
ON source_author_identity(canonical_author_id);

CREATE INDEX IF NOT EXISTS idx_source_poem_canonical -- sarj-noqa: SARJ116 — NO ACTION FK canonical_poem_id -> poem(id); local parent-check plan uses covering SEARCH, otherwise SCAN source_poem_identity.
ON source_poem_identity(canonical_poem_id);

CREATE INDEX IF NOT EXISTS idx_source_revision_import_record -- sarj-noqa: SARJ116 — NO ACTION FK (import_bundle_id, import_ordinal) -> crawl_import_record(bundle_id, ordinal); local parent-check plan uses covering SEARCH, otherwise SCAN poem_source_revision.
ON poem_source_revision(import_bundle_id, import_ordinal);

CREATE INDEX IF NOT EXISTS idx_source_pointer_revision -- sarj-noqa: SARJ116 — NO ACTION FK revision_id -> poem_source_revision(id); local parent-check plan uses covering SEARCH, otherwise SCAN poem_source_pointer.
ON poem_source_pointer(revision_id);

CREATE INDEX IF NOT EXISTS idx_publication_pointer_source_revision -- sarj-noqa: SARJ116 — NO ACTION FK source_revision_id -> poem_source_revision(id); local parent-check plan uses covering SEARCH, otherwise SCAN poem_publication_pointer.
ON poem_publication_pointer(source_revision_id);

CREATE INDEX IF NOT EXISTS idx_publication_pointer_enrichment -- sarj-noqa: SARJ116 — NO ACTION FK enrichment_artifact_id -> enrichment_artifact(id); local parent-check plan uses covering SEARCH, otherwise SCAN poem_publication_pointer.
ON poem_publication_pointer(enrichment_artifact_id);

CREATE INDEX IF NOT EXISTS idx_poem_active_source_revision -- sarj-noqa: SARJ108, SARJ116 — SQLite lacks CONCURRENTLY; NO ACTION FK active_source_revision_id -> poem_source_revision(id) uses covering SEARCH for local parent checks, otherwise SCAN poem.
ON poem(active_source_revision_id);

CREATE INDEX IF NOT EXISTS idx_poem_active_enrichment_artifact -- sarj-noqa: SARJ108, SARJ116 — SQLite lacks CONCURRENTLY; NO ACTION FK active_enrichment_artifact_id -> enrichment_artifact(id) uses covering SEARCH for local parent checks, otherwise SCAN poem.
ON poem(active_enrichment_artifact_id);

CREATE INDEX IF NOT EXISTS idx_poem_model_publication_revision -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON poem_model_publication_pointer(source_revision_id, model_key);

CREATE INDEX IF NOT EXISTS idx_model_enrichment_artifact_revision -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON model_enrichment_artifact(source_revision_id, model_key, created_at);

CREATE INDEX IF NOT EXISTS idx_model_enrichment_validation_artifact -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON model_enrichment_validation(artifact_id, outcome, highest_severity);

CREATE INDEX IF NOT EXISTS idx_enrichment_profile_model -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON enrichment_profile(model_key);

CREATE INDEX IF NOT EXISTS idx_enrichment_profile_backend -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON enrichment_profile(backend_key);

CREATE INDEX IF NOT EXISTS idx_artifact_profile_profile -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON model_enrichment_artifact_profile(profile_key, artifact_id);

CREATE INDEX IF NOT EXISTS idx_legacy_model_attribution_vendor -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON legacy_model_attribution(vendor_key);

CREATE INDEX IF NOT EXISTS idx_legacy_attribution_lookup -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON poem_legacy_payload_attribution(poem_id, legacy_field, source_payload_hash);

CREATE INDEX IF NOT EXISTS idx_legacy_attribution_attribution -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON poem_legacy_payload_attribution(attribution_key, poem_id);

CREATE INDEX IF NOT EXISTS idx_source_revision_fingerprint_line_nfc -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON source_revision_fingerprint(line_nfc_hash, source_revision_id);

CREATE INDEX IF NOT EXISTS idx_source_revision_fingerprint_prompt_material -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON source_revision_fingerprint(prompt_material_hash, source_revision_id);

CREATE INDEX IF NOT EXISTS idx_model_publication_receipt_lookup -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON model_publication_receipt(poem_id, model_key, committed_at);

CREATE INDEX IF NOT EXISTS idx_model_publication_receipt_revision -- sarj-noqa: SARJ116 — Verified existing baseline index, not incremental index growth; preserve schema equivalence.
ON model_publication_receipt(source_revision_id, committed_at);

INSERT INTO "scraper_writer_control" ("singleton", "writer_epoch", "writer_id", "updated_at") VALUES (1, 1, NULL, unixepoch());

INSERT INTO "ai_vendor" ("vendor_key", "display_name", "created_at") VALUES ('anthropic', 'Anthropic', 0);

INSERT INTO "ai_vendor" ("vendor_key", "display_name", "created_at") VALUES ('google', 'Google', 0);

INSERT INTO "ai_vendor" ("vendor_key", "display_name", "created_at") VALUES ('openai', 'OpenAI', 0);

INSERT INTO "ai_model" ("model_key", "vendor_key", "family_key", "version_label", "display_name", "created_at") VALUES ('gpt-5.6-sol', 'openai', 'gpt', '5.6-sol', 'Sol 5.6', 0);

INSERT INTO "inference_backend" ("backend_key", "display_name", "created_at") VALUES ('openai-codex-cli', 'Codex CLI', 0);

INSERT INTO "enrichment_profile" ("profile_key", "public_track_key", "model_key", "backend_key", "runtime_model_id", "prompt_version", "reasoning_effort", "input_schema_version", "output_schema_version", "created_at") VALUES ('sol-5.6/source-v1', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-enrichment-v1', 'high', 1, 1, 0);

INSERT INTO "enrichment_profile" ("profile_key", "public_track_key", "model_key", "backend_key", "runtime_model_id", "prompt_version", "reasoning_effort", "input_schema_version", "output_schema_version", "created_at") VALUES ('sol-5.6/source-v2', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-enrichment-v1', 'high', 2, 1, 0);

INSERT INTO "enrichment_profile" ("profile_key", "public_track_key", "model_key", "backend_key", "runtime_model_id", "prompt_version", "reasoning_effort", "input_schema_version", "output_schema_version", "created_at") VALUES ('sol-5.6/word-gloss-v2/source-v1', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-word-gloss-v2', 'high', 1, 2, 0);

INSERT INTO "enrichment_profile" ("profile_key", "public_track_key", "model_key", "backend_key", "runtime_model_id", "prompt_version", "reasoning_effort", "input_schema_version", "output_schema_version", "created_at") VALUES ('sol-5.6/word-gloss-v2/source-v2', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-word-gloss-v2', 'high', 2, 2, 0);

INSERT INTO "legacy_model_attribution" ("attribution_key", "vendor_key", "family_key", "minimum_version", "maximum_version", "certainty", "display_name", "rationale", "created_at") VALUES ('legacy-claude-1-or-2', 'anthropic', 'claude', '1', '2', 'inferred_range', 'Claude 1 or 2', 'Legacy translation provenance is inferred as Claude generation, but the exact major version is not recoverable.', 0);

INSERT INTO "legacy_model_attribution" ("attribution_key", "vendor_key", "family_key", "minimum_version", "maximum_version", "certainty", "display_name", "rationale", "created_at") VALUES ('legacy-gemini-unknown', 'google', 'gemini', NULL, NULL, 'unknown', 'Gemini (legacy model unknown)', 'The legacy Gemini field identifies the vendor family, but its exact model and generation recipe are not recoverable.', 0);

INSERT INTO "production_deployment_identity" ("scope", "database_id") VALUES ('production', 'ffaae610-4dae-4d7e-bf86-8232f46ca2b5');

CREATE TRIGGER poem_generated_title_guard_before_insert
BEFORE INSERT ON poem
WHEN
  (NEW.name_english IS NOT NULL AND (
    instr(NEW.name_english, char(10)) > 0
    OR length(trim(NEW.name_english)) > 200
  ))
  OR (NEW.poem_title_first_line IS NOT NULL AND (
    instr(NEW.poem_title_first_line, char(10)) > 0
    OR length(trim(NEW.poem_title_first_line)) > 200
  ))
  OR EXISTS (
    SELECT 1
    FROM json_each(json_array(
      '*here is*translat*', '*ai assistant*', '*unable to translat*',
      '*cannot translat*', '*do not*translat*', '*don''t*translat*',
      '*attempt*translat*', '*translated title*', '*from english to arabic*',
      '*you are an arabic*', 'i will not*translat*', 'i will not*provide*',
      'i have nothing*translat*', 'i have nothing*output*',
      'i have not*translat*', 'i presume not*translat*',
      'i did not*translat*', 'i am not able*translat*',
      'you''re right*translat*', '*without proper context*',
      '*copyrighted material*', '*as requested*translat*',
      '*as requested*output*', '*do not speak arabic*',
      '*not attempt to translat*', '*refrain from translat*',
      '*translation capabilities*', '*translation services*',
      '*please provide*arabic*', '*let''s have*discussion*',
      '*let''s have*conversation*', '*entrust you*translate*',
      'nice try*translate*', 'without permission*translate*',
      'translated to *', 'titles translated', 'my poem translation:'
    )) AS rejected
    WHERE lower(COALESCE(NEW.name_english, '')) GLOB rejected.value
      OR lower(COALESCE(NEW.poem_title_first_line, '')) GLOB rejected.value
  )
BEGIN
  SELECT RAISE(ABORT, 'invalid generated poem title');
END;

CREATE TRIGGER poem_generated_title_guard_before_update
BEFORE UPDATE OF name_english, poem_title_first_line ON poem
WHEN
  (NEW.name_english IS NOT NULL AND (
    instr(NEW.name_english, char(10)) > 0
    OR length(trim(NEW.name_english)) > 200
  ))
  OR (NEW.poem_title_first_line IS NOT NULL AND (
    instr(NEW.poem_title_first_line, char(10)) > 0
    OR length(trim(NEW.poem_title_first_line)) > 200
  ))
  OR EXISTS (
    SELECT 1
    FROM json_each(json_array(
      '*here is*translat*', '*ai assistant*', '*unable to translat*',
      '*cannot translat*', '*do not*translat*', '*don''t*translat*',
      '*attempt*translat*', '*translated title*', '*from english to arabic*',
      '*you are an arabic*', 'i will not*translat*', 'i will not*provide*',
      'i have nothing*translat*', 'i have nothing*output*',
      'i have not*translat*', 'i presume not*translat*',
      'i did not*translat*', 'i am not able*translat*',
      'you''re right*translat*', '*without proper context*',
      '*copyrighted material*', '*as requested*translat*',
      '*as requested*output*', '*do not speak arabic*',
      '*not attempt to translat*', '*refrain from translat*',
      '*translation capabilities*', '*translation services*',
      '*please provide*arabic*', '*let''s have*discussion*',
      '*let''s have*conversation*', '*entrust you*translate*',
      'nice try*translate*', 'without permission*translate*',
      'translated to *', 'titles translated', 'my poem translation:'
    )) AS rejected
    WHERE lower(COALESCE(NEW.name_english, '')) GLOB rejected.value
      OR lower(COALESCE(NEW.poem_title_first_line, '')) GLOB rejected.value
  )
BEGIN
  SELECT RAISE(ABORT, 'invalid generated poem title');
END;

CREATE TRIGGER public_poem_count_after_insert
AFTER INSERT ON poem
WHEN NEW.hidden = 0 AND NEW.publishable = 1
BEGIN
  UPDATE author
  SET public_poem_count = public_poem_count + 1
  WHERE id = NEW.author_id;
END;

CREATE TRIGGER public_poem_count_after_delete
AFTER DELETE ON poem
WHEN OLD.hidden = 0 AND OLD.publishable = 1
BEGIN
  UPDATE author
  SET public_poem_count = MAX(public_poem_count - 1, 0)
  WHERE id = OLD.author_id;
END;

CREATE TRIGGER public_poem_count_after_update
AFTER UPDATE OF author_id, hidden, publishable ON poem
BEGIN
  UPDATE author
  SET public_poem_count = MAX(public_poem_count - 1, 0)
  WHERE id = OLD.author_id
    AND OLD.hidden = 0
    AND OLD.publishable = 1;

  UPDATE author
  SET public_poem_count = public_poem_count + 1
  WHERE id = NEW.author_id
    AND NEW.hidden = 0
    AND NEW.publishable = 1;
END;

CREATE TRIGGER author_sort_name_after_insert
AFTER INSERT ON author
BEGIN
  UPDATE author
  SET sort_name_arabic = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    trim(NEW.name_arabic),
    'ـ', ''), 'ً', ''), 'ٌ', ''), 'ٍ', ''), 'َ', ''), 'ُ', ''), 'ِ', ''), 'ّ', ''), 'ْ', ''), 'ٰ', ''),
    'أ', 'ا'), 'إ', 'ا'), 'آ', 'ا'), 'ٱ', 'ا'), 'ى', 'ي'), 'ؤ', 'و'), 'ئ', 'ي')
  WHERE id = NEW.id;
END;

CREATE TRIGGER author_sort_name_after_update
AFTER UPDATE OF name_arabic ON author
WHEN OLD.name_arabic IS NOT NEW.name_arabic
BEGIN
  UPDATE author
  SET sort_name_arabic = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    trim(NEW.name_arabic),
    'ـ', ''), 'ً', ''), 'ٌ', ''), 'ٍ', ''), 'َ', ''), 'ُ', ''), 'ِ', ''), 'ّ', ''), 'ْ', ''), 'ٰ', ''),
    'أ', 'ا'), 'إ', 'ا'), 'آ', 'ا'), 'ٱ', 'ا'), 'ى', 'ي'), 'ؤ', 'و'), 'ئ', 'ي')
  WHERE id = NEW.id;
END;

CREATE TRIGGER poem_sort_name_after_insert
AFTER INSERT ON poem
BEGIN
  UPDATE poem
  SET sort_name_arabic = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    trim(NEW.name_arabic),
    'ـ', ''), 'ً', ''), 'ٌ', ''), 'ٍ', ''), 'َ', ''), 'ُ', ''), 'ِ', ''), 'ّ', ''), 'ْ', ''), 'ٰ', ''),
    'أ', 'ا'), 'إ', 'ا'), 'آ', 'ا'), 'ٱ', 'ا'), 'ى', 'ي'), 'ؤ', 'و'), 'ئ', 'ي'),
    has_english = CASE WHEN
      CASE WHEN json_valid(NEW.translation)
        THEN json_type(NEW.translation, '$.content') = 'array'
          AND json_array_length(NEW.translation, '$.content') > 0
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.translation, '$.content') line
            WHERE line.type <> 'text'
          )
          AND EXISTS (
            SELECT 1 FROM json_each(NEW.translation, '$.content') line
            WHERE line.type = 'text' AND trim(line.value) <> ''
          )
        ELSE 0
      END
      OR CASE WHEN json_valid(NEW.translation_gemini)
        THEN json_type(NEW.translation_gemini, '$.content') = 'array'
          AND json_array_length(NEW.translation_gemini, '$.content') > 0
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.translation_gemini, '$.content') line
            WHERE line.type <> 'text'
          )
          AND EXISTS (
            SELECT 1 FROM json_each(NEW.translation_gemini, '$.content') line
            WHERE line.type = 'text' AND trim(line.value) <> ''
          )
        ELSE 0
      END
    THEN 1 ELSE 0 END,
    has_insights = CASE WHEN
      json_valid(NEW.insights)
      AND json_type(NEW.insights) = 'object'
      AND json_type(NEW.insights, '$.summary') = 'text'
      AND trim(json_extract(NEW.insights, '$.summary')) <> ''
    THEN 1 ELSE 0 END
  WHERE id = NEW.id;
END;

CREATE TRIGGER poem_sort_name_after_update
AFTER UPDATE OF name_arabic ON poem
WHEN OLD.name_arabic IS NOT NEW.name_arabic
BEGIN
  UPDATE poem
  SET sort_name_arabic = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    trim(NEW.name_arabic),
    'ـ', ''), 'ً', ''), 'ٌ', ''), 'ٍ', ''), 'َ', ''), 'ُ', ''), 'ِ', ''), 'ّ', ''), 'ْ', ''), 'ٰ', ''),
    'أ', 'ا'), 'إ', 'ا'), 'آ', 'ا'), 'ٱ', 'ا'), 'ى', 'ي'), 'ؤ', 'و'), 'ئ', 'ي')
  WHERE id = NEW.id;
END;

CREATE TRIGGER poem_catalog_flags_after_update
AFTER UPDATE OF translation, translation_gemini, insights ON poem
BEGIN
  UPDATE poem
  SET has_english = CASE WHEN
      CASE WHEN json_valid(NEW.translation)
        THEN json_type(NEW.translation, '$.content') = 'array'
          AND json_array_length(NEW.translation, '$.content') > 0
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.translation, '$.content') line
            WHERE line.type <> 'text'
          )
          AND EXISTS (
            SELECT 1 FROM json_each(NEW.translation, '$.content') line
            WHERE line.type = 'text' AND trim(line.value) <> ''
          )
        ELSE 0
      END
      OR CASE WHEN json_valid(NEW.translation_gemini)
        THEN json_type(NEW.translation_gemini, '$.content') = 'array'
          AND json_array_length(NEW.translation_gemini, '$.content') > 0
          AND NOT EXISTS (
            SELECT 1 FROM json_each(NEW.translation_gemini, '$.content') line
            WHERE line.type <> 'text'
          )
          AND EXISTS (
            SELECT 1 FROM json_each(NEW.translation_gemini, '$.content') line
            WHERE line.type = 'text' AND trim(line.value) <> ''
          )
        ELSE 0
      END
    THEN 1 ELSE 0 END,
    has_insights = CASE WHEN
      json_valid(NEW.insights)
      AND json_type(NEW.insights) = 'object'
      AND json_type(NEW.insights, '$.summary') = 'text'
      AND trim(json_extract(NEW.insights, '$.summary')) <> ''
    THEN 1 ELSE 0 END
  WHERE id = NEW.id;
END;

CREATE TRIGGER poem_publishability_after_insert
AFTER INSERT ON poem
BEGIN
  UPDATE poem
  SET publishable = CASE
    WHEN NEW.hidden = 0
      AND length(trim(NEW.id)) BETWEEN 1 AND 500
      AND NEW.id = trim(NEW.id)
      AND NEW.id NOT IN ('.', '..')
      AND instr(NEW.id, '/') = 0
      AND length(trim(NEW.slug)) BETWEEN 1 AND 500
      AND NEW.slug = trim(NEW.slug)
      AND NEW.slug NOT IN ('.', '..')
      AND instr(NEW.slug, '/') = 0
      AND length(trim(NEW.name_arabic)) BETWEEN 1 AND 500
      AND NEW.name_arabic = trim(NEW.name_arabic)
      AND NEW.verses BETWEEN 1 AND 1000
      AND CASE WHEN json_valid(NEW.content_arabic)
        THEN json_type(NEW.content_arabic, '$.content') = 'array'
          AND json_array_length(NEW.content_arabic, '$.content') BETWEEN 1 AND 2000
        ELSE 0
      END
      AND NOT EXISTS (
        SELECT 1 FROM json_each(
          CASE WHEN json_valid(NEW.content_arabic)
            THEN NEW.content_arabic ELSE '{"content":[]}' END,
          '$.content'
        ) line
        WHERE line.type <> 'text'
      )
      AND EXISTS (
        SELECT 1 FROM json_each(
          CASE WHEN json_valid(NEW.content_arabic)
            THEN NEW.content_arabic ELSE '{"content":[]}' END,
          '$.content'
        ) line
        WHERE line.type = 'text' AND trim(line.value) <> ''
      )
    THEN 1 ELSE 0
  END
  WHERE id = NEW.id;
END;

CREATE TRIGGER poem_publishability_after_update
AFTER UPDATE OF id, slug, verses, hidden, name_arabic, content_arabic ON poem
BEGIN
  UPDATE poem
  SET publishable = CASE
    WHEN NEW.hidden = 0
      AND length(trim(NEW.id)) BETWEEN 1 AND 500
      AND NEW.id = trim(NEW.id)
      AND NEW.id NOT IN ('.', '..')
      AND instr(NEW.id, '/') = 0
      AND length(trim(NEW.slug)) BETWEEN 1 AND 500
      AND NEW.slug = trim(NEW.slug)
      AND NEW.slug NOT IN ('.', '..')
      AND instr(NEW.slug, '/') = 0
      AND length(trim(NEW.name_arabic)) BETWEEN 1 AND 500
      AND NEW.name_arabic = trim(NEW.name_arabic)
      AND NEW.verses BETWEEN 1 AND 1000
      AND CASE WHEN json_valid(NEW.content_arabic)
        THEN json_type(NEW.content_arabic, '$.content') = 'array'
          AND json_array_length(NEW.content_arabic, '$.content') BETWEEN 1 AND 2000
        ELSE 0
      END
      AND NOT EXISTS (
        SELECT 1 FROM json_each(
          CASE WHEN json_valid(NEW.content_arabic)
            THEN NEW.content_arabic ELSE '{"content":[]}' END,
          '$.content'
        ) line
        WHERE line.type <> 'text'
      )
      AND EXISTS (
        SELECT 1 FROM json_each(
          CASE WHEN json_valid(NEW.content_arabic)
            THEN NEW.content_arabic ELSE '{"content":[]}' END,
          '$.content'
        ) line
        WHERE line.type = 'text' AND trim(line.value) <> ''
      )
    THEN 1 ELSE 0
  END
  WHERE id = NEW.id;
END;

CREATE TRIGGER crawl_import_record_open_bundle_insert -- sarj-noqa: SARJ114 — database-owned immutability and writer-fencing invariant.
BEFORE INSERT ON crawl_import_record
WHEN NOT EXISTS (
  SELECT 1 FROM crawl_import_bundle
  WHERE id = NEW.bundle_id AND status = 'open'
)
AND NOT EXISTS (
  SELECT 1 FROM crawl_import_record
  WHERE bundle_id = NEW.bundle_id
    AND ordinal = NEW.ordinal
    AND record_hash = NEW.record_hash
)
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_NOT_OPEN');
END;

CREATE TRIGGER crawl_import_record_immutable_update -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON crawl_import_record
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECORD_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_record_immutable_delete -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON crawl_import_record
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECORD_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_bundle_identity_immutable -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON crawl_import_bundle
WHEN NEW.id <> OLD.id
  OR NEW.schema_version <> OLD.schema_version
  OR NEW.manifest_hash <> OLD.manifest_hash
  OR NEW.expected_record_count <> OLD.expected_record_count
  OR NEW.created_at <> OLD.created_at
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_IDENTITY_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_bundle_epoch_transition -- sarj-noqa: SARJ114 — database-owned writer-fencing invariant.
BEFORE UPDATE ON crawl_import_bundle
WHEN NEW.writer_epoch <> OLD.writer_epoch
  AND NOT (
    OLD.status IN ('open', 'sealed')
    AND NEW.status = OLD.status
    AND NEW.writer_epoch = (
      SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
    )
    AND NEW.plan_hash IS NULL
    AND NEW.promotion_plan IS NULL
  )
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_EPOCH_INVALID');
END;

CREATE TRIGGER crawl_import_bundle_status_transition -- sarj-noqa: SARJ114 — database-owned state-machine invariant.
BEFORE UPDATE ON crawl_import_bundle
WHEN NOT (
  NEW.status = OLD.status
  OR (OLD.status = 'open' AND NEW.status = 'sealed')
  OR (OLD.status = 'sealed' AND NEW.status = 'promoted')
)
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_STATUS_INVALID');
END;

CREATE TRIGGER crawl_import_bundle_seal_fields_immutable -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON crawl_import_bundle
WHEN (
  OLD.root_hash IS NOT NULL
  AND NEW.root_hash IS NOT OLD.root_hash
) OR (
  OLD.sealed_at IS NOT NULL
  AND NEW.sealed_at IS NOT OLD.sealed_at
) OR (
  OLD.promoted_at IS NOT NULL
  AND NEW.promoted_at IS NOT OLD.promoted_at
)
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_SEAL_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_bundle_plan_immutable -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON crawl_import_bundle
WHEN OLD.plan_hash IS NOT NULL
  AND (
    NEW.plan_hash IS NOT OLD.plan_hash
    OR NEW.promotion_plan IS NOT OLD.promotion_plan
  )
  AND NEW.writer_epoch = OLD.writer_epoch
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_PLAN_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_bundle_delete_forbidden -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON crawl_import_bundle
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_IMMUTABLE');
END;

CREATE TRIGGER source_author_identity_ownership_immutable -- sarj-noqa: SARJ114 — database-owned identity invariant.
BEFORE UPDATE ON source_author_identity
WHEN NEW.id <> OLD.id
  OR NEW.source_name <> OLD.source_name
  OR NEW.external_id <> OLD.external_id
  OR NEW.canonical_url <> OLD.canonical_url
  OR NEW.canonical_author_id IS NOT OLD.canonical_author_id
  OR NEW.first_observed_at <> OLD.first_observed_at
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_AUTHOR_OWNERSHIP_IMMUTABLE');
END;

CREATE TRIGGER source_author_identity_delete_forbidden -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON source_author_identity
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_AUTHOR_IDENTITY_IMMUTABLE');
END;

CREATE TRIGGER source_poem_identity_ownership_immutable -- sarj-noqa: SARJ114 — database-owned identity invariant.
BEFORE UPDATE ON source_poem_identity
WHEN NEW.id <> OLD.id
  OR NEW.source_name <> OLD.source_name
  OR NEW.external_id <> OLD.external_id
  OR NEW.source_author_id <> OLD.source_author_id
  OR NEW.canonical_url <> OLD.canonical_url
  OR NEW.canonical_poem_id IS NOT OLD.canonical_poem_id
  OR NEW.first_observed_at <> OLD.first_observed_at
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POEM_OWNERSHIP_IMMUTABLE');
END;

CREATE TRIGGER source_poem_identity_delete_forbidden -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON source_poem_identity
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_record_author_relationship_insert -- sarj-noqa: SARJ114 — database-owned cross-record identity invariant.
BEFORE INSERT ON crawl_import_record
WHEN NEW.canonical_poem_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM poem
    WHERE id = NEW.canonical_poem_id
      AND author_id = NEW.canonical_author_id
  )
BEGIN
  SELECT RAISE(ABORT, 'STAGED_CANONICAL_OWNERSHIP_INVALID');
END;

CREATE TRIGGER source_poem_identity_author_relationship_insert -- sarj-noqa: SARJ114 — database-owned cross-record identity invariant.
BEFORE INSERT ON source_poem_identity
WHEN NOT EXISTS (
  SELECT 1
  FROM source_author_identity source_author
  JOIN poem canonical_poem
    ON canonical_poem.id = NEW.canonical_poem_id
  WHERE source_author.id = NEW.source_author_id
    AND source_author.source_name = NEW.source_name
    AND canonical_poem.author_id = source_author.canonical_author_id
)
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POEM_AUTHOR_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_source_revision_relationship_insert -- sarj-noqa: SARJ114 — database-owned import lineage invariant.
BEFORE INSERT ON poem_source_revision
WHEN NOT EXISTS (
  SELECT 1
  FROM source_poem_identity identity
  JOIN crawl_import_record staged
    ON staged.bundle_id = NEW.import_bundle_id
    AND staged.ordinal = NEW.import_ordinal
  JOIN crawl_import_bundle bundle ON bundle.id = staged.bundle_id
  WHERE identity.id = NEW.source_poem_id
    AND identity.source_name = staged.source_name
    AND identity.external_id = staged.source_poem_id
    AND staged.content_hash = NEW.content_hash
    AND staged.title_arabic = NEW.title_arabic
    AND staged.content_arabic = NEW.content_arabic
    AND bundle.schema_version = NEW.schema_version
)
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_REVISION_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_source_pointer_relationship_insert -- sarj-noqa: SARJ114 — database-owned pointer lineage invariant.
BEFORE INSERT ON poem_source_pointer
WHEN NOT EXISTS (
  SELECT 1 FROM poem_source_revision revision
  WHERE revision.id = NEW.revision_id
    AND revision.source_poem_id = NEW.source_poem_id
)
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POINTER_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_source_pointer_relationship_update -- sarj-noqa: SARJ114 — database-owned pointer lineage invariant.
BEFORE UPDATE ON poem_source_pointer
WHEN NEW.source_poem_id <> OLD.source_poem_id
  OR NEW.pointer_version <> OLD.pointer_version + 1
  OR NOT EXISTS (
  SELECT 1 FROM poem_source_revision revision
  WHERE revision.id = NEW.revision_id
    AND revision.source_poem_id = NEW.source_poem_id
)
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POINTER_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_publication_pointer_relationship_insert -- sarj-noqa: SARJ114 — database-owned publication lineage invariant.
BEFORE INSERT ON poem_publication_pointer
WHEN NOT EXISTS (
  SELECT 1
  FROM poem_source_revision revision
  JOIN source_poem_identity identity
    ON identity.id = revision.source_poem_id
  LEFT JOIN enrichment_artifact artifact
    ON artifact.id = NEW.enrichment_artifact_id
  WHERE revision.id = NEW.source_revision_id
    AND identity.canonical_poem_id = NEW.poem_id
    AND (
      NEW.enrichment_artifact_id IS NULL
      OR artifact.source_revision_id = NEW.source_revision_id
    )
)
BEGIN
  SELECT RAISE(ABORT, 'PUBLICATION_POINTER_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_publication_pointer_relationship_update -- sarj-noqa: SARJ114 — database-owned publication lineage invariant.
BEFORE UPDATE ON poem_publication_pointer
WHEN NEW.poem_id <> OLD.poem_id
  OR NEW.pointer_version <> OLD.pointer_version + 1
  OR NOT EXISTS (
  SELECT 1
  FROM poem_source_revision revision
  JOIN source_poem_identity identity
    ON identity.id = revision.source_poem_id
  LEFT JOIN enrichment_artifact artifact
    ON artifact.id = NEW.enrichment_artifact_id
  WHERE revision.id = NEW.source_revision_id
    AND identity.canonical_poem_id = NEW.poem_id
    AND (
      NEW.enrichment_artifact_id IS NULL
      OR artifact.source_revision_id = NEW.source_revision_id
    )
)
BEGIN
  SELECT RAISE(ABORT, 'PUBLICATION_POINTER_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_source_revision_immutable_update -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON poem_source_revision
BEGIN
  SELECT RAISE(ABORT, 'POEM_SOURCE_REVISION_IMMUTABLE');
END;

CREATE TRIGGER poem_source_revision_immutable_delete -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON poem_source_revision
BEGIN
  SELECT RAISE(ABORT, 'POEM_SOURCE_REVISION_IMMUTABLE');
END;

CREATE TRIGGER enrichment_artifact_immutable_update -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON enrichment_artifact
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_ARTIFACT_IMMUTABLE');
END;

CREATE TRIGGER enrichment_artifact_immutable_delete -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON enrichment_artifact
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_ARTIFACT_IMMUTABLE');
END;

CREATE TRIGGER enrichment_validation_immutable_update -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON enrichment_validation
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_VALIDATION_IMMUTABLE');
END;

CREATE TRIGGER enrichment_validation_immutable_delete -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON enrichment_validation
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_VALIDATION_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_receipt_immutable_update -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE UPDATE ON crawl_import_receipt
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECEIPT_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_receipt_immutable_delete -- sarj-noqa: SARJ114 — database-owned immutability invariant.
BEFORE DELETE ON crawl_import_receipt
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECEIPT_IMMUTABLE');
END;

CREATE TRIGGER model_enrichment_artifact_immutable_update
BEFORE UPDATE ON model_enrichment_artifact
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_ARTIFACT_IMMUTABLE');
END;

CREATE TRIGGER model_enrichment_artifact_immutable_delete
BEFORE DELETE ON model_enrichment_artifact
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_ARTIFACT_IMMUTABLE');
END;

CREATE TRIGGER model_enrichment_validation_immutable_update
BEFORE UPDATE ON model_enrichment_validation
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_VALIDATION_IMMUTABLE');
END;

CREATE TRIGGER model_enrichment_validation_immutable_delete
BEFORE DELETE ON model_enrichment_validation
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_VALIDATION_IMMUTABLE');
END;

CREATE TRIGGER poem_model_publication_pointer_insert_version
BEFORE INSERT ON poem_model_publication_pointer
WHEN NEW.pointer_version <> 1
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_POINTER_VERSION_INVALID');
END;

CREATE TRIGGER poem_model_publication_pointer_insert_writer
BEFORE INSERT ON poem_model_publication_pointer
WHEN NOT EXISTS (
  SELECT 1
  FROM scraper_writer_control control
  WHERE control.singleton = 1
    AND control.writer_epoch = NEW.writer_epoch
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_WRITER_EPOCH_STALE');
END;

CREATE TRIGGER poem_model_publication_pointer_insert_relationship
BEFORE INSERT ON poem_model_publication_pointer
WHEN NOT EXISTS (
  SELECT 1
  FROM poem canonical_poem
  JOIN poem_source_revision revision
    ON revision.id = NEW.source_revision_id
  JOIN source_poem_identity source_identity
    ON source_identity.id = revision.source_poem_id
  JOIN model_enrichment_artifact artifact
    ON artifact.id = NEW.enrichment_artifact_id
  WHERE canonical_poem.id = NEW.poem_id
    AND canonical_poem.active_source_revision_id = NEW.source_revision_id
    AND source_identity.canonical_poem_id = NEW.poem_id
    AND artifact.source_revision_id = NEW.source_revision_id
    AND artifact.model_key = NEW.model_key
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER poem_model_publication_pointer_update_guard
BEFORE UPDATE ON poem_model_publication_pointer
WHEN NEW.poem_id <> OLD.poem_id
  OR NEW.model_key <> OLD.model_key
  OR NEW.pointer_version <> OLD.pointer_version + 1
  OR NEW.updated_at < OLD.updated_at
  OR NOT EXISTS (
    SELECT 1
    FROM scraper_writer_control control
    WHERE control.singleton = 1
      AND control.writer_epoch = NEW.writer_epoch
  )
  OR NOT EXISTS (
    SELECT 1
    FROM poem canonical_poem
    JOIN poem_source_revision revision
      ON revision.id = NEW.source_revision_id
    JOIN source_poem_identity source_identity
      ON source_identity.id = revision.source_poem_id
    JOIN model_enrichment_artifact artifact
      ON artifact.id = NEW.enrichment_artifact_id
    WHERE canonical_poem.id = NEW.poem_id
      AND canonical_poem.active_source_revision_id = NEW.source_revision_id
      AND source_identity.canonical_poem_id = NEW.poem_id
      AND artifact.source_revision_id = NEW.source_revision_id
      AND artifact.model_key = NEW.model_key
  )
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_UPDATE_INVALID');
END;

CREATE TRIGGER poem_model_publication_pointer_delete_forbidden
BEFORE DELETE ON poem_model_publication_pointer
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_POINTER_DELETE_FORBIDDEN');
END;

CREATE TRIGGER crawl_import_receipt_bundle_valid
BEFORE INSERT ON crawl_import_receipt
WHEN NOT EXISTS (
  SELECT 1
  FROM crawl_import_bundle bundle
  JOIN scraper_writer_control control ON control.singleton = 1
  WHERE bundle.id = NEW.bundle_id
    AND bundle.status IN ('sealed', 'promoted')
    AND bundle.plan_hash = NEW.plan_hash
    AND bundle.writer_epoch = NEW.writer_epoch
    AND control.writer_epoch = NEW.writer_epoch
)
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECEIPT_BUNDLE_INVALID');
END;

CREATE TRIGGER crawl_import_receipt_finalize_bundle
AFTER INSERT ON crawl_import_receipt
BEGIN
  UPDATE crawl_import_bundle
  SET status = 'promoted',
      promoted_at = COALESCE(promoted_at, unixepoch())
  WHERE id = NEW.bundle_id;
END;

CREATE TRIGGER poem_source_pointer_insert_guard
BEFORE INSERT ON poem_source_pointer
WHEN NEW.pointer_version <> 1
  OR NOT EXISTS (
    SELECT 1 FROM scraper_writer_control control
    WHERE control.singleton = 1
      AND control.writer_epoch = NEW.writer_epoch
  )
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POINTER_INSERT_INVALID');
END;

CREATE TRIGGER poem_source_pointer_update_guard
BEFORE UPDATE ON poem_source_pointer
WHEN NEW.source_poem_id IS NOT OLD.source_poem_id
  OR NEW.pointer_version <> OLD.pointer_version + 1
  OR NEW.updated_at < OLD.updated_at
  OR NOT EXISTS (
    SELECT 1 FROM scraper_writer_control control
    WHERE control.singleton = 1
      AND control.writer_epoch = NEW.writer_epoch
  )
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POINTER_UPDATE_INVALID');
END;

CREATE TRIGGER poem_source_pointer_delete_forbidden
BEFORE DELETE ON poem_source_pointer
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_POINTER_DELETE_FORBIDDEN');
END;

CREATE TRIGGER poem_publication_pointer_insert_guard
BEFORE INSERT ON poem_publication_pointer
WHEN NEW.pointer_version <> 1
  OR NOT EXISTS (
    SELECT 1 FROM scraper_writer_control control
    WHERE control.singleton = 1
      AND control.writer_epoch = NEW.writer_epoch
  )
BEGIN
  SELECT RAISE(ABORT, 'PUBLICATION_POINTER_INSERT_INVALID');
END;

CREATE TRIGGER poem_publication_pointer_update_guard
BEFORE UPDATE ON poem_publication_pointer
WHEN NEW.poem_id IS NOT OLD.poem_id
  OR NEW.pointer_version <> OLD.pointer_version + 1
  OR NEW.updated_at < OLD.updated_at
  OR NOT EXISTS (
    SELECT 1 FROM scraper_writer_control control
    WHERE control.singleton = 1
      AND control.writer_epoch = NEW.writer_epoch
  )
BEGIN
  SELECT RAISE(ABORT, 'PUBLICATION_POINTER_UPDATE_INVALID');
END;

CREATE TRIGGER poem_publication_pointer_delete_forbidden
BEFORE DELETE ON poem_publication_pointer
BEGIN
  SELECT RAISE(ABORT, 'PUBLICATION_POINTER_DELETE_FORBIDDEN');
END;

CREATE TRIGGER crawl_import_bundle_canonical_hash_insert
BEFORE INSERT ON crawl_import_bundle
WHEN NEW.manifest_hash GLOB '*[^0-9a-f]*'
  OR (NEW.root_hash IS NOT NULL AND NEW.root_hash GLOB '*[^0-9a-f]*')
  OR (NEW.plan_hash IS NOT NULL AND NEW.plan_hash GLOB '*[^0-9a-f]*')
  OR (
    NEW.promotion_plan IS NOT NULL
    AND length(CAST(NEW.promotion_plan AS BLOB)) > 2000000
  )
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_HASH_INVALID');
END;

CREATE TRIGGER crawl_import_bundle_canonical_hash_update
BEFORE UPDATE OF root_hash, plan_hash, promotion_plan ON crawl_import_bundle
WHEN (NEW.root_hash IS NOT NULL AND NEW.root_hash GLOB '*[^0-9a-f]*')
  OR (NEW.plan_hash IS NOT NULL AND NEW.plan_hash GLOB '*[^0-9a-f]*')
  OR (
    NEW.promotion_plan IS NOT NULL
    AND length(CAST(NEW.promotion_plan AS BLOB)) > 2000000
  )
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_BUNDLE_HASH_INVALID');
END;

CREATE TRIGGER crawl_import_record_document_insert
BEFORE INSERT ON crawl_import_record
WHEN NEW.record_hash GLOB '*[^0-9a-f]*'
  OR NEW.content_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.content_arabic) IS NOT 'object'
  OR json_type(NEW.content_arabic, '$.content') IS NOT 'array'
  OR json_array_length(NEW.content_arabic, '$.content') < 1
  OR length(CAST(NEW.content_arabic AS BLOB)) > 2000000
  OR EXISTS (
    SELECT 1 FROM json_each(NEW.content_arabic, '$.content')
    WHERE type <> 'text'
  )
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECORD_DOCUMENT_INVALID');
END;

CREATE TRIGGER poem_source_revision_document_insert
BEFORE INSERT ON poem_source_revision
WHEN NEW.content_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.content_arabic) IS NOT 'object'
  OR json_type(NEW.content_arabic, '$.content') IS NOT 'array'
  OR json_array_length(NEW.content_arabic, '$.content') < 1
  OR length(CAST(NEW.content_arabic AS BLOB)) > 2000000
  OR EXISTS (
    SELECT 1 FROM json_each(NEW.content_arabic, '$.content')
    WHERE type <> 'text'
  )
BEGIN
  SELECT RAISE(ABORT, 'POEM_SOURCE_REVISION_DOCUMENT_INVALID');
END;

CREATE TRIGGER enrichment_artifact_document_insert
BEFORE INSERT ON enrichment_artifact
WHEN NEW.payload_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.payload) IS NOT 'object'
  OR length(CAST(NEW.payload AS BLOB)) > 2000000
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_ARTIFACT_DOCUMENT_INVALID');
END;

CREATE TRIGGER enrichment_validation_document_insert
BEFORE INSERT ON enrichment_validation
WHEN NEW.report_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.report) IS NOT 'object'
  OR length(CAST(NEW.report AS BLOB)) > 2000000
BEGIN
  SELECT RAISE(ABORT, 'ENRICHMENT_VALIDATION_DOCUMENT_INVALID');
END;

CREATE TRIGGER model_enrichment_artifact_document_insert
BEFORE INSERT ON model_enrichment_artifact
WHEN NEW.payload_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.payload) IS NOT 'object'
  OR length(CAST(NEW.payload AS BLOB)) > 2000000
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_ARTIFACT_DOCUMENT_INVALID');
END;

CREATE TRIGGER model_enrichment_validation_document_insert
BEFORE INSERT ON model_enrichment_validation
WHEN NEW.report_hash GLOB '*[^0-9a-f]*'
  OR json_type(NEW.report) IS NOT 'object'
  OR length(CAST(NEW.report AS BLOB)) > 2000000
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_VALIDATION_DOCUMENT_INVALID');
END;

CREATE TRIGGER crawl_import_receipt_canonical_hash_insert
BEFORE INSERT ON crawl_import_receipt
WHEN NEW.plan_hash GLOB '*[^0-9a-f]*'
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECEIPT_HASH_INVALID');
END;

CREATE TRIGGER source_author_alias_insert_guard
BEFORE INSERT ON source_author_alias
WHEN length(trim(NEW.alias_url)) = 0
  OR NEW.last_observed_at < NEW.first_observed_at
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_AUTHOR_ALIAS_INVALID');
END;

CREATE TRIGGER source_author_alias_update_guard
BEFORE UPDATE ON source_author_alias
WHEN NEW.source_author_id IS NOT OLD.source_author_id
  OR NEW.alias_url IS NOT OLD.alias_url
  OR NEW.first_observed_at IS NOT OLD.first_observed_at
  OR NEW.last_observed_at < OLD.last_observed_at
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_AUTHOR_ALIAS_IMMUTABLE');
END;

CREATE TRIGGER source_author_alias_delete_forbidden
BEFORE DELETE ON source_author_alias
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_AUTHOR_ALIAS_IMMUTABLE');
END;

CREATE TRIGGER crawl_import_bundle_supported_schema_insert
BEFORE INSERT ON crawl_import_bundle
WHEN NEW.schema_version NOT IN (1, 2)
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_SCHEMA_UNSUPPORTED');
END;

CREATE TRIGGER crawl_import_record_revision_envelope_insert
BEFORE INSERT ON crawl_import_record
WHEN (
    (SELECT schema_version FROM crawl_import_bundle WHERE id = NEW.bundle_id) = 1
    AND json_type(NEW.content_arabic, '$.titleArabic') IS NOT NULL
  )
  OR (
    (SELECT schema_version FROM crawl_import_bundle WHERE id = NEW.bundle_id) = 2
    AND (
      json_type(NEW.content_arabic, '$.titleArabic') IS NOT 'text'
      OR json_extract(NEW.content_arabic, '$.titleArabic') <> NEW.title_arabic
    )
  )
BEGIN
  SELECT RAISE(ABORT, 'CRAWL_IMPORT_RECORD_SCHEMA_MISMATCH');
END;

CREATE TRIGGER poem_source_revision_revision_envelope_insert
BEFORE INSERT ON poem_source_revision
WHEN (
    NEW.schema_version = 1
    AND json_type(NEW.content_arabic, '$.titleArabic') IS NOT NULL
  )
  OR (
    NEW.schema_version = 2
    AND (
      json_type(NEW.content_arabic, '$.titleArabic') IS NOT 'text'
      OR json_extract(NEW.content_arabic, '$.titleArabic') <> NEW.title_arabic
    )
  )
  OR NEW.schema_version NOT IN (1, 2)
BEGIN
  SELECT RAISE(ABORT, 'POEM_SOURCE_REVISION_SCHEMA_MISMATCH');
END;

CREATE TRIGGER model_enrichment_artifact_profile_required
BEFORE INSERT ON model_enrichment_artifact
WHEN (
  SELECT count(*)
  FROM enrichment_profile profile
  JOIN poem_source_revision revision
    ON revision.id = NEW.source_revision_id
  WHERE profile.public_track_key = NEW.model_key
    AND profile.runtime_model_id = NEW.model
    AND profile.prompt_version = NEW.prompt_version
    AND profile.reasoning_effort = NEW.reasoning_effort
    AND profile.input_schema_version = revision.schema_version
    AND profile.output_schema_version = NEW.schema_version
) <> 1
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_PROFILE_INVALID');
END;

CREATE TRIGGER model_enrichment_artifact_profile_bind
AFTER INSERT ON model_enrichment_artifact
BEGIN
  INSERT INTO model_enrichment_artifact_profile (
    artifact_id, profile_key, bound_at
  )
  SELECT NEW.id, profile.profile_key, NEW.created_at
  FROM enrichment_profile profile
  JOIN poem_source_revision revision
    ON revision.id = NEW.source_revision_id
  WHERE profile.public_track_key = NEW.model_key
    AND profile.runtime_model_id = NEW.model
    AND profile.prompt_version = NEW.prompt_version
    AND profile.reasoning_effort = NEW.reasoning_effort
    AND profile.input_schema_version = revision.schema_version
    AND profile.output_schema_version = NEW.schema_version
  ON CONFLICT (artifact_id) DO NOTHING;
END;

CREATE TRIGGER model_publication_profile_insert_guard
BEFORE INSERT ON poem_model_publication_pointer
WHEN NOT EXISTS (
  SELECT 1
  FROM model_enrichment_artifact_profile binding
  JOIN enrichment_profile profile ON profile.profile_key = binding.profile_key
  WHERE binding.artifact_id = NEW.enrichment_artifact_id
    AND profile.public_track_key = NEW.model_key
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_PROFILE_INVALID');
END;

CREATE TRIGGER model_publication_profile_update_guard
BEFORE UPDATE ON poem_model_publication_pointer
WHEN NOT EXISTS (
  SELECT 1
  FROM model_enrichment_artifact_profile binding
  JOIN enrichment_profile profile ON profile.profile_key = binding.profile_key
  WHERE binding.artifact_id = NEW.enrichment_artifact_id
    AND profile.public_track_key = NEW.model_key
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_PROFILE_INVALID');
END;

CREATE TRIGGER poem_legacy_payload_attribution_relationship_insert
BEFORE INSERT ON poem_legacy_payload_attribution
WHEN (NEW.legacy_field IN ('translation', 'insights')
      AND NEW.attribution_key <> 'legacy-claude-1-or-2')
  OR (NEW.legacy_field = 'translation_gemini'
      AND NEW.attribution_key <> 'legacy-gemini-unknown')
BEGIN
  SELECT RAISE(ABORT, 'LEGACY_PAYLOAD_ATTRIBUTION_INVALID');
END;

CREATE TRIGGER ai_vendor_immutable_update BEFORE UPDATE ON ai_vendor
BEGIN SELECT RAISE(ABORT, 'AI_VENDOR_IMMUTABLE'); END;

CREATE TRIGGER ai_vendor_immutable_delete BEFORE DELETE ON ai_vendor
BEGIN SELECT RAISE(ABORT, 'AI_VENDOR_IMMUTABLE'); END;

CREATE TRIGGER ai_model_immutable_update BEFORE UPDATE ON ai_model
BEGIN SELECT RAISE(ABORT, 'AI_MODEL_IMMUTABLE'); END;

CREATE TRIGGER ai_model_immutable_delete BEFORE DELETE ON ai_model
BEGIN SELECT RAISE(ABORT, 'AI_MODEL_IMMUTABLE'); END;

CREATE TRIGGER inference_backend_immutable_update BEFORE UPDATE ON inference_backend
BEGIN SELECT RAISE(ABORT, 'INFERENCE_BACKEND_IMMUTABLE'); END;

CREATE TRIGGER inference_backend_immutable_delete BEFORE DELETE ON inference_backend
BEGIN SELECT RAISE(ABORT, 'INFERENCE_BACKEND_IMMUTABLE'); END;

CREATE TRIGGER enrichment_profile_immutable_update BEFORE UPDATE ON enrichment_profile
BEGIN SELECT RAISE(ABORT, 'ENRICHMENT_PROFILE_IMMUTABLE'); END;

CREATE TRIGGER enrichment_profile_immutable_delete BEFORE DELETE ON enrichment_profile
BEGIN SELECT RAISE(ABORT, 'ENRICHMENT_PROFILE_IMMUTABLE'); END;

CREATE TRIGGER model_enrichment_artifact_profile_immutable_update
BEFORE UPDATE ON model_enrichment_artifact_profile
BEGIN SELECT RAISE(ABORT, 'ARTIFACT_PROFILE_IMMUTABLE'); END;

CREATE TRIGGER model_enrichment_artifact_profile_immutable_delete
BEFORE DELETE ON model_enrichment_artifact_profile
BEGIN SELECT RAISE(ABORT, 'ARTIFACT_PROFILE_IMMUTABLE'); END;

CREATE TRIGGER legacy_model_attribution_immutable_update
BEFORE UPDATE ON legacy_model_attribution
BEGIN SELECT RAISE(ABORT, 'LEGACY_MODEL_ATTRIBUTION_IMMUTABLE'); END;

CREATE TRIGGER legacy_model_attribution_immutable_delete
BEFORE DELETE ON legacy_model_attribution
BEGIN SELECT RAISE(ABORT, 'LEGACY_MODEL_ATTRIBUTION_IMMUTABLE'); END;

CREATE TRIGGER poem_legacy_payload_attribution_immutable_update
BEFORE UPDATE ON poem_legacy_payload_attribution
BEGIN SELECT RAISE(ABORT, 'LEGACY_PAYLOAD_ATTRIBUTION_IMMUTABLE'); END;

CREATE TRIGGER poem_legacy_payload_attribution_immutable_delete
BEFORE DELETE ON poem_legacy_payload_attribution
BEGIN SELECT RAISE(ABORT, 'LEGACY_PAYLOAD_ATTRIBUTION_IMMUTABLE'); END;

CREATE TRIGGER scraper_writer_control_insert_forbidden
BEFORE INSERT ON scraper_writer_control
WHEN EXISTS (SELECT 1 FROM scraper_writer_control WHERE singleton = 1)
BEGIN
  SELECT RAISE(ABORT, 'SCRAPER_WRITER_CONTROL_SINGLETON');
END;

CREATE TRIGGER scraper_writer_control_update_guard
BEFORE UPDATE ON scraper_writer_control
WHEN NEW.singleton <> 1
  OR NEW.singleton <> OLD.singleton
  OR NEW.updated_at < OLD.updated_at
  OR NEW.writer_epoch < OLD.writer_epoch
  OR NEW.writer_epoch > OLD.writer_epoch + 1
  OR (
    NEW.writer_epoch = OLD.writer_epoch
    AND NEW.writer_id IS NOT OLD.writer_id
  )
  OR (
    NEW.writer_epoch = OLD.writer_epoch + 1
    AND (NEW.writer_id IS NULL OR length(trim(NEW.writer_id)) = 0)
  )
BEGIN
  SELECT RAISE(ABORT, 'SCRAPER_WRITER_CONTROL_UPDATE_INVALID');
END;

CREATE TRIGGER scraper_writer_control_delete_forbidden
BEFORE DELETE ON scraper_writer_control
BEGIN
  SELECT RAISE(ABORT, 'SCRAPER_WRITER_CONTROL_IMMUTABLE');
END;

CREATE TRIGGER model_enrichment_word_gloss_v2_shape
BEFORE INSERT ON model_enrichment_artifact
WHEN NEW.schema_version = 2 AND (
  json_type(NEW.payload) <> 'object'
  OR (SELECT count(*) FROM json_each(NEW.payload)) <> 4
  OR json_extract(NEW.payload, '$.schemaId') <> 'saqi.poem-enrichment-output'
  OR json_type(NEW.payload, '$.schemaVersion') <> 'integer'
  OR json_extract(NEW.payload, '$.schemaVersion') <> 2
  OR json_type(NEW.payload, '$.translation') <> 'object'
  OR json_type(NEW.payload, '$.translation.lines') <> 'array'
  OR json_array_length(NEW.payload, '$.translation.lines') NOT BETWEEN 1 AND 2000
  OR json_type(NEW.payload, '$.wordGlosses') <> 'object'
  OR json_extract(NEW.payload, '$.wordGlosses.tokenizerVersion')
       <> 'saqi-orthographic-v1'
  OR json_type(NEW.payload, '$.wordGlosses.lines') <> 'array'
  OR json_array_length(NEW.payload, '$.wordGlosses.lines') NOT BETWEEN 1 AND 2000
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_ENRICHMENT_WORD_GLOSS_V2_INVALID');
END;

CREATE TRIGGER source_admission_clock_immutable_update
BEFORE UPDATE ON source_admission_clock
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_ADMISSION_CLOCK_IMMUTABLE');
END;

CREATE TRIGGER source_admission_clock_immutable_delete
BEFORE DELETE ON source_admission_clock
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_ADMISSION_CLOCK_IMMUTABLE');
END;

CREATE TRIGGER source_revision_fingerprint_immutable_update
BEFORE UPDATE ON source_revision_fingerprint
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_REVISION_FINGERPRINT_IMMUTABLE');
END;

CREATE TRIGGER source_revision_fingerprint_immutable_delete
BEFORE DELETE ON source_revision_fingerprint
BEGIN
  SELECT RAISE(ABORT, 'SOURCE_REVISION_FINGERPRINT_IMMUTABLE');
END;

CREATE TRIGGER model_publication_receipt_pointer_precondition
BEFORE INSERT ON model_publication_receipt
WHEN NEW.outcome = 'published' AND (
  NEW.writer_epoch <> (
    SELECT writer_epoch FROM scraper_writer_control WHERE singleton = 1
  )
  OR (
    NEW.expected_pointer_version IS NULL
    AND (
      NEW.pointer_version <> 1
      OR EXISTS (
        SELECT 1 FROM poem_model_publication_pointer
        WHERE poem_id = NEW.poem_id AND model_key = NEW.model_key
      )
    )
  )
  OR (
    NEW.expected_pointer_version IS NOT NULL
    AND (
      NEW.pointer_version <> NEW.expected_pointer_version + 1
      OR NOT EXISTS (
        SELECT 1 FROM poem_model_publication_pointer
        WHERE poem_id = NEW.poem_id
          AND model_key = NEW.model_key
          AND pointer_version = NEW.expected_pointer_version
      )
    )
  )
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_RECEIPT_POINTER_CONFLICT');
END;

CREATE TRIGGER model_publication_receipt_relationship_insert
BEFORE INSERT ON model_publication_receipt
WHEN NEW.outcome = 'already-published' AND NOT EXISTS (
  SELECT 1
  FROM poem_model_publication_pointer publication
  JOIN model_enrichment_artifact artifact
    ON artifact.id = publication.enrichment_artifact_id
  WHERE publication.poem_id = NEW.poem_id
    AND publication.model_key = NEW.model_key
    AND publication.source_revision_id = NEW.source_revision_id
    AND publication.enrichment_artifact_id = NEW.enrichment_artifact_id
    AND publication.pointer_version = NEW.pointer_version
    AND publication.writer_epoch = NEW.writer_epoch
    AND artifact.source_revision_id = NEW.source_revision_id
    AND artifact.model_key = NEW.model_key
)
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_RECEIPT_RELATIONSHIP_INVALID');
END;

CREATE TRIGGER model_publication_receipt_immutable_update
BEFORE UPDATE ON model_publication_receipt
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_RECEIPT_IMMUTABLE');
END;

CREATE TRIGGER model_publication_receipt_immutable_delete
BEFORE DELETE ON model_publication_receipt
BEGIN
  SELECT RAISE(ABORT, 'MODEL_PUBLICATION_RECEIPT_IMMUTABLE');
END;

CREATE TRIGGER legacy_sol_model_pointer_create_only
BEFORE UPDATE OF enrichment_artifact_id ON poem_model_publication_pointer
WHEN NEW.model_key = 'sol-5.6'
  AND NEW.enrichment_artifact_id IS NOT OLD.enrichment_artifact_id
  AND EXISTS (
    SELECT 1
    FROM model_enrichment_artifact_profile binding
    JOIN enrichment_profile profile ON profile.profile_key = binding.profile_key
    WHERE binding.artifact_id = NEW.enrichment_artifact_id
      AND profile.public_track_key = 'sol-5.6'
      AND profile.prompt_version = 'sol-enrichment-v1'
  )
BEGIN
  SELECT RAISE(ABORT, 'LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER');
END;

CREATE TRIGGER legacy_sol_publication_receipt_create_only
BEFORE INSERT ON model_publication_receipt
WHEN NEW.outcome = 'published'
  AND NEW.model_key = 'sol-5.6'
  AND NEW.prompt_version = 'sol-enrichment-v1'
  AND EXISTS (
    SELECT 1
    FROM poem_model_publication_pointer publication
    WHERE publication.poem_id = NEW.poem_id
      AND publication.model_key = NEW.model_key
      AND publication.enrichment_artifact_id IS NOT NEW.enrichment_artifact_id
  )
BEGIN
  SELECT RAISE(ABORT, 'LEGACY_SOL_PUBLICATION_REQUIRES_EMPTY_POINTER');
END;

CREATE TRIGGER model_publication_receipt_publish
AFTER INSERT ON model_publication_receipt
WHEN NEW.outcome = 'published'
BEGIN
  UPDATE poem_model_publication_pointer
  SET source_revision_id = NEW.source_revision_id,
      enrichment_artifact_id = NEW.enrichment_artifact_id,
      pointer_version = NEW.pointer_version,
      writer_epoch = NEW.writer_epoch,
      updated_at = NEW.committed_at
  WHERE poem_id = NEW.poem_id
    AND model_key = NEW.model_key
    AND pointer_version = NEW.expected_pointer_version;

  INSERT INTO poem_model_publication_pointer (
    poem_id, model_key, source_revision_id, enrichment_artifact_id,
    pointer_version, writer_epoch, updated_at
  )
  SELECT NEW.poem_id, NEW.model_key, NEW.source_revision_id,
    NEW.enrichment_artifact_id, NEW.pointer_version,
    NEW.writer_epoch, NEW.committed_at
  WHERE NEW.expected_pointer_version IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM poem_model_publication_pointer
      WHERE poem_id = NEW.poem_id AND model_key = NEW.model_key
    )
  ON CONFLICT(poem_id, model_key) DO NOTHING;
END;

-- New generation recipe; preserve every historical profile and publication.
INSERT INTO enrichment_profile ( -- sarj-noqa: SARJ105 -- Versioned migration must fail on preexisting immutable profile identities, not hide provenance drift.
  profile_key, public_track_key, model_key, backend_key, runtime_model_id,
  prompt_version, reasoning_effort, input_schema_version, output_schema_version,
  created_at
) VALUES
  ('sol-5.6/word-gloss-v3/source-v1', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-word-gloss-v3', 'medium', 1, 2, 0),
  ('sol-5.6/word-gloss-v3/source-v2', 'sol-5.6', 'gpt-5.6-sol', 'openai-codex-cli', 'gpt-5.6-sol', 'sol-word-gloss-v3', 'medium', 2, 2, 0);

-- A delayed historical completion must not replace the newer recipe for the
-- same source revision, including publication through the receipt trigger.
CREATE TRIGGER sol_model_pointer_prevent_recipe_downgrade
BEFORE UPDATE OF enrichment_artifact_id ON poem_model_publication_pointer
WHEN NEW.model_key = 'sol-5.6'
  AND NEW.source_revision_id = OLD.source_revision_id
  AND EXISTS (
    SELECT 1 FROM model_enrichment_artifact incoming
    JOIN model_enrichment_artifact current ON current.id = OLD.enrichment_artifact_id
    WHERE incoming.id = NEW.enrichment_artifact_id
      AND incoming.prompt_version = 'sol-word-gloss-v2'
      AND current.prompt_version = 'sol-word-gloss-v3'
  )
BEGIN
  SELECT RAISE(ABORT, 'LEGACY_SOL_PUBLICATION_SUPERSEDED');
END;

-- Bounded, resumable maintenance authority for adopting source lineage on
-- records that predate source identities. The singleton lease fences every
-- cursor update; conflicts are durable and require an explicit resolution.
CREATE TABLE IF NOT EXISTS source_lineage_maintenance_job (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  state TEXT NOT NULL CHECK (state IN ('idle', 'active', 'blocked', 'failed', 'complete')),
  cursor_poem_id TEXT,
  pass INTEGER NOT NULL DEFAULT 0 CHECK (pass >= 0),
  lease_owner TEXT,
  lease_token TEXT,
  lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
  lease_expires_at INTEGER,
  scanned_total INTEGER NOT NULL DEFAULT 0 CHECK (scanned_total >= 0),
  adopted_total INTEGER NOT NULL DEFAULT 0 CHECK (adopted_total >= 0),
  last_error_code TEXT,
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
  FOREIGN KEY (cursor_poem_id) REFERENCES poem(id) ON DELETE RESTRICT,
  CHECK (
    (lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
    OR
    (lease_owner IS NOT NULL AND length(lease_owner) BETWEEN 1 AND 128
      AND lease_token IS NOT NULL AND length(lease_token) = 36
      AND lease_expires_at IS NOT NULL AND lease_expires_at >= 0)
  )
) STRICT;

INSERT INTO source_lineage_maintenance_job (
  singleton, state, cursor_poem_id, pass, lease_owner, lease_token,
  lease_epoch, lease_expires_at, scanned_total, adopted_total,
  last_error_code, updated_at
) VALUES (1, 'idle', NULL, 0, NULL, NULL, 0, NULL, 0, 0, NULL, unixepoch())
ON CONFLICT(singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS source_lineage_conflict (
  poem_id TEXT PRIMARY KEY,
  error_code TEXT NOT NULL CHECK (
    length(error_code) BETWEEN 3 AND 100
    AND error_code NOT GLOB '*[^A-Z0-9_]*'
  ),
  first_seen_at INTEGER NOT NULL CHECK (first_seen_at >= 0),
  last_seen_at INTEGER NOT NULL CHECK (last_seen_at >= first_seen_at),
  attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
  lease_epoch INTEGER NOT NULL CHECK (lease_epoch > 0),
  resolved_at INTEGER CHECK (resolved_at IS NULL OR resolved_at >= last_seen_at),
  FOREIGN KEY (poem_id) REFERENCES poem(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX IF NOT EXISTS source_lineage_conflict_unresolved -- sarj-noqa: SARJ108, SARJ116 — SQLite lacks CONCURRENTLY; the status endpoint counts unresolved conflicts on every poll, so this partial covering index avoids scanning resolved history.
ON source_lineage_conflict(poem_id)
WHERE resolved_at IS NULL;
