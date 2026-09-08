/** Canonical fresh SQLite schema. Historical ledgers must already be at v35. */
export const CURRENT_SCHEMA_VERSION = 36;

export const FRESH_BASELINE_SQL = `
CREATE TABLE local_schema (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  version INTEGER NOT NULL CHECK(version = 36)
) STRICT;
CREATE TABLE local_source_identity (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        source_name TEXT NOT NULL CHECK(
          length(source_name) BETWEEN 2 AND 64
          AND source_name GLOB '[a-z]*'
          AND source_name NOT GLOB '*[^a-z0-9_-]*'
        ),
        source_origin TEXT NOT NULL CHECK(
          length(source_origin) BETWEEN 9 AND 2048
          AND source_origin GLOB 'https://*'
        )
      ) STRICT;

CREATE TABLE work_item (
        work_key TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        input_json TEXT NOT NULL,
        input_hash TEXT NOT NULL CHECK(length(input_hash) = 64),
        schema_version TEXT NOT NULL,
        implementation_version TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','running','retry_wait','quota_wait','succeeded','dead_letter','imported')),
        available_at INTEGER NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
        lease_owner TEXT,
        lease_token TEXT,
        lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
        lease_expires_at INTEGER,
        output_artifact_hash TEXT,
        last_error_code TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        CHECK((state = 'running') = (lease_token IS NOT NULL)),
        CHECK((lease_token IS NULL) = (lease_owner IS NULL)),
        CHECK((lease_token IS NULL) = (lease_expires_at IS NULL))
      ) STRICT;

CREATE INDEX work_item_claimable ON work_item(state, available_at, priority DESC, created_at, work_key);

CREATE TABLE work_event (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        attempt_id TEXT,
        lease_epoch INTEGER,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
      ) STRICT;

CREATE INDEX work_event_work ON work_event(work_key, sequence);

CREATE TABLE checkpoint (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        checkpoint_id TEXT NOT NULL UNIQUE,
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        attempt_id TEXT NOT NULL,
        lease_epoch INTEGER NOT NULL,
        kind TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        artifact_hash TEXT,
        created_at INTEGER NOT NULL
      ) STRICT;

CREATE INDEX checkpoint_work ON checkpoint(work_key, sequence);

CREATE TABLE origin_gate (
        origin TEXT PRIMARY KEY,
        active_token TEXT UNIQUE,
        lease_epoch INTEGER NOT NULL DEFAULT 0,
        lease_expires_at INTEGER,
        next_allowed_at INTEGER NOT NULL DEFAULT 0,
        last_completed_at INTEGER,
        updated_at INTEGER NOT NULL, cooldown_until INTEGER NOT NULL DEFAULT 0, consecutive_failures INTEGER NOT NULL DEFAULT 0
        CHECK(consecutive_failures >= 0), stop_reason TEXT,
        CHECK((active_token IS NULL) = (lease_expires_at IS NULL))
      ) STRICT;

CREATE INDEX work_item_expired_lease
        ON work_item(lease_expires_at, work_key)
        WHERE state = 'running';

CREATE INDEX work_item_worker_claim
        ON work_item(kind, implementation_version, schema_version, state,
                     available_at, priority DESC, created_at, work_key);

CREATE INDEX checkpoint_latest_kind
        ON checkpoint(work_key, kind, sequence DESC);

CREATE INDEX work_item_output_artifact
        ON work_item(output_artifact_hash)
        WHERE output_artifact_hash IS NOT NULL;

CREATE INDEX checkpoint_artifact
        ON checkpoint(artifact_hash)
        WHERE artifact_hash IS NOT NULL;

CREATE INDEX origin_gate_ready
        ON origin_gate(stop_reason, cooldown_until, next_allowed_at);

CREATE INDEX work_item_completed_input
        ON work_item(kind, schema_version, input_hash, updated_at DESC)
        WHERE state IN ('succeeded','imported') AND output_artifact_hash IS NOT NULL;

CREATE INDEX work_event_completed_scan
        ON work_event(event_type, sequence, work_key);

CREATE INDEX work_item_error_code
        ON work_item(last_error_code)
        WHERE last_error_code IS NOT NULL;

CREATE INDEX work_item_ready_priority
        ON work_item(kind, implementation_version, schema_version,
                     priority DESC, created_at, work_key)
        WHERE state IN ('pending','retry_wait','quota_wait');

CREATE TRIGGER work_item_definition_reject_update
      BEFORE UPDATE OF work_key, kind, input_json, input_hash,
        schema_version, implementation_version ON work_item
      WHEN NEW.work_key IS NOT OLD.work_key
        OR NEW.kind IS NOT OLD.kind
        OR NEW.input_json IS NOT OLD.input_json
        OR NEW.input_hash IS NOT OLD.input_hash
        OR NEW.schema_version IS NOT OLD.schema_version
        OR NEW.implementation_version IS NOT OLD.implementation_version
      BEGIN
        SELECT RAISE(ABORT, 'WORK_ITEM_DEFINITION_IMMUTABLE');
      END;

CREATE TRIGGER work_item_definition_reject_invalid_insert
      BEFORE INSERT ON work_item
      WHEN CASE
          WHEN json_valid(NEW.input_json)
            THEN json_type(NEW.input_json) IS NOT 'object'
          ELSE 1
        END
        OR length(NEW.work_key) <> 64
        OR NEW.work_key GLOB '*[^0-9a-f]*'
        OR length(NEW.input_hash) <> 64
        OR NEW.input_hash GLOB '*[^0-9a-f]*'
        OR length(trim(NEW.kind)) NOT BETWEEN 1 AND 100
        OR length(trim(NEW.schema_version)) NOT BETWEEN 1 AND 100
        OR length(trim(NEW.implementation_version)) NOT BETWEEN 1 AND 100
        OR NEW.priority NOT BETWEEN -1000000 AND 1000000
      BEGIN
        SELECT RAISE(ABORT, 'WORK_ITEM_DEFINITION_INVALID');
      END;

CREATE TRIGGER work_item_priority_reject_invalid_update
      BEFORE UPDATE OF priority ON work_item
      WHEN NEW.priority NOT BETWEEN -1000000 AND 1000000
      BEGIN
        SELECT RAISE(ABORT, 'WORK_ITEM_DEFINITION_INVALID');
      END;

CREATE TRIGGER work_event_reject_update
      BEFORE UPDATE ON work_event
      BEGIN
        SELECT RAISE(ABORT, 'WORK_EVENT_IMMUTABLE');
      END;

CREATE TRIGGER work_event_reject_delete
      BEFORE DELETE ON work_event
      BEGIN
        SELECT RAISE(ABORT, 'WORK_EVENT_IMMUTABLE');
      END;

CREATE TRIGGER checkpoint_reject_update
      BEFORE UPDATE ON checkpoint
      BEGIN
        SELECT RAISE(ABORT, 'CHECKPOINT_IMMUTABLE');
      END;

CREATE TRIGGER checkpoint_reject_delete
      BEFORE DELETE ON checkpoint
      BEGIN
        SELECT RAISE(ABORT, 'CHECKPOINT_IMMUTABLE');
      END;

CREATE INDEX work_item_unknown_recovery
        ON work_item(kind, implementation_version, schema_version,
                     updated_at, created_at, work_key)
        WHERE state IN ('pending','retry_wait','quota_wait','dead_letter')
          AND last_error_code = 'CODEX_OPERATION_OUTCOME_UNKNOWN';

CREATE INDEX work_item_profile_state
        ON work_item(kind, implementation_version, schema_version, state);

CREATE INDEX work_item_kind_error_code
        ON work_item(kind, last_error_code)
        WHERE last_error_code IS NOT NULL;

CREATE TABLE scheduler_state (
        state_key TEXT PRIMARY KEY CHECK(length(trim(state_key)) BETWEEN 1 AND 128),
        state_json TEXT NOT NULL CHECK(json_valid(state_json)),
        state_digest TEXT NOT NULL CHECK(
          length(state_digest) = 64
          AND state_digest NOT GLOB '*[^0-9a-f]*'
        ),
        updated_at INTEGER NOT NULL CHECK(updated_at >= 0)
      ) STRICT;

CREATE TABLE paid_operation_reconciliation (
        operation_key TEXT PRIMARY KEY CHECK(
          length(operation_key) = 64
          AND operation_key NOT GLOB '*[^0-9a-f]*'
        ),
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        attempt_id TEXT NOT NULL CHECK(length(trim(attempt_id)) BETWEEN 1 AND 128),
        state TEXT NOT NULL CHECK(state IN ('unknown','reconciled','quarantined')),
        next_reconcile_at INTEGER NOT NULL CHECK(next_reconcile_at >= 0),
        reconciliation_count INTEGER NOT NULL DEFAULT 0 CHECK(reconciliation_count >= 0),
        first_observed_at INTEGER NOT NULL CHECK(first_observed_at >= 0),
        last_reconciled_at INTEGER,
        updated_at INTEGER NOT NULL CHECK(updated_at >= first_observed_at),
        CHECK((state = 'reconciled') = (last_reconciled_at IS NOT NULL)),
        UNIQUE(work_key, attempt_id)
      ) STRICT;

CREATE INDEX paid_operation_reconciliation_due
        ON paid_operation_reconciliation(state, next_reconcile_at, updated_at)
        WHERE state IN ('unknown','quarantined');

CREATE TRIGGER paid_operation_identity_reject_update
      BEFORE UPDATE OF operation_key, work_key, attempt_id
      ON paid_operation_reconciliation
      WHEN NEW.operation_key IS NOT OLD.operation_key
        OR NEW.work_key IS NOT OLD.work_key
        OR NEW.attempt_id IS NOT OLD.attempt_id
      BEGIN
        SELECT RAISE(ABORT, 'PAID_OPERATION_IDENTITY_IMMUTABLE');
      END;

CREATE TABLE ledger_state_count (
        state TEXT PRIMARY KEY CHECK(state IN ('pending','running','retry_wait','quota_wait','succeeded','dead_letter','imported')),
        item_count INTEGER NOT NULL CHECK(item_count >= 0)
      ) STRICT;

CREATE TABLE ledger_kind_state_count (
        kind TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','running','retry_wait','quota_wait','succeeded','dead_letter','imported')),
        item_count INTEGER NOT NULL CHECK(item_count >= 0),
        PRIMARY KEY(kind, state)
      ) STRICT;

CREATE TABLE ledger_profile_state_count (
        kind TEXT NOT NULL,
        implementation_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','running','retry_wait','quota_wait','succeeded','dead_letter','imported')),
        item_count INTEGER NOT NULL CHECK(item_count >= 0),
        PRIMARY KEY(kind, implementation_version, schema_version, state)
      ) STRICT;

CREATE TABLE ledger_profile_availability_count (
        kind TEXT NOT NULL,
        implementation_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        available_at INTEGER NOT NULL,
        item_count INTEGER NOT NULL CHECK(item_count >= 0),
        PRIMARY KEY(kind, implementation_version, schema_version, available_at)
      ) STRICT;

CREATE INDEX ledger_profile_availability_time
        ON ledger_profile_availability_count(available_at, kind);

CREATE TABLE ledger_error_count (
        error_code TEXT PRIMARY KEY,
        item_count INTEGER NOT NULL CHECK(item_count >= 0)
      ) STRICT;

CREATE TABLE ledger_kind_error_count (
        kind TEXT NOT NULL,
        error_code TEXT NOT NULL,
        item_count INTEGER NOT NULL CHECK(item_count >= 0),
        PRIMARY KEY(kind, error_code)
      ) STRICT;

CREATE TABLE ledger_profile_error_count (
        kind TEXT NOT NULL,
        implementation_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        error_code TEXT NOT NULL,
        item_count INTEGER NOT NULL CHECK(item_count >= 0),
        PRIMARY KEY(
          kind, implementation_version, schema_version, error_code
        )
      ) STRICT;

CREATE TABLE ledger_status_clock (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        last_success_at INTEGER,
        last_failure_at INTEGER
      ) STRICT;

CREATE TABLE ledger_kind_success_clock (
        kind TEXT PRIMARY KEY,
        last_success_at INTEGER NOT NULL CHECK(last_success_at >= 0)
      ) STRICT;

CREATE TABLE ledger_profile_success_clock (
        kind TEXT NOT NULL,
        implementation_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        last_success_at INTEGER NOT NULL CHECK(last_success_at >= 0),
        PRIMARY KEY(kind, implementation_version, schema_version)
      ) STRICT;

CREATE TABLE paid_operation_state_count (
        state TEXT PRIMARY KEY CHECK(state IN ('unknown','reconciled','quarantined')),
        item_count INTEGER NOT NULL CHECK(item_count >= 0)
      ) STRICT;

CREATE TRIGGER ledger_status_work_insert
      AFTER INSERT ON work_item
      BEGIN
        INSERT INTO ledger_state_count(state, item_count) VALUES(NEW.state, 1)
          ON CONFLICT(state) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_kind_state_count(kind, state, item_count)
          VALUES(NEW.kind, NEW.state, 1)
          ON CONFLICT(kind, state) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_state_count(
          kind, implementation_version, schema_version, state, item_count
        ) VALUES(NEW.kind, NEW.implementation_version, NEW.schema_version, NEW.state, 1)
          ON CONFLICT(kind, implementation_version, schema_version, state)
          DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_availability_count(
          kind, implementation_version, schema_version, available_at, item_count
        )
          SELECT NEW.kind, NEW.implementation_version, NEW.schema_version,
                 NEW.available_at, 1
          WHERE NEW.state IN ('pending','retry_wait','quota_wait')
          ON CONFLICT(kind, implementation_version, schema_version, available_at)
          DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_error_count(error_code, item_count)
          SELECT NEW.last_error_code, 1 WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(error_code) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_kind_error_count(kind, error_code, item_count)
          SELECT NEW.kind, NEW.last_error_code, 1 WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(kind, error_code) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_error_count(
          kind, implementation_version, schema_version, error_code, item_count
        )
          SELECT NEW.kind, NEW.implementation_version, NEW.schema_version,
                 NEW.last_error_code, 1
          WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(kind, implementation_version, schema_version, error_code)
          DO UPDATE SET item_count = item_count + 1;
      END;

CREATE TRIGGER ledger_status_work_update
      AFTER UPDATE OF state, kind, implementation_version, schema_version,
                      available_at, last_error_code ON work_item
      BEGIN
        UPDATE ledger_state_count SET item_count = item_count - 1 WHERE state = OLD.state;
        DELETE FROM ledger_state_count WHERE state = OLD.state AND item_count = 0;
        UPDATE ledger_kind_state_count SET item_count = item_count - 1
          WHERE kind = OLD.kind AND state = OLD.state;
        DELETE FROM ledger_kind_state_count
          WHERE kind = OLD.kind AND state = OLD.state AND item_count = 0;
        UPDATE ledger_profile_state_count SET item_count = item_count - 1
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version AND state = OLD.state;
        DELETE FROM ledger_profile_state_count
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version AND state = OLD.state AND item_count = 0;
        UPDATE ledger_profile_availability_count SET item_count = item_count - 1
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version AND available_at = OLD.available_at
            AND OLD.state IN ('pending','retry_wait','quota_wait');
        DELETE FROM ledger_profile_availability_count
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version AND available_at = OLD.available_at
            AND item_count = 0;
        UPDATE ledger_error_count SET item_count = item_count - 1
          WHERE error_code = OLD.last_error_code;
        DELETE FROM ledger_error_count
          WHERE error_code = OLD.last_error_code AND item_count = 0;
        UPDATE ledger_kind_error_count SET item_count = item_count - 1
          WHERE kind = OLD.kind AND error_code = OLD.last_error_code;
        DELETE FROM ledger_kind_error_count
          WHERE kind = OLD.kind AND error_code = OLD.last_error_code AND item_count = 0;
        UPDATE ledger_profile_error_count SET item_count = item_count - 1
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version
            AND error_code = OLD.last_error_code;
        DELETE FROM ledger_profile_error_count
          WHERE kind = OLD.kind AND implementation_version = OLD.implementation_version
            AND schema_version = OLD.schema_version
            AND error_code = OLD.last_error_code AND item_count = 0;

        INSERT INTO ledger_state_count(state, item_count) VALUES(NEW.state, 1)
          ON CONFLICT(state) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_kind_state_count(kind, state, item_count)
          VALUES(NEW.kind, NEW.state, 1)
          ON CONFLICT(kind, state) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_state_count(
          kind, implementation_version, schema_version, state, item_count
        ) VALUES(NEW.kind, NEW.implementation_version, NEW.schema_version, NEW.state, 1)
          ON CONFLICT(kind, implementation_version, schema_version, state)
          DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_availability_count(
          kind, implementation_version, schema_version, available_at, item_count
        )
          SELECT NEW.kind, NEW.implementation_version, NEW.schema_version,
                 NEW.available_at, 1
          WHERE NEW.state IN ('pending','retry_wait','quota_wait')
          ON CONFLICT(kind, implementation_version, schema_version, available_at)
          DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_error_count(error_code, item_count)
          SELECT NEW.last_error_code, 1 WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(error_code) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_kind_error_count(kind, error_code, item_count)
          SELECT NEW.kind, NEW.last_error_code, 1 WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(kind, error_code) DO UPDATE SET item_count = item_count + 1;
        INSERT INTO ledger_profile_error_count(
          kind, implementation_version, schema_version, error_code, item_count
        )
          SELECT NEW.kind, NEW.implementation_version, NEW.schema_version,
                 NEW.last_error_code, 1
          WHERE NEW.last_error_code IS NOT NULL
          ON CONFLICT(kind, implementation_version, schema_version, error_code)
          DO UPDATE SET item_count = item_count + 1;
      END;

CREATE TRIGGER ledger_status_event_insert
      AFTER INSERT ON work_event
      BEGIN
        UPDATE ledger_status_clock
          SET last_success_at = NEW.created_at
          WHERE singleton = 1 AND NEW.event_type IN ('succeeded','imported');
        UPDATE ledger_status_clock
          SET last_failure_at = NEW.created_at
          WHERE singleton = 1 AND NEW.event_type IN ('retry_wait','quota_wait','dead_letter','lease_expired');
        INSERT INTO ledger_kind_success_clock(kind, last_success_at)
          SELECT kind, NEW.created_at FROM work_item
          WHERE work_key = NEW.work_key AND NEW.event_type IN ('succeeded','imported')
          ON CONFLICT(kind) DO UPDATE SET last_success_at = excluded.last_success_at;
        INSERT INTO ledger_profile_success_clock(
          kind, implementation_version, schema_version, last_success_at
        )
          SELECT kind, implementation_version, schema_version, NEW.created_at
          FROM work_item
          WHERE work_key = NEW.work_key AND NEW.event_type IN ('succeeded','imported')
          ON CONFLICT(kind, implementation_version, schema_version)
          DO UPDATE SET last_success_at = excluded.last_success_at;
      END;

CREATE TRIGGER ledger_status_paid_operation_insert
      AFTER INSERT ON paid_operation_reconciliation
      BEGIN
        INSERT INTO paid_operation_state_count(state, item_count) VALUES(NEW.state, 1)
          ON CONFLICT(state) DO UPDATE SET item_count = item_count + 1;
      END;

CREATE TRIGGER ledger_status_paid_operation_update
      AFTER UPDATE OF state ON paid_operation_reconciliation
      WHEN NEW.state IS NOT OLD.state
      BEGIN
        UPDATE paid_operation_state_count SET item_count = item_count - 1
          WHERE state = OLD.state;
        DELETE FROM paid_operation_state_count
          WHERE state = OLD.state AND item_count = 0;
        INSERT INTO paid_operation_state_count(state, item_count) VALUES(NEW.state, 1)
          ON CONFLICT(state) DO UPDATE SET item_count = item_count + 1;
      END;

CREATE TABLE canonical_translation_binding (
        translation_work_key TEXT NOT NULL REFERENCES work_item(work_key),
        binding_id TEXT NOT NULL CHECK(
          length(binding_id) = 64 AND binding_id NOT GLOB '*[^0-9a-f]*'
        ),
        binding_json TEXT NOT NULL CHECK(
          json_valid(binding_json) AND json_type(binding_json) = 'object'
        ),
        poem_id TEXT NOT NULL CHECK(length(trim(poem_id)) BETWEEN 1 AND 200),
        source_revision_id TEXT NOT NULL CHECK(
          length(trim(source_revision_id)) BETWEEN 1 AND 200
        ),
        line_nfc_hash TEXT NOT NULL CHECK(
          length(line_nfc_hash) = 64 AND line_nfc_hash NOT GLOB '*[^0-9a-f]*'
        ),
        prompt_material_hash TEXT NOT NULL CHECK(
          length(prompt_material_hash) = 64
          AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
        ),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        PRIMARY KEY(translation_work_key),
        UNIQUE(translation_work_key, binding_id)
      ) STRICT;

CREATE INDEX canonical_translation_binding_identity
        ON canonical_translation_binding(binding_id, translation_work_key);

CREATE INDEX canonical_translation_binding_poem_revision
        ON canonical_translation_binding(poem_id, source_revision_id);

CREATE TABLE publication_derivation (
        translation_work_key TEXT NOT NULL,
        binding_id TEXT NOT NULL,
        publication_work_key TEXT NOT NULL UNIQUE REFERENCES work_item(work_key),
        approved_artifact_hash TEXT NOT NULL CHECK(
          length(approved_artifact_hash) = 64
          AND approved_artifact_hash NOT GLOB '*[^0-9a-f]*'
        ),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        PRIMARY KEY(translation_work_key),
        FOREIGN KEY(translation_work_key, binding_id)
          REFERENCES canonical_translation_binding(translation_work_key, binding_id)
      ) STRICT;

CREATE INDEX publication_derivation_binding
        ON publication_derivation(binding_id, publication_work_key);

CREATE TRIGGER canonical_translation_binding_reject_update
      BEFORE UPDATE ON canonical_translation_binding
      BEGIN
        SELECT RAISE(ABORT, 'CANONICAL_TRANSLATION_BINDING_IMMUTABLE');
      END;

CREATE TRIGGER canonical_translation_binding_reject_delete
      BEFORE DELETE ON canonical_translation_binding
      BEGIN
        SELECT RAISE(ABORT, 'CANONICAL_TRANSLATION_BINDING_IMMUTABLE');
      END;

CREATE TRIGGER publication_derivation_reject_update
      BEFORE UPDATE ON publication_derivation
      BEGIN
        SELECT RAISE(ABORT, 'PUBLICATION_DERIVATION_IMMUTABLE');
      END;

CREATE TRIGGER publication_derivation_reject_delete
      BEFORE DELETE ON publication_derivation
      BEGIN
        SELECT RAISE(ABORT, 'PUBLICATION_DERIVATION_IMMUTABLE');
      END;

CREATE TABLE checkpoint_attempt_reference (
        attempt_id TEXT NOT NULL CHECK(
          length(attempt_id) = 36
          AND substr(attempt_id, 9, 1) = '-'
          AND substr(attempt_id, 14, 1) = '-'
          AND substr(attempt_id, 19, 1) = '-'
          AND substr(attempt_id, 24, 1) = '-'
          AND lower(attempt_id) NOT GLOB '*[^0-9a-f-]*'
          AND substr(lower(attempt_id), 15, 1) BETWEEN '1' AND '8'
          AND substr(lower(attempt_id), 20, 1) IN ('8','9','a','b')
        ),
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        checkpoint_sequence INTEGER NOT NULL REFERENCES checkpoint(sequence),
        PRIMARY KEY(attempt_id, work_key, checkpoint_sequence)
      ) STRICT, WITHOUT ROWID;

CREATE INDEX checkpoint_attempt_reference_work
        ON checkpoint_attempt_reference(work_key, attempt_id);

CREATE TRIGGER checkpoint_attempt_reference_insert
      AFTER INSERT ON checkpoint
      BEGIN
        INSERT OR IGNORE INTO checkpoint_attempt_reference(
          attempt_id, work_key, checkpoint_sequence
        )
        SELECT DISTINCT attempt_reference.atom, NEW.work_key, NEW.sequence
          FROM json_tree(NEW.payload_json) AS attempt_reference
         WHERE attempt_reference.type = 'text'
           AND length(attempt_reference.atom) = 36
           AND substr(attempt_reference.atom, 9, 1) = '-'
           AND substr(attempt_reference.atom, 14, 1) = '-'
           AND substr(attempt_reference.atom, 19, 1) = '-'
           AND substr(attempt_reference.atom, 24, 1) = '-'
           AND lower(attempt_reference.atom) NOT GLOB '*[^0-9a-f-]*'
           AND substr(lower(attempt_reference.atom), 15, 1) BETWEEN '1' AND '8'
           AND substr(lower(attempt_reference.atom), 20, 1) IN ('8','9','a','b');
      END;

CREATE TRIGGER checkpoint_attempt_reference_reject_update
      BEFORE UPDATE ON checkpoint_attempt_reference
      BEGIN
        SELECT RAISE(ABORT, 'CHECKPOINT_ATTEMPT_REFERENCE_IMMUTABLE');
      END;

CREATE TRIGGER checkpoint_attempt_reference_reject_delete
      BEFORE DELETE ON checkpoint_attempt_reference
      BEGIN
        SELECT RAISE(ABORT, 'CHECKPOINT_ATTEMPT_REFERENCE_IMMUTABLE');
      END;

CREATE TABLE fanout_detail_material (
        detail_fanout_work_key TEXT PRIMARY KEY REFERENCES work_item(work_key),
        source_work_key TEXT NOT NULL REFERENCES work_item(work_key),
        prompt_material_hash TEXT NOT NULL CHECK(
          length(prompt_material_hash) = 64
          AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
        ),
        line_nfc_hash TEXT NOT NULL CHECK(
          length(line_nfc_hash) = 64
          AND line_nfc_hash NOT GLOB '*[^0-9a-f]*'
        ),
        created_at INTEGER NOT NULL CHECK(created_at >= 0)
      ) STRICT;

CREATE INDEX fanout_detail_material_prompt
        ON fanout_detail_material(prompt_material_hash, detail_fanout_work_key);

CREATE TABLE fanout_reusable_enrichment (
        translation_work_key TEXT PRIMARY KEY REFERENCES work_item(work_key),
        fanout_work_key TEXT NOT NULL REFERENCES work_item(work_key),
        model_key TEXT NOT NULL CHECK(length(trim(model_key)) BETWEEN 1 AND 100),
        prompt_material_hash TEXT NOT NULL CHECK(
          length(prompt_material_hash) = 64
          AND prompt_material_hash NOT GLOB '*[^0-9a-f]*'
        ),
        output_artifact_hash TEXT NOT NULL CHECK(
          length(output_artifact_hash) = 64
          AND output_artifact_hash NOT GLOB '*[^0-9a-f]*'
        ),
        created_at INTEGER NOT NULL CHECK(created_at >= 0)
      ) STRICT;

CREATE INDEX fanout_reusable_enrichment_material
        ON fanout_reusable_enrichment(
          model_key, prompt_material_hash, fanout_work_key
        );

CREATE TRIGGER fanout_detail_material_reject_update
      BEFORE UPDATE ON fanout_detail_material
      BEGIN
        SELECT RAISE(ABORT, 'FANOUT_DETAIL_MATERIAL_IMMUTABLE');
      END;

CREATE TRIGGER fanout_detail_material_reject_delete
      BEFORE DELETE ON fanout_detail_material
      BEGIN
        SELECT RAISE(ABORT, 'FANOUT_DETAIL_MATERIAL_IMMUTABLE');
      END;

CREATE TRIGGER fanout_reusable_enrichment_reject_update
      BEFORE UPDATE ON fanout_reusable_enrichment
      BEGIN
        SELECT RAISE(ABORT, 'FANOUT_REUSABLE_ENRICHMENT_IMMUTABLE');
      END;

CREATE TRIGGER fanout_reusable_enrichment_reject_delete
      BEFORE DELETE ON fanout_reusable_enrichment
      BEGIN
        SELECT RAISE(ABORT, 'FANOUT_REUSABLE_ENRICHMENT_IMMUTABLE');
      END;

CREATE TABLE source_author_metadata_revision (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        revision INTEGER NOT NULL CHECK(revision >= 0)
      ) STRICT;

CREATE INDEX work_item_fanout_resolution_pending
        ON work_item(priority DESC, available_at, created_at, work_key)
        WHERE state IN ('pending', 'retry_wait', 'quota_wait')
          AND last_error_code = 'FANOUT_RESOLUTION_PENDING';

CREATE TABLE fanout_priority_hint (
        work_key TEXT PRIMARY KEY REFERENCES work_item(work_key),
        kind TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','running','retry_wait','quota_wait')),
        available_at INTEGER NOT NULL CHECK(available_at >= 0),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        updated_at INTEGER NOT NULL CHECK(updated_at >= created_at)
      ) STRICT, WITHOUT ROWID;

CREATE INDEX fanout_priority_hint_schedule
        ON fanout_priority_hint(kind, created_at, work_key);

CREATE TRIGGER fanout_priority_hint_limit_insert
      BEFORE INSERT ON fanout_priority_hint
      WHEN NOT EXISTS(
          SELECT 1 FROM fanout_priority_hint WHERE work_key = NEW.work_key
        ) AND (SELECT COUNT(*) FROM fanout_priority_hint) >= 1000
      BEGIN
        SELECT RAISE(ABORT, 'FANOUT_PRIORITY_HINT_LIMIT');
      END;

CREATE TRIGGER fanout_priority_hint_work_sync
      AFTER UPDATE OF state, available_at ON work_item
      WHEN NEW.state IN ('pending','running','retry_wait','quota_wait')
      BEGIN
        UPDATE fanout_priority_hint
           SET state = NEW.state,
               available_at = NEW.available_at,
               updated_at = MAX(updated_at, NEW.updated_at)
         WHERE work_key = NEW.work_key;
      END;

CREATE TRIGGER fanout_priority_hint_terminal_cleanup
      AFTER UPDATE OF state ON work_item
      WHEN NEW.state IN ('succeeded', 'dead_letter', 'imported')
      BEGIN
        DELETE FROM fanout_priority_hint WHERE work_key = NEW.work_key;
      END;

CREATE TABLE sol_paid_usage_budget (
        budget_id TEXT PRIMARY KEY,
        maximum_operations INTEGER NOT NULL CHECK(maximum_operations > 0),
        reserved_operations INTEGER NOT NULL DEFAULT 0
          CHECK(reserved_operations >= 0 AND reserved_operations <= maximum_operations),
        state TEXT NOT NULL CHECK(state IN ('active','exhausted','closed')),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        updated_at INTEGER NOT NULL CHECK(updated_at >= created_at)
      ) STRICT;

CREATE UNIQUE INDEX sol_paid_usage_budget_active
        ON sol_paid_usage_budget(state) WHERE state = 'active';

CREATE TABLE sol_paid_usage_reservation (
        budget_id TEXT NOT NULL REFERENCES sol_paid_usage_budget(budget_id),
        attempt_id TEXT NOT NULL,
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        reserved_operations INTEGER NOT NULL CHECK(reserved_operations = 3),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        PRIMARY KEY(budget_id, attempt_id),
        UNIQUE(budget_id, work_key, attempt_id)
      ) STRICT, WITHOUT ROWID;

CREATE TABLE sol_poem_milestone (
        work_key TEXT NOT NULL REFERENCES work_item(work_key),
        milestone TEXT NOT NULL CHECK(milestone IN ('generated','published')),
        implementation_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        completed_at INTEGER NOT NULL CHECK(completed_at >= 0),
        PRIMARY KEY(work_key, milestone)
      ) STRICT, WITHOUT ROWID;

CREATE INDEX sol_poem_milestone_profile_time
        ON sol_poem_milestone(
          implementation_version, schema_version, milestone, completed_at, work_key
        );

CREATE TABLE sol_poem_milestone_backfill (
        event_type TEXT PRIMARY KEY CHECK(event_type IN ('succeeded','imported')),
        cursor_sequence INTEGER NOT NULL CHECK(cursor_sequence >= 0),
        high_watermark INTEGER NOT NULL CHECK(high_watermark >= cursor_sequence),
        completed_at INTEGER CHECK(completed_at IS NULL OR completed_at >= 0)
      ) STRICT, WITHOUT ROWID;

CREATE TRIGGER sol_poem_milestone_event_insert
      AFTER INSERT ON work_event
      WHEN NEW.event_type IN ('succeeded','imported')
      BEGIN
        INSERT INTO sol_poem_milestone(
          work_key, milestone, implementation_version, schema_version, completed_at
        )
        SELECT work_key,
               CASE NEW.event_type WHEN 'succeeded' THEN 'generated' ELSE 'published' END,
               implementation_version, schema_version, NEW.created_at
          FROM work_item
         WHERE work_key = NEW.work_key AND kind = 'poem-enrichment-sol'
           AND (
             NEW.event_type = 'succeeded'
             OR CASE WHEN json_valid(NEW.payload_json) THEN (
               (
                 json_type(NEW.payload_json, '$.artifactHash') = 'text'
                 AND length(json_extract(NEW.payload_json, '$.artifactHash')) = 64
                 AND json_extract(NEW.payload_json, '$.artifactHash') NOT GLOB '*[^0-9a-f]*'
                 AND (SELECT COUNT(*) FROM json_each(NEW.payload_json)) = 1
               ) OR (
                 json_type(NEW.payload_json, '$.publicationWorkKey') = 'text'
                 AND length(json_extract(NEW.payload_json, '$.publicationWorkKey')) = 64
                 AND json_extract(NEW.payload_json, '$.publicationWorkKey') NOT GLOB '*[^0-9a-f]*'
                 AND json_type(NEW.payload_json, '$.receiptArtifactHash') = 'text'
                 AND length(json_extract(NEW.payload_json, '$.receiptArtifactHash')) = 64
                 AND json_extract(NEW.payload_json, '$.receiptArtifactHash') NOT GLOB '*[^0-9a-f]*'
                 AND (SELECT COUNT(*) FROM json_each(NEW.payload_json)) = 2
               )
             ) ELSE 0 END
           )
        ON CONFLICT(work_key, milestone) DO NOTHING;
      END;

CREATE TRIGGER sol_poem_milestone_reject_update
      BEFORE UPDATE ON sol_poem_milestone
      BEGIN SELECT RAISE(ABORT, 'SOL_POEM_MILESTONE_IMMUTABLE'); END;

CREATE TRIGGER sol_poem_milestone_reject_delete
      BEFORE DELETE ON sol_poem_milestone
      BEGIN SELECT RAISE(ABORT, 'SOL_POEM_MILESTONE_IMMUTABLE'); END;



CREATE TABLE runtime_control (
      control_key TEXT PRIMARY KEY,
      enabled INTEGER NOT NULL CHECK(enabled IN (0, 1))
    ) STRICT;

CREATE TABLE monitor_progress_history (
      singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
      payload BLOB NOT NULL CHECK(length(payload) <= 131072),
      updated_at INTEGER NOT NULL CHECK(updated_at >= 0)
    ) STRICT;

CREATE INDEX work_item_fanout_resolution_ready
      ON work_item(kind, available_at, work_key, state)
      WHERE state IN ('pending', 'retry_wait', 'quota_wait')
        AND last_error_code = 'FANOUT_RESOLUTION_PENDING';

CREATE TABLE runtime_provider_concurrency (
      provider TEXT PRIMARY KEY CHECK(provider = 'sol'),
      target INTEGER NOT NULL CHECK(target BETWEEN 1 AND 256),
      initial INTEGER NOT NULL CHECK(initial BETWEEN 1 AND target),
      revision INTEGER NOT NULL CHECK(revision >= 0)
    ) STRICT;

CREATE TABLE sol_operation (
        operation_key TEXT PRIMARY KEY CHECK(length(operation_key) = 64 AND operation_key NOT GLOB '*[^0-9a-f]*'),
        kind TEXT NOT NULL CHECK(kind IN ('generation','review-1','review-2')),
        model TEXT NOT NULL, model_key TEXT NOT NULL, pipeline_version TEXT NOT NULL,
        provider TEXT NOT NULL CHECK(provider = 'sol'), reasoning_effort TEXT NOT NULL,
        current_attempt_id TEXT NOT NULL, current_epoch INTEGER NOT NULL CHECK(current_epoch > 0),
        FOREIGN KEY(operation_key, current_attempt_id, current_epoch)
          REFERENCES sol_invocation_attempt(operation_key, attempt_id, claim_epoch) DEFERRABLE INITIALLY DEFERRED
      ) STRICT;

CREATE TABLE sol_invocation_attempt (
        attempt_id TEXT PRIMARY KEY,
        operation_key TEXT NOT NULL REFERENCES sol_operation(operation_key) DEFERRABLE INITIALLY DEFERRED,
        claim_epoch INTEGER NOT NULL CHECK(claim_epoch > 0),
        input_hash TEXT NOT NULL CHECK(length(input_hash) = 64 AND input_hash NOT GLOB '*[^0-9a-f]*'),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        state TEXT NOT NULL CHECK(state IN ('intent','unknown','known_success','known_rejection','known_invalid')),
        exit_code INTEGER, signal TEXT, finished_at INTEGER,
        turn_started_at INTEGER, session_id TEXT, session_observed_at INTEGER,
        observations_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(observations_json)
          AND length(CAST(observations_json AS BLOB)) <= 16384),
        CHECK((state = 'intent') = (finished_at IS NULL)),
        CHECK(finished_at IS NULL OR finished_at >= created_at),
        CHECK(turn_started_at IS NULL OR turn_started_at >= created_at),
        CHECK((session_id IS NULL) = (session_observed_at IS NULL)),
        CHECK(session_observed_at IS NULL OR session_observed_at >= created_at),
        CHECK(state NOT IN ('known_success','known_invalid') OR (exit_code IS 0 AND signal IS NULL)),
        CHECK(state != 'known_rejection' OR (exit_code IS NOT NULL AND signal IS NULL AND turn_started_at IS NULL)),
        UNIQUE(operation_key, claim_epoch), UNIQUE(operation_key, attempt_id, claim_epoch)
      ) STRICT;

CREATE TRIGGER sol_operation_identity_immutable
      BEFORE UPDATE OF operation_key, kind, model, model_key, pipeline_version, provider, reasoning_effort
      ON sol_operation
      WHEN NEW.operation_key IS NOT OLD.operation_key OR NEW.kind IS NOT OLD.kind OR NEW.model IS NOT OLD.model
        OR NEW.model_key IS NOT OLD.model_key OR NEW.pipeline_version IS NOT OLD.pipeline_version
        OR NEW.provider IS NOT OLD.provider OR NEW.reasoning_effort IS NOT OLD.reasoning_effort
      BEGIN SELECT RAISE(ABORT, 'SOL_OPERATION_IDENTITY_IMMUTABLE'); END;

CREATE TRIGGER sol_invocation_identity_immutable
      BEFORE UPDATE OF attempt_id, operation_key, claim_epoch, input_hash, created_at ON sol_invocation_attempt
      WHEN NEW.attempt_id IS NOT OLD.attempt_id OR NEW.operation_key IS NOT OLD.operation_key
        OR NEW.claim_epoch IS NOT OLD.claim_epoch OR NEW.input_hash IS NOT OLD.input_hash
        OR NEW.created_at IS NOT OLD.created_at
      BEGIN SELECT RAISE(ABORT, 'SOL_INVOCATION_IDENTITY_IMMUTABLE'); END;

CREATE TABLE sol_operation_import_receipt (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        source_digest TEXT NOT NULL CHECK(length(source_digest) = 64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
        record_count INTEGER NOT NULL CHECK(record_count BETWEEN 0 AND 100000),
        source_bytes INTEGER NOT NULL CHECK(source_bytes BETWEEN 0 AND 536870912),
        imported_at INTEGER NOT NULL CHECK(imported_at >= 0)
      ) STRICT;

CREATE TRIGGER sol_operation_import_receipt_immutable BEFORE UPDATE ON sol_operation_import_receipt
      BEGIN SELECT RAISE(ABORT, 'SOL_OPERATION_IMPORT_RECEIPT_IMMUTABLE'); END;

CREATE TRIGGER sol_operation_import_receipt_reject_delete BEFORE DELETE ON sol_operation_import_receipt
      BEGIN SELECT RAISE(ABORT, 'SOL_OPERATION_IMPORT_RECEIPT_IMMUTABLE'); END;

CREATE TABLE runtime_owner (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  epoch INTEGER NOT NULL CHECK(epoch BETWEEN 0 AND 9007199254740991),
  held INTEGER NOT NULL CHECK(held IN (0,1)),
  owner_kind TEXT CHECK(owner_kind IN ('supervisor','maintenance')),
  pid INTEGER CHECK(pid > 0),
  run_id TEXT,
  config_digest TEXT,
  started_at TEXT,
  CHECK((epoch = 0 AND held = 0 AND owner_kind IS NULL AND pid IS NULL AND run_id IS NULL AND config_digest IS NULL AND started_at IS NULL)
    OR (epoch > 0 AND owner_kind IS NOT NULL AND pid IS NOT NULL AND run_id IS NOT NULL AND config_digest IS NOT NULL AND started_at IS NOT NULL))
) STRICT;

CREATE TRIGGER runtime_owner_no_delete BEFORE DELETE ON runtime_owner
BEGIN SELECT RAISE(ABORT, 'RUNTIME_OWNER_DELETE_FORBIDDEN'); END;


CREATE TABLE poem_identity (
  source_name TEXT NOT NULL CHECK(length(source_name) BETWEEN 2 AND 64 AND source_name GLOB '[a-z]*' AND source_name NOT GLOB '*[^a-z0-9_-]*'),
  poem_href TEXT NOT NULL CHECK(length(poem_href) BETWEEN 9 AND 2048 AND poem_href GLOB 'https://*'),
  author_href TEXT NOT NULL CHECK(length(author_href) BETWEEN 9 AND 2048 AND author_href GLOB 'https://*'),
  first_work_key TEXT NOT NULL,
  created_at INTEGER NOT NULL CHECK(created_at >= 0),
  PRIMARY KEY(source_name, poem_href)
) STRICT;
CREATE INDEX work_item_poem_definition_lookup ON work_item(kind, implementation_version, schema_version, input_hash);
CREATE TRIGGER poem_identity_reject_conflicting_insert BEFORE INSERT ON poem_identity
WHEN EXISTS (SELECT 1 FROM poem_identity WHERE source_name = NEW.source_name AND poem_href = NEW.poem_href AND author_href <> NEW.author_href)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_DUPLICATE'); END;
CREATE TRIGGER poem_identity_requires_configured_source_insert BEFORE INSERT ON poem_identity
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER poem_identity_reject_identity_update BEFORE UPDATE ON poem_identity
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_IMMUTABLE'); END;
CREATE TRIGGER poem_identity_reject_delete BEFORE DELETE ON poem_identity
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_IMMUTABLE'); END;
CREATE TRIGGER poem_work_requires_registered_identity_insert BEFORE INSERT ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND NOT EXISTS (
  SELECT 1 FROM poem_identity
  WHERE source_name = (SELECT source_name FROM local_source_identity WHERE singleton = 1)
    AND poem_href = json_extract(NEW.input_json, '$.poemHref')
    AND author_href = json_extract(NEW.input_json, '$.authorHref')
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_REQUIRED'); END;
CREATE TRIGGER poem_work_reject_duplicate_definition_insert BEFORE INSERT ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND EXISTS (
  SELECT 1 FROM work_item
  WHERE kind = NEW.kind AND implementation_version = NEW.implementation_version
    AND schema_version = NEW.schema_version AND input_hash = NEW.input_hash
    AND work_key <> NEW.work_key
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_WORK_DUPLICATE'); END;
CREATE TRIGGER poem_work_requires_registered_identity_update BEFORE UPDATE OF kind, input_json ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND NOT EXISTS (
  SELECT 1 FROM poem_identity
  WHERE source_name = (SELECT source_name FROM local_source_identity WHERE singleton = 1)
    AND poem_href = json_extract(NEW.input_json, '$.poemHref')
    AND author_href = json_extract(NEW.input_json, '$.authorHref')
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_REQUIRED'); END;
CREATE TABLE source_author_metadata (
  source_name TEXT NOT NULL CHECK(length(source_name) BETWEEN 2 AND 64 AND source_name GLOB '[a-z]*' AND source_name NOT GLOB '*[^a-z0-9_-]*'),
  author_href TEXT NOT NULL CHECK(length(author_href) BETWEEN 1 AND 2048),
  author_name_arabic TEXT NOT NULL CHECK(length(trim(author_name_arabic)) BETWEEN 1 AND 512),
  refresh_generation TEXT NOT NULL CHECK(length(refresh_generation) BETWEEN 1 AND 64),
  observed_at INTEGER NOT NULL CHECK(observed_at >= 0),
  PRIMARY KEY(source_name, author_href)
) STRICT;
CREATE TRIGGER source_author_metadata_requires_configured_source_insert BEFORE INSERT ON source_author_metadata
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER source_author_metadata_requires_configured_source_update BEFORE UPDATE OF source_name ON source_author_metadata
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER source_author_metadata_revision_insert AFTER INSERT ON source_author_metadata
BEGIN UPDATE source_author_metadata_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER source_author_metadata_revision_update AFTER UPDATE ON source_author_metadata
BEGIN UPDATE source_author_metadata_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER local_source_identity_reject_update BEFORE UPDATE ON local_source_identity
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_IMMUTABLE'); END;
CREATE TRIGGER local_source_identity_reject_delete BEFORE DELETE ON local_source_identity
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_IMMUTABLE'); END;
`;

/** The only retained compatibility step. It converts the last private schema
 * into the public, source-neutral baseline without rewriting retained work. */
export const UPGRADE_V35_TO_V36_SQL = `
DROP TRIGGER poem_work_requires_registered_identity_insert;
DROP TRIGGER poem_work_reject_duplicate_definition_insert;
DROP TRIGGER poem_work_requires_registered_identity_update;
DROP INDEX work_item_poem_definition_unique;
DROP TRIGGER poem_identity_reject_conflicting_insert;
DROP TRIGGER poem_identity_reject_identity_update;
DROP TRIGGER poem_identity_reject_delete;
ALTER TABLE poem_identity RENAME TO poem_identity_v35;
CREATE TABLE poem_identity (
  source_name TEXT NOT NULL CHECK(length(source_name) BETWEEN 2 AND 64 AND source_name GLOB '[a-z]*' AND source_name NOT GLOB '*[^a-z0-9_-]*'),
  poem_href TEXT NOT NULL CHECK(length(poem_href) BETWEEN 9 AND 2048 AND poem_href GLOB 'https://*'),
  author_href TEXT NOT NULL CHECK(length(author_href) BETWEEN 9 AND 2048 AND author_href GLOB 'https://*'),
  first_work_key TEXT NOT NULL,
  created_at INTEGER NOT NULL CHECK(created_at >= 0),
  PRIMARY KEY(source_name, poem_href)
) STRICT;
INSERT INTO poem_identity(source_name, poem_href, author_href, first_work_key, created_at)
SELECT source_name, poem_href, author_href, first_work_key, created_at
FROM poem_identity_v35;
DROP TABLE poem_identity_v35;
CREATE INDEX work_item_poem_definition_lookup ON work_item(kind, implementation_version, schema_version, input_hash);
CREATE TRIGGER poem_identity_reject_conflicting_insert BEFORE INSERT ON poem_identity
WHEN EXISTS (SELECT 1 FROM poem_identity WHERE source_name = NEW.source_name AND poem_href = NEW.poem_href AND author_href <> NEW.author_href)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_DUPLICATE'); END;
CREATE TRIGGER poem_identity_requires_configured_source_insert BEFORE INSERT ON poem_identity
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER poem_identity_reject_identity_update BEFORE UPDATE ON poem_identity
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_IMMUTABLE'); END;
CREATE TRIGGER poem_identity_reject_delete BEFORE DELETE ON poem_identity
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_IMMUTABLE'); END;
CREATE TRIGGER poem_work_requires_registered_identity_insert BEFORE INSERT ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND NOT EXISTS (
  SELECT 1 FROM poem_identity
  WHERE source_name = (SELECT source_name FROM local_source_identity WHERE singleton = 1)
    AND poem_href = json_extract(NEW.input_json, '$.poemHref')
    AND author_href = json_extract(NEW.input_json, '$.authorHref')
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_REQUIRED'); END;
CREATE TRIGGER poem_work_reject_duplicate_definition_insert BEFORE INSERT ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND EXISTS (
  SELECT 1 FROM work_item
  WHERE kind = NEW.kind AND implementation_version = NEW.implementation_version
    AND schema_version = NEW.schema_version AND input_hash = NEW.input_hash
    AND work_key <> NEW.work_key
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_WORK_DUPLICATE'); END;
CREATE TRIGGER poem_work_requires_registered_identity_update BEFORE UPDATE OF kind, input_json ON work_item
WHEN NEW.kind = (SELECT source_name || '_poem_detail' FROM local_source_identity WHERE singleton = 1)
AND NOT EXISTS (
  SELECT 1 FROM poem_identity
  WHERE source_name = (SELECT source_name FROM local_source_identity WHERE singleton = 1)
    AND poem_href = json_extract(NEW.input_json, '$.poemHref')
    AND author_href = json_extract(NEW.input_json, '$.authorHref')
)
BEGIN SELECT RAISE(ABORT, 'SOURCE_POEM_IDENTITY_REQUIRED'); END;

DROP TRIGGER source_author_metadata_revision_insert;
DROP TRIGGER source_author_metadata_revision_update;
ALTER TABLE source_author_metadata RENAME TO source_author_metadata_v35;
CREATE TABLE source_author_metadata (
  source_name TEXT NOT NULL CHECK(length(source_name) BETWEEN 2 AND 64 AND source_name GLOB '[a-z]*' AND source_name NOT GLOB '*[^a-z0-9_-]*'),
  author_href TEXT NOT NULL CHECK(length(author_href) BETWEEN 1 AND 2048),
  author_name_arabic TEXT NOT NULL CHECK(length(trim(author_name_arabic)) BETWEEN 1 AND 512),
  refresh_generation TEXT NOT NULL CHECK(length(refresh_generation) BETWEEN 1 AND 64),
  observed_at INTEGER NOT NULL CHECK(observed_at >= 0),
  PRIMARY KEY(source_name, author_href)
) STRICT;
INSERT INTO source_author_metadata(source_name, author_href, author_name_arabic, refresh_generation, observed_at)
SELECT source_name, author_href, author_name_arabic, refresh_generation, observed_at
FROM source_author_metadata_v35;
DROP TABLE source_author_metadata_v35;
CREATE TRIGGER source_author_metadata_requires_configured_source_insert BEFORE INSERT ON source_author_metadata
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER source_author_metadata_requires_configured_source_update BEFORE UPDATE OF source_name ON source_author_metadata
WHEN NEW.source_name IS NOT (SELECT source_name FROM local_source_identity WHERE singleton = 1)
BEGIN SELECT RAISE(ABORT, 'LOCAL_SOURCE_IDENTITY_MISMATCH'); END;
CREATE TRIGGER source_author_metadata_revision_insert AFTER INSERT ON source_author_metadata
BEGIN UPDATE source_author_metadata_revision SET revision = revision + 1 WHERE singleton = 1; END;
CREATE TRIGGER source_author_metadata_revision_update AFTER UPDATE ON source_author_metadata
BEGIN UPDATE source_author_metadata_revision SET revision = revision + 1 WHERE singleton = 1; END;

DROP TRIGGER retired_scheduler_state_reject_update;
DROP TRIGGER retired_scheduler_state_reject_delete;
DROP TABLE retired_scheduler_state;
UPDATE local_schema SET version = 36 WHERE singleton = 1 AND version = 35;
`;
