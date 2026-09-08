# Saqi local crawler

Restart-safe collection and Codex enrichment. The SQLite ledger and
content-addressed artifact store are the source of truth; rerunning the same
configuration resumes pending work instead of creating a second operation.

## Run

From `typescript/`:

```sh
yarn workspace @saqi/crawler-local build
cp packages/crawler-local/unified-rig.example.json ./saqi-rig.json
node packages/crawler-local/dist/cli.js run --config ./saqi-rig.json
```

`run` creates or migrates the local ledger automatically. Paths in the config
are resolved relative to the config file. Use `--max-cycles N` for bounded test
runs or `--maximum-runtime-ms N` to drain after a time budget. The first
`SIGINT`/`SIGTERM` requests a graceful drain; a second requests a hard stop.

Run `node packages/crawler-local/dist/cli.js --help` for the complete command
surface.

## Pause controls

`pause`, `resume`, and `pause-paid` update the ledger's `runtime_control` table.
`resume-paid --maximum-sol-operations N` requires a finite budget (a positive
multiple of three); an exhausted prior budget additionally requires `--rearm`.
Resuming all work does not clear the paid-work pause or replenish a budget.

The first pause-state read transaction imports legacy `PAUSED` and
`PAID_WORK_PAUSED` flags and records `legacy_pause_imported`. Thereafter SQLite
is authoritative: adding or deleting those files has no control effect. Use
the CLI commands instead. CLI writes retain derived flag mirrors only for
rollback to an older runner. Back up the ledger before rollback, and use the
current CLI to establish the intended pause state before starting an old
release. Database or import errors stop admission instead of assuming resumed.

## Safe defaults

Managed service intent is stored in the `service_enabled` row of `runtime_control`. Its first
read imports the legacy `SERVICE_ENABLED` file exactly once. Later file edits
have no effect; `service-control start` and `stop` commit SQLite intent before
signaling launchd and keep the file only as a rollback mirror. The ledger must
already exist and contain the current runtime-control schema; missing or
invalid authority fails closed.

The LaunchAgent checks intent at login (`RunAtLoad`) and retries unsuccessful
exits. A disabled `run-service` exits successfully before credentials or work
lanes are initialized. An enabled runtime exits unsuccessfully after its
supervisor stops so launchd can restore it; an explicit stop first clears the
SQLite intent and then drains the runtime. Service intent never changes pause
state or authorizes/replenishes a translation budget.

The example enables only the Sol enrichment lane. Production baseline import,
inventory reconciliation, fanout, and D1 publication remain disabled until
configured. Publication also fails closed
when the D1 tier is unknown, capacity or backup proof is insufficient, the
endpoint origin is not allowlisted, or authentication is unavailable.

Retention is enabled in planning mode but `retention.apply` is `false`; it does
not move diagnostic files until explicitly enabled. Even when applied, inputs,
outputs, manifests, result files, operation intents, and CAS artifacts are
preserved. `purgeArchivedDiagnostics` is a separate destructive opt-in and is
`false` by default.

Keep Cloudflare Access credentials out of JSON. Publication reads either
`CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET`, or the configured JWT
environment variable. Provider CLIs use their own authenticated local sessions.
Production canaries must invoke the service's configured credential-loading
wrapper, not the raw Node.js CLI. The raw CLI intentionally remains portable
and reads only its inherited environment; it does not access platform keychains.

Generated LaunchAgent plists invoke the internal `run-service` entry point. It
loads `saqi-source-name`, `saqi-source-base-url`, and
`saqi-source-adapter-v1` from the
`saqi-publication` macOS Keychain account before initializing the crawler. If
any entry is missing or invalid, startup fails with
`SOURCE_KEYCHAIN_UNAVAILABLE`; neither value is written to the plist, command
line, or diagnostics. Interactive `run` commands require the identity variables
and raw `SAQI_SOURCE_ADAPTER_CONFIG` JSON together. The JSON is parsed directly;
it is never base64-encoded.

## Recover legacy translation sources

Legacy translations without source-bound detail evidence must be recrawled;
their model artifacts are never accepted as source admissions. Use a freshly
exported production baseline plus a content-addressed, sorted UUID manifest:

```json
{
  "authorsSha256": "SHA256_OF_AUTHORS_NDJSON_BYTES",
  "manifestHash": "SHA256_OF_THE_OTHER_FIELDS_IN_CANONICAL_JSON",
  "poemIds": ["00000000-0000-4000-8000-000000000001"],
  "poemsSha256": "SHA256_OF_POEMS_NDJSON_BYTES",
  "schemaId": "saqi.production-detail-recovery-manifest",
  "schemaVersion": 2
}
```

The two export hashes bind the reviewed source files to the manifest. Both are
verified before the ledger is opened for mutation.

Preview first; dry-run is the default and performs no ledger writes:

```sh
node packages/crawler-local/dist/cli.js recover-legacy-sources \
  --state-dir /path/to/state \
  --manifest recovery.json \
  --authors fresh-authors.ndjson \
  --poems fresh-poems.ndjson
```

The report separates new work, active existing work, terminal work, completed
work requiring refresh, and already refreshed work. After reviewing it, repeat
with `--apply`; apply fails closed unless the SQLite paid-work pause is enabled
in the selected state directory. Recovery work uses priority 1000, including a
priority raise for matching active work. The manifest hash becomes the refresh
generation, making replay idempotent while preserving every prior artifact and
event. This command itself only adds local detail-crawl work and has no network
or publication client. A running rig may subsequently crawl, source-admit,
reuse the approved translation, and publish it; the paid-work fence prevents a
fresh model operation during that recovery.

## Observe and recover

```sh
node packages/crawler-local/dist/cli.js status --config ./saqi-rig.json
node packages/crawler-local/dist/cli.js health --config ./saqi-rig.json --format json --fail-on-blocked
node packages/crawler-local/dist/cli.js doctor --config ./saqi-rig.json
node packages/crawler-local/dist/cli.js verify --config ./saqi-rig.json
```

`status` reports the ledger, pause marker, run lock, and supervisor snapshot.
`health` validates `health/latest.json`; with `--fail-on-blocked` it exits 2 for
a blocker, stale health, a fenced lane, or an open quota signal. `doctor` checks
the SQLite schema/integrity and crash residue. `verify` additionally hashes all
artifacts and every ledger reference.

Recovery is deliberately explicit:

- `pause` stops admission of new work; active leases drain. `resume` removes
  the pause marker. Both commands are idempotent.
- `set-concurrency --config FILE --provider PROVIDER --value N` accepts only
  `sol` and `2`, `4`, `8`, `16`, `32`, `128`, or `256`. It
  serializes the atomic provider update with service acknowledgement. A stopped
  service stays stopped; a restart failure reports the saved desired setting
  separately from the still-running configuration.
- Schema 33 stores initial/target concurrency in `runtime_provider_concurrency`.
  Runtime startup imports the JSON pair once into the existing/migrated ledger;
  subsequent controls update SQLite only. Configuration/status inspection is
  read-only and overlays SQLite before computing the desired configuration
  digest. JSON tuning fields become bootstrap values, not ongoing controls.
  Missing or corrupt imported authority fails closed. A pre-bootstrap stopped
  service may still display the legacy pair without importing it.
  Rollback requires a schema-compatible runtime; old binaries cannot read the
  upgraded ledger or its desired concurrency authority.
- Provider concurrency is a selectable ceiling, not a promise to spawn every
  lane. The shared host gate defaults to 64 active provider descendants, a
  256 MiB memory reservation and 6 MiB buffered-output reservation per child,
  with 16 GiB and 384 MiB aggregate budgets. `status` exposes
  `providerHostAdmission`; raise the schema-validated budgets for 128 or 256
  only after measuring the target Mac under representative work.
- Restart the same config to recover expired leases and quota/retry waits.
- `clear-source-stop --confirm` clears a persisted Source origin stop only
  after the operator has resolved its cause and explicitly confirms the
  exceptional override. Ordinary human-challenge probes resume automatically
  after their durable bounded cooldown and do not require this command.
- To recover allowlisted rendering failures, set
  `collector.recovery.enabled: true`, restart the supervised runtime, inspect
  `collector-recovery --action status --config FILE`, and explicitly arm it
  with `collector-recovery --action arm --config FILE`. Recovery releases and
  observes only its durable 1, 5, 20, then 100-item cohorts; it never drains the
  ordinary pending queue. A running disarmed supervisor polls for an external
  arm at `restart.idlePollMs`; no second restart is required.
- Do not delete `RUN.lock`. A retained or mismatched lock fences launchd
  control so the process identity can be investigated instead of guessed.

Diagnostics use versioned JSON contracts: `saqi.pipeline-health@1`,
`saqi.pipeline-signal@1`, and `saqi.launchd-control@1`. Consumers must reject
unknown schema versions rather than interpreting them optimistically.
Collector failure diagnostics classify source access as an HTTP denial, managed
challenge, or Turnstile challenge. That structured metadata retains only bounded
status and boolean evidence, never page content, URLs, headers, cookies, tokens,
or screenshots.

## Upgrade the live service

Treat a crawler upgrade as a controlled data migration, not as an in-place
binary replacement. The repository requires Node 24 or newer; verify the Node
binary reachable from the LaunchAgent's configured `PATH`, not only the one in
an interactive shell.

1. Pause admission and let active leases drain.
2. Run `doctor` and `verify`, then copy the SQLite ledger together with its
   `-wal` and `-shm` files while the service is stopped.
3. Build and test the exact commit in a separate runtime checkout.
4. Run a bounded invocation against a copy of the ledger and repeat `doctor`
   and `verify` before changing the LaunchAgent target.
5. Restart through `service-control`; require a matching `RUN.lock`, a fresh
   health snapshot, and the expected config digest before resuming admission.

Ledger migrations are forward-only. An older crawler refuses a ledger whose
schema is newer than it supports, so rolling back the executable also requires
restoring the matching stopped-service ledger snapshot. Never point the old
binary at a newly migrated live ledger.

`verify` hashes each stored artifact once and reuses that result for ledger
references. References absent from the inventory are checked serially. This
preserves corruption and missing-reference reporting without opening a file for
every reference at once. Verification is a point-in-time pass, not protection
against subsequent filesystem changes; stop writers for a coherent rollout
snapshot as described above.

### Offline Codex operation-index import

Schema 34 provides `SolOperationStore` and the offline
`importLegacySolOperations` API and `import-sol-operations` CLI. This runner uses
SQLite exclusively for operation authority and refuses construction without a
completed import receipt. Installing the schema does not import legacy files.
Do not enable its import-completion marker manually.
`Ledger.solOperations` exposes the migrated connection's typed store without
implicitly importing legacy files. Claims require both the completion marker
and a valid immutable receipt.

After completing the stopped-owner precautions below, inspect with
`saqi-crawler import-sol-operations --state-dir DIR --dry-run`, then apply with
`saqi-crawler import-sol-operations --state-dir DIR --apply --expected-digest SHA256`.

Omitting `--apply` is read-only. The import command also accepts `--config FILE` instead
of `--state-dir DIR`; neither mode starts a runner or rearms a budget.
Do not restart a legacy JSON-authoritative runner after applying the import.
Result files and event logs remain retained evidence, not alternate operation,
session, terminal, or cleanup authority. Valid retained results can still be
adopted without a new provider call when the paid budget is exhausted. The
unified runtime currently gates its artifact-reconciliation lane on both global
and paid-work pauses, so that lane does not advance while either pause is set.
Adoption alone never invents a successful invocation terminal: an ambiguous
attempt retains its `intent`/`unknown` state and its provider session. Session
cleanup requires separately proven terminal evidence. A crash after session
unlink but before its SQLite cleanup receipt can leave a visible pending cleanup
record; the runner does not silently declare that deletion proven.

The public baseline accepts existing v35 ledgers only. Upgrade any older ledger
with the archived private release before installing the public build. The
default status command never initializes or migrates a ledger.

- Stop **all** legacy runners, including standalone `run-enrichment`; that
  command does not share the supervised `RUN.lock`. A maintenance lock cannot
  fence arbitrary old processes which ignore the managed controls.
- Use a trusted state directory with no concurrent directory or path replacement,
  including its ancestors. Existing symlinked/nonregular ledger and lock paths
  are rejected before use, but these checks do not provide race-free directory
  traversal against a hostile filesystem writer.
- Keep the SQLite service disabled and both global and paid-work pauses set.
  Their one-time legacy imports must be complete. No running Sol work or
  existing `RUN.lock` is allowed. No existing lock is deleted or guessed stale;
  the importer releases only the maintenance lock it acquired.
- Back up the stopped ledger and retain the complete legacy attempt tree.
  Call the API without `apply` for a read-only plan. Apply requires
  `apply: true` and the plan's exact `expectedDigest`. Apply takes `RUN.lock`
  and a SQLite write transaction, rechecks the guards, and rereads the inputs.
- Any corrupt/inconsistent record, source change, duplicate attempt, legacy
  operation lock, symlink, or size-limit violation aborts the entire import.
  Limits are 100,000 operation records, 512 MiB total source bytes, 512 KiB per
  JSON file, 2 MiB per event stream, and 16 KiB retained auxiliary observations.
- Original operation keys and model/prompt profile identities are copied, not
  regenerated using today's hashing rules. Manifest identity and canonical
  input hash must match the indexed attempt. Creation time is a conservative
  lower bound derived from manifest mtime and retained observation timestamps.
  The exact six-field Sol-only v1 shape is accepted only for the historically
  proven model/pipeline/effort and original inputHash/kind operation key. Its
  provider/model key mapping is recorded as typed revision-bound provenance;
  dry-run counts these normalizations. Partial fields, wrong profiles, changed
  keys, and mismatched original index/manifest shapes are not normalized.
  Proven historical reconciliation formats are retained as separate typed
  continuation or artifact evidence with the original object, exact source-file
  SHA256 and writer revision. A legacy continuation was a separate paid
  `codex exec resume` turn: its success never promotes the original invocation,
  permits replay, or authorizes session cleanup. Historical fractional session
  mtime timestamps are floored only for integer SQLite timing while their exact
  original values remain in provenance. Dry-run reports each compatibility
  count; unknown formats and invalid ranges remain blockers.
- `known_success` and `known_invalid` require clean-exit evidence and retain
  their meaning. `known_rejection` retains even exit-zero structured-error
  rejections, but any durable turn-start evidence makes it inconsistent.
  A structurally valid legacy `known_rejection` with null exit code, a signal,
  or durable turn-start evidence is imported as `unknown`, retaining the exact
  terminal/session evidence and typed `legacy_rejection_quarantine_v1`
  provenance. Dry-run reports quarantined records and each reason count; these
  records cannot authorize a new inference or session cleanup. Structural,
  identity, input-hash, unsafe-file, and invalid-success errors still abort.
  Legacy `completed` is conservatively `unknown`; absent terminal evidence
  remains unresolved `intent`. Neither permits a fresh inference.
- Attempt rows, typed session/credential/cleanup observations, the immutable
  receipt and completion marker commit together. Retry after failure rolls
  back completely; retry after success returns the stored receipt without
  rereading stale legacy files. Paid budgets and pause controls never change.
- Input, output and diagnostic artifacts remain untouched. Do not resume or
  remove legacy authority files until the separately reviewed runner wiring
  is deployed and verified. This staged API performs no inference, session
  deletion, or publication.
