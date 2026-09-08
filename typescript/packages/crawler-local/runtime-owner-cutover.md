# Runtime owner authority

The public baseline accepts fresh databases and the final private schema only.
Older private schemas must be upgraded with the archived private release before
installing this release.

## Transactional contract

One singleton in the existing ledger holds a monotonic epoch, held state,
owner kind, PID, run ID, configuration digest, and start time. A claim runs
under `BEGIN IMMEDIATE`; a supervisor claim replaces a held owner (including a
dead maintenance owner) only when the PID is definitely dead (`ESRCH`).
Permission failures, unknown PID state, and PID
reuse remain fenced. Maintenance claims never recover a held owner. This scope
also protects standalone `run-enrichment` as a strict exclusive job owner, not
only migrations; it retains ownership until work settles and ledger closure
succeeds. A closure failure preserves ownership for definite-dead recovery.

Release matches epoch, PID, and run ID in the same write transaction. It retains
the previous identity as released evidence. An old release cannot remove a new
owner. Signaling must validate the current identity while holding the same
transaction through the synchronous signal action. External PID reuse remains
an operating-system identity limitation; do not claim birth-time verification.

SQLite automatically releases an unfinished transaction when its process dies.
There is no orphan sidecar guard and no independently mutable owner database.
Typed reads reject incomplete rows and unsupported schema versions, rather than
interpreting malformed authority as unlocked. The existing diagnostic record
shape can remain stable while the epoch is internal to ownership handles.

## Cutover policy implemented in code

- The v36 baseline registers the singleton; diagnostic owner reads require a
  supported schema and never create a database.
- Only a genuinely empty database may initialize fresh. The complete baseline
  and seed rows are created in one immediate transaction.
- Existing v35 upgrades only while its SQL owner is released and no work item
  is running. The transaction preserves retained work and removes retired
  scheduler state. Schemas older than v35 fail closed.
- Stop all old supervised and standalone processes. Preserve and explicitly
  dispose of old lock evidence; never silently import or delete it. Old binary
  startup after cutover is unsupported and must remain disabled.
- Supervisor acquire/release, standalone `run-enrichment`, importer
  strict maintenance acquisition, launcher installation checks, launcher stop
  cleanup and signaling, CLI diagnostics, and state inventory use SQL authority.
  Initial and escalated signals hold the same owner transaction through the
  synchronous signal. Stop persists disabled intent before refusing an
  unavailable/historical owner; it never signals an unverified historical PID.
- Remove direct filesystem existence and unlink decisions from those callers.
  Legacy files are evidence/blockers, never fallback ownership authority.
- Require an existing migrated ledger for owner reads and claims; status must
  not initialize or migrate. Preserve receipt checks before inference and all
  budget/pause/origin-stop behavior.

## Acceptance still required

The disposable fresh-CLI regression exercises `init`, `pause`, `pause-paid`,
repeat `init`, `import-sol-operations --dry-run`, explicit digest-bound apply,
and `run-enrichment --max 1` while paused. It proves no Codex process is invoked,
no budget/reservation is armed, controls survive initialization, and the owner
is released after the bounded job. Initialization never fabricates an import
receipt. It creates an empty operation index only when the attempts directory
was genuinely absent; preexisting attempts without an index remain a refusal
requiring recovery of their original evidence, not an invented empty history.

These tests demonstrate local admission and recovery mechanics, not permission
to resume paid work or a successful managed-service production cutover.

Integrated regression coverage includes atomic fresh initialization, guarded
v35 upgrade with retained-data comparison, refusal while an owner or work item
is active, fail-closed older/future schemas, standalone exclusion, maintenance
exclusion, release failure retaining ownership, and launcher status and stop
behavior.

Core tests already exercise two live competing processes, definite-dead owner
replacement, stale release fencing, permission ambiguity, and process death
immediately before transaction commit. These tests use disposable ledgers only.
Production migration, launcher deployment, and live ownership verification are
separate pending acceptance steps.
