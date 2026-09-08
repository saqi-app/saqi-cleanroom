# Schema baseline and recovery

The repository has one fresh-install schema baseline:
`0037_retire_legacy_translation_tasks.sql`. Its filename is deliberately retained
because production already recorded that exact migration on September 6, 2026.
Wrangler identifies applied migrations by filename, not by Git history or SQL
contents. Do not rename this file or manufacture migration receipts.

The baseline includes the final schema formerly introduced by
`0038_readable_sol_medium_profile.sql`. Production recorded the genuine 0038
receipt before that forward migration was folded into the fresh-install
baseline. A remote receipt without a corresponding local file is harmless; it
remains evidence of the SQL that actually ran.

## Supported starting states

| Database state | Required action |
| --- | --- |
| Empty catalog, no migration receipts | Apply the baseline through Wrangler |
| Current schema with the real 0037 and 0038 receipts | No pending SQL; preserve existing rows and receipts |
| Intermediate schema with only the real 0037 receipt | Apply 0038 from the preserved pre-cutover release before switching |
| Any existing catalog without the 0037 receipt | Stop and finish the historical upgrade using the pre-cutover release |

The baseline creates the final tables, indexes, immutable profile seeds and
publication guards directly. It does not replay historical backfills, trigger
replacements, or retired-task updates. Its first statement intentionally creates
`author` without `IF NOT EXISTS`, so an existing older Saqi catalog fails before
new schema objects are created. Never respond to that error by deleting tables
or inserting a baseline receipt.

## Historical upgrade and restore

Before switching an existing deployment to this baseline, use the preserved
pre-cutover release `47db477b9e66e56a6594d61767e1b15ca35a14e5` to apply the original
0021–0037 chain. That release contains the supported historical-state matrix and
the staged retired-worker drain procedure. The operator's private repository
history bundle preserves it independently of the public repository cutover.

An old database backup must be restored and upgraded with that matching
pre-cutover code first. A current backup must retain its real
`d1_migrations` table. Do not infer completed migrations from similar columns.

## Apply and verify

1. Confirm the intended database identity. Preserve and restore-test a database
   export, a Time Travel bookmark, and the matching application release.
2. Run migration compatibility, task-integrity and catalog tests.
3. List pending migrations through Wrangler. An empty catalog should show only
   the baseline; current production should show none.
4. For a fresh local database, run
   `yarn workspace @saqi/app db:migrate:local`.
5. Verify the real receipt, `PRAGMA foreign_key_check`, and representative
   author/poem reads. Listing pending migrations again must return none.

The normal production deployment workflow still requires the real 0037 receipt;
it is not an empty-database provisioning workflow. Migration seeds and Worker
bindings retain the existing production database identity. A new remote
deployment requires its own explicitly verified identity configuration.

## Future changes

Add forward-only migrations numbered after 0038. Do not edit this baseline for
new production schema changes: a database with its receipt will skip it.
Preserve the rollback application and database backup before applying changes.
