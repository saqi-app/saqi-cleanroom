# Saqi Rig Monitor

Native macOS menu-bar progress and safe-control surface for the local Saqi collection/enrichment rig.

```sh
cd macos/SaqiRigMonitor
swift test
swift build -c release
.build/release/SaqiRigMonitor
```

Defaults target `~/code/saqi/.runtime/saqi-unified-rig-live.json`, the immutable
installed crawler CLI under `~/Library/Application Support/Saqi/current`, the
`net.saqi.crawler` user LaunchAgent, and
`~/code/saqi-runtime`. Override them with `SAQI_RIG_CONFIG`,
`SAQI_CRAWLER_CLI`, `SAQI_SERVICE_LABEL`, and `SAQI_STATE_DIR`.

Start, Stop, and Restart call the crawler's serialized launchd controller. The
LaunchAgent must already be installed and loaded. The controller waits for
`RUN.lock` acknowledgements and signals only the PID reported by launchd after
matching it to the lock; a mismatch is fenced. Pause and Resume are idempotent
admission controls, and “Pause after current work” does not interrupt active
leases.

The Sol control applies one of `2`, `8`, `16`, `32`, `128`, or `256` to the
Codex provider's starting target and ceiling. The
validated atomic update and its service acknowledgement share one mutation
lock. A stopped service stays stopped, and restart failures visibly retain the
desired setting without misreporting it as running. Adaptive schedulers can
still reduce selected concurrency when provider evidence requires it; each lane
shows actual/selected/ceiling state.

The monitor reads the versioned `health/latest.json` and supervisor `status.json`
snapshots every two seconds, even while its menu is closed. It distinguishes
actionable auth/source failures from self-healing network, quota, rate-limit,
retry, and resource waits, and shows actual/selected/ceiling concurrency. An
intentionally disabled production lane is informational, not an alert.

It retains a bounded six-hour local rate history across restarts in the runtime
ledger's `monitor_progress_history` singleton (schema 31, at most 256 samples and
128 KiB). Install/migrate the runtime before updating the widget. The first normal
widget start imports the legacy UserDefaults history once and removes that copy
only after SQLite commits. Read-only initialization never imports or writes.
Missing, corrupt, or locked storage reports persistence unavailable; in-memory
sampling continues without writing another state file. A model rate
appears after at least ten eligible minutes and two accepted translations;
provider downtime is excluded. ETA follows the slowest parallel model lane and
becomes unavailable during waits, blocked, paused, stale, or identity-mismatch
states. These are local processing estimates, not production publication
promises.

The More menu's “Start at login” toggle persists the
`monitor_autostart_enabled` row in the existing runtime ledger's `runtime_control`
table. The first startup imports legacy flag files once (an explicit disabled
flag wins); after that, editing those files cannot change the preference.
The LaunchAgent and legacy flags are derived projections retained for repair
and rollback. Missing or corrupt SQLite authority leaves the LaunchAgent
untouched; it never creates a replacement ledger. Test-only store initialization
reads the preference without importing state or installing a LaunchAgent.
