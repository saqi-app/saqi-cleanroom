export const STATUS_HEARTBEAT_MS = 5 * 60_000;

// Allow two missed heartbeats plus one in-progress snapshot before fencing a
// healthy service. Aggregate ledger snapshots can take longer than a minute.
export const MAXIMUM_STATUS_AGE_MS = 3 * STATUS_HEARTBEAT_MS;
