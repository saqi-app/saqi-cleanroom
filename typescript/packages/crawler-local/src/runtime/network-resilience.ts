const NETWORK_ERROR_PATTERN =
  /(?:\bEAI_AGAIN\b|\bENETDOWN\b|\bENETUNREACH\b|\bENOTFOUND\b|\bEHOSTUNREACH\b|\bECONNREFUSED\b|\bECONNRESET\b|\bETIMEDOUT\b|ERR_INTERNET_DISCONNECTED|connection (?:reset|refused|timed out)|failed to (?:connect|lookup|resolve)|dns (?:error|failure|lookup)|getaddrinfo|name or service not known|network (?:is )?(?:down|offline|unavailable|unreachable)|no route to host|socket hang up|temporary failure in name resolution)/i;

const PREDISPATCH_NETWORK_ERROR_PATTERN =
  /(?:\bEAI_AGAIN\b|\bENETDOWN\b|\bENETUNREACH\b|\bENOTFOUND\b|\bEHOSTUNREACH\b|\bECONNREFUSED\b|ERR_INTERNET_DISCONNECTED|failed to (?:connect|lookup|resolve)|dns (?:error|failure|lookup)|getaddrinfo|name or service not known|network (?:is )?(?:down|offline|unavailable|unreachable)|no route to host|temporary failure in name resolution)/i;

const DEFAULT_BASE_DELAY_MS = 5_000;
const DEFAULT_MAXIMUM_DELAY_MS = 5 * 60_000;

export const NETWORK_UNAVAILABLE_ERROR_CODE = "ENRICHMENT_NETWORK_UNAVAILABLE";
export const COLLECTOR_NETWORK_UNAVAILABLE_ERROR_CODE =
  "SOURCE_NETWORK_UNAVAILABLE";

export function isNetworkFailureText(value: string): boolean {
  return NETWORK_ERROR_PATTERN.test(value);
}

/** Only failures that prove the connection was never established are safe to
 * submit again. Stream resets and timeouts are deliberately excluded because
 * the remote provider may already have accepted and billed the operation. */
export function isPredispatchNetworkFailureText(value: string): boolean {
  return PREDISPATCH_NETWORK_ERROR_PATTERN.test(value);
}

export function networkProbeDelayMs(
  consecutiveFailures: number,
  random: () => number = Math.random,
  baseDelayMs = DEFAULT_BASE_DELAY_MS,
  maximumDelayMs = DEFAULT_MAXIMUM_DELAY_MS,
): number {
  if (!Number.isSafeInteger(consecutiveFailures) || consecutiveFailures < 1)
    throw new Error("NETWORK_FAILURE_COUNT_INVALID");
  if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs < 1)
    throw new Error("NETWORK_BACKOFF_BASE_INVALID");
  if (!Number.isSafeInteger(maximumDelayMs) || maximumDelayMs < baseDelayMs)
    throw new Error("NETWORK_BACKOFF_MAXIMUM_INVALID");
  const sample = random();
  if (!Number.isFinite(sample) || sample < 0 || sample >= 1)
    throw new Error("NETWORK_BACKOFF_RANDOM_INVALID");
  const ceiling = Math.min(
    maximumDelayMs,
    baseDelayMs * 2 ** Math.min(consecutiveFailures - 1, 20),
  );
  return Math.max(1, Math.floor(ceiling * (0.5 + sample * 0.5)));
}
