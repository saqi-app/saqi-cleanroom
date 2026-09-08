// Keep a live reference so in-process credential refreshes are observable.
// Persistent secret storage remains the responsibility of the process manager.
// eslint-disable-next-line @sarj/no-raw-env -- This module is the validated runtime environment boundary.
export const RUNTIME_ENVIRONMENT: Readonly<NodeJS.ProcessEnv> = process.env;
