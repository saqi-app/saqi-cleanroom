# Contributing to Saqi

Thank you for helping improve Saqi. Keep changes focused, include tests for
behavioral changes, and avoid committing credentials, production data, local
runtime state, or generated build output.

## Development setup

The TypeScript workspace requires Node.js 24 or newer and Yarn 4:

```sh
cd typescript
corepack enable
yarn install --immutable
yarn check
yarn test
yarn knip
```

Changes to the macOS monitor should also pass:

```sh
cd macos/SaqiRigMonitor
swift test
```

Run the smallest relevant test while iterating, then run the complete checks
for each affected workspace before opening a pull request. Do not commit files
under ignored build, coverage, runtime, or credential paths.

The local runtime's POSIX fixture tests use the operating system's `mkfifo`
utility to verify that unsafe named pipes are rejected without blocking. Knip
lists this exact binary alongside macOS `plutil` in `ignoreBinaries` because
neither is an npm dependency. This exception does not skip either test or
ignore application files or exports.

## Pull requests

- Explain the user-visible or operational effect and how it was verified.
- Keep migrations forward-only and document their rollback or compatibility
  behavior.
- Preserve source provenance and publication validation invariants.
- Call out changes that affect paid work, production deployment, data
  retention, credentials, or security boundaries.
- Update documentation when configuration or operator actions change.

Security vulnerabilities must be reported privately as described in
[SECURITY.md](SECURITY.md), not through a public issue or pull request.

## License

By submitting original code for inclusion, you agree to license your
contribution under the project's [Zero-Clause BSD (0BSD) license](LICENSE).
Only contribute material you have the right to license. Preserve applicable
third-party licenses and notices; collected content is not relicensed by the
software license.
