# Saqi

Saqi is an Arabic poetry corpus, translation pipeline, and public reading
experience. The repository contains:

- a restart-safe local collection and Codex translation service;
- a versioned corpus and publication API;
- Cloudflare Workers for ingestion and operations;
- the public Astro site; and
- a native macOS health and control widget.

The local pipeline stores durable operational state in SQLite. Generated model
artifacts are not considered published until their source binding and validation
evidence have been accepted by the publication API.

## Development

The TypeScript workspace requires Node.js 24 or newer and Yarn 4:

```sh
cd typescript
yarn install --immutable
yarn check
yarn test
```

The macOS monitor requires the current Xcode toolchain:

```sh
cd macos/SaqiRigMonitor
swift test
```

Local runtime state, credentials, production exports, and browser profiles must
remain outside Git. See [SECURITY.md](SECURITY.md) for private vulnerability
reporting and secret-handling requirements.

Collection deployments must provide source identity and a declarative adapter
profile together. Keep their values outside the repository: the macOS service
reads `saqi-source-name`, `saqi-source-base-url`, and
`saqi-source-adapter-v1` from Keychain. The profile is bounded JSON containing
route templates, selectors, and literal labels; it cannot execute code or
inject regular expressions. Hosted deployments inject the source identity from
the platform secret manager. Development and tests use an inert synthetic
source profile.

The GitHub `production` environment must define `CF_ACCESS_AUD`,
`CF_ACCESS_TEAM_DOMAIN`, `CF_CACHE_PURGE_TOKEN`, `CF_ZONE_ID`,
`SAQI_SOURCE_ADAPTER_CONFIG`, `SAQI_SOURCE_BASE_URL`, and `SAQI_SOURCE_NAME`.
The deploy workflow validates all seven and replaces the Worker bindings in one
atomic secret operation before deploying the authenticated operations Worker.
Use a dedicated token
limited to cache purge for `CF_CACHE_PURGE_TOKEN`; do not reuse either Workers
deployment token.

The generated LaunchAgent uses the `saqi-publication` Keychain account entries
`saqi-source-name`, `saqi-source-base-url`, and `saqi-source-adapter-v1` and
fails closed if any entry is missing or invalid. Source values are never
embedded in its plist or arguments.

## License

Saqi's original source code is licensed under [Zero-Clause BSD (0BSD)](LICENSE).
You may use, modify, and redistribute it for any purpose, including commercial
use, without an attribution requirement.

Third-party dependencies and bundled fonts retain their own licenses and
notices. This software license does not grant rights to third-party poems,
translations, or other collected content.
