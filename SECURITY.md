# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through the repository's
Security tab. Do not open a public issue, pull request, or discussion containing
exploit details, credentials, personal data, or other sensitive material.

Include the affected component, reproduction conditions, potential impact, and
the smallest safe proof of concept. Maintainers will acknowledge a report,
triage its severity, and coordinate remediation and disclosure through the
private report.

## Supported versions

Only the current default branch and the currently deployed release receive
security fixes. Historical commits, development branches, and old local runtime
artifacts are unsupported.

## Secrets

Never commit credentials, tokens, cookies, browser profiles, production data,
or local runtime state. Use the deployment platform's secret store or an
operator-managed local credential store. If a secret enters Git history, revoke
and rotate it before removing it from every reachable ref; rewriting history
alone does not revoke a credential.
