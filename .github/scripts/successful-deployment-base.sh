#!/usr/bin/env bash
set -euo pipefail

repository=${GITHUB_REPOSITORY:-}
current_sha=${GITHUB_SHA:-}
api_url=${GITHUB_API_URL:-https://api.github.com}
token=${GITHUB_TOKEN:-}
output=${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}

fallback() {
  echo 'base_sha=' >> "$output"
  echo "::warning::$1; dependency audit remains enabled"
  exit 0
}

if [[ ! "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ||
  ! "$current_sha" =~ ^[0-9a-f]{40}$ || -z "$token" ]]; then
  fallback 'Cannot resolve a trusted production deployment baseline'
fi

api_get() {
  command curl --fail --silent --show-error \
    --connect-timeout 10 --max-time 20 --retry 1 --retry-all-errors \
    -H 'Accept: application/vnd.github+json' \
    -H "Authorization: Bearer $token" \
    -H 'X-GitHub-Api-Version: 2022-11-28' "$1"
}

deployments_url="$api_url/repos/$repository/deployments?environment=production&per_page=100"
if ! deployments=$(api_get "$deployments_url"); then
  fallback 'GitHub deployments query failed'
fi
if ! jq -e 'type == "array"' >/dev/null <<< "$deployments"; then
  fallback 'GitHub deployments response was invalid'
fi

base_sha=
while IFS=$'\t' read -r deployment_id deployed_sha; do
  [[ "$deployed_sha" == "$current_sha" ]] && continue
  [[ "$deployment_id" =~ ^[0-9]+$ && "$deployed_sha" =~ ^[0-9a-f]{40}$ ]] || continue
  statuses_url="$api_url/repos/$repository/deployments/$deployment_id/statuses?per_page=1"
  if ! statuses=$(api_get "$statuses_url"); then
    fallback 'GitHub deployment-status query failed'
  fi
  if ! state=$(jq -er 'if type == "array" then (.[0].state // "") else error("invalid") end' <<< "$statuses"); then
    fallback 'GitHub deployment-status response was invalid'
  fi
  if [[ "$state" == "success" ]] &&
    git cat-file -e "$deployed_sha^{commit}" 2>/dev/null &&
    git merge-base --is-ancestor "$deployed_sha" "$current_sha"; then
    base_sha=$deployed_sha
    break
  fi
done < <(jq -r '.[] | [.id, .sha] | @tsv' <<< "$deployments")

echo "base_sha=$base_sha" >> "$output"
if [[ -z "$base_sha" ]]; then
  echo '::warning::No trusted successful production deployment baseline found; dependency audit remains enabled'
fi
