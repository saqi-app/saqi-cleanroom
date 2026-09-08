#!/usr/bin/env bash
set -euo pipefail

readonly curl_timeout_seconds=30
canary_dir="$(mktemp -d)"
trap 'rm -rf -- "$canary_dir"' EXIT

curl_probe() {
  local name=$1
  shift
  echo "canary: ${name}" >&2
  curl --silent --show-error --connect-timeout 10 \
    --max-time "$curl_timeout_seconds" "$@"
}

is_cloudflare_challenge() {
  local headers=$1
  grep -Eiq '^cf-mitigated:[[:space:]]*challenge[[:space:]]*\r?$' "$headers" \
    && grep -Eiq '^server:[[:space:]]*cloudflare[[:space:]]*\r?$' "$headers" \
    && grep -Eiq '^cf-ray:[[:space:]]*[^[:space:]]+' "$headers"
}

public_headers="${canary_dir}/public-home.headers"
public_status="$(curl_probe 'public homepage edge classification' \
  --dump-header "$public_headers" \
  --output "${canary_dir}/home.html" \
  --write-out '%{http_code}' \
  --user-agent 'SaqiProductionCanary/1.0' \
  https://saqi.app/)"

public_content_available=false
if [[ "$public_status" == '200' ]]; then
  public_content_available=true
elif [[ "$public_status" == '403' ]] \
  && is_cloudflare_challenge "$public_headers"; then
  echo 'PUBLIC_CANARY_FAILED: Cloudflare challenged the public probe; production content was not verified' >&2
  exit 1
else
  echo "public homepage canary returned unexpected HTTP ${public_status}" >&2
  sed -n '1,40p' "$public_headers" >&2
  exit 1
fi

if [[ "$public_content_available" == true ]]; then
  curl_probe 'public author page' --fail --retry 2 --retry-all-errors \
    --output "${canary_dir}/author.html" \
    https://saqi.app/author/poet-abn-rumi
  curl_probe 'public poem page' --fail --retry 2 --retry-all-errors \
    --output "${canary_dir}/poem.html" \
    https://saqi.app/author/poet-abn-rumi/poem/5cf197dd-692d-4024-acc5-ee081560263f
  curl_probe 'public sitemap' --fail --retry 2 --retry-all-errors \
    --output "${canary_dir}/sitemap-index.xml" \
    https://saqi.app/sitemap-index.xml

  grep -Fq 'rel="canonical" href="https://saqi.app/author/poet-abn-rumi"' \
    "${canary_dir}/author.html"
  grep -Fq 'lang="ar"' "${canary_dir}/poem.html"
  grep -Fq '<sitemapindex' "${canary_dir}/sitemap-index.xml"
  if grep -Fq 'cloudflareinsights' "${canary_dir}/home.html"; then
    echo 'public homepage unexpectedly contains Cloudflare browser analytics' >&2
    exit 1
  fi

  http_status() {
    curl_probe "$1" --output /dev/null --write-out '%{http_code}' "$2"
  }

  test "$(http_status 'missing pagination page' \
    https://saqi.app/author/poet-abn-rumi/page/999999)" = '404'
  test "$(http_status 'empty pagination page' \
    https://saqi.app/author/poet-abn-rumi/page/2)" = '404'
  test "$(http_status 'canonical-host redirect' https://www.saqi.app/)" = '308'
fi

access_client_id=${CF_ACCESS_CLIENT_ID:-}
access_client_secret=${CF_ACCESS_CLIENT_SECRET:-}
if [[ -z "$access_client_id" || -z "$access_client_secret" ]]; then
  echo 'authenticated publication canary credentials are required' >&2
  exit 1
fi

access_headers=(
  --header "CF-Access-Client-Id: ${access_client_id}"
  --header "CF-Access-Client-Secret: ${access_client_secret}"
)

curl_probe 'authenticated publication identity' --fail --retry 2 --retry-all-errors \
  "${access_headers[@]}" \
  https://ops.saqi.app/api/corpus-import \
  | node scripts/verify-publication-identity.mjs

readonly resolution_url='https://ops.saqi.app/api/corpus-resolution'
readonly source_request='{"schemaId":"saqi.production-resolution-request","schemaVersion":2,"targets":[{"modelKeys":["sol-5.6"],"sourceAuthorSlug":"poet-Abdelkader-El-Djezairi","sourcePoemId":"47644"}]}'

curl_probe 'authenticated source resolution bootstrap' --fail --retry 2 --retry-all-errors \
  "${access_headers[@]}" \
  --header 'Content-Type: application/json' \
  --header 'Origin: https://ops.saqi.app' \
  --header 'Sec-Fetch-Mode: cors' \
  --header 'Sec-Fetch-Site: same-origin' \
  --request POST \
  --data "$source_request" \
  --dump-header "${canary_dir}/source-resolution.headers" \
  --output "${canary_dir}/source-resolution.json" \
  "$resolution_url"

canonical_request="$(node scripts/verify-production-resolution.mjs \
  source "$source_request" \
  "${canary_dir}/source-resolution.headers" \
  "${canary_dir}/source-resolution.json")"

curl_probe 'authenticated exact-current canonical v2 resolution' \
  --fail --retry 2 --retry-all-errors \
  "${access_headers[@]}" \
  --header 'Content-Type: application/json' \
  --header 'Origin: https://ops.saqi.app' \
  --header 'Sec-Fetch-Mode: cors' \
  --header 'Sec-Fetch-Site: same-origin' \
  --request POST \
  --data "$canonical_request" \
  --dump-header "${canary_dir}/canonical-resolution.headers" \
  --output "${canary_dir}/canonical-resolution.json" \
  "$resolution_url"

node scripts/verify-production-resolution.mjs \
  canonical "$canonical_request" \
  "${canary_dir}/canonical-resolution.headers" \
  "${canary_dir}/canonical-resolution.json"

fingerprint_request="$(yarn workspace @saqi/app db:execute --remote --json \
  --command "WITH active AS (SELECT fingerprint.algorithm, fingerprint.line_nfc_hash AS lineNfcHash, fingerprint.prompt_material_hash AS promptMaterialHash, revision.source_poem_id FROM source_revision_fingerprint fingerprint JOIN poem_source_revision revision ON revision.id = fingerprint.source_revision_id JOIN poem ON poem.active_source_revision_id = revision.id JOIN source_poem_identity source_poem ON source_poem.id = revision.source_poem_id AND source_poem.canonical_poem_id = poem.id AND source_poem.tombstoned_at IS NULL JOIN poem_source_pointer source_pointer ON source_pointer.source_poem_id = source_poem.id AND source_pointer.revision_id = revision.id), unique_fingerprint AS (SELECT algorithm, lineNfcHash, promptMaterialHash FROM active GROUP BY algorithm, lineNfcHash, promptMaterialHash HAVING COUNT(DISTINCT source_poem_id) = 1) SELECT algorithm, lineNfcHash, promptMaterialHash FROM unique_fingerprint LIMIT 1" \
  | node scripts/select-production-resolution-fingerprint.mjs)"

curl_probe 'authenticated dual-fingerprint v3 resolution' \
  --fail --retry 2 --retry-all-errors \
  "${access_headers[@]}" \
  --header 'Content-Type: application/json' \
  --header 'Origin: https://ops.saqi.app' \
  --header 'Sec-Fetch-Mode: cors' \
  --header 'Sec-Fetch-Site: same-origin' \
  --request POST \
  --data "$fingerprint_request" \
  --dump-header "${canary_dir}/fingerprint-resolution.headers" \
  --output "${canary_dir}/fingerprint-resolution.json" \
  "$resolution_url"

node scripts/verify-production-resolution.mjs \
  fingerprint "$fingerprint_request" \
  "${canary_dir}/fingerprint-resolution.headers" \
  "${canary_dir}/fingerprint-resolution.json"

test "$(curl_probe 'unauthenticated operations boundary' \
  --output /dev/null --write-out '%{http_code}' https://ops.saqi.app/)" = '403'

if [[ "$public_content_available" == true ]]; then
  echo 'PUBLIC_CANARY_OK: public application content and authenticated operations verified' >&2
else
  echo 'PUBLIC_CANARY_CHALLENGED: authenticated operations verified; public application content remains unverified by CI' >&2
fi
