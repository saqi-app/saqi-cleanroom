#!/usr/bin/env bash
set -euo pipefail

resolver=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/successful-deployment-base.sh
fixture=$(mktemp -d)
mock_bin=$(mktemp -d)
output=$(mktemp)
trap 'rm -rf "$fixture" "$mock_bin"; rm -f "$output"' EXIT

git -C "$fixture" init -q
git -C "$fixture" config user.email ci-test@saqi.app
git -C "$fixture" config user.name 'Saqi CI test'
touch "$fixture/.seed"
git -C "$fixture" add .seed
git -C "$fixture" commit -qm seed
base=$(git -C "$fixture" rev-parse HEAD)
touch "$fixture/failed.ts"
git -C "$fixture" add failed.ts
git -C "$fixture" commit -qm failed
failed=$(git -C "$fixture" rev-parse HEAD)
touch "$fixture/source.ts"
git -C "$fixture" add source.ts
git -C "$fixture" commit -qm source
head=$(git -C "$fixture" rev-parse HEAD)

cat > "$mock_bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
url=${!#}
if [[ "${MOCK_API_FAILURE:-false}" == "true" ]]; then
  exit 22
fi
case "$url" in
  *'/deployments?'*)
    printf '[{"id":101,"sha":"%s"},{"id":102,"sha":"%s"},{"id":103,"sha":"%s"}]\n' \
      "$CURRENT_SHA_FOR_MOCK" "$FAILED_SHA_FOR_MOCK" "$BASE_SHA_FOR_MOCK"
    ;;
  *'/deployments/101/statuses?'*)
    printf '[{"state":"failure"}]\n'
    ;;
  *'/deployments/102/statuses?'*)
    printf '[{"state":"failure"}]\n'
    ;;
  *'/deployments/103/statuses?'*)
    printf '[{"state":"success"}]\n'
    ;;
  *)
    exit 22
    ;;
esac
EOF
chmod +x "$mock_bin/curl"

run_resolver() {
  (
    cd "$fixture"
    PATH="$mock_bin:$PATH" \
      GITHUB_REPOSITORY=saqi-app/saqi \
      GITHUB_SHA=$head \
      GITHUB_API_URL=https://example.invalid \
      GITHUB_TOKEN=test-token \
      GITHUB_OUTPUT=$output \
      CURRENT_SHA_FOR_MOCK=$head \
      FAILED_SHA_FOR_MOCK=$failed \
      BASE_SHA_FOR_MOCK=$base \
      MOCK_API_FAILURE=${1:-false} \
      bash "$resolver"
  )
}

run_resolver
if [[ "$(cat "$output")" != "base_sha=$base" ]]; then
  echo 'resolver did not select the prior successful deployment' >&2
  exit 1
fi

: > "$output"
run_resolver true
if [[ "$(cat "$output")" != 'base_sha=' ]]; then
  echo 'resolver did not fail closed after an API failure' >&2
  exit 1
fi

echo 'successful-deployment-base tests passed'
