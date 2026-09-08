#!/usr/bin/env bash
set -euo pipefail

classifier=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/affected-artifacts.sh

assert_case() {
  local mode=$1
  local path=$2
  local expected=$3
  local fixture output base head actual changed_path
  local -a paths=("$path")
  if [[ $# -ge 4 ]]; then
    paths+=("$4")
  fi
  fixture=$(mktemp -d)
  output=$(mktemp)
  trap 'rm -rf "$fixture"; rm -f "$output"' RETURN
  git -C "$fixture" init -q
  git -C "$fixture" config user.email ci-test@saqi.app
  git -C "$fixture" config user.name 'Saqi CI test'
  touch "$fixture/.seed"
  git -C "$fixture" add .seed
  git -C "$fixture" commit -qm seed
  base=$(git -C "$fixture" rev-parse HEAD)
  for changed_path in "${paths[@]}"; do
    mkdir -p "$fixture/$(dirname "$changed_path")"
    touch "$fixture/$changed_path"
    git -C "$fixture" add "$changed_path"
  done
  git -C "$fixture" commit -qm change
  head=$(git -C "$fixture" rev-parse HEAD)
  (
    cd "$fixture"
    GITHUB_OUTPUT=$output bash "$classifier" "$mode" "$base" "$head" pull_request
  )
  actual=$(tr '\n' ' ' < "$output" | sed 's/ $//')
  if [[ "$actual" != "$expected" ]]; then
    echo "classification mismatch for $path: $actual" >&2
    exit 1
  fi
  rm -rf "$fixture"
  rm -f "$output"
  trap - RETURN
}

assert_case ci typescript/packages/crawler-local/src/cli.ts \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=false deployment_required=false'
assert_case ci typescript/packages/source-adapter/src/index.ts \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=false deployment_required=false'
assert_case ci typescript/packages/crawler-local/src/cli.ts \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=false deployment_required=false' \
  typescript/packages/source-adapter/src/index.ts
assert_case ci typescript/packages/precedent-node/src/index.ts \
  'app=true migrate=false site=false www=false crawler_only=false dependency_audit=false deployment_required=true'
assert_case ci typescript/packages/precedent-iso/src/index.ts \
  'app=true migrate=false site=true www=false crawler_only=false dependency_audit=false deployment_required=true'
assert_case deploy typescript/packages/app/migrations/9999_test.sql \
  'app=true migrate=true site=true www=false crawler_only=false dependency_audit=false deployment_required=true'
assert_case ci typescript/packages/site/www-worker.ts \
  'app=false migrate=false site=false www=true crawler_only=false dependency_audit=false deployment_required=true'
assert_case deploy .github/scripts/new-script.sh \
  'app=true migrate=true site=true www=true crawler_only=false dependency_audit=false deployment_required=true'
assert_case ci typescript/packages/future/src/index.ts \
  'app=true migrate=false site=true www=true crawler_only=false dependency_audit=false deployment_required=true'
assert_case ci typescript/packages/crawler-local/package.json \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=true deployment_required=false'
assert_case ci typescript/.yarnrc.yml \
  'app=true migrate=false site=true www=true crawler_only=false dependency_audit=true deployment_required=true'
assert_case ci typescript/.nvmrc \
  'app=true migrate=false site=true www=true crawler_only=false dependency_audit=true deployment_required=true'

assert_dispatch_case() {
  local path=$1
  local expected=$2
  local fixture output base head actual
  fixture=$(mktemp -d)
  output=$(mktemp)
  trap 'rm -rf "$fixture"; rm -f "$output"' RETURN
  git -C "$fixture" init -q
  git -C "$fixture" config user.email ci-test@saqi.app
  git -C "$fixture" config user.name 'Saqi CI test'
  touch "$fixture/.seed"
  git -C "$fixture" add .seed
  git -C "$fixture" commit -qm seed
  base=$(git -C "$fixture" rev-parse HEAD)
  mkdir -p "$fixture/$(dirname "$path")"
  touch "$fixture/$path"
  git -C "$fixture" add "$path"
  git -C "$fixture" commit -qm change
  head=$(git -C "$fixture" rev-parse HEAD)
  (
    cd "$fixture"
    GITHUB_OUTPUT=$output bash "$classifier" deploy "$base" "$head" workflow_dispatch
  )
  actual=$(tr '\n' ' ' < "$output" | sed 's/ $//')
  if [[ "$actual" != "$expected" ]]; then
    echo "dispatch classification mismatch for $path: $actual" >&2
    exit 1
  fi
  rm -rf "$fixture"
  rm -f "$output"
  trap - RETURN
}

assert_dispatch_case typescript/packages/app/src/index.ts \
  'app=true migrate=false site=false www=false crawler_only=false dependency_audit=false deployment_required=true'
assert_dispatch_case typescript/yarn.lock \
  'app=true migrate=false site=true www=true crawler_only=false dependency_audit=true deployment_required=true'
assert_dispatch_case typescript/packages/crawler-local/src/cli.ts \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=false deployment_required=false'
assert_dispatch_case typescript/packages/crawler-local/package.json \
  'app=false migrate=false site=false www=false crawler_only=true dependency_audit=true deployment_required=false'
assert_dispatch_case README.md \
  'app=false migrate=false site=false www=false crawler_only=false dependency_audit=false deployment_required=false'
assert_dispatch_case typescript/packages/app/migrations/9999_test.sql \
  'app=true migrate=true site=true www=false crawler_only=false dependency_audit=false deployment_required=true'

output=$(mktemp)
trap 'rm -f "$output"' EXIT
GITHUB_OUTPUT=$output bash "$classifier" deploy '' "$(git rev-parse HEAD)" workflow_dispatch
if [[ "$(tr '\n' ' ' < "$output" | sed 's/ $//')" != \
  'app=true migrate=true site=true www=true crawler_only=false dependency_audit=true deployment_required=true' ]]; then
  echo 'dispatch classification did not fail closed without a baseline' >&2
  exit 1
fi
rm -f "$output"
trap - EXIT

echo 'affected-artifacts tests passed'
