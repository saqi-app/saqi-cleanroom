#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: affected-artifacts.sh <ci|deploy> <base-sha> <head-sha> <event-name>}
base_sha=${2:-}
head_sha=${3:-}
event_name=${4:-}

if [[ "$mode" != "ci" && "$mode" != "deploy" ]]; then
  echo "unsupported mode: $mode" >&2
  exit 2
fi

if [[ ! "$head_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "head revision must be a full Git commit ID" >&2
  exit 2
fi
if [[ "$event_name" != "workflow_dispatch" && ! "$base_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "base revision must be a full Git commit ID" >&2
  exit 2
fi

app=false
migrate=false
site=false
www=false
crawler_only=false
dependency_audit=false

select_all() {
  app=true
  site=true
  www=true
}

is_dependency_audit_path() {
  case "$1" in
    typescript/.nvmrc|typescript/.yarn/*|typescript/.yarnrc.yml|typescript/package.json|typescript/yarn.lock|typescript/packages/*/package.json)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

trusted_dispatch_baseline=false
if [[ "$event_name" == "workflow_dispatch" &&
  "$base_sha" =~ ^[0-9a-f]{40}$ ]] &&
  git cat-file -e "$base_sha^{commit}" 2>/dev/null &&
  git merge-base --is-ancestor "$base_sha" "$head_sha"; then
  trusted_dispatch_baseline=true
fi

if [[ "$event_name" == "workflow_dispatch" && "$trusted_dispatch_baseline" != true ]]; then
  select_all
  migrate=true
  # Without a trusted successful-deployment baseline, retain the audit.
  dependency_audit=true
elif [[ -z "$base_sha" || "$base_sha" =~ ^0+$ ]]; then
  select_all
  dependency_audit=true
  migrate=true
else
  crawler_only=true
  saw_change=false
  while IFS= read -r -d '' path; do
    saw_change=true
    if is_dependency_audit_path "$path"; then
      dependency_audit=true
    fi
    if [[ "$path" != typescript/packages/crawler-local/* &&
      "$path" != typescript/packages/source-adapter/* ]]; then
      crawler_only=false
    fi
    case "$path" in
      .github/scripts/*|.github/workflows/*)
        select_all
        [[ "$mode" == "deploy" ]] && migrate=true
        ;;
      typescript/package.json|typescript/yarn.lock|typescript/doctor.config.json|typescript/eslint.config.mjs|typescript/eslint.strict.mjs|typescript/knip.json|typescript/tsconfig.base.json)
        select_all
        ;;
      typescript/packages/precedent-iso/*)
        app=true
        site=true
        ;;
      typescript/packages/precedent-node/*)
        app=true
        ;;
      typescript/packages/crawler-local/*|typescript/packages/source-adapter/*)
        # Local runtime packages do not produce a Cloudflare artifact.
        ;;
      typescript/packages/app/migrations/*)
        app=true
        migrate=true
        site=true
        ;;
      typescript/packages/app/*)
        app=true
        ;;
      typescript/packages/site/www-worker.ts|typescript/packages/site/wrangler.www.jsonc)
        www=true
        ;;
      typescript/packages/site/*)
        site=true
        ;;
      typescript/*)
        # Fail closed for new workspace-level tooling and future packages.
        select_all
        ;;
    esac
  done < <(git diff --name-only -z "$base_sha" "$head_sha")
  if [[ "$saw_change" != true ]]; then
    crawler_only=false
  fi
fi

deployment_required=false
if [[ "$app" == true || "$migrate" == true ||
  "$site" == true || "$www" == true ]]; then
  deployment_required=true
fi

{
  echo "app=$app"
  echo "migrate=$migrate"
  echo "site=$site"
  echo "www=$www"
  echo "crawler_only=$crawler_only"
  echo "dependency_audit=$dependency_audit"
  echo "deployment_required=$deployment_required"
} >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"
