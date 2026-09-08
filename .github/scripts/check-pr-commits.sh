#!/usr/bin/env bash
set -euo pipefail

readonly object_id_pattern='^[0-9a-f]{40}$'
if [[ ! "${BASE_SHA:-}" =~ $object_id_pattern || ! "${HEAD_SHA:-}" =~ $object_id_pattern ]]; then
  echo 'Commit policy requires exact base and head object IDs.' >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$BASE_SHA" "$HEAD_SHA"; then
  echo 'Pull-request base is not an ancestor of its head.' >&2
  exit 1
fi

subjects=()
while IFS= read -r subject; do
  subjects[${#subjects[@]}]=$subject
done < <(git log --format=%s --no-merges --reverse "$BASE_SHA..$HEAD_SHA")
readonly commit_count=${#subjects[@]}
readonly maximum_commits=5
readonly header_pattern='^(\([1-9][0-9]*/[1-9][0-9]*\) )?(\[([A-Z][A-Z0-9]*-[0-9]+|NO-TICKET)\] )?(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\([^()[:cntrl:]]+\))?!?: [^[:space:]].*$'
for subject in "${subjects[@]}"; do
  if [[ ! "$subject" =~ $header_pattern ]]; then
    echo "Invalid commit header: $subject" >&2
    echo 'Use [(i/n) ][TICKET] type(scope)!: description; prefixes and scope are optional.' >&2
    exit 1
  fi
done

if ((commit_count <= maximum_commits)); then
  echo "PR commits: $commit_count non-merge commit(s); limit $maximum_commits"
  exit 0
fi

readonly numbered_pattern='^\(([1-9][0-9]*)/([1-9][0-9]*)\) [^[:space:]].*$'
for ((index = 1; index <= commit_count; index++)); do
  subject=${subjects[index - 1]}
  if [[ ! "$subject" =~ $numbered_pattern ]]; then
    echo "Commit $index must start with ($index/$commit_count)." >&2
    exit 1
  fi
  if ((10#${BASH_REMATCH[1]} != index || 10#${BASH_REMATCH[2]} != commit_count)); then
    echo "Commit $index has an incomplete or out-of-order series marker." >&2
    exit 1
  fi
done
echo "PR commits: complete ordered ($commit_count/$commit_count) series"
