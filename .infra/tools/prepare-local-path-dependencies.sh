#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
config="$project_root/.infra/ci/local-path-dependencies.tsv"
[ -f "$config" ] || exit 0
while IFS=$'\t' read -r relative_path repository_url branch; do
    [[ -z "$relative_path" || "$relative_path" == \#* ]] && continue
    branch=${branch%$'\r'}
    [[ "$relative_path" == ../* && "$relative_path" != *$'\t'* ]] || {
        echo "error: invalid local dependency path '$relative_path'" >&2; exit 1;
    }
    [[ -n "$repository_url" && -n "$branch" ]] || {
        echo "error: incomplete local dependency entry '$relative_path'" >&2; exit 1;
    }
    target="$project_root/$relative_path"
    [ -e "$target/.git" ] && continue
    mkdir -p "$(dirname "$target")"
    git clone --depth 1 --branch "$branch" "$repository_url" "$target"
done < "$config"
