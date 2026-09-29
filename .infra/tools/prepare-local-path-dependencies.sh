#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
config="$project_root/.infra/ci/local-path-dependencies.tsv"
[ -f "$config" ] || exit 0
while IFS=$'\t' read -r relative_path repository_url revision; do
    [[ -z "$relative_path" || "$relative_path" == \#* ]] && continue
    revision=${revision%$'\r'}
    [[ "$relative_path" == ../* && "$relative_path" != *$'\t'* ]] || {
        echo "error: invalid local dependency path '$relative_path'" >&2; exit 1;
    }
    [[ -n "$repository_url" && -n "$revision" ]] || {
        echo "error: incomplete local dependency entry '$relative_path'" >&2; exit 1;
    }
    target="$project_root/$relative_path"
    [ -e "$target/.git" ] && continue
    mkdir -p "$(dirname "$target")"
    if [[ "$revision" =~ ^[0-9a-fA-F]{40}$ ]]; then
        git clone "$repository_url" "$target"
        git -C "$target" checkout --detach "$revision"
    else
        git clone --depth 1 --branch "$revision" "$repository_url" "$target"
    fi
done < "$config"
