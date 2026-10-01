#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
export RS_INFRA_STYLE_TOOLCHAIN="${RS_INFRA_STYLE_TOOLCHAIN:-nightly-2026-06-05}"
if [ -f "$project_root/.infra/style/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/.infra/style/rustfmt.toml"
elif [ -f "$project_root/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/rustfmt.toml"
fi
"$project_root/.infra/bin/prepare-local-path-dependencies.sh"
"$project_root/.infra/bin/infra-tool.sh" rs-infra-ci --project "$project_root" "$@" check

coverage_selected=true
explicit_only=false
for ((index = 1; index <= $#; index++)); do
    argument=${!index}
    if [ "$argument" = "--only" ]; then
        explicit_only=true
        next_index=$((index + 1))
        selected_tasks=${!next_index:-}
        break
    elif [[ "$argument" == --only=* ]]; then
        explicit_only=true
        selected_tasks=${argument#--only=}
        break
    fi
done
if [ "$explicit_only" = true ]; then
    coverage_selected=false
    IFS=',' read -r -a selected_task_list <<< "$selected_tasks"
    for task in "${selected_task_list[@]}"; do
        if [ "$task" = coverage ]; then
            coverage_selected=true
        fi
    done
fi
if [ "$coverage_selected" = true ] && [ -f "$project_root/target/infra/coverage/raw.json" ]; then
    "$project_root/.infra/lib/coverage-report.sh"
fi
