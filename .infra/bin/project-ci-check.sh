#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
project_hook="$project_root/scripts/project-ci-check.sh"
if [ -x "$project_hook" ]; then
    "$project_hook" "$@"
elif [ -f "$project_hook" ]; then
    bash "$project_hook" "$@"
else
    echo "No project-specific CI hook is configured."
fi
