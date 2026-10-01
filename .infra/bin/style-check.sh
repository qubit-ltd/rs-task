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
"$project_root/.infra/bin/infra-tool.sh" rs-infra-style --project "$project_root" check "$@"
