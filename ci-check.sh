#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
export RS_INFRA_STYLE_TOOLCHAIN="${RS_INFRA_STYLE_TOOLCHAIN:-nightly-2026-06-05}"
if [ -f "$project_root/.infra/style/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/.infra/style/rustfmt.toml"
elif [ -f "$project_root/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/rustfmt.toml"
fi
"$project_root/.infra/tools/prepare-local-path-dependencies.sh"
exec "$project_root/.infra/tools/infra-tool.sh" rs-infra-ci --project "$project_root" "$@" check
