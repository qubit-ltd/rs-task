#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
config="$project_root/.infra/ci/tools.toml"
install_root="$project_root/.infra/tools/bin"
bin_dir="$install_root/bin"

die() {
    echo "error: $*" >&2
    exit 1
}

[ "$#" -ge 1 ] || die "usage: infra-tool.sh TOOL [ARGS...]"
tool="$1"
shift
[ -f "$config" ] || die "tool configuration was not found at '$config'"

tool_value() {
    local key="$1"
    awk -v section="[$tool]" -v key="$key" '
        $0 == section { in_section = 1; next }
        /^\[/ { in_section = 0 }
        in_section && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
            value = $0
            sub(/^[^=]*=[[:space:]]*"/, "", value)
            sub(/"[[:space:]]*$/, "", value)
            print value
            exit
        }
    ' "$config"
}

source=$(tool_value source)
revision=$(tool_value revision)
binary=$(tool_value binary)
package=$(tool_value package)
[ -n "$source" ] || die "source is missing for '$tool'"
[ -n "$revision" ] || die "revision is missing for '$tool'"
[ -n "$binary" ] || die "binary is missing for '$tool'"
[ -n "$package" ] || die "package is missing for '$tool'"

mkdir -p "$bin_dir"
target="$bin_dir/$binary"
revision_marker="$install_root/$tool.revision"
installed_revision=""
if [ -f "$revision_marker" ]; then
    IFS= read -r installed_revision < "$revision_marker" || true
fi
if [ ! -x "$target" ] || [ "$installed_revision" != "$revision" ]; then
    echo "==> installing $tool@$revision"
    cargo install --git "$source" --rev "$revision" --locked --force --root "$install_root" \
        "$package" --bin "$binary"
    marker_tmp="$revision_marker.tmp"
    printf '%s\n' "$revision" > "$marker_tmp"
    mv "$marker_tmp" "$revision_marker"
fi

if [ "$tool" = "rs-infra-ci" ]; then
    "$script_dir/infra-tool.sh" rs-infra-style --help >/dev/null
    "$script_dir/infra-tool.sh" rs-infra-verify --help >/dev/null
    "$script_dir/infra-tool.sh" rs-infra-coverage --help >/dev/null
fi

exec env RS_INFRA_BIN_DIR="$bin_dir" "$target" "$@"
