#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
[ "$#" -ge 1 ] || { echo "usage: infra-tool.sh rs-infra-TOOL [ARGS...]" >&2; exit 2; }
tool="$1"
shift
case "$tool" in
    rs-infra-ci|rs-infra-coverage|rs-infra-dependency|rs-infra-pages|rs-infra-style|rs-infra-tools|rs-infra-verify) ;;
    *) echo "error: unknown infra tool '$tool'" >&2; exit 2 ;;
esac
role="${tool#rs-infra-}"
tool_root="$project_root/.infra/$role"
config="$tool_root/tool.toml"
[ -f "$config" ] || { echo "error: missing tool configuration '$config'" >&2; exit 1; }
value() { awk -F '"' -v key="$1" '$0 ~ "^[[:space:]]*" key "[[:space:]]*=" { print $2; exit }' "$config"; }
source=$(value source)
revision=$(value revision)
binary=$(value binary)
package=$(value package)
[ -n "$source" ] && [ -n "$revision" ] && [ -n "$binary" ] && [ -n "$package" ] || {
    echo "error: incomplete tool configuration '$config'" >&2; exit 1;
}
bin_dir="$tool_root/bin"
target="$bin_dir/$binary"
marker="$tool_root/tool.revision"
mkdir -p "$bin_dir"
installed=""
[ -f "$marker" ] && IFS= read -r installed < "$marker" || true
if [ ! -x "$target" ] || [ "$installed" != "$revision" ]; then
    echo "==> installing $tool@$revision"
    PATH="$bin_dir:${PATH:-}" cargo install --git "$source" --rev "$revision" --locked --force --root "$tool_root" "$package" --bin "$binary"
    printf '%s\n' "$revision" > "$marker.tmp"
    mv "$marker.tmp" "$marker"
fi
if [ "$tool" = "rs-infra-ci" ]; then
    for dependency in rs-infra-style rs-infra-verify rs-infra-coverage; do
        "$script_dir/infra-tool.sh" "$dependency" --help >/dev/null
    done
fi
all_tool_bins="$project_root/.infra/ci/bin:$project_root/.infra/coverage/bin:$project_root/.infra/dependency/bin:$project_root/.infra/pages/bin:$project_root/.infra/style/bin:$project_root/.infra/tools/bin:$project_root/.infra/verify/bin"
env -u RS_INFRA_BIN_DIR PATH="$all_tool_bins:${PATH:-}" "$target" "$@"
