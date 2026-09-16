#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
"$script_dir/prepare-local-path-dependencies.sh"
policy_config="$project_root/.infra/dep/policy.toml"
tool_runner="$project_root/.infra/tools/infra-tool.sh"
mode="update"

usage() {
    cat <<'EOF_USAGE'
Usage: ./dependency-update.sh [--check|--update]

Options:
  --check   Check only; do not modify Cargo.toml files.
  --update  Synchronize dependency declarations, then check them (the default).
  -h, --help
EOF_USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --check) mode="check" ;;
        --update) mode="update" ;;
        -h | --help) usage; exit 0 ;;
        *) die "unknown option '$1'" ;;
    esac
    shift
done

[ -f "$project_root/Cargo.toml" ] || die "Cargo.toml was not found at '$project_root/Cargo.toml'"
[ -f "$policy_config" ] || die "dependency policy configuration was not found at '$policy_config'"

run_policy() {
    "$tool_runner" rs-infra-dependency --project "$project_root" "$@"
}

cd "$project_root"
if [ "$mode" = "check" ]; then
    run_policy check
    echo "Dependency baseline check passed."
    exit 0
fi

echo "==> synchronizing dependency declarations"
run_policy sync
echo "==> checking dependency baseline"
run_policy check
echo "Dependency baseline is synchronized."
