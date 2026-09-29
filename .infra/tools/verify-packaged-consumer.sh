#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
manifest="$project_root/Cargo.toml"
source_cargo_home=${CARGO_HOME:-${HOME:?HOME must be set}/.cargo}
workspace=$(mktemp -d /tmp/rs-task-packaged-consumer.XXXXXXXX)
package_target=/tmp/superpowers-rs-task-gc6n4m1u/package-target

validate_workspace() {
    local resolved
    resolved=$(cd "$workspace" && pwd -P)
    [[ "$resolved" == /tmp/rs-task-packaged-consumer.* && -f "$resolved/.rs-task-owned-temp" ]]
    [[ $(cat "$resolved/.rs-task-owned-temp") == rs-task-package-verification ]]
}

cleanup() {
    if validate_workspace; then
        command rm -rf -- "$workspace"
    else
        printf 'Refusing to remove unvalidated temporary path: %s\n' "$workspace" >&2
    fi
}
trap cleanup EXIT
printf 'rs-task-package-verification\n' > "$workspace/.rs-task-owned-temp"
validate_workspace

check_cargo_config() {
    local config_path=$1
    [[ -f "$config_path" ]] || return 0
    python3 - "$config_path" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as cargo_config:
    config = tomllib.load(cargo_config)
if config.get("patch"):
    raise SystemExit(f"Cargo config contains [patch], refusing packaged verification: {sys.argv[1]}")
PY
}

for cargo_config in config.toml config; do
    if [[ -f "$source_cargo_home/$cargo_config" ]]; then
        check_cargo_config "$source_cargo_home/$cargo_config"
        command cp "$source_cargo_home/$cargo_config" "$workspace/$cargo_config"
    fi
done
check_cargo_config "$project_root/.cargo/config.toml"
check_cargo_config "$project_root/.cargo/config"
config_parent=$(dirname "$workspace")
while :; do
    check_cargo_config "$config_parent/.cargo/config.toml"
    check_cargo_config "$config_parent/.cargo/config"
    [[ "$config_parent" == / ]] && break
    config_parent=$(dirname "$config_parent")
done
for credentials_file in credentials.toml credentials; do
    if [[ -f "$source_cargo_home/$credentials_file" ]]; then
        command cp -p "$source_cargo_home/$credentials_file" "$workspace/$credentials_file"
    fi
done

export CARGO_HOME="$workspace/cargo-home"
mkdir -p "$CARGO_HOME"
for cargo_config in config.toml config; do
    [[ ! -e "$workspace/$cargo_config" ]] || command cp "$workspace/$cargo_config" "$CARGO_HOME/$cargo_config"
done
for credentials_file in credentials.toml credentials; do
    if [[ -f "$workspace/$credentials_file" ]]; then
        command cp -p "$workspace/$credentials_file" "$CARGO_HOME/$credentials_file"
    fi
done

printf 'Package source: crates.io registry dependencies (no sibling path patches)\n'
printf 'Temporary workspace: %s\n' "$workspace"
cd "$workspace"

package_file_list="$workspace/packaged-files.txt"
cargo package --manifest-path "$manifest" --locked --no-default-features --features sqlite,conformance --list > "$package_file_list"
printf 'Package file list reviewed for secrets and sibling dependency patches:\n'
cat "$package_file_list"
if rg -ni '(^|/)(\.env([^/]*|$)|credentials?([^/]*|$)|[^/]*\.(pem|key))' "$package_file_list"; then
    printf 'Refusing to package a likely secret or credential file.\n' >&2
    exit 1
fi

# Cargo's own verification is deliberately enabled; --allow-dirty includes the reviewed worktree diff.
cargo package --manifest-path "$manifest" \
    --target-dir "$package_target" \
    --locked --allow-dirty --no-default-features --features sqlite,conformance

archive="$package_target/package/qubit-task-0.8.0.crate"
[[ -f "$archive" ]] || { printf 'Packaged archive not found: %s\n' "$archive" >&2; exit 1; }
mkdir "$workspace/unpacked"
tar -xzf "$archive" -C "$workspace/unpacked"
package_dir="$workspace/unpacked/qubit-task-0.8.0"
[[ -f "$package_dir/Cargo.toml" ]] || { printf 'Invalid package archive: %s\n' "$archive" >&2; exit 1; }

consumer="$workspace/consumer"
mkdir "$consumer"
command cp -R "$project_root/tests/fixtures/conformance-consumer/." "$consumer"
python3 - "$consumer/Cargo.toml" "$package_dir" <<'PY'
import pathlib
import sys

manifest = pathlib.Path(sys.argv[1])
package = pathlib.Path(sys.argv[2])
text = manifest.read_text()
text = text.replace('path = "../../.."', f'path = "{package}"')
manifest.write_text(text)
PY
cd "$consumer"

cargo check --manifest-path Cargo.toml --locked --no-default-features
cargo check --manifest-path Cargo.toml --locked --no-default-features --features conformance
cargo check --manifest-path Cargo.toml --locked --no-default-features --features sqlite
cargo check --manifest-path Cargo.toml --locked --no-default-features --features sqlite,conformance
cargo run --manifest-path Cargo.toml --locked --no-default-features --features sqlite,conformance
cargo check --manifest-path Cargo.toml --locked --all-features
cargo metadata --manifest-path Cargo.toml --locked --format-version 1 > "$workspace/consumer-metadata.json"
python3 - "$workspace/consumer-metadata.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)
sources = sorted({
    dependency["source"]
    for package in metadata["packages"]
    for dependency in package["dependencies"]
    if dependency.get("source", "").startswith("registry+")
})
if not sources:
    raise SystemExit("No registry dependencies were resolved for the packaged consumer")
print("Resolved registry sources:")
for source in sources:
    print(f"  {source}")
PY
