#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
manifest="$project_root/Cargo.toml"
source_cargo_home=${CARGO_HOME:-${HOME:?HOME must be set}/.cargo}
package_identity=$(python3 - "$manifest" <<'PY'
import re
import sys
import tomllib

with open(sys.argv[1], "rb") as source:
    package = tomllib.load(source).get("package", {})
name = package.get("name")
version = package.get("version")
component = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
if not isinstance(name, str) or not component.fullmatch(name) or name in {".", ".."}:
    raise SystemExit("Cargo package name is not a safe filename component")
if not isinstance(version, str) or not component.fullmatch(version) or version in {".", ".."}:
    raise SystemExit("Cargo package version is not a safe filename component")
print(f"{name}\t{version}")
PY
)
IFS=$'\t' read -r package_name package_version <<< "$package_identity"
workspace=$(mktemp -d /tmp/rs-task-packaged-consumer.XXXXXXXX)
package_target="$workspace/package-target"
consumer_target="$workspace/consumer-target"

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

cargo_config_files=()
for cargo_config in config.toml config; do
    if [[ -f "$source_cargo_home/$cargo_config" ]]; then
        cargo_config_files+=("$source_cargo_home/$cargo_config")
        command cp "$source_cargo_home/$cargo_config" "$workspace/$cargo_config"
    fi
done
for cargo_config in "$project_root/.cargo/config.toml" "$project_root/.cargo/config"; do
    [[ ! -f "$cargo_config" ]] || cargo_config_files+=("$cargo_config")
done
config_parent=$(dirname "$workspace")
while :; do
    for cargo_config in "$config_parent/.cargo/config.toml" "$config_parent/.cargo/config"; do
        [[ ! -f "$cargo_config" ]] || cargo_config_files+=("$cargo_config")
    done
    [[ "$config_parent" == / ]] && break
    config_parent=$(dirname "$config_parent")
done
python3 - "${cargo_config_files[@]}" <<'PY'
import os
import sys
import tomllib
from urllib.parse import urlparse, urlsplit

def redact_url(value):
    sparse_prefix = "sparse+" if value.startswith("sparse+") else ""
    parts = urlsplit(value.removeprefix(sparse_prefix))
    if not parts.scheme or not parts.hostname:
        raise SystemExit("Cargo registry URL has no verifiable scheme and hostname")
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    port = f":{parts.port}" if parts.port is not None else ""
    return f"{sparse_prefix}{parts.scheme}://{host}{port}"

sources = {}
registries = {}

def merge(left, right):
    merged = dict(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge(merged[key], value)
        else:
            merged[key] = value
    return merged

inherited_source_overrides = sorted(key for key in os.environ if key.startswith("CARGO_SOURCE_"))
if inherited_source_overrides:
    raise SystemExit("Inherited CARGO_SOURCE_* Cargo overrides are forbidden for packaged verification: " + ", ".join(inherited_source_overrides))
inherited_registry_indexes = sorted(
    key for key in os.environ
    if key.startswith("CARGO_REGISTRIES_") and key.endswith("_INDEX")
)
if inherited_registry_indexes:
    raise SystemExit("Inherited CARGO_REGISTRIES_*_INDEX overrides are forbidden for packaged verification: " + ", ".join(inherited_registry_indexes))

for filename in sys.argv[1:]:
    with open(filename, "rb") as cargo_config:
        config = tomllib.load(cargo_config)
    if config.get("patch"):
        raise SystemExit(f"Cargo config contains [patch], refusing packaged verification: {filename}")
    for source_name, source in config.get("source", {}).items():
        if not isinstance(source, dict):
            continue
        if "directory" in source or "local-registry" in source:
            raise SystemExit(f"Cargo source {source_name!r} in {filename} uses a local directory/registry")
        registry = source.get("registry")
        if registry and urlparse(registry.removeprefix("sparse+")).scheme != "https":
            raise SystemExit(f"Cargo source {source_name!r} in {filename} is not a remote HTTPS registry")
    for registry_name, registry in config.get("registries", {}).items():
        index = registry.get("index") if isinstance(registry, dict) else None
        if index and urlparse(index.removeprefix("sparse+")).scheme != "https":
            raise SystemExit(f"Cargo registry {registry_name!r} in {filename} is not a remote HTTPS index")
    sources = merge(sources, config.get("source", {}))
    registries = merge(registries, config.get("registries", {}))

crates_io = sources.get("crates-io", {})
replacement = crates_io.get("replace-with")
direct_registry = crates_io.get("registry")
if "directory" in crates_io or "local-registry" in crates_io:
    raise SystemExit("crates-io is configured as a local directory/registry; refusing packaged verification")
if replacement:
    chain = []
    seen = set()
    source_name = replacement
    while source_name:
        if source_name in seen:
            raise SystemExit(f"Cargo source replacement cycle at {source_name!r}")
        seen.add(source_name)
        source = sources.get(source_name)
        if not isinstance(source, dict):
            raise SystemExit(f"Cargo source replacement {source_name!r} is undefined; refusing to guess its registry")
        if "directory" in source or "local-registry" in source:
            raise SystemExit(f"Cargo source replacement {source_name!r} uses a local directory/registry")
        registry = source.get("registry")
        next_source = source.get("replace-with")
        if registry:
            normalized = registry.removeprefix("sparse+")
            if urlparse(normalized).scheme != "https":
                raise SystemExit(f"Cargo source replacement {source_name!r} is not a remote HTTPS registry")
            chain.append(f"{source_name}={redact_url(registry)}")
        elif not next_source:
            raise SystemExit(f"Cargo source replacement {source_name!r} is not a verifiable remote registry")
        source_name = next_source
    if not chain:
        raise SystemExit("Cargo source replacement chain contains no remote registry")
    print("Validated Cargo registry replacement chain: " + " -> ".join(chain))
elif direct_registry:
    normalized = direct_registry.removeprefix("sparse+")
    if urlparse(normalized).scheme != "https":
        raise SystemExit("Configured crates-io registry is not a remote HTTPS registry")
    print(f"Validated Cargo registry index: {redact_url(direct_registry)}")
else:
    configured_index = registries.get("crates-io", {}).get("index")
    if configured_index:
        normalized = configured_index.removeprefix("sparse+")
        if urlparse(normalized).scheme != "https":
            raise SystemExit("Configured crates-io index is not a remote HTTPS registry")
        print(f"Validated Cargo registry index: {redact_url(configured_index)}")
    else:
        print("Validated Cargo registry index: crates.io (Cargo default)")
PY
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

printf 'Package dependencies must resolve from validated remote Cargo registries; sibling path patches are forbidden.\n'
printf 'Temporary workspace: %s\n' "$workspace"
cd "$workspace"

package_file_list="$workspace/packaged-files.txt"
cargo package --manifest-path "$manifest" --locked --all-features --list > "$package_file_list"
printf 'Package file list reviewed for secrets and sibling dependency patches:\n'
cat "$package_file_list"
if rg -ni '(^|/)(\.env([^/]*|$)|credentials?([^/]*|$)|[^/]*\.(pem|key))' "$package_file_list"; then
    printf 'Refusing to package a likely secret or credential file.\n' >&2
    exit 1
fi

# Cargo's own verification is deliberately enabled; --allow-dirty includes the reviewed worktree diff.
cargo package --manifest-path "$manifest" \
    --target-dir "$package_target" \
    --locked --allow-dirty --all-features

archive="$package_target/package/$package_name-$package_version.crate"
[[ -f "$archive" ]] || { printf 'Packaged archive not found: %s\n' "$archive" >&2; exit 1; }
mkdir "$workspace/unpacked"
tar -xzf "$archive" -C "$workspace/unpacked"
package_dir="$workspace/unpacked/$package_name-$package_version"
[[ -f "$package_dir/Cargo.toml" ]] || { printf 'Invalid package archive: %s\n' "$archive" >&2; exit 1; }
python3 - "$manifest" "$package_dir/Cargo.toml" <<'PY'
import sys
import tomllib

def identity(path):
    with open(path, "rb") as source:
        package = tomllib.load(source).get("package", {})
    return package.get("name"), package.get("version")

source_identity = identity(sys.argv[1])
archive_identity = identity(sys.argv[2])
if source_identity != archive_identity:
    raise SystemExit(f"Packaged archive identity mismatch: expected {source_identity!r}, got {archive_identity!r}")
print(f"Validated packaged archive identity: {archive_identity[0]} {archive_identity[1]}")
PY

all_features_consumer="$workspace/all-features-consumer"
mkdir "$all_features_consumer"
command cp -R "$project_root/tests/fixtures/all-features-consumer/." "$all_features_consumer"
python3 "$project_root/.infra/tools/packaged_consumer_manifest.py" \
    "$all_features_consumer/Cargo.toml" "$package_dir"
cd "$all_features_consumer"
# Resolve this separate all-feature package from the audited registry, never a sibling checkout.
cargo generate-lockfile --manifest-path Cargo.toml
cargo check --manifest-path Cargo.toml --target-dir "$consumer_target" --locked --all-features
cargo metadata --manifest-path Cargo.toml --locked --format-version 1 > "$workspace/all-features-metadata.json"
python3 - "$workspace/all-features-metadata.json" "$package_name" "$package_version" "$package_dir" "$all_features_consumer/Cargo.toml" <<'PY'
import json
import os
import sys
from urllib.parse import urlsplit

def redact_source(value):
    prefix = "registry+"
    url = value.removeprefix(prefix)
    sparse_prefix = "sparse+" if url.startswith("sparse+") else ""
    parts = urlsplit(url.removeprefix(sparse_prefix))
    if not parts.scheme or not parts.hostname:
        return prefix + "<unparseable-registry-url>"
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    port = f":{parts.port}" if parts.port is not None else ""
    return prefix + sparse_prefix + f"{parts.scheme}://{host}{port}"

with open(sys.argv[1], encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)
name, version, package_dir, consumer_manifest = sys.argv[2:]
package_dir = os.path.realpath(package_dir)
consumer_manifest = os.path.realpath(consumer_manifest)
matches = [package for package in metadata["packages"] if package.get("name") == name]
if len(matches) != 1:
    raise SystemExit(f"Expected exactly one {name!r} package in consumer metadata, found {len(matches)}")
task = matches[0]
if task.get("version") != version:
    raise SystemExit(f"Consumer resolved {name} {task.get('version')!r}, expected {version!r}")
if task.get("source") is not None:
    raise SystemExit(f"Consumer resolved {name} from a non-local source")
if os.path.realpath(task.get("manifest_path", "")) != os.path.join(package_dir, "Cargo.toml"):
    raise SystemExit(f"Consumer {name} manifest_path does not point into this run's unpacked package")
for package in metadata["packages"]:
    if os.path.realpath(package.get("manifest_path", "")) == consumer_manifest or package is task:
        continue
    source = package.get("source")
    if not isinstance(source, str) or not source.startswith("registry+"):
        raise SystemExit(f"Consumer dependency {package.get('name')!r} did not resolve from a registry")
sources = sorted({
    redact_source(dependency["source"])
    for package in metadata["packages"]
    for dependency in package["dependencies"]
    if isinstance(dependency.get("source"), str) and dependency["source"].startswith("registry+")
})
if not sources:
    raise SystemExit("No registry dependencies were resolved for the all-features packaged consumer")
print("All-features consumer registry sources:")
for source in sources:
    print(f"  {source}")
print(f"Validated packaged consumer: {name} {version} from {task['manifest_path']}")
PY
printf 'Packaged all-features consumer verification passed.\n'
