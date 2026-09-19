#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
tools_config="$project_root/.infra/ci/tools.toml"
policy_config="$project_root/.infra/dep/policy.toml"
dry_run=0
if [ "${1:-}" = "--dry-run" ]; then
    dry_run=1
    shift
fi
[ "$#" -eq 0 ] || {
    echo "usage: $0 [--dry-run]" >&2
    exit 2
}

[ -f "$tools_config" ] || { echo "error: missing $tools_config" >&2; exit 1; }
[ -f "$policy_config" ] || { echo "error: missing $policy_config" >&2; exit 1; }
command -v git >/dev/null 2>&1 || { echo "error: git is required" >&2; exit 1; }

updates=()
while IFS=$'\t' read -r name source current; do
    [ -n "$name" ] || continue
    latest=$(git ls-remote "$source" HEAD | awk 'NR == 1 { print $1 }')
    if [[ ! "$latest" =~ ^[0-9a-f]{40}$ ]]; then
        echo "error: unable to resolve HEAD for $name ($source)" >&2
        exit 1
    fi
    if [ "$current" = "$latest" ]; then
        echo "infra: $name is current ($current)"
    else
        echo "infra: $name $current -> $latest"
        updates+=("$name=$latest")
    fi
done < <(
    awk -F'"' '
        /^\[rs-infra-/ { name = substr($0, 2, length($0) - 2); source = ""; revision = "" }
        /^[[:space:]]*source[[:space:]]*=/ { source = $2 }
        /^[[:space:]]*revision[[:space:]]*=/ { revision = $2; if (name != "" && source != "" && revision != "" && name != "rs-infra-dependency") print name "\t" source "\t" revision }
    ' "$tools_config"
    source=$(awk -F'"' '/^[[:space:]]*source[[:space:]]*=/ { print $2; exit }' "$policy_config")
    revision=$(awk -F'"' '/^[[:space:]]*revision[[:space:]]*=/ { print $2; exit }' "$policy_config")
    printf 'rs-infra-dependency-policy\t%s\t%s\n' "$source" "$revision"
)

if [ "$dry_run" -eq 1 ]; then
    echo "infra: dry run complete; ${#updates[@]} update(s) available"
    exit 0
fi

if [ "${#updates[@]}" -gt 0 ]; then
    UPDATE_PAIRS=$(printf '%s\n' "${updates[@]}") python3 - "$tools_config" "$policy_config" <<'PY'
import os
import re
import sys
from pathlib import Path

tools_path, policy_path = map(Path, sys.argv[1:])
updates = dict(line.split("=", 1) for line in os.environ["UPDATE_PAIRS"].splitlines())

text = tools_path.read_text()
for name, revision in updates.items():
    if name == "rs-infra-dependency-policy":
        continue
    pattern = rf"(\[{re.escape(name)}\].*?^revision\s*=\s*\")[0-9a-f]+(\")"
    text, count = re.subn(pattern, lambda match: f"{match.group(1)}{revision}{match.group(2)}", text, count=1, flags=re.MULTILINE | re.DOTALL)
    if count != 1:
        raise SystemExit(f"unable to update revision for {name}")
tools_path.write_text(text)

if "rs-infra-dependency-policy" in updates:
    policy = policy_path.read_text()
    policy, count = re.subn(r"(^revision\s*=\s*\")[0-9a-f]+(\")", lambda match: f"{match.group(1)}{updates['rs-infra-dependency-policy']}{match.group(2)}", policy, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit("unable to update dependency policy revision")
    policy_path.write_text(policy)
PY
fi

echo "infra: update complete; ${#updates[@]} revision(s) updated"
