#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
dry_run=0
if [ "${1:-}" = "--dry-run" ]; then dry_run=1; shift; fi
[ "$#" -eq 0 ] || { echo "usage: $0 [--dry-run]" >&2; exit 2; }
if [ "$dry_run" -eq 0 ]; then
    "$project_root/.infra/bin/infra-tool.sh" rs-infra-tools sync-scripts --project "$project_root"
fi
updates=()
for role in ci coverage dependency pages style tools verify; do
    config="$project_root/.infra/$role/tool.toml"
    [ -f "$config" ] || continue
    source_url=$(awk -F '"' '/^[[:space:]]*source[[:space:]]*=/ { print $2; exit }' "$config")
    current=$(awk -F '"' '/^[[:space:]]*revision[[:space:]]*=/ { print $2; exit }' "$config")
    latest=$(git ls-remote "$source_url" HEAD | awk 'NR == 1 { print $1 }')
    [[ "$latest" =~ ^[0-9a-f]{40}$ ]] || { echo "error: unable to resolve $role ($source_url)" >&2; exit 1; }
    if [ "$current" = "$latest" ]; then echo "infra: rs-infra-$role is current ($current)"; else echo "infra: rs-infra-$role $current -> $latest"; updates+=("$role=$latest"); fi
done
policy_config="$project_root/.infra/dependency/policy.toml"
if [ -f "$policy_config" ]; then
    policy_source=$(awk -F '"' '/^[[:space:]]*source[[:space:]]*=/ { print $2; exit }' "$policy_config")
    policy_current=$(awk -F '"' '/^[[:space:]]*revision[[:space:]]*=/ { print $2; exit }' "$policy_config")
    policy_latest=$(git ls-remote "$policy_source" HEAD | awk 'NR == 1 { print $1 }')
    [[ "$policy_latest" =~ ^[0-9a-f]{40}$ ]] || { echo "error: unable to resolve dependency policy ($policy_source)" >&2; exit 1; }
    if [ "$policy_current" = "$policy_latest" ]; then echo "infra: dependency policy is current ($policy_current)"; else echo "infra: dependency policy $policy_current -> $policy_latest"; updates+=("policy=$policy_latest"); fi
fi
if [ "$dry_run" -eq 1 ]; then echo "infra: dry run complete; ${#updates[@]} update(s) available"; exit 0; fi
if [ "${#updates[@]}" -gt 0 ]; then
    UPDATE_PAIRS=$(printf '%s\n' "${updates[@]}") python3 - "$project_root/.infra" <<'PYTHON_UPDATE'
import os, re, sys
from pathlib import Path
root = Path(sys.argv[1])
for line in os.environ["UPDATE_PAIRS"].splitlines():
    role, revision = line.split("=", 1)
    path = root / "dependency/policy.toml" if role == "policy" else root / role / "tool.toml"
    text = path.read_text()
    text, count = re.subn(r'(?m)^(revision\s*=\s*")[0-9a-f]{40}("\s*)$', rf'\g<1>{revision}\2', text, count=1)
    if count != 1: raise SystemExit(f"unable to update revision in {path}")
    path.write_text(text)
PYTHON_UPDATE
fi
"$project_root/.infra/bin/infra-tool.sh" rs-infra-tools sync-scripts --project "$project_root"
for role in ci coverage dependency pages style tools verify; do
    config="$project_root/.infra/$role/tool.toml"
    [ -f "$config" ] || continue
    "$project_root/.infra/bin/infra-tool.sh" "rs-infra-$role" --help >/dev/null
done
echo "infra: update complete; ${#updates[@]} revision(s) updated"
