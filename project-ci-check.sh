#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
required=(README.md README.zh_CN.md LICENSE docs/user-guide.md docs/user-guide.zh_CN.md docs/design.md docs/design.zh_CN.md)
for path in "${required[@]}"; do
  [[ -f "$repo_root/$path" ]] || { echo "missing required documentation: $path" >&2; exit 1; }
done
if rg -n '\bTaskId\b|pub fn await_|service\.await_' "$repo_root/src" "$repo_root/README.md" "$repo_root/README.zh_CN.md"; then
  echo "legacy task identity or wait API found" >&2
  exit 1
fi
cargo package --allow-dirty --no-verify --list | rg -q '^LICENSE$'
cargo package --allow-dirty --no-verify --list | rg -q '^docs/design\.md$'
echo "project CI checks passed"
