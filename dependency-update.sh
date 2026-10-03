#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
runner=("$project_root/.infra/bin/infra-tool.sh" rs-infra-dependency --project "$project_root")
case "${1:---check}" in
  --check) exec "${runner[@]}" check ;;
  --update) "${runner[@]}" sync; exec "${runner[@]}" check ;;
  -h|--help) echo "Usage: ./dependency-update.sh [--check|--update]" ;;
  *) echo "error: unknown option '$1'" >&2; exit 2 ;;
esac
