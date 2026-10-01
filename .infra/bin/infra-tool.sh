#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
"$project_root/.infra/lib/infra-tool.sh" "$@"
