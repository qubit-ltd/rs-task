#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
source "$project_root/.infra/lib/cleanup-build-artifacts.sh"
"$project_root/.infra/bin/prepare-local-path-dependencies.sh"
"$project_root/.infra/bin/infra-tool.sh" rs-infra-coverage --project "$project_root" collect "$@"
"$project_root/.infra/lib/coverage-report.sh"
"$project_root/.infra/bin/infra-tool.sh" rs-infra-coverage --project "$project_root" check --input "$project_root/coverage.json"
