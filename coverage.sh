#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
exec "$project_root/.infra/bin/infra-tool.sh" rs-infra-coverage --project "$project_root" collect "$@"
