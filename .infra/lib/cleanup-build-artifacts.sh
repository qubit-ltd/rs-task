#!/usr/bin/env bash
set -euo pipefail

project_root=${project_root:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)}

cleanup_build_artifacts() {
    local task_status=$?
    local cleanup_status=0
    local path

    for path in \
        "$project_root/target/debug" \
        "$project_root/target/release" \
        "$project_root/target/llvm-cov-target" \
        "$project_root/target/rs-ci-feature-matrix" \
        "$project_root/target/infra-feature-matrix" \
        "$project_root/target/infra/coverage" \
        "$project_root/target/tmp" \
        "$project_root/fuzz/target"; do
        if [ -d "$path" ]; then
            echo "Cleaning transient build artifacts: $path"
            command rm -rf -- "$path" || {
                echo "error: unable to clean transient build artifacts: $path" >&2
                cleanup_status=1
            }
        fi
    done

    for path in "$project_root"/.infra/*/tool.revision.tmp; do
        if [ -f "$path" ]; then
            command rm -f -- "$path" || {
                echo "error: unable to clean transient infra marker: $path" >&2
                cleanup_status=1
            }
        fi
    done

    if [ "$task_status" -ne 0 ]; then
        echo "Build artifact cleanup completed after failure (exit code $task_status)"
        return "$task_status"
    fi
    if [ "$cleanup_status" -ne 0 ]; then
        return 1
    fi
    echo "Build artifact cleanup completed"
}

# Only the outermost infra command owns cleanup; nested commands may leave
# intermediate outputs for their caller to consume.
if [ -z "${RS_INFRA_CLEANUP_OWNER_PID:-}" ]; then
    export RS_INFRA_CLEANUP_OWNER_PID=$$
    trap cleanup_build_artifacts EXIT
fi
