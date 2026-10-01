#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)
cleanup_script="$project_root/.infra/lib/cleanup-build-artifacts.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/infra-cleanup-test.XXXXXX")
trap 'command rm -rf -- "$test_root"' EXIT

run_case() {
    local name="$1"
    local expected_status="$2"
    local case_root="$test_root/$name"
    local actual_status

    mkdir -p \
        "$case_root/target/debug" \
        "$case_root/target/release" \
        "$case_root/target/llvm-cov-target" \
        "$case_root/target/rs-ci-feature-matrix" \
        "$case_root/target/infra-feature-matrix" \
        "$case_root/target/infra/coverage" \
        "$case_root/target/tmp" \
        "$case_root/target/llvm-cov/html" \
        "$case_root/target/doc" \
        "$case_root/fuzz/target"

    set +e
    (
        project_root="$case_root"
        source "$cleanup_script"
        exit "$expected_status"
    )
    actual_status=$?
    set -e

    if [ "$actual_status" -ne "$expected_status" ]; then
        echo "error: $name returned $actual_status, expected $expected_status" >&2
        return 1
    fi

    for directory in \
        "$case_root/target/debug" \
        "$case_root/target/release" \
        "$case_root/target/llvm-cov-target" \
        "$case_root/target/rs-ci-feature-matrix" \
        "$case_root/target/infra-feature-matrix" \
        "$case_root/target/infra/coverage" \
        "$case_root/target/tmp" \
        "$case_root/fuzz/target"; do
        if [ -e "$directory" ]; then
            echo "error: transient directory remains after $name: $directory" >&2
            return 1
        fi
    done

    for directory in "$case_root/target/llvm-cov/html" "$case_root/target/doc"; do
        if [ ! -d "$directory" ]; then
            echo "error: deliverable directory was removed after $name: $directory" >&2
            return 1
        fi
    done
}

run_case success 0
run_case failure 23

run_nested_case() {
    local case_root="$test_root/nested"
    local profile="$case_root/target/llvm-cov-target/coverage.profraw"

    mkdir -p "$(dirname "$profile")"
    : > "$profile"

    (
        project_root="$case_root"
        source "$cleanup_script"
        export cleanup_script test_project_root="$case_root"

        bash -c '
            project_root="$test_project_root"
            source "$cleanup_script"
            [ -f "$project_root/target/llvm-cov-target/coverage.profraw" ]
        '

        if [ ! -f "$profile" ]; then
            echo "error: nested cleanup removed coverage profile before report generation" >&2
            exit 1
        fi
    )

    if [ -e "$case_root/target/llvm-cov-target" ]; then
        echo "error: outer cleanup left transient coverage profiles behind" >&2
        return 1
    fi
}

run_nested_case
echo "Build artifact cleanup tests passed."
