#!/usr/bin/env bash

infra_run_with_retry() {
    local description="$1"
    shift
    local max_attempts="${RS_INFRA_NETWORK_MAX_ATTEMPTS:-4}"
    local delay_seconds="${RS_INFRA_NETWORK_RETRY_DELAY_SECONDS:-2}"
    local attempt=1
    local status=0

    [[ "$max_attempts" =~ ^[0-9]+$ ]] && [ "$max_attempts" -gt 0 ] || {
        echo "error: RS_INFRA_NETWORK_MAX_ATTEMPTS must be a positive integer" >&2
        return 2
    }
    [[ "$delay_seconds" =~ ^[0-9]+$ ]] || {
        echo "error: RS_INFRA_NETWORK_RETRY_DELAY_SECONDS must be a non-negative integer" >&2
        return 2
    }

    while [ "$attempt" -le "$max_attempts" ]; do
        if "$@"; then
            return 0
        else
            status=$?
        fi
        if [ "$attempt" -ge "$max_attempts" ]; then
            echo "error: $description failed after $attempt attempt(s)" >&2
            return "$status"
        fi
        echo "warning: $description failed (attempt $attempt/$max_attempts); retrying in ${delay_seconds}s" >&2
        if [ -n "${RS_INFRA_PROGRESS_FILE:-}" ]; then
            printf 'network retry %s (attempt %s/%s; waiting %ss)\n' \
                "$description" "$attempt" "$max_attempts" "$delay_seconds" \
                >> "$RS_INFRA_PROGRESS_FILE"
        fi
        sleep "$delay_seconds"
        delay_seconds=$((delay_seconds * 2))
        attempt=$((attempt + 1))
    done
}
