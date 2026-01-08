#!/bin/bash
source build.env
echo "running tests for " "$@"

GOTESTSUM_ARGS="--format github-actions --format-hide-empty-pkg --hide-summary=skipped"
if [[ -n "${JUNIT_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS="$GOTESTSUM_ARGS --junitfile $JUNIT_OUTPUT"
fi
if [[ -n "${JSON_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS="$GOTESTSUM_ARGS --jsonfile $JSON_OUTPUT"
fi

go tool gotestsum $GOTESTSUM_ARGS -- "$@" -alsologtostderr -count=1
