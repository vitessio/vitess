#!/bin/bash

source build.env

echo "running tests for $PACKAGES"

if [[ -z "${PACKAGES:-}" ]]; then
	echo "ERROR: PACKAGES is empty"
	exit 1
fi

GOTESTSUM_ARGS=(
	--format github-actions
	--rerun-fails=3
	--rerun-fails-max-failures=10
	--rerun-fails-run-root-test
	--format-hide-empty-pkg
	--hide-summary=skipped
)

if [[ -n "${JUNIT_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS+=("--junitfile" "$JUNIT_OUTPUT")
fi

go tool gotestsum "${GOTESTSUM_ARGS[@]}" --packages "$PACKAGES" -- -v -count=1 "$@" -args -alsologtostderr
