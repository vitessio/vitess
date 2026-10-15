#!/bin/bash

source build.env

echo "running tests for $PACKAGES"

if [[ -z "$PACKAGES" ]]; then
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
	--post-run-command "bash tools/assert_tests_ran.sh"
)

if [[ -n "$JUNIT_OUTPUT" ]]; then
	GOTESTSUM_ARGS+=("--junitfile" "$JUNIT_OUTPUT")
fi

# Compile the tests with the same CGO_ENABLED=0 and -trimpath that "make build"
# uses for the binaries. Both are part of Go's build cache key, so the tests
# then reuse the packages "make build" already compiled instead of compiling
# the whole dependency graph a second time.
#
# The test packages must come before "$@" on the go test command line: "$@"
# can contain test-binary flags such as --topo-flavor or -keep-data, which
# go test only forwards to the test binary when they appear after the package
# list. In --packages mode gotestsum appends the packages last, so any
# test-binary flag made go test silently test the empty current directory
# instead ("EMPTY Package ., DONE 0 tests") while still exiting 0.
# On a rerun of a failed test, gotestsum appends "-run=^TestFoo$ <package>" to
# this command; go test applies the -run flag (the last occurrence wins) and
# hands the trailing package string to the test binary, which ignores it.
# shellcheck disable=SC2086
CGO_ENABLED=0 go tool -modfile=tools/gotestsum/go.mod gotestsum "${GOTESTSUM_ARGS[@]}" --raw-command -- go test -trimpath -json -v -count=1 $PACKAGES "$@"
