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

<<<<<<< HEAD
go tool -modfile=tools/gotestsum/go.mod gotestsum "${GOTESTSUM_ARGS[@]}" --packages "$PACKAGES" -- -v -count=1 "$@" -args -alsologtostderr
||||||| parent of ad9dd184b3 (test: fix topo-flavor e2e shards silently running zero tests (#20556))
go tool -modfile=tools/gotestsum/go.mod gotestsum "${GOTESTSUM_ARGS[@]}" --packages "$PACKAGES" -- -v -count=1 "$@"
=======
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
go tool -modfile=tools/gotestsum/go.mod gotestsum "${GOTESTSUM_ARGS[@]}" --raw-command -- go test -json -v -count=1 $PACKAGES "$@"
>>>>>>> ad9dd184b3 (test: fix topo-flavor e2e shards silently running zero tests (#20556))
