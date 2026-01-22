#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Custom Go unit test runner which runs all unit tests using gotestsum. Failed tests are
# automatically retried up to 3 times to handle flaky tests.

# Set VT_GO_PARALLEL variable in the same way as the Makefile does.
# We repeat this here because this script is called directly by test.go
# and not via the Makefile.

source build.env

if [[ -z $VT_GO_PARALLEL && -n $VT_GO_PARALLEL_VALUE ]]; then
	VT_GO_PARALLEL="-p $VT_GO_PARALLEL_VALUE"
fi

# Enable race detector if RACE=1
RACE_FLAG=""
if [[ "$RACE" == "1" ]]; then
	RACE_FLAG="-race"
fi

# Mac makes long temp directories for os.TempDir(). MySQL can't connect to
# sockets in those directories. Tell Golang to use /tmp/vttest_XXXXXX instead.
kernel="$(uname -s)"
case "$kernel" in
darwin | Darwin)
	TMPDIR=${TMPDIR:-}
	if [ -z "$TMPDIR" ]; then
		TMPDIR="$(mktemp -d /tmp/vttest_XXXXXX)"
		export TMPDIR
	fi
	echo "Using temporary directory for tests: $TMPDIR"
	;;
esac

# All Go packages with test files, excluding endtoend tests.
packages_with_tests=$(go list ./go/... | grep -v endtoend)

if [[ "$VTEVALENGINETEST" == "1" ]]; then
	packages_with_tests=$(echo "$packages_with_tests" | grep "evalengine")
fi

if [[ "$VTEVALENGINETEST" == "0" ]]; then
	packages_with_tests=$(echo "$packages_with_tests" | grep -v "evalengine")
fi

# Build gotestsum args. Failed tests are retried up to 3 times, but if more than 10 tests fail
# initially we skip retries to avoid wasting time on a real widespread failure.
GOTESTSUM_FORMAT="${GOTESTSUM_FORMAT:-github-actions}"

GOTESTSUM_ARGS="--format ${GOTESTSUM_FORMAT} --rerun-fails=3 --rerun-fails-max-failures=10 --rerun-fails-run-root-test --format-hide-empty-pkg --hide-summary=skipped"
if [[ -n "${JUNIT_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS="$GOTESTSUM_ARGS --junitfile $JUNIT_OUTPUT"
fi
if [[ -n "${JSON_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS="$GOTESTSUM_ARGS --jsonfile $JSON_OUTPUT"
fi

# shellcheck disable=SC2086
go tool gotestsum $GOTESTSUM_ARGS --packages="$packages_with_tests" -- $VT_GO_PARALLEL $RACE_FLAG -count=1
