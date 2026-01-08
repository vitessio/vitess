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

# Custom Go endtoend test runner which runs all endtoend tests using gotestsum. Failed tests are
# automatically retried up to 3 times to handle flaky tests.

source build.env

if [[ -z $VT_GO_PARALLEL && -n $VT_GO_PARALLEL_VALUE ]]; then
	VT_GO_PARALLEL="-p $VT_GO_PARALLEL_VALUE"
fi

# All endtoend packages except go/test/endtoend (cluster tests).
packages_with_tests=$(go list ./go/.../endtoend/... | grep -v "go/test/endtoend")

# Build gotestsum args. Failed tests are retried up to 3 times, but if more than 10 tests fail
# initially we skip retries to avoid wasting time on a real widespread failure.
GOTESTSUM_ARGS="--format github-actions --rerun-fails=3 --rerun-fails-max-failures=10 --format-hide-empty-pkg --hide-summary=skipped"
if [[ -n "${JUNIT_OUTPUT:-}" ]]; then
	GOTESTSUM_ARGS="$GOTESTSUM_ARGS --junitfile $JUNIT_OUTPUT"
fi

go tool gotestsum $GOTESTSUM_ARGS --packages="$packages_with_tests" -- $VT_GO_PARALLEL -count=1
