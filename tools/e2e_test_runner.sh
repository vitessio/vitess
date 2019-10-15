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

# Custom Go endtoend test runner which runs all endtoend tests in parallel
# except for known flaky tests.
# Flaky unit tests are run sequentially in the second phase and retried up to
# three times.

# Why are there flaky unit tests?
#
# Some of the Go unit tests are inherently flaky e.g. because they use the
# real timer implementation and might fail when things take longer as usual.
# In particular, this happens when the system is under load and threads do not
# get scheduled as fast as usual. Then, the expected timings do not match.

# Set VT_GO_PARALLEL variable in the same way as the Makefile does.
# We repeat this here because this script is called directly by test.go
# and not via the Makefile.
if [[ -z $VT_GO_PARALLEL && -n $VT_GO_PARALLEL_VALUE ]]; then
  VT_GO_PARALLEL="-p $VT_GO_PARALLEL_VALUE"
fi

# This can be removed once the docker images are rebuilt
export GO111MODULE=on

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*
packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/.../endtoend/... | sort)

# Flaky tests have the suffix "_flaky_test.go".
all_except_flaky_tests=$(echo "$packages_with_tests" | grep -vE ".+ .+_flaky_test\.go" | cut -d" " -f1)
flaky_tests=$(echo "$packages_with_tests" | grep -E ".+ .+_flaky_test\.go" | cut -d" " -f1)

# Run non-flaky tests.
echo "$all_except_flaky_tests" | xargs go test $VT_GO_PARALLEL
if [ $? -ne 0 ]; then
  echo "ERROR: Go unit tests failed. See above for errors."
  echo
  echo "This should NOT happen. Did you introduce a flaky unit test?"
  echo "If so, please rename it to the suffix _flaky_test.go."
  exit 1
fi

# Run flaky tests sequentially. Retry when necessary.
for pkg in $flaky_tests; do
  max_attempts=3
  attempt=1
  # Set a timeout because some tests may deadlock when they flake.
  until go test -timeout 30s $VT_GO_PARALLEL $pkg; do
    echo "FAILED (try $attempt/$max_attempts) in $pkg (return code $?). See above for errors."
    if [ $((++attempt)) -gt $max_attempts ]; then
      echo "ERROR: Flaky Go unit tests in package $pkg failed too often (after $max_attempts retries). Please reduce the flakiness."
      exit 1
    fi
  done
done
