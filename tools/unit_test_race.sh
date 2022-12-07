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

source build.env

if [[ -z $VT_GO_PARALLEL && -n $VT_GO_PARALLEL_VALUE ]]; then
  VT_GO_PARALLEL="-p $VT_GO_PARALLEL_VALUE"
fi

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*

packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}{{if len .XTestGoFiles}}{{.ImportPath}} {{join .XTestGoFiles " "}}{{end}}' ./go/... | sort)

# exclude end to end tests
packages_to_test=$(echo "$packages_with_tests" | cut -d" " -f1 | grep -v "endtoend")
all_except_flaky_tests=$(echo "$packages_to_test" | grep -vE ".+ .+_flaky_test\.go" | cut -d" " -f1 | grep -v "endtoend")
flaky_tests=$(echo "$packages_to_test" | grep -E ".+ .+_flaky_test\.go" | cut -d" " -f1)

# Flaky tests have the suffix "_flaky_test.go".
# Exclude endtoend tests
all_except_flaky_tests=$(echo "$packages_with_tests" | grep -vE ".+ .+_flaky_test\.go" | cut -d" " -f1 | grep -v "endtoend")
flaky_tests=$(echo "$packages_with_tests" | grep -E ".+ .+_flaky_test\.go" | cut -d" " -f1)

# Run non-flaky tests.
echo "$all_except_flaky_tests" | xargs go test $VT_GO_PARALLEL -race -count=1
if [ $? -ne 0 ]; then
  echo "ERROR: Go unit tests failed. See above for errors."
  echo
  echo "This should NOT happen. Did you introduce a flaky unit test?"
  echo "If so, please rename it to the suffix _flaky_test.go."
  exit 1
fi

echo '# Flaky tests (3 attempts permitted)'

# Run flaky tests sequentially. Retry when necessary.
for pkg in $flaky_tests; do
  max_attempts=3
  attempt=1
  # Set a timeout because some tests may deadlock when they flake.
  until go test -timeout 2m $VT_GO_PARALLEL $pkg -race -count=1; do
    echo "FAILED (try $attempt/$max_attempts) in $pkg (return code $?). See above for errors."
    if [ $((++attempt)) -gt $max_attempts ]; then
      echo "ERROR: Flaky Go unit tests in package $pkg failed too often (after $max_attempts retries). Please reduce the flakiness."
      exit 1
    fi
  done
done

