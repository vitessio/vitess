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

# These test uses excutables and launch them as process
# After that all tests run, here we are testing those

# All Go packages with only sharding test files.
# Output per line: <full Go package name> <all _test.go files in the package>*

source build.env

packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/.../endtoend/... | sort)

cluster_tests=$(echo "$packages_with_tests" | grep -E "go/test/endtoend/sharding" | cut -d" " -f1)

# Run cluster test sequentially
echo "running cluster tests $cluster_tests"
echo "$cluster_tests" | xargs go test -v -p=1
if [ $? -ne 0 ]; then
  echo "ERROR: Go cluster tests failed. See above for errors."
  echo
  echo "This should NOT happen. Did you introduce a flaky unit test?"
  echo "If so, please rename it to the suffix _flaky_test.go."
  exit 1
fi
