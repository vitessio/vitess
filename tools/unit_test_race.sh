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

if [[ -z $VT_GO_PARALLEL && -n $VT_GO_PARALLEL_VALUE ]]; then
  VT_GO_PARALLEL="-p $VT_GO_PARALLEL_VALUE"
fi

# All Go packages with test files.
# Output per line: <full Go package name> <all _test.go files in the package>*
# TODO: This tests ./go/vt/... instead of ./go/... due to a historical reason.
# When https://github.com/vitessio/vitess/issues/5493 is closed, we should change it.

packages_with_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/vt/... | sort)

# endtoend tests should be in a directory called endtoend
all_except_e2e_tests=$(echo "$packages_with_tests" | cut -d" " -f1 | grep -v "endtoend")

# Run non endtoend tests.
echo "$all_except_e2e_tests" | xargs go test $VT_GO_PARALLEL -race

if [ $? -ne 0 ]; then
  echo "WARNING: POSSIBLE DATA RACE"
  echo
  echo "ERROR: go test -race failed. See log above."
  exit 1
fi
