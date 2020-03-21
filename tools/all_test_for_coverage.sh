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

# The purpose of this script is to run testcase and get coverage report
# These script runs all unit testcases including go endtoend testcase
# Here we ignore any error from testcase as the purpose is to collect coverage.
# So if there is a flaky test, it will get only chance to run, if it fails we ignore coverage from that.

### Execute unit testcase ###
source build.env
make tools
make build
echo "--------- executing unit testcases ---------"
packages_with_all_tests=$(go list -f '{{if len .TestGoFiles}}{{.ImportPath}} {{join .TestGoFiles " "}}{{end}}' ./go/... | sort)
all_except_endtoend_tests=$(echo "$packages_with_all_tests" | grep -v "endtoend" | cut -d" " -f1)

counter=0
for pkg in $all_except_endtoend_tests; do
	go test -coverpkg=vitess.io/vitess/go/... -coverprofile "/tmp/unit_$counter.out" $pkg -v -p=1 || :
	counter=$((counter + 1))
done

## Copy the test files to get instrumented binaries ###
cp ./tools/coverage-go/vtctl_test.go ./go/cmd/vtctl/vtctl_test.go
cp ./tools/coverage-go/vtctld_test.go ./go/cmd/vtctld/vtctld_test.go
cp ./tools/coverage-go/mysqlctl_test.go ./go/cmd/mysqlctl/mysqlctl_test.go
cp ./tools/coverage-go/vtctlclient_test.go ./go/cmd/vtctlclient/vtctlclient_test.go
cp ./tools/coverage-go/vttablet_test.go ./go/cmd/vttablet/vttablet_test.go
cp ./tools/coverage-go/vtgate_test.go ./go/cmd/vtgate/vtgate_test.go
cp ./tools/coverage-go/vtworker_test.go ./go/cmd/vtworker/vtworker_test.go
cp ./tools/coverage-go/vtworkerclient_test.go ./go/cmd/vtworkerclient/vtworkerclient_test.go

go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtctl -o ./bin/vtctl
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtctld -o ./bin/vtctld
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/mysqlctl -o ./bin/mysqlctl
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtctlclient -o ./bin/vtctlclient
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vttablet -o ./bin/vttablet
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtgate -o ./bin/vtgate
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtworker -o ./bin/vtworker
go test -coverpkg=vitess.io/vitess/go/... -c vitess.io/vitess/go/cmd/vtworkerclient -o ./bin/vtworkerclient

### Execute go/test/endtoend testcase ###
echo "--------- executing endtoend testcases ---------"
cluster_tests=$(echo "$packages_with_all_tests" | grep -E "go/test/endtoend" | cut -d" " -f1)

# Run cluster test sequentially
for i in $cluster_tests; do
	echo "starting test for $i"
	go test $i -v -p=1 -is-coverage=true || :
done
