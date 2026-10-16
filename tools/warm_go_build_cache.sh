#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

# Fills the Go build cache with everything one group of CI jobs compiles, so
# the go_cache_warm workflow can save it for those jobs to restore. Usage:
#
#   tools/warm_go_build_cache.sh <build|unit|race|tools>
#
# Each entry must use exactly the flags its jobs use: the flags are part of
# every package's cache key, and so is the source directory of every package
# compiled without -trimpath.
#
# Test packages are warmed with "go test -exec /bin/true": that compiles, vets
# and links every test binary the way the jobs do, without running any of
# them. The end-to-end packages would otherwise start clusters in TestMain.
# -count=1 keeps those runs out of the test result cache.

set -euo pipefail

warm_tests() {
	go test "$@" -count=1 -run '^$' -exec /bin/true >/dev/null
}

# The packages tools/unit_test_runner.sh tests.
unit_packages() {
	go list ./go/... | grep -v endtoend
}

# The packages tools/e2e_test_race.sh tests.
e2e_race_packages() {
	go list ./go/.../endtoend/... | grep -v "go/test/endtoend"
}

case "${1:-}" in
build)
	# "make build" and the end-to-end tests (tools/e2e_go_test.sh), which
	# compile with the same CGO_ENABLED=0 and -trimpath, and the gotestsum
	# binary that script runs under CGO_ENABLED=0. Compiling is enough for
	# "make build"; its link outputs are not cached.
	export CGO_ENABLED=0
	go list -trimpath -export -deps ./go/... >/dev/null
	warm_tests -trimpath ./go/.../endtoend/...
	go tool -n -modfile=tools/gotestsum/go.mod gotestsum >/dev/null
	;;
unit)
	# "make unit_test": the unit tests, the demo it installs, and gotestsum.
	# shellcheck disable=SC2046
	warm_tests $(unit_packages)
	go list -export -deps ./examples/demo/demo.go >/dev/null
	go tool -n -modfile=tools/gotestsum/go.mod gotestsum >/dev/null
	;;
race)
	# "make unit_test_race" and "make e2e_test_race".
	# shellcheck disable=SC2046
	warm_tests -race $(unit_packages) $(e2e_race_packages)
	go tool -n -modfile=tools/gotestsum/go.mod gotestsum >/dev/null
	;;
tools)
	# The static checks: the generators and checks they "go run" from
	# go/tools, and the tool binaries they run with "go tool".
	go list -export -deps ./go/tools/... >/dev/null
	for modfile in tools/*/go.mod; do
		tool="$(basename "$(dirname "${modfile}")")"
		go tool -n -modfile="${modfile}" "${tool}" >/dev/null
	done
	;;
*)
	echo "usage: $0 <build|unit|race|tools>" >&2
	exit 1
	;;
esac
