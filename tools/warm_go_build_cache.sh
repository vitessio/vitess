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
# compiled without -trimpath. Every entry uses -trimpath, so it fits any runner
# of its architecture, whatever its checkout and module cache paths. This is
# also why the jobs run the tools under tools/ with "go run -trimpath" rather
# than "go tool", which takes no -trimpath.
#
# "unit" is built on top of "build", and "tools" on top of both: they share
# its flags, and the workflow saves only what each adds.
#
# Test packages are warmed with "go test -exec /bin/true": that compiles, vets
# and links every test binary the way the jobs do, without running any of
# them. The end-to-end packages would otherwise start clusters in TestMain.
# -count=1 keeps those runs out of the test result cache. Tools are warmed the
# same way with "go run -exec /bin/true", which compiles and links them into
# the cache without running them.
#
# "tools" also runs golangci-lint over the whole tree, which fills its
# analysis cache (~/.cache/golangci-lint) for the workflow to save, and the
# Go build cache with the packages and test packages it loads that "build" and
# "unit" do not already hold. The analysis cache keys the files of other
# modules by their absolute path, so it only fits runners whose module cache
# is at the same path.

set -euo pipefail

warm_tests() {
	go test "$@" -count=1 -run '^$' -exec /bin/true >/dev/null
}

# Warms the tools/<name> tool, the way the jobs run it.
warm_tool() {
	local modfile="tools/$1/go.mod"
	local pkg
	pkg="$(sed -n 's/^tool //p' "${modfile}")"
	go run -exec /bin/true -trimpath -modfile="${modfile}" "${pkg}"
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
	warm_tool gotestsum
	;;
unit)
	# "make unit_test": the unit tests and the demo it installs, with the
	# flags of "build". gotestsum is already in "build".
	export CGO_ENABLED=0
	# shellcheck disable=SC2046
	warm_tests -trimpath $(unit_packages)
	go list -trimpath -export -deps ./examples/demo/demo.go >/dev/null
	;;
race)
	# "make unit_test_race" and "make e2e_test_race". The race detector needs
	# cgo, so gotestsum runs with it too.
	# shellcheck disable=SC2046
	warm_tests -race -trimpath $(unit_packages) $(e2e_race_packages)
	warm_tool gotestsum
	;;
tools)
	# The static checks, which set CGO_ENABLED=0 and GOFLAGS=-trimpath: the
	# generators and checks they "go run" from go/tools, the tools under
	# tools/, and their golangci-lint run. Its findings are the static checks'
	# business, so they do not fail this.
	export CGO_ENABLED=0
	go list -trimpath -export -deps ./go/tools/... >/dev/null
	for modfile in tools/*/go.mod; do
		warm_tool "$(basename "$(dirname "${modfile}")")"
	done
	GOFLAGS=-trimpath go run -trimpath -modfile=tools/golangci-lint/go.mod \
		github.com/golangci/golangci-lint/v2/cmd/golangci-lint \
		run --issues-exit-code=0 --timeout 10m ./go/... >/dev/null
	;;
*)
	echo "usage: $0 <build|unit|race|tools>" >&2
	exit 1
	;;
esac
