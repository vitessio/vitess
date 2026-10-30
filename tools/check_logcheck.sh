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

# Runs the logcheck analyzer (go/tools/logcheck) with go vet over the packages
# listed in go/tools/logcheck/packages.txt: one exact import path per line,
# with blank lines and # comments ignored. Patterns such as ./... are rejected,
# so that a package is only checked once it is listed.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

list=go/tools/logcheck/packages.txt
if [[ ! -f "${list}" ]]; then
	echo "${list}: not found" >&2
	exit 1
fi

pkgs=()
lineno=0
while IFS= read -r line || [[ -n "${line}" ]]; do
	lineno=$((lineno + 1))
	line="${line%%#*}"
	line="${line#"${line%%[![:space:]]*}"}"
	line="${line%"${line##*[![:space:]]}"}"
	if [[ -z "${line}" ]]; then
		continue
	fi
	if [[ ! "${line}" =~ ^vitess\.io/vitess/go(/[A-Za-z0-9_-][A-Za-z0-9_.-]*)+$ || "${line}" == *...* ]]; then
		echo "${list}:${lineno}: '${line}' is not an import path under vitess.io/vitess/go" >&2
		exit 1
	fi
	pkgs+=("${line}")
done <"${list}"

# go vet without packages would check the current directory.
if [[ ${#pkgs[@]} -eq 0 ]]; then
	echo "${list}: no packages listed"
	exit 0
fi

bin="${VTROOTBIN:-${PWD}/bin}/logcheck"
go build -o "${bin}" ./go/tools/logcheck/main
go vet -vettool="${bin}" "${pkgs[@]}"
