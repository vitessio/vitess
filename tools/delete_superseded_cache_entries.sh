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

# Deletes the GitHub Actions cache entries of the current ref whose key starts
# with PREFIX, except KEY, the entry the go_cache_warm workflow just saved.
# Usage:
#
#   REPO=<owner/repo> REF=<git ref> GH_TOKEN=<token> \
#     tools/delete_superseded_cache_entries.sh <prefix> <key>
#
# actions/cache/save only warns when its upload fails, so this first makes
# sure KEY exists, and otherwise fails and keeps every entry: without the new
# one, the older entries are all the jobs have.

set -euo pipefail

prefix="$1"
key="$2"

entries="$(gh cache list --repo "${REPO}" --ref "${REF}" --key "${prefix}" --limit 100 \
	--json id,key --jq '.[] | [.id, .key] | @tsv')"
if ! cut -f2 <<<"${entries}" | grep -q -F -x -- "${key}"; then
	echo "::error::${key} was not saved; keeping the older entries."
	exit 1
fi
while IFS=$'\t' read -r id entry; do
	if [ "${entry}" != "${key}" ]; then
		gh cache delete --repo "${REPO}" "${id}"
	fi
done <<<"${entries}"
