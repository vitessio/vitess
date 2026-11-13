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

# Downloads MicroCeph, its base (core24) and the snapd snap that snaps on that
# base need, with their assertions, into DIR as <snap>.snap and <snap>.assert,
# so that the setup-microceph action can install them without the Snap Store.
#
# The go_cache_warm workflow saves them in a cache entry whose key holds this
# file's hash, so any change here downloads them again on the next push to main
# or a release branch. To take newer revisions of the same channels, or to
# replace an entry whose snaps don't install, increase this generation: 1.

set -euo pipefail

dir="$1"
mkdir -p "${dir}"

# The Snap Store sometimes doesn't answer GitHub's runners (connection
# timeouts, EOFs, "unable to contact snap store"), so retry each download.
attempts=5

download() {
	local snap="$1" channel="$2" attempt
	for attempt in $(seq 1 "${attempts}"); do
		if snap download "${snap}" --channel="${channel}" --target-directory="${dir}" --basename="${snap}"; then
			return 0
		fi
		if [[ "${attempt}" -eq "${attempts}" ]]; then
			return 1
		fi
		echo "snap download ${snap} failed (attempt ${attempt} of ${attempts}); retrying in 30 seconds"
		sleep 30
	done
}

download snapd stable
download core24 stable
# Pin the Ceph release. The store's default track is squid (Ceph 19);
# latest/stable currently still points at reef (Ceph 18). Bumping this is
# a deliberate change, as the MinIO package version was before.
download microceph squid/stable
