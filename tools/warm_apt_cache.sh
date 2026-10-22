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

# Downloads the packages that the test jobs on one MySQL flavor install with
# apt into DIR, for the go_cache_warm workflow to save as the cache that the
# restore-apt-cache action restores. The packages are downloaded in a fresh
# ubuntu:24.04 container that first installs, uncached, the tools that every
# runner has, so that the cache holds what the jobs download and nothing
# more. Usage:
#
#   tools/warm_apt_cache.sh <mysql-5.7|mysql-8.0|mysql-8.4|percona> DIR
#
# A package that a job installs but this does not download still installs:
# the job downloads it.

set -euo pipefail

set_name="$1"
dir="$(realpath "$2")"
mkdir -p "${dir}"

# The tune-os action's package, and the test jobs' "Get dependencies" steps.
tune_os_packages="eatmydata"
cluster_packages="${tune_os_packages} make unzip g++ etcd-client etcd-server curl git wget xz-utils libncurses6"
unit_packages="${tune_os_packages} make unzip g++ curl git wget ant default-jre-headless"

case "${set_name}" in
mysql-5.7 | mysql-8.0)
	# The unit tests, on the setup-mysql action's MySQL.
	script="
		FLAVOR=${set_name} DOWNLOAD_ONLY=1 /src/.github/actions/setup-mysql/setup-mysql.sh
		apt-get install -y --download-only ${unit_packages}"
	;;
mysql-8.4)
	# The unit tests, and the cluster tests, which also install mysql-shell.
	script="
		FLAVOR=${set_name} DOWNLOAD_ONLY=1 /src/.github/actions/setup-mysql/setup-mysql.sh
		apt-get install -y --download-only ${unit_packages} ${cluster_packages} mysql-shell"
	;;
percona)
	# The cluster tests that need XtraBackup, which install Percona Server as
	# the setup-percona-repo action and cluster_endtoend.yml do.
	script="
		curl -fsSLo percona-release_latest.deb https://repo.percona.com/apt/percona-release_latest.\$(lsb_release -sc)_all.deb
		apt-get install -y --download-only gnupg2 lsb-release
		dpkg -i percona-release_latest.deb
		percona-release setup pdps8.0
		percona-release setup pxb-80
		percona-release enable ps-80 release
		apt-get update -qq
		apt-get install -y --download-only percona-server-server percona-server-client percona-xtrabackup-80 lz4 ${cluster_packages}"
	;;
*)
	echo "unknown package set: ${set_name}" >&2
	exit 1
	;;
esac

docker run --rm --platform linux/amd64 \
	-v "${PWD}:/src:ro" -v "${dir}:/var/cache/vitess-apt" -w /tmp \
	-e DEBIAN_FRONTEND=noninteractive -e APT_CACHE_DIR=/var/cache/vitess-apt \
	ubuntu:24.04 bash -euo pipefail -c "
		apt-get update -qq
		apt-get install -qq -y sudo ca-certificates curl wget gnupg dirmngr lsb-release make unzip g++ git xz-utils > /dev/null
		/src/.github/actions/restore-apt-cache/use-apt-cache.sh
		${script}"

find "${dir}" -maxdepth 1 -name '*.deb' -printf '%s\n' |
	awk '{ n++; s += $1 } END { printf "Downloaded %d packages, %.0f MB\n", n, s / 1e6 }'
