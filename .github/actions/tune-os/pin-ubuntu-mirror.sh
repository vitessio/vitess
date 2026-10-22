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

# Points apt at a responsive Ubuntu archive server before the job's first apt
# run. The archive's host name resolves to several servers, some of which
# intermittently answer only after 10-20 seconds and then send 50 kB/s, and apt
# keeps downloading from them however long that takes; the Azure mirror that
# the runner images list first sometimes does not answer at all. This times a
# download of the release file from the Azure mirror and from each of the
# archive's addresses, and if one of the archive's addresses is fastest, pins
# the archive's host names to it in /etc/hosts and points apt at the archive
# instead of the Azure mirror. When the Azure mirror is fastest, or nothing
# answers, it changes nothing.

set -uo pipefail

if [[ "$(dpkg --print-architecture)" == "arm64" ]]; then
	archive=ports.ubuntu.com
	path=ubuntu-ports
else
	archive=archive.ubuntu.com
	path=ubuntu
fi
codename="$(awk -F= '$1 == "VERSION_CODENAME" {print $2}' /etc/os-release)"

# Prints the seconds it took to download the release file from HOST via IP,
# or nothing if that failed or took longer than 3 seconds.
probe() {
	local host="$1" ip="$2" root="$3"
	curl -fsS -o /dev/null --connect-timeout 2 --max-time 3 --resolve "${host}:80:${ip}" \
		-w '%{time_total}\n' "http://${host}/${root}/dists/${codename}/InRelease" 2> /dev/null
}

results="$(mktemp)"
trap 'rm -f "${results}"' EXIT
for ip in $(getent ahostsv4 "${archive}" | awk '{print $1}' | sort -u); do
	(t="$(probe "${archive}" "${ip}" "${path}")" && echo "${t} ${ip}" >> "${results}") &
done
if [[ "${archive}" == archive.ubuntu.com ]]; then
	for ip in $(getent ahostsv4 azure.archive.ubuntu.com | awk '{print $1}' | sort -u); do
		(t="$(probe azure.archive.ubuntu.com "${ip}" ubuntu)" && echo "${t} azure" >> "${results}") &
	done
fi
wait

sort -n "${results}" | sed 's/^/Ubuntu archive probe: /'
best="$(sort -n "${results}" | head -1 | cut -d' ' -f2)"
if [[ -z "${best}" ]]; then
	echo "No Ubuntu archive server answered within 3 seconds; leaving apt as it is."
	exit 0
fi
if [[ "${best}" == azure ]]; then
	echo "The Azure mirror is fastest; leaving apt as it is."
	exit 0
fi

# security.ubuntu.com is served by the same servers as archive.ubuntu.com.
names="${archive}"
if getent ahostsv4 security.ubuntu.com | awk '{print $1}' | grep -qx "${best}"; then
	names="${names} security.ubuntu.com"
fi
echo "Pinning ${names} to ${best}"
echo "${best} ${names}" | sudo tee -a /etc/hosts > /dev/null
files="$(sudo grep -rlF azure.archive.ubuntu.com /etc/apt 2> /dev/null || true)"
if [[ -n "${files}" ]]; then
	echo "${files}" | xargs sudo sed -i 's|azure\.archive\.ubuntu\.com|archive.ubuntu.com|g'
fi
