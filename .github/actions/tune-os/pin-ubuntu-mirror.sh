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
# keeps downloading from them however long that takes. The Azure mirror that
# the runner images list first sometimes does not answer at all, or sends
# packages at 40 kB/s while it serves the release file instantly, so a probe
# cannot tell when to trust it. This times a download of the release file over
# HTTP and HTTPS from each of the archive's addresses, pins the archive's host
# names in /etc/hosts to every address that answered both within 3 times the
# fastest one's time, so that whatever resolves them still has other addresses
# to fail over to, and points apt at the archive instead of the Azure mirror.
# When no address answers, the archive itself is down, so it leaves apt on the
# Azure mirror.

set -uo pipefail

if [[ "$(dpkg --print-architecture)" == "arm64" ]]; then
	archive=ports.ubuntu.com
	path=ubuntu-ports
else
	archive=archive.ubuntu.com
	path=ubuntu
fi
codename="$(awk -F= '$1 == "VERSION_CODENAME" {print $2}' /etc/os-release)"

# Prints HOST's IPv4 addresses. The resolver of GitHub's hosted runners returns
# only one of the archive's addresses, which may be a slow one, so this also
# asks two public resolvers over HTTPS for all of them.
resolve() {
	local host="$1"
	{
		getent ahostsv4 "${host}" | awk '{print $1}'
		curl -fsS --max-time 3 -H 'accept: application/dns-json' \
			"https://cloudflare-dns.com/dns-query?name=${host}&type=A" 2> /dev/null |
			grep -oE '"data": ?"[0-9.]+"' | grep -oE '[0-9.]+'
		curl -fsS --max-time 3 "https://dns.google/resolve?name=${host}&type=A" 2> /dev/null |
			grep -oE '"data": ?"[0-9.]+"' | grep -oE '[0-9.]+'
	} | grep -xE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u
}

# Prints the seconds it took to download the release file from HOST via IP
# over SCHEME, or nothing if that failed or took longer than 3 seconds.
probe() {
	local host="$1" ip="$2" root="$3" scheme="$4" port=80
	if [[ "${scheme}" == https ]]; then
		port=443
	fi
	curl -fsS -o /dev/null --connect-timeout 2 --max-time 3 --resolve "${host}:${port}:${ip}" \
		-w '%{time_total}\n' "${scheme}://${host}/${root}/dists/${codename}/InRelease" 2> /dev/null
}

# Prints the slower of the HTTP and HTTPS times, or nothing if either failed.
probe_both() {
	local http https
	http="$(probe "$@" http)" && https="$(probe "$@" https)" &&
		awk -v a="${http}" -v b="${https}" 'BEGIN { print (a > b ? a : b) }'
}

results="$(mktemp)"
trap 'rm -f "${results}"' EXIT
for ip in $(resolve "${archive}"); do
	(
		if t="$(probe_both "${archive}" "${ip}" "${path}")"; then
			echo "${t} ${ip}" >> "${results}"
		else
			echo "Ubuntu archive probe: ${ip} did not answer within 3 seconds"
		fi
	) &
done
wait

sort -n "${results}" | sed 's/^/Ubuntu archive probe: /'
best="$(sort -n "${results}" | head -1 | cut -d' ' -f2)"
if [[ -z "${best}" ]]; then
	echo "No Ubuntu archive server answered within 3 seconds; leaving apt as it is."
	exit 0
fi
# security.ubuntu.com is served by the same servers as archive.ubuntu.com.
security="$(resolve security.ubuntu.com)"
fastest="$(sort -n "${results}" | head -1 | cut -d' ' -f1)"
sort -n "${results}" | awk -v max="$(awk -v t="${fastest}" 'BEGIN { print 3 * t }')" '$1 <= max { print $2 }' |
	while read -r ip; do
		names="${archive}"
		if grep -qx "${ip}" <<< "${security}"; then
			names="${names} security.ubuntu.com"
		fi
		echo "Pinning ${names} to ${ip}"
		echo "${ip} ${names}" | sudo tee -a /etc/hosts > /dev/null
	done
files="$(sudo grep -rlF azure.archive.ubuntu.com /etc/apt 2> /dev/null || true)"
if [[ -n "${files}" ]]; then
	echo "${files}" | xargs sudo sed -i 's|azure\.archive\.ubuntu\.com|archive.ubuntu.com|g'
fi
