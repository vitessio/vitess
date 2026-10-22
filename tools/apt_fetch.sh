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

# Downloads the .deb files that "apt-get install PACKAGE..." needs into apt's
# archive directory, as tools/warm_apt_cache.sh needs them, with curl rather
# than apt. Some of the servers behind a mirror's host name intermittently
# answer only after 10-20 seconds and then send 50 kB/s, and apt keeps
# downloading from such a server however long that takes. This tries each of
# the host's addresses in turn, gives up on one that sends less than 256 kB/s
# for 10 seconds and avoids it for the rest of the run, then checks every file
# against apt's hash. ARCHIVES overrides the archive directory. Usage:
#
#   [ARCHIVES=DIR] tools/apt_fetch.sh PACKAGE...

set -euo pipefail

if [[ -n "${ARCHIVES:-}" ]]; then
	archives="${ARCHIVES%/}"
else
	eval "$(apt-config shell archives Dir::Cache::Archives/d)"
fi
apt_options=(-o "Dir::Cache::Archives=${archives}/")

fetch() {
	local uri="$1" file="$2" size="$3" hash="$4"
	local dest="${archives}/${file}" tmp host port ip
	if [[ -f "${dest}" && "$(stat -c %s "${dest}")" == "${size}" ]]; then
		return 0
	fi
	tmp="$(mktemp "${archives}/partial/fetch.XXXXXX")"
	host="$(echo "${uri}" | awk -F/ '{print $3}')"
	port=80
	if [[ "${uri}" == https://* ]]; then
		port=443
	fi
	local fetched=""
	# The addresses that were too slow go last, in case all of them are.
	for ip in $(getent ahostsv4 "${host}" | awk '{print $1}' | sort -u | shuf |
		awk -v slow="${slow}" 'BEGIN { while ((getline l < slow) > 0) s[l] = 1 }
			s[$0] { last = last $0 "\n"; next } { print } END { printf "%s", last }'); do
		if curl -fsS --connect-timeout 5 --speed-limit 262144 --speed-time 10 \
			--resolve "${host}:${port}:${ip}" -o "${tmp}" "${uri}"; then
			fetched=1
			break
		fi
		echo "${file}: ${host} via ${ip} was too slow, trying the next address"
		echo "${ip}" >> "${slow}"
	done
	# Every address too slow: take the file at whatever speed.
	if [[ -z "${fetched}" ]]; then
		curl -fsS --retry 3 -o "${tmp}" "${uri}"
	fi
	local sum
	case "${hash%%:*}" in
	SHA512) sum=sha512sum ;;
	SHA256) sum=sha256sum ;;
	SHA1) sum=sha1sum ;;
	MD5Sum) sum=md5sum ;;
	*)
		echo "${file}: unknown hash ${hash}" >&2
		return 1
		;;
	esac
	if [[ "$("${sum}" "${tmp}" | cut -d' ' -f1)" != "${hash#*:}" ]]; then
		echo "${file}: hash mismatch" >&2
		rm -f "${tmp}"
		return 1
	fi
	mv "${tmp}" "${dest}"
}
# Kept across runs, which tools/warm_apt_cache.sh makes several of.
slow="${TMPDIR:-/tmp}/apt_fetch.slow-addresses"
touch "${slow}"
export -f fetch
export archives slow

apt-get install "${apt_options[@]}" -y -qq --print-uris "$@" | sed -n "s/^'\([^']*\)' /\1 /p" |
	xargs -r -P 4 -L 1 bash -c 'fetch "$@"' _

# apt checks what is there, and downloads anything the above did not.
apt-get install "${apt_options[@]}" -y -qq --download-only "$@"
