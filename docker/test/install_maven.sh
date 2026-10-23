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

# Installs Maven into DIR, unless it is already there, and links LINK to it.
# Maven is not in the bootstrap image, to avoid CVE-2024-47554: run.sh installs
# it in each test container, and the go_cache_warm workflow fills the cache of
# it that docker_ci restores. Maven is unpacked next to its final location and
# renamed into place, so an interrupted install never leaves a partial one.
#
# Usage: docker/test/install_maven.sh DIR LINK

set -euo pipefail

maven_version=3.9.12

dir="$1"
link="$2"
maven="${dir}/apache-maven-${maven_version}"

if [[ ! -x "${maven}/bin/mvn" ]]; then
	echo "Installing Maven ${maven_version}..."
	tmp="$(mktemp -d "${dir}/install.XXXXXX")"
	curl -fsSL --connect-timeout 10 --retry 3 "https://archive.apache.org/dist/maven/maven-3/${maven_version}/binaries/apache-maven-${maven_version}-bin.tar.gz" | tar -xz -C "${tmp}"
	mv -T "${tmp}/apache-maven-${maven_version}" "${maven}"
	rmdir "${tmp}"
	echo "Maven installed"
fi
ln -sfn "${maven}" "${link}"
