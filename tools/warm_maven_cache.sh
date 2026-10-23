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

# Fills the Maven cache that docker_ci restores and docker/test/run.sh shares
# with the test containers: Maven itself in MAVEN_DIR (/tmp/maven), and the
# dependencies and plugins of the java shard's builds, "make java_test" and
# test/client_test.sh, in M2_DIR (/tmp/mavencache), which the containers use
# as ~/.m2. The go_cache_warm workflow runs it from an empty cache, so the
# entry it saves holds nothing stale. Usage:
#
#   tools/warm_maven_cache.sh MAVEN_DIR M2_DIR

set -euo pipefail

maven_dir="$1"
m2_dir="$2"

mkdir -p "${maven_dir}" "${m2_dir}"
link="$(mktemp -u)"
docker/test/install_maven.sh "${maven_dir}" "${link}"
mvn() {
	"${link}/bin/mvn" -B -Dmaven.repo.local="${m2_dir}/repository" "$@"
}

# "make java_test", without running its tests. Surefire only resolves its
# test framework provider when its scan finds a test class, so select every
# class, but a method that none of them has.
mvn -f java/pom.xml clean verify -Dtest='*#warmMavenCacheOnly' -Dsurefire.failIfNoSpecifiedTests=false
# test/client_java.sh and test/client_jdbc.sh.
mvn -f java/pom.xml -pl example -am install -DskipTests
mvn -f java/example/pom.xml exec:help
mvn -f java/pom.xml clean

# The install above put the client's own snapshot in the cache; every build
# makes its own.
rm -rf "${m2_dir}/repository/io/vitess"
rm -f "${link}"
