#!/bin/bash
# shellcheck disable=SC2164

# Copyright 2019 The Vitess Authors.
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

### This file is executed by 'make tools'. You do not need to execute it directly.

source ./dev.env

# Outline of this file.
# 0. Initialization and helper methods.
# 1. Installation of dependencies.

BUILD_JAVA=${BUILD_JAVA:-1}
BUILD_CONSUL=${BUILD_CONSUL:-1}
<<<<<<< HEAD
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
BUILD_PROTOC=${BUILD_PROTOC:-1}
=======
BUILD_PROTOC=${BUILD_PROTOC:-1}
# Maven is off by default: the bootstrap image runs bootstrap.sh and must not
# contain it (CVE-2024-47554). docker/test/run.sh turns it on in the test
# containers.
BUILD_MAVEN=${BUILD_MAVEN:-0}
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))

VITESS_RESOURCES_DOWNLOAD_BASE_URL="https://github.com/vitessio/vitess-resources/releases/download"
VITESS_RESOURCES_RELEASE="v4.0"
VITESS_RESOURCES_DOWNLOAD_URL="${VITESS_RESOURCES_DOWNLOAD_BASE_URL}/${VITESS_RESOURCES_RELEASE}"
#
# 0. Initialization and helper methods.
#

[[ "$(dirname "$0")" = "." ]] || fail "bootstrap.sh must be run from its current directory"

# install_dep is a helper function to generalize the download and installation of dependencies.
#
# If the installation is successful, it puts the installed version string into
# the $dist/.installed_version file. If the version has not changed, bootstrap
# will skip future installations.
install_dep() {
  if [[ $# != 4 ]]; then
    fail "install_dep function requires exactly 4 parameters (and not $#). Parameters: $*"
  fi
  local name="$1"
  local version="$2"
  local dist="$3"
  local install_func="$4"

  version_file="$dist/.installed_version"
  if [[ -f "$version_file" && "$(cat "$version_file")" == "$version" ]]; then
    echo "skipping $name install. remove $dist to force re-install."
    return
  fi

  echo "<<< Installing $name $version >>>"

  # shellcheck disable=SC2064
  trap "fail '$name build failed'; exit 1" ERR

  # Cleanup any existing data and re-create the directory.
  rm -rf "$dist"
  mkdir -p "$dist"

  # Change $CWD to $dist before calling "install_func".
  pushd "$dist" >/dev/null
  # -E (same as "set -o errtrace") makes sure that "install_func" inherits the
  # trap. If here's an error, the trap will be called which will exit this
  # script.
  set -E
  $install_func "$version" "$dist"
  set +E
  popd >/dev/null

  trap - ERR

  echo "$version" > "$version_file"
}


#
# 1. Installation of dependencies.
#

# We should not use the arch command, since it is not reliably
# available on macOS or some linuxes:
# https://www.gnu.org/software/coreutils/manual/html_node/arch-invocation.html
get_arch() {
<<<<<<< HEAD
  uname -m
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	uname -m
}

# verify_sha256 verifies the SHA256 checksum of a file.
#
# Usage: verify_sha256 <file> <expected_sha256>
verify_sha256() {
	local file="$1"
	local expected="$2"

	if command -v sha256sum &>/dev/null; then
		echo "$expected  $file" | sha256sum -c - &>/dev/null || fail "SHA256 checksum verification failed for $file"
	elif command -v shasum &>/dev/null; then
		echo "$expected  $file" | shasum -a 256 -c - &>/dev/null || fail "SHA256 checksum verification failed for $file"
	else
		fail "Neither sha256sum nor shasum found. Cannot verify checksum."
	fi
	echo "OK: SHA256 checksum verified for $file"
}

# verify_sha512 verifies the SHA512 checksum of a file.
#
# Usage: verify_sha512 <file> <expected_sha512>
verify_sha512() {
	local file="$1"
	local expected="$2"

	if command -v sha512sum &>/dev/null; then
		echo "$expected  $file" | sha512sum -c - &>/dev/null || fail "SHA512 checksum verification failed for $file"
	elif command -v shasum &>/dev/null; then
		echo "$expected  $file" | shasum -a 512 -c - &>/dev/null || fail "SHA512 checksum verification failed for $file"
	else
		fail "Neither sha512sum nor shasum found. Cannot verify checksum."
	fi
	echo "OK: SHA512 checksum verified for $file"
=======
	uname -m
}

# Downloads are kept here, outside the directories that install_dep wipes,
# so that they survive a re-install and can be cached (CI does).
DOWNLOAD_CACHE="$VTROOT/dist/download-cache"

# checksum_matches reports whether a file exists and has the expected checksum.
#
# Usage: checksum_matches <256|512> <file> <expected_checksum>
checksum_matches() {
	local bits="$1"
	local file="$2"
	local expected="$3"

	[[ -f "$file" ]] || return 1
	if command -v "sha${bits}sum" &>/dev/null; then
		echo "$expected  $file" | "sha${bits}sum" -c - &>/dev/null
	elif command -v shasum &>/dev/null; then
		echo "$expected  $file" | shasum -a "$bits" -c - &>/dev/null
	else
		fail "Neither sha${bits}sum nor shasum found. Cannot verify checksum."
	fi
}

# download_verified makes sure that $DOWNLOAD_CACHE/<file> exists and has the
# expected checksum. A cached file is used only if its checksum matches;
# otherwise it is downloaded again from the first URL that works. Only a
# download that passes the checksum is moved into the cache, with a rename that
# replaces the cached file atomically. The cached file is never deleted first:
# the docker test containers share the cache, and another one may have just
# published it and be about to extract it.
#
# Usage: download_verified <file> <256|512> <expected_checksum> <url>...
download_verified() {
	local file="$1"
	local bits="$2"
	local expected="$3"
	shift 3

	local cached="$DOWNLOAD_CACHE/$file"
	if checksum_matches "$bits" "$cached" "$expected"; then
		echo "OK: using cached $cached"
		return
	fi

	mkdir -p "$DOWNLOAD_CACHE"
	# A unique name, as the docker test containers share the cache.
	local partial
	partial="$(mktemp "$cached.XXXXXX")"
	local url
	for url in "$@"; do
		# Tries must be bounded: wget-retry retries a 404 forever by default and
		# the next URL would never be tried.
		if "${VTROOT}/tools/wget-retry" -q -t 3 -O "$partial" "$url"; then
			if checksum_matches "$bits" "$partial" "$expected"; then
				# mktemp makes the file private, but the docker builds and test
				# containers read it as another user.
				chmod 644 "$partial"
				mv "$partial" "$cached"
				echo "OK: SHA${bits} checksum verified for $cached"
				return
			fi
			rm -f "$partial"
			fail "SHA${bits} checksum verification failed for $file downloaded from $url"
		fi
	done
	rm -f "$partial"
	fail "Could not download $file"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}

# Install protoc.
install_protoc() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=osx;;
    *) echo "ERROR: unsupported platform for protoc"; exit 1;;
  esac

  case $(get_arch) in
      aarch64)  local target=aarch_64;;
      x86_64)  local target=x86_64;;
      arm64) case "$platform" in
          osx) local target=aarch_64;;
          *) echo "ERROR: unsupported architecture for protoc"; exit 1;;
      esac;;
      *)   echo "ERROR: unsupported architecture for protoc"; exit 1;;
  esac

  # This is how we'd download directly from source:
  "${VTROOT}/tools/wget-retry" -q -t 3 https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform-${target}.zip
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/protoc-$version-$platform-${target}.zip"
  unzip "protoc-$version-$platform-${target}.zip"

<<<<<<< HEAD
  ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	local file="protoc-${version}-${platform}-${target}.zip"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q -t 3 "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"

	ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
=======
	local file="protoc-${version}-${platform}-${target}.zip"

	# This is how we'd download from the Vitess resources mirror:
	# "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	download_verified "$file" 256 "$sha256" "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
	unzip "$DOWNLOAD_CACHE/$file"

	ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}


# Install Zookeeper.
install_zookeeper() {
<<<<<<< HEAD
  local version="$1"
  local dist="$2"
  zk="zookeeper-$version"
  # This is how we'd download directly from source:
  # wget "https://dlcdn.apache.org/zookeeper/$zk/apache-$zk.tar.gz"
  "${VTROOT}/tools/wget-retry" -q -t 3 "${VITESS_RESOURCES_DOWNLOAD_URL}/apache-${zk}.tar.gz"
  tar -xzf "$dist/apache-$zk.tar.gz"
  mvn -q -f $dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/pom.xml clean install -P fatjar -DskipTests
  mkdir -p $dist/lib
  cp "$dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/target/$zk-fatjar.jar" "$dist/lib/$zk-fatjar.jar"
  rm -rf "$dist/apache-$zk"
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	local version="$1"
	local dist="$2"
	local zk="zookeeper-$version"
	local file="apache-${zk}-bin.tar.gz"

	# SHA512 checksum for Zookeeper 3.9.6 from Apache archives.
	local sha512="e999626df06de30dc8bb53bb51da9bb786b1658406adafc6b92f104d72ca25e869f70a57c83a726f8b9558cc0fb519fc20f201aa5d6da09884bc4be5fe6dd3b0"

	# dlcdn.apache.org only serves current releases; fall back to archive.apache.org for older versions.
	# Tries must be bounded: wget-retry retries a 404 forever by default and the fallback would never run.
	"${VTROOT}/tools/wget-retry" -q -t 3 "https://dlcdn.apache.org/zookeeper/${zk}/${file}" || \
		"${VTROOT}/tools/wget-retry" -q -t 3 "https://archive.apache.org/dist/zookeeper/${zk}/${file}"
	verify_sha512 "$dist/$file" "$sha512"
	tar -xzf "$dist/$file"
	mkdir -p "$dist"/lib
	cp "$dist/apache-$zk-bin/lib/"*.jar "$dist/lib/"
	rm -rf "$dist/apache-$zk-bin"
=======
	local version="$1"
	local dist="$2"
	local zk="zookeeper-$version"
	local file="apache-${zk}-bin.tar.gz"

	# SHA512 checksum for Zookeeper 3.9.7 from Apache archives.
	local sha512="69ce6bf0b6df9a9ecdf080bdc15758eef97ef176a88f1408214f2327fe6e5349fedf738031977e6fc2f52cb636b45779653389a6bfda75be9c82dd3e72f24f25"

	# dlcdn.apache.org only serves current releases; fall back to archive.apache.org for older versions.
	download_verified "$file" 512 "$sha512" \
		"https://dlcdn.apache.org/zookeeper/${zk}/${file}" \
		"https://archive.apache.org/dist/zookeeper/${zk}/${file}"
	tar -xzf "$DOWNLOAD_CACHE/$file"
	mkdir -p "$dist"/lib
	cp "$dist/apache-$zk-bin/lib/"*.jar "$dist/lib/"
	rm -rf "$dist/apache-$zk-bin"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}

<<<<<<< HEAD
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
=======
# Install Maven.
install_maven() {
	local version="$1"
	local file="apache-maven-${version}-bin.tar.gz"

	# SHA512 checksum for Maven 3.9.16 from Apache downloads.
	local sha512="831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6"

	# Maven Central keeps every Maven release, unlike dlcdn.apache.org, and
	# archive.apache.org keeps timing out from the CI runners.
	download_verified "$file" 512 "$sha512" \
		"https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${version}/${file}"
	tar -xzf "$DOWNLOAD_CACHE/$file" --strip-components=1
}
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))

# Download and install etcd, link etcd binary into our root.
install_etcd() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux; local ext=tar.gz;;
    Darwin) local platform=darwin; local ext=zip;;
    *)   echo "ERROR: unsupported platform for etcd"; exit 1;;
  esac

  case $(get_arch) in
      aarch64)  local target=arm64;;
      x86_64)  local target=amd64;;
      arm64)  local target=arm64;;
      *)   echo "ERROR: unsupported architecture for etcd"; exit 1;;
  esac

  file="etcd-${version}-${platform}-${target}.${ext}"

<<<<<<< HEAD
  # This is how we'd download directly from source:
  "${VTROOT}/tools/wget-retry" -q -t 3 "https://github.com/etcd-io/etcd/releases/download/$version/$file"
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$file"
  else
    unzip "$file"
  fi
  rm "$file"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	local file="etcd-${version}-${platform}-${target}.${ext}"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q -t 3 "https://github.com/etcd-io/etcd/releases/download/$version/$file"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	if [ "$ext" = "tar.gz" ]; then
		tar xzf "$file"
	else
		unzip "$file"
	fi
	rm "$file"
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
=======
	local file="etcd-${version}-${platform}-${target}.${ext}"

	# This is how we'd download from the Vitess resources mirror:
	# "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	download_verified "$file" 256 "$sha256" "https://github.com/etcd-io/etcd/releases/download/$version/$file"
	if [ "$ext" = "tar.gz" ]; then
		tar xzf "$DOWNLOAD_CACHE/$file"
	else
		unzip "$DOWNLOAD_CACHE/$file"
	fi
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
	ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}

# Download and install consul, link consul binary into our root.
install_consul() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
    *)   echo "ERROR: unsupported platform for consul"; exit 1;;
  esac

  case $(get_arch) in
    aarch64)  local target=arm64;;
    x86_64)  local target=amd64;;
    arm64)  local target=arm64;;
    *)   echo "ERROR: unsupported architecture for consul"; exit 1;;
  esac

  # This is how we'd download directly from source:
  # download_url=https://releases.hashicorp.com/consul
  # wget "${download_url}/${version}/consul_${version}_${platform}_${target}.zip"
  "${VTROOT}/tools/wget-retry" -q -t 3 "${VITESS_RESOURCES_DOWNLOAD_URL}/consul_${version}_${platform}_${target}.zip"
  unzip "consul_${version}_${platform}_${target}.zip"
  ln -snf "$dist/consul" "$VTROOT/bin/consul"
}


<<<<<<< HEAD
# Download and install toxiproxy, link toxiproxy binary into our root.
install_toxiproxy() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
    *)   echo "WARNING: unsupported platform. Some tests that rely on toxiproxy will not function."; return;;
  esac

  case $(get_arch) in
    aarch64)  local target=arm64;;
    x86_64)  local target=amd64;;
    arm64)  local target=arm64;;
    *)   echo "WARNING: unsupported architecture. Some tests that rely on toxiproxy will not function."; return;;
  esac

  # This is how we'd download directly from source:
  file="toxiproxy-server-${platform}-${target}"
  "${VTROOT}/tools/wget-retry" -q -t 3 "https://github.com/Shopify/toxiproxy/releases/download/$version/$file"
  chmod +x "$dist/$file"
  ln -snf "$dist/$file" "$VTROOT/bin/toxiproxy-server"
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	# This is how we'd download directly from source:
	# download_url=https://releases.hashicorp.com/consul
	# wget "${download_url}/${version}/${file}"
	"${VTROOT}/tools/wget-retry" -q -t 3 "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"
	ln -snf "$dist/consul" "$VTROOT/bin/consul"
=======
	# This is how we'd download directly from source:
	# download_url=https://releases.hashicorp.com/consul
	# wget "${download_url}/${version}/${file}"
	download_verified "$file" 256 "$sha256" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	unzip "$DOWNLOAD_CACHE/$file"
	ln -snf "$dist/consul" "$VTROOT/bin/consul"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}

install_all() {
  echo "##local system details..."
  echo "##platform: $(uname) target:$(get_arch) OS: $OSTYPE"
  # protoc
  install_dep "protoc" "$PROTOC_VER" "$VTROOT/dist/vt-protoc-$PROTOC_VER" install_protoc

  # zk
  if [ "$BUILD_JAVA" == 1 ] ; then
    install_dep "Zookeeper" "$ZK_VER" "$VTROOT/dist/vt-zookeeper-$ZK_VER" install_zookeeper
  fi

  # etcd
  install_dep "etcd" "$ETCD_VER" "$VTROOT/dist/etcd" install_etcd

  # consul
  if [ "$BUILD_CONSUL" == 1 ] ; then
    install_dep "Consul" "$CONSUL_VER" "$VTROOT/dist/consul" install_consul
  fi

<<<<<<< HEAD
  # toxiproxy
  install_dep "toxiproxy" "$TOXIPROXY_VER" "$VTROOT/dist/toxiproxy" install_toxiproxy

  echo
  echo "bootstrap finished - run 'make build' to compile"
||||||| parent of 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
	echo
	echo "bootstrap finished - run 'make build' to compile"
=======
	# Maven, on the PATH of the bootstrap image as dist/maven/bin.
	if [ "$BUILD_MAVEN" == 1 ]; then
		install_dep "Maven" "$MAVEN_VER" "$VTROOT/dist/maven" install_maven
	fi

	echo
	echo "bootstrap finished - run 'make build' to compile"
>>>>>>> 3a06228183 (ci: bump ZooKeeper to 3.9.7 and Maven to 3.9.16, and cache the verified tool downloads (#21886))
}

install_all
