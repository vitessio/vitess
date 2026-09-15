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
  uname -m
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
  "${VTROOT}/tools/wget-retry" -q https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform-${target}.zip
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/protoc-$version-$platform-${target}.zip"
  unzip "protoc-$version-$platform-${target}.zip"

<<<<<<< HEAD
  ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
||||||| parent of fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
	local file="protoc-${version}-${platform}-${target}.zip"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"

	ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
=======
	local file="protoc-${version}-${platform}-${target}.zip"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q -t 3 "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${file}"
	#"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"

	ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
>>>>>>> fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
}


# Install Zookeeper.
install_zookeeper() {
<<<<<<< HEAD
  local version="$1"
  local dist="$2"
  zk="zookeeper-$version"
  # This is how we'd download directly from source:
  # wget "https://dlcdn.apache.org/zookeeper/$zk/apache-$zk.tar.gz"
  "${VTROOT}/tools/wget-retry" -q "${VITESS_RESOURCES_DOWNLOAD_URL}/apache-${zk}.tar.gz"
  tar -xzf "$dist/apache-$zk.tar.gz"
  mvn -q -f $dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/pom.xml clean install -P fatjar -DskipTests
  mkdir -p $dist/lib
  cp "$dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/target/$zk-fatjar.jar" "$dist/lib/$zk-fatjar.jar"
  rm -rf "$dist/apache-$zk"
||||||| parent of fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
	local version="$1"
	local dist="$2"
	local zk="zookeeper-$version"
	local file="apache-${zk}-bin.tar.gz"

	# SHA512 checksum for Zookeeper 3.9.5 from Apache archives.
	local sha512="baa1c21dda4d57238fca751e4fa2bbf1daff9a28612b125e497dccd5c188ee6449e2f79947e474c2dd4d19992789d4d36b27b1ba2feb80c2b0c45e7df0e22aa8"

	# dlcdn.apache.org only serves current releases; fall back to archive.apache.org for older versions.
	"${VTROOT}/tools/wget-retry" -q "https://dlcdn.apache.org/zookeeper/${zk}/${file}" || \
		"${VTROOT}/tools/wget-retry" -q "https://archive.apache.org/dist/zookeeper/${zk}/${file}"
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
>>>>>>> fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
}


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
  "${VTROOT}/tools/wget-retry" -q "https://github.com/etcd-io/etcd/releases/download/$version/$file"
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$file"
  else
    unzip "$file"
  fi
  rm "$file"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
||||||| parent of fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
	local file="etcd-${version}-${platform}-${target}.${ext}"

	# This is how we'd download directly from source:
	"${VTROOT}/tools/wget-retry" -q "https://github.com/etcd-io/etcd/releases/download/$version/$file"
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
>>>>>>> fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
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
  "${VTROOT}/tools/wget-retry" -q "${VITESS_RESOURCES_DOWNLOAD_URL}/consul_${version}_${platform}_${target}.zip"
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
  "${VTROOT}/tools/wget-retry" -q "https://github.com/Shopify/toxiproxy/releases/download/$version/$file"
  chmod +x "$dist/$file"
  ln -snf "$dist/$file" "$VTROOT/bin/toxiproxy-server"
||||||| parent of fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
	# This is how we'd download directly from source:
	# download_url=https://releases.hashicorp.com/consul
	# wget "${download_url}/${version}/${file}"
	"${VTROOT}/tools/wget-retry" -q "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"
	ln -snf "$dist/consul" "$VTROOT/bin/consul"
=======
	# This is how we'd download directly from source:
	# download_url=https://releases.hashicorp.com/consul
	# wget "${download_url}/${version}/${file}"
	"${VTROOT}/tools/wget-retry" -q -t 3 "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
	verify_sha256 "$file" "$sha256"
	unzip "$file"
	ln -snf "$dist/consul" "$VTROOT/bin/consul"
>>>>>>> fc22202c49 (ci: bump Apache ZooKeeper to 3.9.6 and bound download retries (#21099))
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

  # toxiproxy
  install_dep "toxiproxy" "$TOXIPROXY_VER" "$VTROOT/dist/toxiproxy" install_toxiproxy

  echo
  echo "bootstrap finished - run 'make build' to compile"
}

install_all
