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
  "${VTROOT}/tools/wget-retry" https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform-${target}.zip
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/protoc-$version-$platform-${target}.zip"
  unzip "protoc-$version-$platform-${target}.zip"

  ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
}


# Install Zookeeper.
install_zookeeper() {
  local version="$1"
  local dist="$2"
  zk="zookeeper-$version"
  # This is how we'd download directly from source:
  # wget "https://dlcdn.apache.org/zookeeper/$zk/apache-$zk.tar.gz"
  "${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/apache-${zk}.tar.gz"
  tar -xzf "$dist/apache-$zk.tar.gz"
  mvn -f $dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/pom.xml clean install -P fatjar -DskipTests
  mkdir -p $dist/lib
  cp "$dist/apache-$zk/zookeeper-contrib/zookeeper-contrib-fatjar/target/$zk-fatjar.jar" "$dist/lib/$zk-fatjar.jar"
  rm -rf "$dist/apache-$zk"
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

  # This is how we'd download directly from source:
  "${VTROOT}/tools/wget-retry" "https://github.com/etcd-io/etcd/releases/download/$version/$file"
  #"${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/${file}"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$file"
  else
    unzip "$file"
  fi
  rm "$file"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
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
  "${VTROOT}/tools/wget-retry" "${VITESS_RESOURCES_DOWNLOAD_URL}/consul_${version}_${platform}_${target}.zip"
  unzip "consul_${version}_${platform}_${target}.zip"
  ln -snf "$dist/consul" "$VTROOT/bin/consul"
}


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
  "${VTROOT}/tools/wget-retry" "https://github.com/Shopify/toxiproxy/releases/download/$version/$file"
  chmod +x "$dist/$file"
  ln -snf "$dist/$file" "$VTROOT/bin/toxiproxy-server"
}

install_all() {
  echo "##local system details..."
  echo "##platform: $(uname) target:$(get_arch) OS: $os"
  # protoc
  protoc_ver=21.3
  install_dep "protoc" "$protoc_ver" "$VTROOT/dist/vt-protoc-$protoc_ver" install_protoc

  # zk
  zk_ver=${ZK_VERSION:-3.8.0}
  if [ "$BUILD_JAVA" == 1 ] ; then
    install_dep "Zookeeper" "$zk_ver" "$VTROOT/dist/vt-zookeeper-$zk_ver" install_zookeeper
  fi

  # etcd
  install_dep "etcd" "v3.5.6" "$VTROOT/dist/etcd" install_etcd

  # consul
  if [ "$BUILD_CONSUL" == 1 ] ; then
    install_dep "Consul" "1.11.4" "$VTROOT/dist/consul" install_consul
  fi

  # toxiproxy
  install_dep "toxiproxy" "v2.5.0" "$VTROOT/dist/toxiproxy" install_toxiproxy

  echo
  echo "bootstrap finished - run 'make build' to compile"
}

install_all
