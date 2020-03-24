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

#
# 0. Initialization and helper methods.
#

[[ "$(dirname "$0")" = "." ]] || fail "bootstrap.sh must be run from its current directory"

# install_dep is a helper function to generalize the download and installation of dependencies.
#
# If the installation is successful, it puts the installed version string into
# the $dist/.installed_version file. If the version has not changed, bootstrap
# will skip future installations.
function install_dep() {
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

  echo "installing $name $version"

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
function get_arch() {
  uname -m
}

# Install protoc.
function install_protoc() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=osx;;
  esac

  case $(get_arch) in
      aarch64)  local target=aarch_64;;
      x86_64)  local target=x86_64;;
      *)   echo "ERROR: unsupported architecture"; exit 1;;
  esac

  wget https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform-${target}.zip
  unzip "protoc-$version-$platform-${target}.zip"
  ln -snf "$dist/bin/protoc" "$VTROOT/bin/protoc"
}
protoc_ver=3.6.1
install_dep "protoc" "$protoc_ver" "$VTROOT/dist/vt-protoc-$protoc_ver" install_protoc


# Install Zookeeper.
function install_zookeeper() {
  local version="$1"
  local dist="$2"

  zk="zookeeper-$version"
  wget "https://apache.org/dist/zookeeper/$zk/$zk.tar.gz"
  tar -xzf "$zk.tar.gz"
  ant -f "$zk/build.xml" package
  ant -f "$zk/zookeeper-contrib/zookeeper-contrib-fatjar/build.xml" jar
  mkdir -p lib
  cp "$zk/build/zookeeper-contrib/zookeeper-contrib-fatjar/zookeeper-dev-fatjar.jar" "lib/$zk-fatjar.jar"
  zip -d "lib/$zk-fatjar.jar" 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*SF' || true # needed for >=3.4.10 <3.5
  rm -rf "$zk" "$zk.tar.gz"
}

zk_ver=${ZK_VERSION:-3.4.14}
if [ "$BUILD_JAVA" == 1 ] ; then
  install_dep "Zookeeper" "$zk_ver" "$VTROOT/dist/vt-zookeeper-$zk_ver" install_zookeeper
fi

# Download and install etcd, link etcd binary into our root.
function install_etcd() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux; local ext=tar.gz;;
    Darwin) local platform=darwin; local ext=zip;;
  esac

  case $(get_arch) in
      aarch64)  local target=arm64;;
      x86_64)  local target=amd64;;
      *)   echo "ERROR: unsupported architecture"; exit 1;;
  esac

  download_url=https://github.com/coreos/etcd/releases/download
  file="etcd-${version}-${platform}-${target}.${ext}"

  wget "$download_url/$version/$file"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$file"
  else
    unzip "$file"
  fi
  rm "$file"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcd" "$VTROOT/bin/etcd"
  ln -snf "$dist/etcd-${version}-${platform}-${target}/etcdctl" "$VTROOT/bin/etcdctl"
}
command -v etcd && echo "etcd already installed" || install_dep "etcd" "v3.3.10" "$VTROOT/dist/etcd" install_etcd


# Download and install k3s, link k3s binary into our root
function install_k3s() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    *)   echo "ERROR: unsupported platform. K3s only supports running on Linux"; exit 1;;
  esac

  case $(get_arch) in
      aarch64)  local target="-arm64";;
      x86_64)  local target="";;
      *)   echo "ERROR: unsupported architecture"; exit 1;;
  esac

  download_url=https://github.com/rancher/k3s/releases/download
  file="k3s${target}"

  local dest="$dist/k3s${target}-${version}-${platform}"
  wget -O  $dest "$download_url/$version/$file"
  chmod +x $dest
  ln -snf  $dest "$VTROOT/bin/k3s"
}
command -v  k3s || install_dep "k3s" "v1.0.0" "$VTROOT/dist/k3s" install_k3s


# Download and install consul, link consul binary into our root.
function install_consul() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
  esac

  case $(get_arch) in
      aarch64)  local target=arm64;;
      x86_64)  local target=amd64;;
      *)   echo "ERROR: unsupported architecture"; exit 1;;
  esac

  download_url=https://releases.hashicorp.com/consul
  wget "${download_url}/${version}/consul_${version}_${platform}_${target}.zip"
  unzip "consul_${version}_${platform}_${target}.zip"
  ln -snf "$dist/consul" "$VTROOT/bin/consul"
}

if [ "$BUILD_CONSUL" == 1 ] ; then
  install_dep "Consul" "1.4.0" "$VTROOT/dist/consul" install_consul
fi

echo
echo "bootstrap finished - run 'make build' to compile"
