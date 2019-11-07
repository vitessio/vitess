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

# This is a next-gen bootstrap which skips Python and Java tests,
# and does not use the VTROOT/VTTOP layout.
#
# My original intention was to use the same bootstrap.sh and gate
# for new features, but it has turned out to be difficult to do,
# due to the way that Docker cache works in the CI environment.

function fail() {
  echo "ERROR: $1"
  exit 1
}

[[ "$(dirname "$0")" = "." ]] || fail "bootstrap.sh must be run from its current directory"

# Create main directories.

VTROOT="$PWD"

mkdir -p dist
mkdir -p bin
mkdir -p lib
mkdir -p vthook

source ./dev.env

go version &>/dev/null  || fail "Go is not installed or is not on \$PATH"
goversion_min 1.12 || fail "Go is not version 1.12+"

# Set up required soft links.
# TODO(mberlin): Which of these can be deleted?
ln -snf "$VTROOT/py" "$VTROOT/py-vtdb"
ln -snf "$VTROOT/go/vt/zkctl/zksrv.sh" "$VTROOT/bin/zksrv.sh"
ln -snf "$VTROOT/test/vthook-test.sh" "$VTROOT/vthook/test.sh"
ln -snf "$VTROOT/test/vthook-test_backup_error" "$VTROOT/vthook/test_backup_error"
ln -snf "$VTROOT/test/vthook-test_backup_transform" "$VTROOT/vthook/test_backup_transform"

# git hooks are only required if someone intends to contribute.

echo "creating git hooks"
mkdir -p "$VTROOT/.git/hooks"
ln -sf "$VTROOT/misc/git/pre-commit" "$VTROOT/.git/hooks/pre-commit"
ln -sf "$VTROOT/misc/git/commit-msg" "$VTROOT/.git/hooks/commit-msg"
git config core.hooksPath "$VTROOT/.git/hooks"

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

# Wrapper around the `arch` command which plays nice with OS X
function get_arch() {
  case $(uname) in
    Linux) arch;;
    Darwin) uname -m;;
  esac
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
}
install_dep "etcd" "v3.3.10" "$VTROOT/dist/etcd" install_etcd

echo
echo "bootstrap finished"
