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

BUILD_PYTHON=${BUILD_PYTHON:-1}
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


# Install the gRPC Python library (grpcio) and the protobuf gRPC Python plugin (grpcio-tools) from PyPI.
# Dependencies like the Python protobuf package will be installed automatically.
function install_grpc() {
  local version="$1"
  local dist="$2"

  # Python requires a very recent version of virtualenv.
  # We also require a recent version of pip, as we use it to
  # upgrade the other tools.
  # For instance, setuptools doesn't work with pip 6.0:
  # https://github.com/pypa/setuptools/issues/945
  # (and setuptools is used by grpc install).
  grpc_virtualenv="$dist/usr/local"
  $VIRTUALENV -v "$grpc_virtualenv"
  PIP=$grpc_virtualenv/bin/pip
  $PIP install --upgrade pip
  $PIP install --upgrade --ignore-installed virtualenv
  $PIP install mysql-connector-python

  grpcio_ver=$version
  $PIP install --upgrade grpcio=="$grpcio_ver" grpcio-tools=="$grpcio_ver"
}

if [ "$BUILD_PYTHON" == 1 ] ; then
    install_dep "gRPC" "1.16.0" "$VTROOT/dist/grpc" install_grpc
fi

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

# Install py-mock.
function install_pymock() {
  local version="$1"
  local dist="$2"

  # For some reason, it seems like setuptools won't create directories even with the --prefix argument
  mkdir -p lib/python2.7/site-packages
  PYTHONPATH=$(prepend_path "$PYTHONPATH" "$dist/lib/python2.7/site-packages")
  export PYTHONPATH

  pushd "$VTROOT/third_party/py" >/dev/null
  tar -xzf "mock-$version.tar.gz"
  cd "mock-$version"
  $PYTHON ./setup.py install --prefix="$dist"
  cd ..
  rm -r "mock-$version"
  popd >/dev/null
}
pymock_version=1.0.1
if [ "$BUILD_PYTHON" == 1 ] ; then
    install_dep "py-mock" "$pymock_version" "$VTROOT/dist/py-mock-$pymock_version" install_pymock
fi

# Download Selenium (necessary to run test/vtctld_web_test.py).
function install_selenium() {
  local version="$1"
  local dist="$2"

  PYTHONPATH='' $VIRTUALENV "$dist"
  PIP="$dist/bin/pip"
  # PYTHONPATH is removed for `pip install` because otherwise it can pick up go/dist/grpc/usr/local/lib/python2.7/site-packages
  # instead of go/dist/selenium/lib/python3.5/site-packages and then can't find module 'pip._vendor.requests'
  PYTHONPATH='' $PIP install selenium
}
if [ "$BUILD_PYTHON" == 1 ] ; then
    install_dep "Selenium" "latest" "$VTROOT/dist/selenium" install_selenium
fi

# Download chromedriver (necessary to run test/vtctld_web_test.py).
function install_chromedriver() {
  local version="$1"
  local dist="$2"

  if [ "$(arch)" == "aarch64" ] ; then
      os=$(cat /etc/*release | grep "^ID=" | cut -d '=' -f 2)
      case $os in
          ubuntu|debian)
              sudo apt-get update -y && sudo apt install -y --no-install-recommends unzip libglib2.0-0 libnss3 libx11-6
	      ;;
	  centos|fedora)
	      sudo yum update -y && yum install -y libX11 unzip wget
	      ;;
      esac
      echo "For Arm64, using prebuilt binary from electron (https://github.com/electron/electron/) of version 76.0.3809.126"
      wget https://github.com/electron/electron/releases/download/v6.0.3/chromedriver-v6.0.3-linux-arm64.zip
      unzip -o -q chromedriver-v6.0.3-linux-arm64.zip -d "$dist"
      rm chromedriver-v6.0.3-linux-arm64.zip
  else
      curl -sL "https://chromedriver.storage.googleapis.com/$version/chromedriver_linux64.zip" > chromedriver_linux64.zip
      unzip -o -q chromedriver_linux64.zip -d "$dist"
      rm chromedriver_linux64.zip
  fi
}
install_dep "chromedriver" "73.0.3683.20" "$VTROOT/dist/chromedriver" install_chromedriver

if [ "$BUILD_PYTHON" == 1 ] ; then
  PYTHONPATH='' $PIP install mysql-connector-python
fi

echo
echo "bootstrap finished - run 'make build' to compile"
