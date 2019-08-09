#!/bin/bash
# shellcheck disable=SC2164

# Copyright 2017 Google Inc.
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


# Outline of this file.
# 0. Initialization and helper methods.
# 1. Installation of dependencies.
# 2. Installation of Go tools and vendored Go dependencies.
# 3. Installation of development related steps e.g. creating Git hooks.

BUILD_TESTS=${BUILD_TESTS:-1}

#
# 0. Initialization and helper methods.
#

# Run parallel make, based on number of cores available.
case $(uname) in
  Linux)  NB_CORES=$(grep -c '^processor' /proc/cpuinfo);;
  Darwin) NB_CORES=$(sysctl hw.ncpu | awk '{ print $2 }');;
esac
if [ -n "$NB_CORES" ]; then
  export MAKEFLAGS="-j$((NB_CORES+1)) -l${NB_CORES}"
fi

function fail() {
  echo "ERROR: $1"
  exit 1
}

[[ "$(dirname "$0")" = "." ]] || fail "bootstrap.sh must be run from its current directory"

go version &>/dev/null  || fail "Go is not installed or is not on \$PATH"
[[ "$(go version 2>&1)" =~ go1\.[1-9][1-9] ]] || fail "Go is not version 1.11+"

# Set up the proper GOPATH for go get below.
if [ "$BUILD_TESTS" == 1 ] ; then
    source ./dev.env
else
    source ./build.env
fi

# Create main directories.
mkdir -p "$VTROOT/dist"
mkdir -p "$VTROOT/bin"
mkdir -p "$VTROOT/lib"
mkdir -p "$VTROOT/vthook"

if [ "$BUILD_TESTS" == 1 ] ; then
    # Set up required soft links.
    # TODO(mberlin): Which of these can be deleted?
    ln -snf "$VTTOP/config" "$VTROOT/config"
    ln -snf "$VTTOP/data" "$VTROOT/data"
    ln -snf "$VTTOP/py" "$VTROOT/py-vtdb"
    ln -snf "$VTTOP/go/vt/zkctl/zksrv.sh" "$VTROOT/bin/zksrv.sh"
    ln -snf "$VTTOP/test/vthook-test.sh" "$VTROOT/vthook/test.sh"
    ln -snf "$VTTOP/test/vthook-test_backup_error" "$VTROOT/vthook/test_backup_error"
    ln -snf "$VTTOP/test/vthook-test_backup_transform" "$VTROOT/vthook/test_backup_transform"
else
    ln -snf "$VTTOP/config" "$VTROOT/config"
    ln -snf "$VTTOP/data" "$VTROOT/data"
    ln -snf "$VTTOP/go/vt/zkctl/zksrv.sh" "$VTROOT/bin/zksrv.sh"
fi

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

  grpcio_ver=$version
  $PIP install --upgrade grpcio=="$grpcio_ver" grpcio-tools=="$grpcio_ver"
}

if [ "$BUILD_TESTS" == 1 ] ; then
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

  wget "https://github.com/google/protobuf/releases/download/v$version/protoc-$version-$platform-x86_64.zip"
  unzip "protoc-$version-$platform-x86_64.zip"
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
install_dep "Zookeeper" "$zk_ver" "$VTROOT/dist/vt-zookeeper-$zk_ver" install_zookeeper


# Download and install etcd, link etcd binary into our root.
function install_etcd() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux; local ext=tar.gz;;
    Darwin) local platform=darwin; local ext=zip;;
  esac

  download_url=https://github.com/coreos/etcd/releases/download
  file="etcd-${version}-${platform}-amd64.${ext}"

  wget "$download_url/$version/$file"
  if [ "$ext" = "tar.gz" ]; then
    tar xzf "$file"
  else
    unzip "$file"
  fi
  rm "$file"
  ln -snf "$dist/etcd-${version}-${platform}-amd64/etcd" "$VTROOT/bin/etcd"
}
install_dep "etcd" "v3.3.10" "$VTROOT/dist/etcd" install_etcd


# Download and install consul, link consul binary into our root.
function install_consul() {
  local version="$1"
  local dist="$2"

  case $(uname) in
    Linux)  local platform=linux;;
    Darwin) local platform=darwin;;
  esac

  download_url=https://releases.hashicorp.com/consul
  wget "${download_url}/${version}/consul_${version}_${platform}_amd64.zip"
  unzip "consul_${version}_${platform}_amd64.zip"
  ln -snf "$dist/consul" "$VTROOT/bin/consul"
}
install_dep "Consul" "1.4.0" "$VTROOT/dist/consul" install_consul


# Install py-mock.
function install_pymock() {
  local version="$1"
  local dist="$2"

  # For some reason, it seems like setuptools won't create directories even with the --prefix argument
  mkdir -p lib/python2.7/site-packages
  PYTHONPATH=$(prepend_path "$PYTHONPATH" "$dist/lib/python2.7/site-packages")
  export PYTHONPATH

  pushd "$VTTOP/third_party/py" >/dev/null
  tar -xzf "mock-$version.tar.gz"
  cd "mock-$version"
  $PYTHON ./setup.py install --prefix="$dist"
  cd ..
  rm -r "mock-$version"
  popd >/dev/null
}
pymock_version=1.0.1
if [ "$BUILD_TESTS" == 1 ] ; then
    install_dep "py-mock" "$pymock_version" "$VTROOT/dist/py-mock-$pymock_version" install_pymock
fi

# Download Selenium (necessary to run test/vtctld_web_test.py).
function install_selenium() {
  local version="$1"
  local dist="$2"

  $VIRTUALENV "$dist"
  PIP="$dist/bin/pip"
  # PYTHONPATH is removed for `pip install` because otherwise it can pick up go/dist/grpc/usr/local/lib/python2.7/site-packages
  # instead of go/dist/selenium/lib/python3.5/site-packages and then can't find module 'pip._vendor.requests'
  PYTHONPATH='' $PIP install selenium
}
if [ "$BUILD_TESTS" == 1 ] ; then
    install_dep "Selenium" "latest" "$VTROOT/dist/selenium" install_selenium
fi


# Download chromedriver (necessary to run test/vtctld_web_test.py).
function install_chromedriver() {
  local version="$1"
  local dist="$2"

  curl -sL "https://chromedriver.storage.googleapis.com/$version/chromedriver_linux64.zip" > chromedriver_linux64.zip
  unzip -o -q chromedriver_linux64.zip -d "$dist"
  rm chromedriver_linux64.zip
}
if [ "$BUILD_TESTS" == 1 ] ; then
    install_dep "chromedriver" "73.0.3683.20" "$VTROOT/dist/chromedriver" install_chromedriver
fi


#
# 2. Installation of Go tools and vendored Go dependencies.
#


# Install third-party Go tools used as part of the development workflow.
#
# DO NOT ADD LIBRARY DEPENDENCIES HERE. Instead use govendor as described below.
#
# Note: We explicitly do not vendor the tools below because a) we want to stay
# on their latest version and b) it's easier to "go install" them this way.
gotools=" \
       github.com/golang/mock/mockgen \
       github.com/kardianos/govendor \
       golang.org/x/lint/golint \
       golang.org/x/tools/cmd/cover \
       golang.org/x/tools/cmd/goimports \
       golang.org/x/tools/cmd/goyacc \
"
echo "Installing dev tools with 'go get'..."
# shellcheck disable=SC2086
go get -u $gotools || fail "Failed to download some Go tools with 'go get'. Please re-run bootstrap.sh in case of transient errors."

# Download dependencies that are version-pinned via govendor.
#
# To add a new dependency, run:
#   govendor fetch <package_path>
#
# Existing dependencies can be updated to the latest version with 'fetch' as well.
#
# Then:
#   git add vendor/vendor.json
#   git commit
#
# See https://github.com/kardianos/govendor for more options.
echo "Updating govendor dependencies..."
govendor sync || fail "Failed to download/update dependencies with govendor. Please re-run bootstrap.sh in case of transient errors."

#
# 3. Installation of development related steps e.g. creating Git hooks.
#

if [ "$BUILD_TESTS" == 1 ] ; then
 # Create the Git hooks.
 echo "creating git hooks"
 mkdir -p "$VTTOP/.git/hooks"
 ln -sf "$VTTOP/misc/git/pre-commit" "$VTTOP/.git/hooks/pre-commit"
 ln -sf "$VTTOP/misc/git/prepare-commit-msg.bugnumber" "$VTTOP/.git/hooks/prepare-commit-msg"
 ln -sf "$VTTOP/misc/git/commit-msg" "$VTTOP/.git/hooks/commit-msg"
 (cd "$VTTOP" && git config core.hooksPath "$VTTOP/.git/hooks")
 echo
 echo "bootstrap finished - run 'source dev.env' in your shell before building."
else
 echo
 echo "bootstrap finished - run 'source build.env' in your shell before building."
fi

