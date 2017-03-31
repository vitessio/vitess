#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

SKIP_ROOT_INSTALLS=False
if [ "$1" = "--skip_root_installs" ]; then
  SKIP_ROOT_INSTALLS=True
fi

# Run parallel make, based on number of cores available.
NB_CORES=$(grep -c '^processor' /proc/cpuinfo)
if [ -n "$NB_CORES" ]; then
  export MAKEFLAGS="-j$((NB_CORES+1)) -l${NB_CORES}"
fi

function fail() {
  echo "ERROR: $1"
  exit 1
}

[ -f bootstrap.sh ] || fail "bootstrap.sh must be run from its current directory"

[ "$USER" != "root" ] || fail "Vitess cannot run as root. Please bootstrap with a non-root user."

go version 2>&1 >/dev/null || fail "Go is not installed or is not on \$PATH"

# Set up the proper GOPATH for go get below.
source ./dev.env

mkdir -p $VTROOT/dist
mkdir -p $VTROOT/bin
mkdir -p $VTROOT/lib
mkdir -p $VTROOT/vthook

echo "Updating git submodules..."
git submodule update --init

# install zookeeper
zk_ver=3.4.6
zk_dist=$VTROOT/dist/vt-zookeeper-$zk_ver
if [ -f $zk_dist/.build_finished ]; then
  echo "skipping zookeeper build. remove $zk_dist to force rebuild."
else
  rm -rf $zk_dist
  (cd $VTROOT/dist && \
    wget http://apache.org/dist/zookeeper/zookeeper-$zk_ver/zookeeper-$zk_ver.tar.gz && \
    tar -xzf zookeeper-$zk_ver.tar.gz && \
    mkdir -p $zk_dist/lib && \
    cp zookeeper-$zk_ver/contrib/fatjar/zookeeper-$zk_ver-fatjar.jar $zk_dist/lib && \
    rm -rf zookeeper-$zk_ver zookeeper-$zk_ver.tar.gz)
  [ $? -eq 0 ] || fail "zookeeper build failed"
  touch $zk_dist/.build_finished
fi

# Download and install etcd, link etcd binary into our root.
etcd_version=v3.1.0-rc.1
etcd_dist=$VTROOT/dist/etcd
etcd_version_file=$etcd_dist/version
if [[ -f $etcd_version_file && "$(cat $etcd_version_file)" == "$etcd_version" ]]; then
  echo "skipping etcd install. remove $etcd_version_file to force re-install."
else
  rm -rf $etcd_dist
  mkdir -p $etcd_dist
  download_url=https://github.com/coreos/etcd/releases/download
  (cd $etcd_dist && \
    wget ${download_url}/${etcd_version}/etcd-${etcd_version}-linux-amd64.tar.gz && \
    tar xzf etcd-${etcd_version}-linux-amd64.tar.gz)
  [ $? -eq 0 ] || fail "etcd download failed"
  echo "$etcd_version" > $etcd_version_file
fi
ln -snf $etcd_dist/etcd-${etcd_version}-linux-amd64/etcd $VTROOT/bin/etcd

# Download and install consul, link consul binary into our root.
consul_version=0.7.2
consul_dist=$VTROOT/dist/consul
consul_version_file=$consul_dist/version
if [[ -f $consul_version_file && "$(cat $consul_version_file)" == "$consul_version" ]]; then
  echo "skipping consul install. remove $consul_version_file to force re-install."
else
  rm -rf $consul_dist
  mkdir -p $consul_dist
  download_url=https://releases.hashicorp.com/consul
  (cd $consul_dist && \
    wget ${download_url}/${consul_version}/consul_${consul_version}_linux_amd64.zip && \
    unzip consul_${consul_version}_linux_amd64.zip)
  [ $? -eq 0 ] || fail "consul download failed"
  echo "$consul_version" > $consul_version_file
fi
ln -snf $consul_dist/consul $VTROOT/bin/consul

# install gRPC C++ base, so we can install the python adapters.
# this also installs protobufs
grpc_dist=$VTROOT/dist/grpc
grpc_ver=v1.0.0
if [ $SKIP_ROOT_INSTALLS == "True" ]; then
  echo "skipping grpc build, as root version was already installed."
elif [[ -f $grpc_dist/.build_finished && "$(cat $grpc_dist/.build_finished)" == "$grpc_ver" ]]; then
  echo "skipping gRPC build. remove $grpc_dist to force rebuild."
else
  # unlink homebrew's protobuf, to be able to compile the downloaded protobuf package
  if [[ `uname -s` == "Darwin" && "$(brew list -1 | grep google-protobuf)" ]]; then
    brew unlink grpc/grpc/google-protobuf
  fi

  # protobuf used to be a separate package, now we use the gRPC one.
  rm -rf $VTROOT/dist/protobuf

  # Cleanup any existing data and re-create the directory.
  rm -rf $grpc_dist
  mkdir -p $grpc_dist

  ./travis/install_grpc.sh $grpc_dist || fail "gRPC build failed"
  echo "$grpc_ver" > $grpc_dist/.build_finished

  # link homebrew's protobuf back
  if [[ `uname -s` == "Darwin" && "$(brew list -1 | grep google-protobuf)" ]]; then
    brew link grpc/grpc/google-protobuf
  fi

  # Add newly installed Python code to PYTHONPATH such that other Python module
  # installations can reuse it. (Once bootstrap.sh has finished, run
  # source dev.env instead to set the correct PYTHONPATH.)
  export PYTHONPATH=$(prepend_path $PYTHONPATH $grpc_dist/usr/local/lib/python2.7/dist-packages)
fi

# Install third-party Go tools used as part of the development workflow.
#
# DO NOT ADD LIBRARY DEPENDENCIES HERE. Instead use govendor as described below.
#
# Note: We explicitly do not vendor the tools below because a) we want to stay
# their latest version and b) it's easier to "go install" them this way.
gotools=" \
       github.com/golang/lint/golint \
       github.com/golang/mock/mockgen \
       github.com/kardianos/govendor \
       golang.org/x/tools/cmd/goimports \
       golang.org/x/tools/cmd/goyacc \
       honnef.co/go/tools/cmd/unused \
"

# The cover tool needs to be installed into the Go toolchain, so it will fail
# if Go is installed somewhere that requires root access.
source tools/shell_functions.inc
if goversion_min 1.4; then
  gotools+=" golang.org/x/tools/cmd/cover"
else
  gotools+=" code.google.com/p/go.tools/cmd/cover"
fi

echo "Installing dev tools with 'go get'..."
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

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh
ln -snf $VTTOP/test/vthook-test_backup_error $VTROOT/vthook/test_backup_error
ln -snf $VTTOP/test/vthook-test_backup_transform $VTROOT/vthook/test_backup_transform

# find mysql and prepare to use libmysqlclient
if [ -z "$MYSQL_FLAVOR" ]; then
  export MYSQL_FLAVOR=MySQL56
  echo "MYSQL_FLAVOR environment variable not set. Using default: $MYSQL_FLAVOR"
fi
case "$MYSQL_FLAVOR" in
  "MySQL56")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version`
    [[ "$myversion" =~ Distrib\ 5\.[67] ]] || fail "Couldn't find MySQL 5.6+ in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
    echo "Found MySQL 5.6+ installation in $VT_MYSQL_ROOT."
    ;;

  "MariaDB")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version`
    [[ "$myversion" =~ MariaDB ]] || fail "Couldn't find MariaDB in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
    echo "Found MariaDB installation in $VT_MYSQL_ROOT."
    ;;

  *)
    fail "Unsupported MYSQL_FLAVOR $MYSQL_FLAVOR"
    ;;

esac

# save the flavor that was used in bootstrap, so it can be restored
# every time dev.env is sourced.
echo "$MYSQL_FLAVOR" > $VTROOT/dist/MYSQL_FLAVOR

# generate pkg-config, so go can use mysql C client
[ -x $VT_MYSQL_ROOT/bin/mysql_config ] || fail "Cannot execute $VT_MYSQL_ROOT/bin/mysql_config. Did you install a client dev package?"

cp $VTTOP/config/gomysql.pc.tmpl $VTROOT/lib/gomysql.pc
myversion=`$VT_MYSQL_ROOT/bin/mysql_config --version`
echo "Version:" "$myversion" >> $VTROOT/lib/gomysql.pc
echo "Cflags:" "$($VT_MYSQL_ROOT/bin/mysql_config --cflags) -ggdb -fPIC" >> $VTROOT/lib/gomysql.pc
# Note we add $VT_MYSQL_ROOT/lib as an extra path in the case where
# we installed the standard MySQL packages from a distro into a sub-directory
# and the provided mysql_config assumes the <root>/lib directory
# is already in the library path.
if [[ "$MYSQL_FLAVOR" == "MariaDB" || "$myversion" =~ ^5\.7\. ]]; then
  # Use static linking because the shared library doesn't export
  # some internal functions we use, like cli_safe_read.
  echo "Libs:" "-L$VT_MYSQL_ROOT/lib $($VT_MYSQL_ROOT/bin/mysql_config --libs_r | sed -e 's,-lmysqlclient\(_r\)*,-l:libmysqlclient.a -lstdc++,')" >> $VTROOT/lib/gomysql.pc
else
  echo "Libs:" "-L$VT_MYSQL_ROOT/lib $($VT_MYSQL_ROOT/bin/mysql_config --libs_r)" >> $VTROOT/lib/gomysql.pc
fi

# install mock
mock_dist=$VTROOT/dist/py-mock-1.0.1
if [ -f $mock_dist/.build_finished ]; then
  echo "skipping mock python build"
else
  # Cleanup any existing data
  # (e.g. necessary for Travis CI caching which creates .build_finished as directory and prevents this script from creating it as file).
  rm -rf $mock_dist
  # For some reason, it seems like setuptools won't create directories even with the --prefix argument
  mkdir -p $mock_dist/lib/python2.7/site-packages
  export PYTHONPATH=$(prepend_path $PYTHONPATH $mock_dist/lib/python2.7/site-packages)
  cd $VTTOP/third_party/py && \
    tar -xzf mock-1.0.1.tar.gz && \
    cd mock-1.0.1 && \
    $PYTHON ./setup.py install --prefix=$mock_dist && \
    touch $mock_dist/.build_finished && \
    cd .. && \
    rm -r mock-1.0.1
fi

# Create the Git hooks.
echo "creating git hooks"
mkdir -p $VTTOP/.git/hooks
ln -sf $VTTOP/misc/git/pre-commit $VTTOP/.git/hooks/pre-commit
ln -sf $VTTOP/misc/git/prepare-commit-msg.bugnumber $VTTOP/.git/hooks/prepare-commit-msg
ln -sf $VTTOP/misc/git/commit-msg.bugnumber $VTTOP/.git/hooks/commit-msg
(cd $VTTOP && git config core.hooksPath $VTTOP/.git/hooks)

# Download chromedriver
echo "Installing selenium and chromedriver"
selenium_dist=$VTROOT/dist/selenium
mkdir -p $selenium_dist
$VIRTUALENV $selenium_dist
PIP=$selenium_dist/bin/pip
$PIP install selenium
mkdir -p $VTROOT/dist/chromedriver
curl -sL http://chromedriver.storage.googleapis.com/2.25/chromedriver_linux64.zip > chromedriver_linux64.zip
unzip -o -q chromedriver_linux64.zip -d $VTROOT/dist/chromedriver
rm chromedriver_linux64.zip

echo
echo "bootstrap finished - run 'source dev.env' in your shell before building."
