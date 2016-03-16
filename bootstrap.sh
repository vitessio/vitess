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

function zk_patch_mac() {
  if [ `uname -s` == "Darwin" ]; then
    cd zookeeper-$zk_ver && \
    wget https://issues.apache.org/jira/secure/attachment/12673210/ZOOKEEPER-2049.noprefix.branch-3.4.patch && \
    patch -p0 < ZOOKEEPER-2049.noprefix.branch-3.4.patch && \
    cd ..
  fi
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

# install zookeeper
zk_ver=3.4.6
zk_dist=$VTROOT/dist/vt-zookeeper-$zk_ver
if [ -f $zk_dist/.build_finished ]; then
  echo "skipping zookeeper build. remove $zk_dist to force rebuild."
else
  rm -rf $zk_dist
  (cd $VTROOT/dist && \
    wget http://archive.apache.org/dist/zookeeper/zookeeper-$zk_ver/zookeeper-$zk_ver.tar.gz && \
    tar -xzf zookeeper-$zk_ver.tar.gz && \
    zk_patch_mac && \
    mkdir -p $zk_dist/lib && \
    cp zookeeper-$zk_ver/contrib/fatjar/zookeeper-$zk_ver-fatjar.jar $zk_dist/lib && \
    (cd zookeeper-$zk_ver/src/c && \
    ./configure --prefix=$zk_dist && \
    make install) && rm -rf zookeeper-$zk_ver zookeeper-$zk_ver.tar.gz)
  [ $? -eq 0 ] || fail "zookeeper build failed"
  touch $zk_dist/.build_finished
fi

# install gRPC C++ base, so we can install the python adapters.
# this also installs protobufs
grpc_dist=$VTROOT/dist/grpc
grpc_ver=release-0_13_0
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

ln -nfs $VTTOP/third_party/go/launchpad.net $VTROOT/src
go install launchpad.net/gozk/zookeeper

# Download third-party Go libraries.
# (We use one go get command (and therefore one variable) for all repositories because this saves us several seconds of execution time.)
repos="github.com/golang/glog \
       github.com/golang/lint/golint \
       github.com/golang/mock/gomock \
       github.com/golang/mock/mockgen \
       github.com/golang/protobuf/proto \
       github.com/golang/protobuf/protoc-gen-go \
       github.com/olekukonko/tablewriter \
       github.com/tools/godep \
       golang.org/x/crypto/ssh/terminal \
       golang.org/x/net/context \
       golang.org/x/oauth2/google \
       golang.org/x/tools/cmd/goimports \
       google.golang.org/cloud \
       google.golang.org/cloud/storage \
       google.golang.org/grpc \
"

# Packages for uploading code coverage to coveralls.io (used by Travis CI).
repos+=" github.com/modocache/gover github.com/mattn/goveralls"
# The cover tool needs to be installed into the Go toolchain, so it will fail
# if Go is installed somewhere that requires root access.
source tools/shell_functions.inc
if goversion_min 1.4; then
  repos+=" golang.org/x/tools/cmd/cover"
else
  repos+=" code.google.com/p/go.tools/cmd/cover"
fi

go get -u $repos || fail "Failed to download some Go dependencies with 'go get'. Please re-run bootstrap.sh in case of transient errors."

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

# find mysql and prepare to use libmysqlclient
if [ -z "$MYSQL_FLAVOR" ]; then
  export MYSQL_FLAVOR=MySQL56
  echo "MYSQL_FLAVOR environment variable not set. Using default: $MYSQL_FLAVOR"
fi
case "$MYSQL_FLAVOR" in
  "MySQL56")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version | grep 'Distrib 5\.6'`
    [ "$myversion" != "" ] || fail "Couldn't find MySQL 5.6 in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
    echo "Found MySQL 5.6 installation in $VT_MYSQL_ROOT."
    ;;

  "MariaDB")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version | grep MariaDB`
    [ "$myversion" != "" ] || fail "Couldn't find MariaDB in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
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
echo "Version:" "$($VT_MYSQL_ROOT/bin/mysql_config --version)" >> $VTROOT/lib/gomysql.pc
echo "Cflags:" "$($VT_MYSQL_ROOT/bin/mysql_config --cflags) -ggdb -fPIC" >> $VTROOT/lib/gomysql.pc
if [ "$MYSQL_FLAVOR" == "MariaDB" ]; then
  # Use static linking because the shared library doesn't export
  # some internal functions we use, like cli_safe_read.
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r | sed -r 's,-lmysqlclient(_r)?,-l:libmysqlclient.a -lstdc++,')" >> $VTROOT/lib/gomysql.pc
else
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r)" >> $VTROOT/lib/gomysql.pc
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
    python ./setup.py install --prefix=$mock_dist && \
    touch $mock_dist/.build_finished && \
    cd .. && \
    rm -r mock-1.0.1
fi

# create pre-commit hooks
echo "creating git pre-commit hooks"
mkdir -p $VTTOP/.git/hooks
ln -sf $VTTOP/misc/git/pre-commit $VTTOP/.git/hooks/pre-commit

echo
echo "bootstrap finished - run 'source dev.env' in your shell before building."
