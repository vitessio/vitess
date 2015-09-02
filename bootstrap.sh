#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

SKIP_ROOT_INSTALLS=False
if [ "$1" = "--skip_root_installs" ]; then
  SKIP_ROOT_INSTALLS=True
fi

if [ ! -f bootstrap.sh ]; then
  echo "bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

if [ "$USER" == "root" ]; then
  echo "Vitess cannot run as root. Please bootstrap with a non-root user."
  exit 1
fi

go version 2>&1 >/dev/null
if [ $? != 0 ]; then
    echo "Go is not installed or is not on \$PATH"
    exit 1
fi

. ./dev.env

mkdir -p $VTROOT/dist
mkdir -p $VTROOT/bin
mkdir -p $VTROOT/lib
mkdir -p $VTROOT/vthook

# install zookeeper
zk_dist=$VTROOT/dist/vt-zookeeper-3.3.5
if [ -f $zk_dist/.build_finished ]; then
  echo "skipping zookeeper build. remove $zk_dist to force rebuild."
else
  rm -rf $zk_dist
  (cd $VTTOP/third_party/zookeeper && \
    tar -xjf zookeeper-3.3.5.tbz && \
    mkdir -p $zk_dist/lib && \
    cp zookeeper-3.3.5/contrib/fatjar/zookeeper-3.3.5-fatjar.jar $zk_dist/lib && \
    (cd zookeeper-3.3.5/src/c && \
    ./configure --prefix=$zk_dist && \
    make -j3 install) && rm -rf zookeeper-3.3.5)
  if [ $? -ne 0 ]; then
    echo "zookeeper build failed"
    exit 1
  fi
  touch $zk_dist/.build_finished
fi

# install protoc and proto python libraries
protobuf_dist=$VTROOT/dist/protobuf
if [ $SKIP_ROOT_INSTALLS == "True" ]; then
  echo "skipping protobuf build, as root version was already installed."
elif [ -f $protobuf_dist/.build_finished ]; then
  echo "skipping protobuf build. remove $protobuf_dist to force rebuild."
else
  rm -rf $protobuf_dist
  mkdir -p $protobuf_dist/lib/python2.7/site-packages
  # The directory may not have existed yet, so it may not have been
  # picked up by dev.env yet, but the install needs it to exist first,
  # and be in PYTHONPATH.
  export PYTHONPATH=$(prepend_path $PYTHONPATH $protobuf_dist/lib/python2.7/site-packages)
  ./travis/install_protobuf.sh $protobuf_dist
  if [ $? -ne 0 ]; then
    echo "protobuf build failed"
    exit 1
  fi
  touch $protobuf_dist/.build_finished
fi

# install gRPC C++ base, so we can install the python adapters
grpc_dist=$VTROOT/dist/grpc
if [ $SKIP_ROOT_INSTALLS == "True" ]; then
  echo "skipping grpc build, as root version was already installed."
elif [ -f $grpc_dist/.build_finished ]; then
  echo "skipping gRPC build. remove $grpc_dist to force rebuild."
else
  rm -rf $grpc_dist
  mkdir -p $grpc_dist
  ./travis/install_grpc.sh $grpc_dist
  if [ $? -ne 0 ]; then
    echo "gRPC build failed"
    exit 1
  fi
  touch $grpc_dist/.build_finished
fi

ln -nfs $VTTOP/third_party/go/launchpad.net $VTROOT/src
go install launchpad.net/gozk/zookeeper

# Download third-party Go libraries.
# (We use one go get command (and therefore one variable) for all repositories because this saves us several seconds of execution time.)
repos="github.com/golang/glog \
       github.com/golang/lint/golint \
       github.com/golang/protobuf/proto \
       github.com/golang/protobuf/protoc-gen-go \
       github.com/tools/godep \
       golang.org/x/net/context \
       golang.org/x/tools/cmd/goimports \
       google.golang.org/grpc \
"

# Packages for uploading code coverage to coveralls.io (used by Travis CI).
# TODO(mberlin): Replace "deplist" fork with the original repo "github.com/cespare/deplist" when our changes are merged there.
repos+=" github.com/axw/gocov/gocov github.com/mattn/goveralls github.com/michael-berlin/deplist"
# The cover tool needs to be installed into the Go toolchain, so it will fail
# if Go is installed somewhere that requires root access.
source tools/shell_functions.inc
if goversion_min 1.4; then
  repos+=" golang.org/x/tools/cmd/cover"
else
  repos+=" code.google.com/p/go.tools/cmd/cover"
fi

go get -u $repos

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

# install mysql
if [ -z "$MYSQL_FLAVOR" ]; then
  export MYSQL_FLAVOR=MariaDB
fi
case "$MYSQL_FLAVOR" in
  "MySQL56")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version | grep 'Distrib 5\.6'`
    if [ "$myversion" == "" ]; then
      echo "Couldn't find MySQL 5.6 in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
      exit 1
    fi
    echo "Found MySQL 5.6 installation in $VT_MYSQL_ROOT."
    ;;

  "MariaDB")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version | grep MariaDB`
    if [ "$myversion" == "" ]; then
      echo "Couldn't find MariaDB in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
      exit 1
    fi
    echo "Found MariaDB installation in $VT_MYSQL_ROOT."
    ;;

  *)
    echo "Unsupported MYSQL_FLAVOR $MYSQL_FLAVOR"
    exit 1
    ;;

esac

# save the flavor that was used in bootstrap, so it can be restored
# every time dev.env is sourced.
echo "$MYSQL_FLAVOR" > $VTROOT/dist/MYSQL_FLAVOR

# generate pkg-config, so go can use mysql C client
if [ ! -x $VT_MYSQL_ROOT/bin/mysql_config ]; then
  echo "Cannot execute $VT_MYSQL_ROOT/bin/mysql_config. Did you install a client dev package?" 1>&2
  exit 1
fi

cp $VTTOP/config/gomysql.pc.tmpl $VTROOT/lib/gomysql.pc
echo "Version:" "$($VT_MYSQL_ROOT/bin/mysql_config --version)" >> $VTROOT/lib/gomysql.pc
echo "Cflags:" "$($VT_MYSQL_ROOT/bin/mysql_config --cflags) -ggdb -fPIC" >> $VTROOT/lib/gomysql.pc
if [ "$MYSQL_FLAVOR" == "MariaDB" ]; then
  # Use static linking because the shared library doesn't export
  # some internal functions we use, like cli_safe_read.
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r | sed 's,-lmysqlclient_r,-l:libmysqlclient.a -lstdc++,')" >> $VTROOT/lib/gomysql.pc
else
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r)" >> $VTROOT/lib/gomysql.pc
fi

# install bson
bson_dist=$VTROOT/dist/py-vt-bson-0.3.2
if [ -f $bson_dist/lib/python2.7/site-packages/bson/__init__.py ]; then
  echo "skipping bson python build"
else
  cd $VTTOP/third_party/py/bson-0.3.2 && \
    python ./setup.py install --prefix=$bson_dist && \
    rm -r build
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

# install cbson
cbson_dist=$VTROOT/dist/py-cbson
if [ -f $cbson_dist/lib/python2.7/site-packages/cbson.so ]; then
  echo "skipping cbson python build"
else
  cd $VTTOP/py/cbson && \
    python ./setup.py install --prefix=$cbson_dist
fi

# create pre-commit hooks
echo "creating git pre-commit hooks"
ln -sf $VTTOP/misc/git/pre-commit $VTTOP/.git/hooks/pre-commit

echo
echo "bootstrap finished - run 'source dev.env' in your shell before building."

