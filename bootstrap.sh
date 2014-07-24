#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

if [ ! -f bootstrap.sh ]; then
  echo "bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

. ./dev.env

mkdir -p $VTROOT/dist
mkdir -p $VTROOT/bin
mkdir -p $VTROOT/lib
mkdir -p $VTROOT/vthook

# install zookeeper
zk_dist=$VTROOT/dist/vt-zookeeper-3.3.5
if [ -d $zk_dist ]; then
  echo "skipping zookeeper build"
else
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
fi

ln -nfs $VTTOP/third_party/go/launchpad.net $VTROOT/src
go install launchpad.net/gozk/zookeeper

go get code.google.com/p/goprotobuf/proto
go get code.google.com/p/go.tools/cmd/goimports
go get github.com/golang/glog

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-copy_snapshot_from_storage.sh $VTROOT/vthook/copy_snapshot_from_storage
ln -snf $VTTOP/test/vthook-copy_snapshot_to_storage.sh $VTROOT/vthook/copy_snapshot_to_storage
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

# install mysql
if [ -d $VTROOT/dist/mysql ];
then
  echo "skipping MySQL build"
else
  export VT_MYSQL_ROOT=$VTROOT/dist/mysql

  case "$MYSQL_FLAVOR" in
    "MariaDB")
      echo "Getting and compiling MariaDB"
      # MariaDB may require cmake and libaio-dev to compile, I ran:
      # sudo apt-get install cmake
      # sudo apt-get install libaio-dev
      echo "Downloading and compiling MariaDB"
      pushd third_party
      version="10.0.10"
      wget https://downloads.mariadb.org/interstitial/mariadb-${version}/kvm-tarbake-jaunty-x86/mariadb-${version}.tar.gz
      tar xzf mariadb-${version}.tar.gz
      popd

      pushd third_party/mariadb-${version}
      cmake . -DBUILD_CONFIG=mysql_release -DCMAKE_INSTALL_PREFIX=$VTROOT/dist/mysql
      make -j 8 install
      rm -rf $VTROOT/dist/mysql/mysql-test
      rm -rf $VTROOT/dist/mysql/sql-bench
      popd
      rm -rf third_party/mariadb-${version}.tar.gz third_party/mariadb-${version}
      ;;

    *)
      echo "Getting and compiling Google MySQL"
      git clone https://code.google.com/r/sougou-vitess-mysql/ third_party/mysql
      pushd third_party/mysql
      set -e
      git apply ../mysql.patch
      source google/env.inc
      source google/compile.inc

      # Install
      make -s install #DESTDIR=$VTROOT/dist/mysql
      rm -rf $VTROOT/dist/mysql/mysql-test
      rm -rf $VTROOT/dist/mysql/sql-bench
      popd
      rm -rf third_party/mysql
      ;;
  esac
fi

# generate pkg-config, so go can use mysql C client
if [ ! -x $VT_MYSQL_ROOT/bin/mysql_config ]; then
  echo "cannot execute $VT_MYSQL_ROOT/bin/mysql_config, exiting" 1>&2
  exit 1
fi

cp $VTTOP/config/gomysql.pc.tmpl $VTROOT/lib/gomysql.pc
echo "Version:" "$($VT_MYSQL_ROOT/bin/mysql_config --version)" >> $VTROOT/lib/gomysql.pc
echo "Cflags:" "$($VT_MYSQL_ROOT/bin/mysql_config --cflags) -ggdb -fPIC" >> $VTROOT/lib/gomysql.pc
echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r)" >> $VTROOT/lib/gomysql.pc

# install bson
bson_dist=$VTROOT/dist/py-vt-bson-0.3.2
if [ -d $bson_dist ]; then
  echo "skipping bson python build"
else
  cd $VTTOP/third_party/py/bson-0.3.2 && \
    python ./setup.py install --prefix=$bson_dist && \
    rm -r build
fi

# install cbson
cbson_dist=$VTROOT/dist/py-cbson
if [ -d $cbson_dist ]; then
  echo "skipping cbson python build"
else
  cd $VTTOP/py/cbson && \
    python ./setup.py install --prefix=$cbson_dist
fi

# create pre-commit hooks
echo "creating git pre-commit hooks"
ln -sf $VTTOP/misc/git/pre-commit $VTTOP/.git/hooks/pre-commit
echo "source dev.env in your shell to complete the setup."
