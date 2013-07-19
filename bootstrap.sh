#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

if [ ! -f bootstrap.sh ]; then
  echo "bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

. ./dev.env

diff misc/hg/hooks.hgrc .hg/hgrc | grep -qle '<'
if [ $? != 1 ]; then
  echo "installing hgrc hooks"
  cat misc/hg/hooks.hgrc >> .hg/hgrc
fi

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

# FIXME(szopa): Get rid of this dependency.
# install opts-go
go get  code.google.com/p/opts-go

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/cmd/mysqlctl/mysqlctl $VTROOT/bin/mysqlctl
ln -snf $VTTOP/go/cmd/normalizer/normalizer $VTROOT/bin/normalizer
ln -snf $VTTOP/go/cmd/vtaction/vtaction $VTROOT/bin/vtaction
ln -snf $VTTOP/go/cmd/vtclient2/vtclient2 $VTROOT/bin/vtclient2
ln -snf $VTTOP/go/cmd/vtctl/vtctl $VTROOT/bin/vtctl
ln -snf $VTTOP/go/cmd/vtctld/vtctld $VTROOT/bin/vtctld
ln -snf $VTTOP/go/cmd/vtocc/vtocc $VTROOT/bin/vtocc
ln -snf $VTTOP/go/cmd/vttablet/vttablet $VTROOT/bin/vttablet
ln -snf $VTTOP/go/cmd/zk/zk $VTROOT/bin/zk
ln -snf $VTTOP/go/cmd/zkctl/zkctl $VTROOT/bin/zkctl
ln -snf $VTTOP/go/cmd/zkocc/zkocc $VTROOT/bin/zkocc
ln -snf $VTTOP/go/cmd/zkclient2/zkclient2 $VTROOT/bin/zkclient2
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

# install mysql
if [ -d $VTROOT/dist/mysql ];
then
  echo "skipping MySQL build"
else
  git clone https://code.google.com/p/google-mysql/ third_party/mysql
  pushd third_party/mysql
  cp client/mysqlbinlog.cc client/vt_mysqlbinlog.cc
  git apply ../mysql.patch
  set -e
  enable_minimal="--enable-minimal"
  source google/env.inc
  source ../mysql-compile.inc 2>&1 | tee log

  VT_MYSQL_ROOT=$VTROOT/dist/mysql

# Install
  make -s install #DESTDIR=$VTROOT/dist/mysql
  rm -rf $VTROOT/dist/mysql/mysql-test && \
    rm -rf $VTROOT/dist/mysql/sql-bench
  popd
  rm -rf third_party/mysql
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
    python ./setup.py install --prefix=$bson_dist
fi

# install cbson
cbson_dist=$VTROOT/dist/py-cbson
if [ -d $cbson_dist ]; then
  echo "skipping cbson python build"
else
  cd $VTTOP/py/cbson && \
    python ./setup.py install --prefix=$cbson_dist
fi


echo "source dev.env in your shell to complete the setup."
