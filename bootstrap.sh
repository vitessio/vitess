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

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/cmd/mysqlctl/mysqlctl $VTROOT/bin/mysqlctl
ln -snf $VTTOP/go/cmd/normalizer/normalizer $VTROOT/bin/normalizer
ln -snf $VTTOP/go/cmd/vtaction/vtaction $VTROOT/bin/vtaction
ln -snf $VTTOP/go/cmd/vtclient2/vtclient2 $VTROOT/bin/vtclient2
ln -snf $VTTOP/go/cmd/vtctl/vtctl $VTROOT/bin/vtctl
ln -snf $VTTOP/go/cmd/vtocc/vtocc $VTROOT/bin/vtocc
ln -snf $VTTOP/go/cmd/vttablet/vttablet $VTROOT/bin/vttablet
ln -snf $VTTOP/go/cmd/zk/zk $VTROOT/bin/zk
ln -snf $VTTOP/go/cmd/zkctl/zkctl $VTROOT/bin/zkctl
ln -snf $VTTOP/go/cmd/zkocc/zkocc $VTROOT/bin/zkocc
ln -snf $VTTOP/go/cmd/zkclient2/zkclient2 $VTROOT/bin/zkclient2
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

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
