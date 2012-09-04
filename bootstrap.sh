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

if [ ! -d $VTROOT/dist ]; then
  mkdir $VTROOT/dist
fi

if [ ! -d $VTROOT/bin ]; then
  mkdir $VTROOT/bin
fi

#install bson
bson_dist=$VTROOT/dist/py-vt-bson-0.3.2
if [ -d $bson_dist ]; then
  echo "skipping bson python build"
else
  cd $VTTOP/third_party/py/bson-0.3.2 && \
    python ./setup.py install --prefix=$bson_dist
fi

# FIXME: cbson doesn't work with streaming. Need to debug
# install cbson
#cbson_dist=$VTROOT/dist/py-cbson
#if [ -d $cbson_dist ]; then
#  echo "skipping cbson python build"
#else
#  cd $VTTOP/py/cbson && \
#    python ./setup.py install --prefix=$cbson_dist
#fi

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

echo "source dev.env in your shell to complete the setup."
