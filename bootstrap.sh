#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

if [ ! -f bootstrap.sh ]; then
  echo "bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

. ./dev.env

if [ ! -d $VTROOT/dist ]; then
  mkdir $VTROOT/dist
fi

#install bson
bson_dist=$VTROOT/dist/py-vt-bson-0.3.2
if [ -d $bson_dist ]; then
  echo "skipping bson python build"
else
  cd $VTTOP/third_party/py/bson-0.3.2 && \
    python ./setup.py install --prefix=$bson_dist
fi

#install vtdb
vtdb_dist=$VTROOT/dist/py-vtdb
if [ -d $vtdb_dist ]; then
  echo "skipping vtdb build"
else
  cd $VTTOP/py && \
    python ./setup.py install --prefix=$vtdb_dist
fi

echo "source dev.env in your shell to complete the setup."
