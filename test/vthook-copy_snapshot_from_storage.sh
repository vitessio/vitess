#!/bin/bash

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This script is a hook used during tests to copy snasphots from the central
# storage place.

# find a suitable source, under VTDATAROOT/tmp
if [ "$VTDATAROOT" == "" ]; then
  VTDATAROOT=/vt
fi
SOURCE=$VTDATAROOT/tmp/snapshot-from-$SOURCE_TABLET_ALIAS-for-$KEYRANGE.tar

# no source -> can't copy it, exit
if ! test -e $SOURCE ; then
  exit 1
fi

# and untar the directory from there to here
mkdir -p $SNAPSHOT_PATH || exit 1
cd $SNAPSHOT_PATH && tar xvf $SOURCE
exit $?
