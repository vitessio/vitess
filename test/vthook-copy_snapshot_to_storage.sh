#!/bin/bash

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This script is a hook used during tests to copy snasphots to a fake
# central storage place.

# find a suitable destination, under VTDATAROOT/tmp
if [ "$VTDATAROOT" == "" ]; then
  VTDATAROOT=/vt
fi
DESTINATION=$VTDATAROOT/tmp/snapshot-from-$TABLET_ALIAS-for-$KEYRANGE.tar

# and tar the directory to there
cd $SNAPSHOT_PATH && tar cvf $DESTINATION *
exit $?
