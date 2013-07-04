#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This script is a sample hook to test the plumbing

EXIT_CODE=0

echo "TABLET_ALIAS:" $TABLET_ALIAS

for arg in $@ ; do
  echo "PARAM:" $arg
  if [ "$arg" == "--to-stderr" ]; then
    echo "ERR:" $arg 1>&2
  fi
  if [ "$arg" == "--exit-error" ]; then
    EXIT_CODE=1
  fi
done

exit $EXIT_CODE
