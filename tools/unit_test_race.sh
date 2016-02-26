#!/bin/bash

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

temp_log_file="$(mktemp --suffix .unit_test_race.log)"
trap '[ -f "$temp_log_file" ] && rm $temp_log_file' EXIT

# Wrapper around go test -race.

# This script exists because the -race test doesn't allow to distinguish
# between a failed (e.g. flaky) unit test and a found data race.
# Although Go 1.5 says 'exit status 66' in case of a race, it exits with 1.
# Therefore, we manually check the output of 'go test' for data races and
# exit with an error if one was found.

# NOTE: Go binaries <1.5 had a bug which prevented go test -race from exiting
# with a non-zero code when a race is found.
# The fix for the bug seems to be: https://go-review.googlesource.com/#/c/4371/
# To work-around bugged go (<1.5) binaries, we enable "halt_on_error".
GORACE=halt_on_error=1 go test $VT_GO_PARALLEL -race ./go/vt/... 2>&1 | tee $temp_log_file
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  if grep "WARNING: DATA RACE" -q $temp_log_file; then
    echo
    echo "ERROR: go test -race found a data race. See log above."
    exit 2
  fi
fi

echo
echo "SUCCESS: No data race was found."
