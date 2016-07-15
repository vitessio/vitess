# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Library of functions which are used by bootstrap.sh or the Makefile.

# goversion_min returns true if major.minor go version is at least some value.
function goversion_min() {
  [[ "$(go version)" =~ go([0-9]+)\.([0-9]+) ]]
  gotmajor=${BASH_REMATCH[1]}
  gotminor=${BASH_REMATCH[2]}
  [[ "$1" =~ ([0-9]+)\.([0-9]+) ]]
  wantmajor=${BASH_REMATCH[1]}
  wantminor=${BASH_REMATCH[2]}
  [ "$gotmajor" -lt "$wantmajor" ] && return 1
  [ "$gotmajor" -gt "$wantmajor" ] && return 0
  [ "$gotminor" -lt "$wantminor" ] && return 1
  return 0
}

# prepend_path returns $2 prepended the colon separated path $1.
# If it's already part of the path, it won't be added again.
#
# Note the first time it's called, the original value is empty,
# and the second value has the path to add. We just end up adding it regardless
# of its existence.
function prepend_path() {
  # $1 path variable
  # $2 path to add
  if [ -d "$2" ] && [[ ":$1:" != *":$2:"* ]]; then
    echo "$2:$1"
  else
    echo "$1"
  fi
}
