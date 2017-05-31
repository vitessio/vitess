# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
