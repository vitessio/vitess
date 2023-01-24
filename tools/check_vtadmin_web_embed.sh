#!/bin/bash

# Copyright 2022 The Vitess Authors.
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
#

set -e

# This script checks that:
#   1. The vtadmin-web front-end builds successfully
#   2. The build files committed to (and embedded by) the 'go/vt/vtadmin/web/**' directory 
#      match the build produced in CI.
#
# The goal is to ensure that the built/embedded files are always in sync
# with the source code; that is, to make sure developers that are updating 
# the front-end remember to run `make vtadmin_web_embed` and commit the result.
#
# This script is intended to be run in CI and makes the following assumptions:
#
#   1. The working directory is clean. This script uses git operations for diffing,
#      and allowing uncommitted changes to 'go/vt/vtadmin/web/**' would complicate this.
#
#   2. It is fine to overwrite (but not commit) the current build files in 'go/vt/vtadmin/web/**'.
#      This is fine in CI, but it might be annoying if you are running this script
#      on your development machine.
# 
#   3. Certain file paths are okay to ignore _for the purposes of this check_.
#      Some built files will produce a (functionally) meaningless diff every time
#      `make vtadmin_web_embed` is run. For example, CSS/JS source maps have values
#      that change on every build. Since a meaningful change
#      in a css.map file would be accompanied by a corresponding change in the .css source file, 
#      ignoring changes to _just_ the css.map files is acceptable for this check.
#

if [[ $(git status --porcelain) != '' ]]; then
  echo 'ERROR: Working directory is dirty.'
  exit 1
fi

# Build the front-end files, which overwrites both 'web/vtadmin/build'
# and 'go/vt/vtadmin/web/**'. See note #2 above.
make vtadmin_web_embed

# The file patterns prefixed with ":!" below are excluded from the diff.
# See note #3 above.
output=$(git status --porcelain . -- ':!*.css.map' ':!*.js.map')

if [[ $output != '' ]]; then
    echo 'ERROR: Working directory is dirty after build.'
    echo "$output"
    git diff
    echo 'Please run "make vtadmin_web_embed" and commit the results'
    exit 1
fi
