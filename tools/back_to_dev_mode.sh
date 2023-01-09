#!/bin/bash

# Copyright 2023 The Vitess Authors.
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

function updateJava () {
  cd $ROOT/java || exit 1
  mvn versions:set -DnewVersion=$1
}

# First argument is the Release Version the docker release script should be set to (for instance: v15.0.0)
function updateDockerReleaseScript () {
  sed -i.bak -E "s/vt_base_version=.*/vt_base_version='$1'/g" $ROOT/docker/release.sh
  rm -f $ROOT/docker/release.sh.bak
}

function updateVersionGo () {

  cat << EOF > ${ROOT}/go/vt/servenv/version.go
/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

// THIS FILE IS AUTO-GENERATED DURING NEW RELEASES BY ./tools/do_releases.sh
// DO NOT EDIT

const versionName = "${1}"
EOF

}

# Putting the branch back into dev mode
function doBackToDevMode () {
  checkoutNewBranch "back-to-dev"

  # Preparing the "dev mode" commit
  updateJava $DEV_VERSION
  updateDockerReleaseScript $DEV_VERSION
  updateVersionGo $DEV_VERSION

  git add --all
  git commit -n -s -m "Back to dev mode"

  push_branches+=($current_branch)
}

git_status_output=$(git status --porcelain)
if [ "$git_status_output" == "" ]; then
  	echo so much clean
else
    echo "cannot do release with dirty git state"
    exit 1
fi

push_branches=()
current_branch=""

doBackToDevMode
