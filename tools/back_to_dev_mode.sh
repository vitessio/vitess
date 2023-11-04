#!/bin/bash

set -euo pipefail

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

source ./build.env
source ./tools/release_utils.sh

ROOT=$(pwd)
if [ "$VTROOT" != "" ]; then
    ROOT=$VTROOT
fi

if [ "$BASE_REMOTE" == "" ]; then
  echo "Set the env var BASE_REMOTE with the name of the remote on which the release branch is located."
  exit 1
fi

if [ "$BASE_BRANCH" == "" ]; then
  echo "Set the env var BASE_BRANCH with the name of the branch on which the release will take place."
  exit 1
fi

if [ "$RELEASE_VERSION" == "" ]; then
  echo "Set the env var RELEASE_VERSION with the release version"
  exit 1
fi

if [ "$DEV_VERSION" == "" ]; then
  echo "Set the env var DEV_VERSION with the next release version of this branch"
  exit 1
fi

# Putting the branch back into dev mode
function doBackToDevMode () {
  # Preparing the "dev mode" commit
  updateJava $DEV_VERSION
  updateDockerReleaseScript $DEV_VERSION
  updateVersionGo $DEV_VERSION

  git add --all
  git commit -n -s -m "Back to dev mode"
}

checkGitState

current_branch=""
checkoutNewBranch "back_to_dev"

doBackToDevMode

echo " "
echo " "
echo "----------------"
echo "Back-to-dev mode successful."
echo " "
echo "One branch created: $current_branch"
echo " "
echo "Please push $current_branch to a remote. You will then create a Pull Request to merge into $BASE_REMOTE:$BASE_BRANCH."
echo " "
echo "   git push upstream $current_branch"
echo " "
echo " "
echo "Once pushed, please execute the following gh command to create the Pull Requests. Please replace 'USER_ON_WHICH_YOU_PUSHED' with the user/org on which you pushed the branch."
echo " "
echo "   gh pr create -w --title 'Back to dev mode after v$RELEASE_VERSION' --base $BASE_BRANCH --head USER_ON_WHICH_YOU_PUSHED:$current_branch --label 'Type: Release','Component: General' --body 'Includes the changes required to go back into dev mode (v$DEV_VERSION) after the release of v$RELEASE_VERSION.'"
echo " "
echo "----------------"