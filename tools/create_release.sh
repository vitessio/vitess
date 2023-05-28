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

set -euo pipefail

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

if [ "$VTOP_VERSION" == "" ]; then
  echo "Warning: The VTOP_VERSION env var is not set, the Docker tag of the vitess-operator image will not be changed."
  echo -n "If you wish to continue anyhow press enter, otherwise CTRL+C to cancel."
  read line
fi

# Preparing and tagging the release
function createRelease () {
  # Unfreeze the branch
  sed -i.bak -E "s/exit (.*)/exit 0/g" ./.github/workflows/code_freeze.yml
  rm -f ./.github/workflows/code_freeze.yml.bak

  # Wait for release notes to be injected in the code base
  echo -n Pausing so relase notes can be added. Press enter to continue
  read line

  git add --all
  git commit -n -s -m "Release notes for $RELEASE_VERSION" || true

  # Preparing the release commit
  updateVitessExamples $RELEASE_VERSION $VTOP_VERSION
  updateJava $RELEASE_VERSION
  updateDockerReleaseScript $RELEASE_VERSION
  updateVersionGo $RELEASE_VERSION

  ## Create the commit for this release and tag it
  git add --all
  git commit -n -s -m "Release commit for $RELEASE_VERSION"
}

checkGitState

current_branch=""
checkoutNewBranch "create_release"

createRelease

echo " "
echo " "
echo "----------------"
echo "Release preparations successful."
echo " "
echo "One branch created: $current_branch"
echo " "
echo "Please push $current_branch to a remote. You will then create a Pull Request to merge into $BASE_REMOTE:$BASE_BRANCH."
echo " "
echo "   git push upstream $current_branch"
echo " "
echo " "
echo "Once pushed, please execute the following gh command to create the Pull Requests. Please replace 'USER_ON_WHICH_YOU_PUSHED' with the user/org on which you pushed the two branches."
echo " "
echo "   gh pr create -w --title 'Release of v$RELEASE_VERSION' --base $BASE_BRANCH --head USER_ON_WHICH_YOU_PUSHED:$current_branch --label 'Type: Release','Component: General','Do Not Merge' --body 'Includes the release notes and release commit for the v$RELEASE_VERSION release. Once this PR is merged, we will be able to tag v$RELEASE_VERSION on the merge commit.'"
echo " "
echo "----------------"
