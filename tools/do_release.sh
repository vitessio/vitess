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

ROOT=$(pwd)
if [ "$VTROOT" != "" ]; then
    ROOT=$VTROOT
fi

if [ "$RELEASE_VERSION" == "" ]; then
  echo "Set the env var RELEASE_VERSION with the release version"
  exit 1
fi

if [ "$DEV_VERSION" == "" ]; then
  echo "Set the env var DEV_VERSION with the version the dev branch should have after release"
  exit 1
fi

if [ "$VTOP_VERSION" == "" ]; then
  echo "Warning: The VTOP_VERSION env var is not set, the Docker tag of the vitess-operator image will not be changed."
  echo -n "If you wish to continue anyhow press enter, otherwise CTRL+C to cancel."
  read line
fi

if [ "$GODOC_RELEASE_VERSION" == "" ]; then
  echo "Warning: The GODOC_RELEASE_VERSION env var is not set, no go doc tag will be created."
  echo -n "If you wish to continue anyhow press enter, otherwise CTRL+C to cancel."
  read line
fi

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

function updateJava () {
  cd $ROOT/java || exit 1
  mvn versions:set -DnewVersion=$1
}

# First argument is the Release Version (for instance: v12.0.0)
# Second argument is the Vitess Operator version
function updateVitessExamples () {
  compose_example_files=$(find -E $ROOT/examples/compose/* -regex ".*.(go|yml)")
  compose_example_sub_files=$(find -E $ROOT/examples/compose/**/* -regex ".*.(go|yml)")
  vtop_example_files=$(find -E $ROOT/examples/operator -name "*.yaml")
  sed -i.bak -E "s/vitess\/lite:(.*)/vitess\/lite:v$1/g" $compose_example_files $compose_example_sub_files $vtop_example_files
  sed -i.bak -E "s/vitess\/lite:\${VITESS_TAG:-latest}/vitess\/lite:v$1/g" $compose_example_sub_files $vtop_example_files
  sed -i.bak -E "s/vitess\/lite:(.*)-mysql80/vitess\/lite:v$1-mysql80/g" $(find -E $ROOT/examples/operator -name "*.md")
  if [ "$2" != "" ]; then
  		sed -i.bak -E "s/planetscale\/vitess-operator:(.*)/planetscale\/vitess-operator:v$2/g" $vtop_example_files
  fi
  rm -f $(find -E $ROOT/examples/operator -regex ".*.(md|yaml).bak")
  rm -f $(find -E $ROOT/examples/compose/* -regex ".*.(go|yml).bak")
  rm -f $(find -E $ROOT/examples/compose/**/* -regex ".*.(go|yml).bak")
}

git_status_output=$(git status --porcelain)
if [ "$git_status_output" == "" ]; then
  	echo so much clean
else
    echo "cannot do release with dirty git state"
    exit 1
fi

# Preparing the release commit
updateVitessExamples $RELEASE_VERSION $VTOP_VERSION
updateJava $RELEASE_VERSION
updateVersionGo $RELEASE_VERSION

## Wait for release notes to be injected in the code base
echo -n Pausing so relase notes can be added. Press enter to continue
read line

## Create the commit for this release and tag it
git add --all
git commit -n -s -m "Release commit for $RELEASE_VERSION"
git tag -m Version\ $RELEASE_VERSION v$RELEASE_VERSION

## Also tag the commit with the GoDoc tag if needed
if [ "$GODOC_RELEASE_VERSION" != "" ]; then
    git tag -a v$GODOC_RELEASE_VERSION -m "Tagging $RELEASE_VERSION also as $GODOC_RELEASE_VERSION for godoc/go modules"
fi

# Preparing the "dev mode" commit
updateJava $DEV_VERSION
updateVersionGo $DEV_VERSION

git add --all
git commit -n -s -m "Back to dev mode"
echo "Release preparations successful"

if [ "$GODOC_RELEASE_VERSION" != "" ]; then
  echo "Two git tags were created, you can push them with:"
  echo "   git push upstream v$RELEASE_VERSION && git push upstream v$GODOC_RELEASE_VERSION"
else
  echo "One git tag was created, you can push it with:"
  echo "   git push upstream v$RELEASE_VERSION"
fi

echo "The git branch has also been updated. You need to push it and get it merged"
