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

function checkGitState() {
  git_status_output=$(git status --porcelain)
  if [ "$git_status_output" == "" ]; then
    	echo so much clean
  else
      echo "cannot do release with dirty git state"
      exit 1
  fi
}

function updateDockerReleaseScript () {
  sed -i.bak -E "s/vt_base_version=.*/vt_base_version='$1'/g" $ROOT/docker/release.sh
  rm -f $ROOT/docker/release.sh.bak
}

function checkoutNewBranch () {
  branch_name=$1

  current_branch=$BASE_BRANCH-$branch_name-1

  failed=0
  git checkout -b $current_branch $BASE_REMOTE/$BASE_BRANCH || failed=1
  for (( i = 2; $failed != 0; i++ )); do
    failed=0
    current_branch=$BASE_BRANCH-$branch_name-$i
    git checkout -b $current_branch $BASE_REMOTE/$BASE_BRANCH || failed=1
  done
}

function updateVersionGo () {
  YEAR=$(date +'%Y')
  cat << EOF > ${ROOT}/go/vt/servenv/version.go
/*
Copyright $YEAR The Vitess Authors.

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