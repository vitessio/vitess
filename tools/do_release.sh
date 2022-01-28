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

if [ "$RELEASE_VERSION" == "" ]; then
		echo "Set the env var RELEASE_VERSION with the release version"
		exit 1
fi

if [ "$DEV_VERSION" == "" ]; then
		echo "Set the env var DEV_VERSION with the version the dev branch should have after release"
		exit 1
fi

git_status_output=$(git status --porcelain)
if [ "$git_status_output" == "" ]; then
  	echo so much clean
else
    echo cannot do release with dirty git state
    exit 1
    echo so much win
fi

cd java && mvn versions:set -DnewVersion=$RELEASE_VERSION
echo package servenv > go/vt/servenv/version.go
echo  >> go/vt/servenv/version.go
echo const versionName = \"$RELEASE_VERSION\" >> go/vt/servenv/version.go
echo -n Pausing so relase notes can be added. Press enter to continue
read line
git add --all
git commit -n -s -m "Release commit for $RELEASE_VERSION"
git tag -m Version\ $RELEASE_VERSION v$RELEASE_VERSION

if [ "$GODOC_RELEASE_VERSION" != "" ]; then
    git tag -a v$GODOC_RELEASE_VERSION -m "Tagging $RELEASE_VERSION also as $GODOC_RELEASE_VERSION for godoc/go modules"
fi

cd java && mvn versions:set -DnewVersion=$DEV_VERSION
echo package servenv > go/vt/servenv/version.go
echo  >> go/vt/servenv/version.go
echo const versionName = \"$DEV_VERSION)\" >> go/vt/servenv/version.go
git add --all
git commit -n -s -m "Back to dev mode"
echo "Release preparations successful"

if [ "$GODOC_RELEASE_VERSION" != "" ]; then
	echo "One git tag was created, you can push it with:"
	echo "   git push upstream v$RELEASE_VERSION"
else
  echo "Two git tags were created, you can push them with:"
  echo "   git push upstream v$RELEASE_VERSION && git push upstream v$GODOC_RELEASE_VERSION"
fi

echo "The git branch has also been updated. You need to push it and get it merged"
