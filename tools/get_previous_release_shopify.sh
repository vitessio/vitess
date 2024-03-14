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

# This script is used to build and copy the Angular 2 based vtctld UI
# into the release folder (app) for checkin. Prior to running this script,
# bootstrap.sh and bootstrap_web.sh should already have been run.

# github.base_ref $1

target_release=""

major_release=$(cat ./go/vt/servenv/version.go| grep versionName | awk -v 'FS= ' '{print $4}' | tr -d '"' | sed 's/\..*//')
target_major_release=$((major_release-1))
target_release_number=$(git show-ref | grep -E 'refs/remotes/origin/v[0-9]*.[0-9]*.[0-9]*-shopify-[0-9]*$' | sed 's/[a-z0-9]* refs\/remotes\/origin\/v//' | awk -v FS=. -v RELEASE="$target_major_release" '{if ($1 == RELEASE) print; }' | sort -Vr | head -n1)
if [ ! -z "$target_release_number" ]
then
  target_release="v$target_release_number"
fi

echo "$target_release"
