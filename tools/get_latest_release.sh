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

base_release_branch=$(echo "$1" | grep -E 'release-[0-9]*.0$')
if [ "$base_release_branch" != "" ]; then
  major_release=$(echo "$base_release_branch" | sed 's/release-*//' | sed 's/\.0//')
  target_release="release-$((${major_release}-1)).0"
else
  target_release="release-$(git show-ref --heads | grep -E 'refs/heads/release-[0-9]*\.0$' | sed 's/[a-z0-9]* refs\/heads\/release-//' | sort -nr | head -n1)"
fi

echo $target_release