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

# github.base_ref $1

target_release=""

base_release_branch=$(echo "$1" | grep -E 'release-[0-9]*.0$')
if [ "$base_release_branch" == "" ]; then
  base_release_branch=$(echo "$2" | grep -E 'release-[0-9]*.0$')
fi
if [ "$base_release_branch" != "" ]; then
  latest_major_release=$(git show-ref | grep -E 'refs/remotes/origin/release-[0-9]*\.0$' | sed 's/[a-z0-9]* refs\/remotes\/origin\/release-//' | sed 's/\.0//' | sort -nr | head -n1)
  major_release=$(echo "$base_release_branch" | sed 's/release-*//' | sed 's/\.0//')
  target_major_release=$((major_release+1))
  target_release="release-$target_major_release.0"
  if [ "$major_release" == "$latest_major_release" ]; then
    target_release="main"
  fi
fi

echo "$target_release"