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

# This script returns the latest stable release tag for the previous major
# version of Vitess. For example, if the current branch is release-23.0,
# this script returns the latest v22.0.x tag (e.g., v22.0.4).
#
# github.base_ref $1
# github.ref $2

set -euo pipefail

target_major_release=""

base_release_branch=$(echo "$1" | grep -E 'release-[0-9]*.0$' || true)
if [ "$base_release_branch" == "" ]; then
  base_release_branch=$(echo "$2" | grep -E 'release-[0-9]*.0$' || true)
fi
if [ "$base_release_branch" != "" ]; then
  major_release=$(echo "$base_release_branch" | sed 's/release-*//' | sed 's/\.0//')
  target_major_release=$((major_release-1))
else
  target_major_release=$(gh release list --repo vitessio/vitess --limit 100 --json tagName,isPrerelease \
    --jq '[.[] | select(.isPrerelease == false) | .tagName | capture("v(?<major>[0-9]+)\\.") | .major | tonumber] | max')
fi

# Find the latest stable (non-prerelease) release tag for the target major version.
release_tag=$(gh release list --repo vitessio/vitess --limit 100 --json tagName,isPrerelease \
  --jq "[.[] | select(.isPrerelease == false) | select(.tagName | startswith(\"v${target_major_release}.0.\")) | .tagName][0]")

if [ -z "$release_tag" ]; then
  echo "ERROR: No stable release found for major version ${target_major_release}" >&2
  exit 1
fi

echo "$release_tag"
