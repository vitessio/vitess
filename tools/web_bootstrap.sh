#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

# This script is used to check prerequisites and install dependencies 
# for compiling and running the vtctld2 UI.

set -euo pipefail

web_dir="$VTROOT/web/vtctld2"

TARGET_NODE_VERSION="v8.0.0"
node_version=$(node -v)

TARGET_NPM_VERSION="5.0.0"
npm_version=$(npm -v)

if [[ -z ${node_version+x} || -z ${npm_version+x} ]]; then 
	echo "You must install node@${TARGET_NODE_VERSION} and npm@${TARGET_NPM_VERSION}."
	echo "Using a node versioning manager like nvm is strongly recommended: https://github.com/nvm-sh/nvm"
fi

if [[ $node_version != "$TARGET_NODE_VERSION" || $npm_version != "$TARGET_NPM_VERSION" ]]; then
	if [[ $node_version != "$TARGET_NODE_VERSION" ]]; then
		echo "node version does not match: version ${TARGET_NODE_VERSION} required, got ${node_version}."
	fi

	if [[ $npm_version != "$TARGET_NPM_VERSION" ]]; then
		echo "npm version does not match: version ${TARGET_NPM_VERSION} required, got ${npm_version}."
	fi

	echo "Using a node versioning manager like nvm is strongly recommended: https://github.com/nvm-sh/nvm"
	echo "If you already have nvm installed, check your versions with 'nvm list' and switch with 'nvm use 8.0.0'"
	exit 1
fi 

echo "⚠️  Warning! This project relies on very out-of-date node dependencies, many with significant security vulnerabilities. Install at your own risk."
echo "Installing node_modules... this might take awhile..."

# Unfortunately, since we're on such an old version of node, it is expected that we will see a LOT of
# deprecation, security, and possibly build warnings. It is also likely that the `npm install` step will fail,
# even if it installs "good enough" to do a build afterwards. So: don't fail this script if the npm install fails.
(cd "$web_dir" && npm install) || true
