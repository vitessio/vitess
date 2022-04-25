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

set -e

# This script builds the front-end into a set of static files,
# which are then placed in the same folder as vtadmin-api's "web" package,
# to be embedded as part of the final "vtadmin" binary.

# TODO check node version
# TODO check npm install
# TODO check that this script can run from anywhere

script_root=$(dirname "${BASH_SOURCE[0]}")
web_root="$script_root/.."

# Initially build into the local web/vtadmin/build/ directory,
# which simplifies the react-snap postbuild step by not requiring
# a separate "reactSnap.source" config in the package.json".
build_path="$web_root/build"

# Clear out any existing build, since react-snap requires a clean directory.
rm -rf "$build_path"

# The destination directory of the build must be in the same package directory
# (or subdirectory) as the file with the "//go:embed" directive.
dest_path="$script_root/../../../go/vt/vtadmin/web/build"

BUILD_PATH="$build_path" \
    REACT_APP_VTADMIN_API_ADDRESS="" \
    npm --prefix "$web_root" run build

rm -rf "$dest_path"
mv "$build_path" "$dest_path"
