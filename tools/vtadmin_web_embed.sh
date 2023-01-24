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

set -ex

# This script builds the front-end into a set of static files,
# which are then placed in the same folder as vtadmin-api's "web" package,
# to be embedded as part of the final "vtadmin" binary.

web_root="web/vtadmin"

# Initially build into the web/vtadmin/build directory since
# - it simplifies react-snap configuration
# - it acts as a temporary/working (gitignored) directory for react-snap
build_folder="build"
build_path="$web_root/$build_folder"

# Clear out any existing build, since react-snap requires a clean directory.
rm -rf "$build_path"

# The destination directory of the build must be in the same package directory
# (or subdirectory) as the file with the "//go:embed" directive.
dest_path="go/vt/vtadmin/web/build"

BUILD_PATH="$build_folder" \
    REACT_APP_VTADMIN_API_ADDRESS="" \
    npm --prefix "$web_root" run build

# Overwrite the files we just built in build/ with the react-snap output.
npm --prefix "$web_root" run build:embed

rm -rf "$dest_path"
mv "$build_path" "$dest_path"
rm -rf "$build_path"
