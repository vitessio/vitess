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

# This script generates JavaScript/TypeScript bindings from
# the Vitess .proto files.

set -e

vtadmin_root="$VTROOT/web/vtadmin"
vitess_proto_root="$VTROOT/proto"

pbjs_bin="$vtadmin_root/node_modules/.bin/pbjs"
pbts_bin="$vtadmin_root/node_modules/.bin/pbts"

proto_targets="vtadmin.proto"
output_filename="vtadmin"
output_dir="$vtadmin_root/src/proto"

mkdir -p "$output_dir"

$pbjs_bin \
	--keep-case \
	-p "$vitess_proto_root" \
	-t static-module \
	-w commonjs \
	-o "$output_dir/$output_filename.js" \
	"$proto_targets"

$pbts_bin \
 	-o "$output_dir/$output_filename.d.ts" \
 	"$output_dir/$output_filename.js"
