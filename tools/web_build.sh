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

# This script does a minified production build of the vtctld UI.
# In production, the vtctld UI is hosted with [go.rice](https://github.com/GeertJohan/go.rice).
# All front-end assets must be built, minified, and embedded in the executable.
set -euo pipefail

web_dir="$VTROOT/web/vtctld2"
web_app_dir="$web_dir/app"
web_dist_dir="$web_dir/dist"
rice_dir="$VTROOT/go/vt/vtctld"

(cd "$web_dir" && npm run build:prod)
cp -f "$web_dir"/src/{favicon.ico,plotly-latest.min.js,primeui-ng-all.min.css} "$web_dist_dir"

# We could probably side-step this rm/cp step by configuring `ng build`
# to output to `dist/` or, alternatively, map ricebox to read from `dist/`
# instead of `app/` but... shrug!
rm -rf "$web_app_dir"
cp -r "$web_dist_dir" "$web_app_dir"

(cd "$rice_dir" && go run github.com/GeertJohan/go.rice/rice embed-go && go build .)
