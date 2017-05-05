#!/bin/bash

# Copyright 2017 Google Inc.
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

set -e

vtctld2_dir=$VTTOP/web/vtctld2
if [[ -d $vtctld2_dir/app ]]; then
  rm -rf $vtctld2_dir/app
fi
cd $vtctld2_dir && ng build -prod --output-path app/
rm -rf $vtctld2_dir/app/assets
cp -f $vtctld2_dir/src/{favicon.ico,plotly-latest.min.js,primeui-ng-all.min.css} $vtctld2_dir/app/
