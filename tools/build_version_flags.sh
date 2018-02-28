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

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/shell_functions.inc

echo "\
  -X 'vitess.io/vitess/go/vt/servenv.buildHost=$(hostname)' \
  -X 'vitess.io/vitess/go/vt/servenv.buildUser=$(whoami)' \
  -X 'vitess.io/vitess/go/vt/servenv.buildGitRev=$(git rev-parse --short HEAD)' \
  -X 'vitess.io/vitess/go/vt/servenv.buildGitBranch=$(git rev-parse --abbrev-ref HEAD)' \
  -X 'vitess.io/vitess/go/vt/servenv.buildTime=$(LC_ALL=C date)' \
  -X 'vitess.io/vitess/go/vt/servenv.jenkinsBuildNumberStr=${BUILD_NUMBER}' \
"
