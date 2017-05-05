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

flavor=$1

if [[ -z "$flavor" ]]; then
  echo "Flavor must be specified as first argument."
  exit 1
fi

if [[ ! -f bootstrap.sh ]]; then
  echo "This script should be run from the root of the Vitess source tree - e.g. ~/src/github.com/youtube/vitess"
  exit 1
fi

# To avoid AUFS permission issues, files must allow access by "other"
chmod -R o=g *

docker build --no-cache -f docker/bootstrap/Dockerfile.$flavor -t vitess/bootstrap:$flavor .
