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

set -e

tmpdir=`mktemp -d`

script="go get github.com/youtube/vitess/go/cmd/vtctlclient && \
  git clone https://github.com/github/orchestrator.git src/github.com/github/orchestrator && \
  go install github.com/github/orchestrator/go/cmd/orchestrator"

echo "Building orchestrator..."
docker run -ti --name=vt_orc_build golang:1.7 bash -c "$script"
docker cp vt_orc_build:/go/bin/orchestrator $tmpdir
docker cp vt_orc_build:/go/bin/vtctlclient $tmpdir
docker cp vt_orc_build:/go/src/github.com/github/orchestrator/resources $tmpdir
docker rm vt_orc_build

echo "Building Docker image..."
cp Dockerfile orchestrator.conf.json $tmpdir
(cd $tmpdir && docker build -t vitess/orchestrator .)

# Clean up
rm -r $tmpdir

