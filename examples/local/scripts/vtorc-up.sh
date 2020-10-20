#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

source ./env.sh

port=3000
echo "Starting vtorc..."
vtorc \
 $TOPOLOGY_FLAGS \
 -config=vtorc-config.json \
 -log_dir=$VTDATAROOT \
 -alsologtostderr \
 http \
 > $VTDATAROOT/vtorc.out 2>&1 &

# Block waiting for orchestrator to be listening
# Not the same as healthy

for i in $(seq 0 300); do
 curl -I "http://$hostname:$port/api/status" >/dev/null 2>&1 && break
 sleep 0.1
done

# check one last time
curl -I "http://$hostname:$port/api/status" || fail "vtorc could not be started!"
