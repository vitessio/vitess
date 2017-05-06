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

# This is an example script that runs vtworker.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting vtworker..."
exec $VTROOT/bin/vtworker \
  $TOPOLOGY_FLAGS \
  -cell test \
  -log_dir $VTDATAROOT/tmp \
  -alsologtostderr \
  -service_map=grpc-vtworker \
  -grpc_port 15033 \
  -port 15032 \
  -pid_file $VTDATAROOT/tmp/vtworker.pid \
  -use_v3_resharding_mode &
