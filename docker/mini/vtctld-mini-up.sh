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

# This is an example script that starts vtctld.

source ./env.sh

cell=${CELL:-'test'}
grpc_port=15999

echo "- Starting vtctld..."
# shellcheck disable=SC2086
vtctld \
 $TOPOLOGY_FLAGS \
 --disable-active-reparents \
 -cell $cell \
 -service-map 'grpc-vtctl' \
 -backup-storage-implementation file \
 -file-backup-storage-root $VTDATAROOT/backups \
 -log_dir $VTDATAROOT/tmp \
 -port $vtctld_web_port \
 -grpc-port $grpc_port \
 -pid-file $VTDATAROOT/tmp/vtctld.pid \
  > $VTDATAROOT/tmp/vtctld.out 2>&1 &
echo "+ started"
