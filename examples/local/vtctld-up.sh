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

# This is an example script that starts vtctld.

set -e

cell='test'
grpc_port=15999

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting vtctld..."
$VTROOT/bin/vtctld \
  $TOPOLOGY_FLAGS \
  -cell $cell \
  -web_dir $VTTOP/web/vtctld \
  -web_dir2 $VTTOP/web/vtctld2/app \
  -workflow_manager_init \
  -workflow_manager_use_election \
  -service_map 'grpc-vtctl' \
  -backup_storage_implementation file \
  -file_backup_storage_root $VTDATAROOT/backups \
  -log_dir $VTDATAROOT/tmp \
  -port $vtctld_web_port \
  -grpc_port $grpc_port \
  -pid_file $VTDATAROOT/tmp/vtctld.pid \
  > $VTDATAROOT/tmp/vtctld.out 2>&1 &
disown -a

echo "Access vtctld web UI at http://$hostname:$vtctld_web_port"
echo "Send commands with: vtctlclient -server $hostname:$grpc_port ..."
