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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

cell=${CELL:-'test'}
grpc_port=15999

echo "Starting vtctld..."
# shellcheck disable=SC2086
#TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
vtctld \
 $TOPOLOGY_FLAGS \
 --cell $cell \
 --service-map 'grpc-vtctl,grpc-vtctld' \
 --backup-storage-implementation file \
 --file-backup-storage-root $VTDATAROOT/backups \
 --log_dir $VTDATAROOT/tmp \
 --port $vtctld_web_port \
 --grpc-port $grpc_port \
 --pid-file $VTDATAROOT/tmp/vtctld.pid \
 --pprof-http \
  > $VTDATAROOT/tmp/vtctld.out 2>&1 &

echo "Curling \"http://${hostname}:${vtctld_web_port}/debug/status\" to check if vtctld is up"

for _ in {0..300}; do
 curl -I "http://${hostname}:${vtctld_web_port}/debug/status" &>/dev/null && break
 sleep 0.1
done

# check one last time
curl -I "http://${hostname}:${vtctld_web_port}/debug/status" &>/dev/null || fail "vtctld could not be started!"

echo -e "vtctld is running!"
