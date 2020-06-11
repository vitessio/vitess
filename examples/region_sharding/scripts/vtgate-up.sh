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

# This is an example script that starts a single vtgate.

source ./env.sh

cell=${CELL:-'test'}
web_port=15001
grpc_port=15991
mysql_server_port=15306
mysql_server_socket_path="/tmp/mysql.sock"

# Start vtgate.
# shellcheck disable=SC2086
vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -log_queries_to_file $VTDATAROOT/tmp/vtgate_querylog.txt \
  -port $web_port \
  -grpc_port $grpc_port \
  -mysql_server_port $mysql_server_port \
  -mysql_server_socket_path $mysql_server_socket_path \
  -cell $cell \
  -cells_to_watch $cell \
  -tablet_types_to_wait MASTER,REPLICA \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  -mysql_auth_server_impl none \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

# Block waiting for vtgate to be listening
# Not the same as healthy

echo "Waiting for vtgate to be up..."
while true; do
 curl -I "http://$hostname:$web_port/debug/status" >/dev/null 2>&1 && break
 sleep 0.1
done;
echo "vtgate is up!"

echo "Access vtgate at http://$hostname:$web_port/debug/status"

disown -a
