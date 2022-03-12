#!/bin/bash

# Copyright 2022 The Vitess Authors.
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

# This is an example script that creates a single-node consul datacenter.

source ./env.sh

cell=${CELL:-'test'}
consul_http_port=${CONSUL_HTTP_PORT:-'8500'}
consul_server_port=${CONSUL_SERVER_PORT:-'8300'}

# Check that consul is not already running
curl "http://${CONSUL_SERVER}:${consul_http_port}" &> /dev/null && fail "consul is already running. Exiting."

set -x
consul agent \
    -server \
    -bootstrap-expect=1 \
    -node=vitess-consul \
    -bind="${CONSUL_SERVER}" \
    -server-port="${consul_server_port}" \
    -data-dir="${VTDATAROOT}/consul/" -ui > "${VTDATAROOT}/tmp/consul.out" 2>&1 &

PID=$!
echo $PID > "${VTDATAROOT}/tmp/consul.pid"
sleep 5

# Add the CellInfo description for the cell.
# If the node already exists, it's fine, means we used existing data.
echo "add $cell CellInfo"
set +e
# shellcheck disable=SC2086
vtctl $TOPOLOGY_FLAGS VtctldCommand AddCellInfo \
  --root "vitess/$cell" \
  --server-address "${CONSUL_SERVER}:${consul_http_port}" \
  "$cell"
set -e

echo "consul start done..."
