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

# This is an example script that creates a quorum of ZooKeeper servers.

set -e
cell=${CELL:-'test'}

script_root=$(dirname "${BASH_SOURCE[0]}")

# shellcheck source=./env.sh
# shellcheck disable=SC1091
source "${script_root}/env.sh"

${ETCD_BINDIR}/etcd --data-dir "${VTDATAROOT}/etcd/"  --listen-client-urls "http://${ETCD_SERVER}" --advertise-client-urls "http://${ETCD_SERVER}" > "${VTDATAROOT}"/tmp/etcd.out 2>&1 &
sleep 5

echo "add /vitess/global"
${ETCD_BINDIR}/etcdctl --endpoints "http://${ETCD_SERVER}" mkdir /vitess/global &


echo "add /vitess/$cell"
${ETCD_BINDIR}/etcdctl --endpoints "http://${ETCD_SERVER}" mkdir /vitess/$cell &

# And also add the CellInfo description for the cell.
# If the node already exists, it's fine, means we used existing data.
echo "add $cell CellInfo"
set +e
# shellcheck disable=SC2086
"${VTROOT}"/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/$cell \
  -server_address "${ETCD_SERVER}" \
  $cell
set -e

echo "etcd start done..."



