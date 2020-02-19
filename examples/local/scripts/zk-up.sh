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

# This is an example script that creates a quorum of ZooKeeper servers.

source ./env.sh

cell=${CELL:-'test'}

# Start ZooKeeper servers.
# The "zkctl init" command won't return until the server is able to contact its
# peers, so we need to start them all in the background and then wait for them.
echo "Starting zk servers..."
for zkid in $zkids; do
  action='init'
  printf -v zkdir 'zk_%03d' $zkid
  if [ -f $VTDATAROOT/$zkdir/myid ]; then
    echo "Resuming from existing ZK data dir:"
    echo "    $VTDATAROOT/$zkdir"
    action='start'
  fi
  zkctl -zk.myid $zkid -zk.cfg $zkcfg -log_dir $VTDATAROOT/tmp $action \
    > $VTDATAROOT/tmp/zkctl_$zkid.out 2>&1 &
  pids[$zkid]=$!
done

# Wait for all the zkctl commands to return.
echo "Waiting for zk servers to be ready..."

for zkid in $zkids; do
  if ! wait ${pids[$zkid]}; then
    echo "ZK server number $zkid failed to start. See log:"
    echo "    $VTDATAROOT/tmp/zkctl_$zkid.out"
  fi
done

echo "Started zk servers."

# Add the CellInfo description for the $CELL cell.
# If the node already exists, it's fine, means we used existing data.
set +e
# shellcheck disable=SC2086
vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/$cell \
  -server_address $ZK_SERVER \
  $cell
set -e

echo "Configured zk servers."
