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

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

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
  $VTROOT/bin/zkctl -zk.myid $zkid -zk.cfg $zkcfg -log_dir $VTDATAROOT/tmp $action \
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

# Add the CellInfo description for the 'test' cell.
# If the node already exists, it's fine, means we used existing data.
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo \
  -root /vitess/test \
  -server_address $ZK_SERVER \
  test || /bin/true

echo "Configured zk servers."
