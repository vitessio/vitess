#!/bin/bash

# This is an example script that creates a quorum of ZooKeeper servers.

set -e

hostname=`hostname -f`

# Each ZooKeeper server needs a list of all servers in the quorum.
# Since we're running them all locally, we need to give them unique ports.
# In a real deployment, these should be on different machines, and their
# respective hostnames should be given.
zkcfg=(\
    "1@$hostname:28881:38881:21811" \
    "2@$hostname:28882:38882:21812" \
    "3@$hostname:28883:38883:21813" \
    )
printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}

# Set up environment.
export LD_LIBRARY_PATH=$VTROOT/dist/vt-zookeeper-3.3.5/lib:$LD_LIBRARY_PATH
mkdir -p $VTDATAROOT/tmp

# Start ZooKeeper servers.
# The "zkctl init" command won't return until the server is able to contact its
# peers, so we need to start them all in the background and then wait for them.
echo "Starting zk servers..."
for zkid in 1 2 3; do
  $VTROOT/bin/zkctl -zk.myid $zkid -zk.cfg $zkcfg -log_dir $VTDATAROOT/tmp init \
    > $VTDATAROOT/tmp/zkctl_$zkid.out 2>&1 &
done

# Wait for all the zkctl commands to return.
echo "Waiting for zk servers to be ready..."
wait
