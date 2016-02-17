#!/bin/bash

# This is an example script that stops the ZooKeeper servers started by zk-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Stop ZooKeeper servers.
echo "Stopping zk servers..."
for zkid in $zkids; do
  $VTROOT/bin/zkctl -zk.myid $zkid -zk.cfg $zkcfg -log_dir $VTDATAROOT/tmp shutdown
done

