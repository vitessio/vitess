#!/bin/bash

# This is an example script that starts a single vtgate.

set -e

cell='test'
port=15001

hostname=`hostname -f`

# We expect to find zk-client-conf.json in the same folder as this script.
script_root=`dirname "${BASH_SOURCE}"`

# Set up environment.
export LD_LIBRARY_PATH=$VTROOT/dist/vt-zookeeper-3.3.5/lib:$LD_LIBRARY_PATH
export ZK_CLIENT_CONFIG=$script_root/zk-client-conf.json
mkdir -p $VTDATAROOT/tmp

# Start vtgate.
$VTROOT/bin/vtgate -log_dir $VTDATAROOT/tmp -port $port -cell $cell \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &
echo "Access vtgate at http://$hostname:$port/debug/status"

disown -a
