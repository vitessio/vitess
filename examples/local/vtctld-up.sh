#!/bin/bash

# This is an example script that starts vtctld.

set -e

port=15000

hostname=`hostname -f`

# We expect to find zk-client-conf.json in the same folder as this script.
script_root=`dirname "${BASH_SOURCE}"`

# Set up environment.
export VTTOP=$VTROOT/src/github.com/youtube/vitess
export LD_LIBRARY_PATH=$VTROOT/dist/vt-zookeeper-3.3.5/lib:$LD_LIBRARY_PATH
export ZK_CLIENT_CONFIG=$script_root/zk-client-conf.json
mkdir -p $VTDATAROOT/tmp

echo "Starting vtctld..."
$VTROOT/bin/vtctld -debug -templates $VTTOP/go/cmd/vtctld/templates \
  -schema-editor-dir $VTTOP/go/cmd/vtctld \
  -log_dir $VTDATAROOT/tmp -port $port > $VTDATAROOT/tmp/vtctld.out 2>&1 &
disown -a

echo "Access vtctld at http://$hostname:$port"
