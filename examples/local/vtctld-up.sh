#!/bin/bash

# This is an example script that starts vtctld.

set -e

web_port=15000
grpc_port=15999

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
  -web_dir $VTTOP/web/vtctld \
  -tablet_protocol grpc \
  -tablet_manager_protocol grpc \
  -service_map 'grpc-vtctl' \
  -backup_storage_implementation file \
  -file_backup_storage_root $VTDATAROOT/backups \
  -log_dir $VTDATAROOT/tmp \
  -port $web_port \
  -grpc_port $grpc_port \
  > $VTDATAROOT/tmp/vtctld.out 2>&1 &
disown -a

echo "Access vtctld web UI at http://$hostname:$web_port"
echo "Send commands with: vtctlclient -server $hostname:$grpc_port ..."
