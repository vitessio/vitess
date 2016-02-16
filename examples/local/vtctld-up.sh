#!/bin/bash

# This is an example script that starts vtctld.

set -e

web_port=15000
grpc_port=15999

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting vtctld..."
$VTROOT/bin/vtctld \
  -web_dir $VTTOP/web/vtctld \
  -tablet_protocol grpc \
  -tablet_manager_protocol grpc \
  -service_map 'grpc-vtctl' \
  -backup_storage_implementation file \
  -file_backup_storage_root $VTDATAROOT/backups \
  -log_dir $VTDATAROOT/tmp \
  -port $web_port \
  -grpc_port $grpc_port \
  -pid_file $VTDATAROOT/tmp/vtctld.pid \
  > $VTDATAROOT/tmp/vtctld.out 2>&1 &
disown -a

echo "Access vtctld web UI at http://$hostname:$web_port"
echo "Send commands with: vtctlclient -server $hostname:$grpc_port ..."

