#!/bin/bash

# This is an example script that starts a single vtgate.

set -e

cell='test'
web_port=15001
grpc_port=15991

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Start vtgate.
$VTROOT/bin/vtgate \
  -log_dir $VTDATAROOT/tmp \
  -port $web_port \
  -grpc_port $grpc_port \
  -cell $cell \
  -cells_to_watch $cell \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://$hostname:$web_port/debug/status"

disown -a

