#!/bin/bash

# This is an example script that starts a single vtgate.

set -e

cell='test'
web_port=15001 # This is also the bsonrpc port.
grpc_port=15991

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Start vtgate.
$VTROOT/bin/vtgate \
  -log_dir $VTDATAROOT/tmp \
  -port $web_port \
  -grpc_port $grpc_port \
  -cell $cell \
  -tablet_protocol grpc \
  -service_map 'bsonrpc-vt-vtgateservice,grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://$hostname:$port/debug/status"

disown -a

