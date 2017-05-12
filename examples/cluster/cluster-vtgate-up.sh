#!/bin/bash

# This is an example script that starts a single vtgate.

set -e

web_port=15001
grpc_port=15991
mysql_server_port=15306

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/cluster-env.sh

# Start vtgate.
$VTROOT/bin/vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -port $web_port \
  -grpc_port $grpc_port \
  -mysql_server_port $mysql_server_port \
  -mysql_auth_server_static_file /opt/vitess/src/github.com/youtube/vitess/examples/cluster/vtgate-credentials.json \
  -cell $cell \
  -cells_to_watch $cell \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  $optional_tls_args \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://$hostname:$web_port/debug/status"

disown -a

