#!/bin/bash

# This is an example script that runs vtworker.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting vtworker..."
exec $VTROOT/bin/vtworker \
  $TOPOLOGY_FLAGS \
  -cell test \
  -log_dir $VTDATAROOT/tmp \
  -alsologtostderr \
  -service_map=grpc-vtworker \
  -grpc_port 15033 \
  -port 15032 \
  -pid_file $VTDATAROOT/tmp/vtworker.pid \
  -use_v3_resharding_mode &
