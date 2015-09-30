#!/bin/bash

# This is an example script that starts vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating vtctld service..."
$KUBECTL create -f vtctld-service.yaml

echo "Creating vtctld replicationcontroller..."
$KUBECTL create -f vtctld-controller.yaml

server=$(get_vtctld_addr)
echo
echo "vtctld address: http://$server"

