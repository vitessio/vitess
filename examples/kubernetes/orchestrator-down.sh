#!/bin/bash

# This is an example script that stops orchestrator.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VITESS_NAME=${VITESS_NAME:-'default'}

echo "Stopping orchestrator replicationcontroller..."
$KUBECTL delete replicationcontroller orchestrator --namespace=$VITESS_NAME

echo "Deleting orchestrator service..."
$KUBECTL delete service orchestrator --namespace=$VITESS_NAME

