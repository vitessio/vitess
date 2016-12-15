#!/bin/bash

# This is an example script that stops orchestrator.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping orchestrator replicationcontroller..."
$KUBECTL delete replicationcontroller orchestrator --namespace=$VITESS_NAME

echo "Deleting orchestrator service..."
$KUBECTL delete service orchestrator --namespace=$VITESS_NAME

echo "Deleting orchestrator configmap..."
$KUBECTL delete --namespace=$VITESS_NAME configmap orchestrator-conf
