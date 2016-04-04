#!/bin/bash

# This is an example script that stops vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VITESS_NAME=${VITESS_NAME:-'default'}

echo "Stopping vtctld replicationcontroller..."
$KUBECTL delete replicationcontroller vtctld --namespace=$VITESS_NAME

echo "Deleting vtctld service..."
$KUBECTL delete service vtctld --namespace=$VITESS_NAME
