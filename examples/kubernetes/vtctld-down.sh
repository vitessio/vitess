#!/bin/bash

# This is an example script that stops vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping vtctld replicationcontroller..."
$KUBECTL --server=$KUBERENETAPI delete replicationcontroller vtctld --namespace=$VITESS_NAME

echo "Deleting vtctld service..."
$KUBECTL --server=$KUBERENETAPI delete service vtctld --namespace=$VITESS_NAME
