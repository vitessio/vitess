#!/bin/bash

# This is an example script that stops vtgate.

set -e

VITESS_NAME=${VITESS_NAME:-'default'}
CELLS=${CELLS:-'test'}
cells=`echo $CELLS | tr ',' ' '`

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

for cell in $cells; do
  echo "Stopping vtgate replicationcontroller in cell $cell..."
  $KUBECTL delete replicationcontroller vtgate-$cell --namespace=$VITESS_NAME

  echo "Deleting vtgate service in cell $cell..."
  $KUBECTL delete service vtgate-$cell --namespace=$VITESS_NAME
done

