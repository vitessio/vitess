#!/bin/bash

# This is an example script that stops vtgate.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

cells=`echo $CELLS | tr ',' ' '`

for cell in $cells; do
  echo "Stopping vtgate replicationcontroller in cell $cell..."
  $KUBECTL delete replicationcontroller vtgate-$cell --namespace=$VITESS_NAME

  echo "Deleting vtgate service in cell $cell..."
  $KUBECTL delete service vtgate-$cell --namespace=$VITESS_NAME
done

