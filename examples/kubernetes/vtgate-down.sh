#!/bin/bash

# This is an example script that stops vtgate.

set -e

CELLS=${CELLS:-'test'}
cells=`echo $CELLS | tr ',' ' '`

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

for cell in $cells; do
  echo "Stopping vtgate replicationcontroller in cell $cell..."
  $KUBECTL stop replicationcontroller vtgate-$cell

  echo "Deleting vtgate service in cell $cell..."
  $KUBECTL delete service vtgate-$cell
done

