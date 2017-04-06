#!/bin/bash

# This is an example script that stops vtgate.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

cells=`echo $CELLS | tr ',' ' '`

for cell in $cells; do
  echo "Stopping vtgate replicationcontroller in cell $cell..."
  $KUBECTL $KUBECTL_OPTIONS delete replicationcontroller vtgate-$cell

  echo "Deleting vtgate service in cell $cell..."
  $KUBECTL $KUBECTL_OPTIONS delete service vtgate-$cell
done

