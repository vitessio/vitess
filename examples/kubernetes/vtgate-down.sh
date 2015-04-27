#!/bin/bash

# This is an example script that stops vtgate.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}

echo "Stopping vtgate replicationController..."
for uid in `seq 1 $VTGATE_REPLICAS`; do
  $KUBECTL stop replicationController vtgate-$uid
done

echo "Deleting vtgate service..."
$KUBECTL delete service vtgate
