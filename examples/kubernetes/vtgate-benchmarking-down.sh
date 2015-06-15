#!/bin/bash

# This is an example script that stops vtgate.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}

for uid in `seq 1 $VTGATE_REPLICAS`; do
  echo "Deleting pod for vtgate ${uid}..."
  $KUBECTL delete pod vtgate-$uid
done

echo "Deleting vtgate service..."
$KUBECTL delete service vtgate
