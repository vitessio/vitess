#!/bin/bash

# This is an example script that stops vtgate.

set -e

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}

echo "Stopping vtgate replicationController..."
for uid in `seq 1 $VTGATE_REPLICAS`; do
  kubectl stop replicationController vtgate-$uid
done

echo "Deleting vtgate service..."
kubectl delete service vtgate
