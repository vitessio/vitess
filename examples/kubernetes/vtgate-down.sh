#!/bin/bash

# This is an example script that stops vtgate.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

echo "Stopping vtgate replicationController..."
kubectl.sh stop replicationController vtgate

echo "Deleting vtgate service..."
kubectl.sh delete service vtgate
