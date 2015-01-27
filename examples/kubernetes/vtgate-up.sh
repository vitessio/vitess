#!/bin/bash

# This is an example script that starts a vtgate replicationController.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

set -e

echo "Creating vtgate service..."
kubectl.sh create -f vtgate-service.yaml

echo "Creating vtgate replicationController..."
kubectl.sh create -f vtgate-controller.yaml
