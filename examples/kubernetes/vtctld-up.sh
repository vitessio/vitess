#!/bin/bash

# This is an example script that starts vtctld.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

set -e

echo "Creating vtctld service..."
kubectl.sh create -f vtctld-service.yaml

echo "Creating vtctld pod..."
kubectl.sh create -f vtctld-pod.yaml
