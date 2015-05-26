#!/bin/bash

# This is an example script that starts vtctld.

set -e

echo "Creating vtctld service..."
kubectl create -f vtctld-service.yaml

echo "Creating vtctld pod..."
kubectl create -f vtctld-pod.yaml
