#!/bin/bash

# This is an example script that stops vtctld.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

echo "Deleting vtctld pod..."
kubectl.sh delete pod vtctld

echo "Deleting vtctld service..."
kubectl.sh delete service vtctld
