#!/bin/bash

# This is an example script that starts vtctld.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

set -e

echo "Creating vtctld service..."
kubecfg.sh -c vtctld-service.yaml create services

echo "Creating vtctld pod..."
kubecfg.sh -c vtctld-pod.yaml create pods
