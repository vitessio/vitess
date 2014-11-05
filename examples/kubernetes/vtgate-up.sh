#!/bin/bash

# This is an example script that starts a vtgate replicationController.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

set -e

echo "Creating vtgate service..."
kubecfg.sh -c vtgate-service.yaml create services

echo "Creating vtgate replicationController..."
kubecfg.sh -c vtgate-controller.yaml create replicationControllers
