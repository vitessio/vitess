#!/bin/bash

# This is an example script that stops vtgate.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

echo "Deleting pods created by vtgate replicationController..."
kubecfg.sh stop vtgateController

echo "Deleting vtgate replicationController..."
kubecfg.sh delete replicationControllers/vtgateController

echo "Deleting vtgate service..."
kubecfg.sh delete services/vtgate
