#!/bin/bash

# This is an example script that stops vtctld.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

echo "Deleting vtctld pod..."
kubecfg.sh delete pods/vtctld

echo "Deleting vtctld service..."
kubecfg.sh delete services/vtctld
