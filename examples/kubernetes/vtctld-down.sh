#!/bin/bash

# This is an example script that stops vtctld.

set -e

echo "Deleting vtctld pod..."
kubectl delete pod vtctld

echo "Deleting vtctld service..."
kubectl delete service vtctld
