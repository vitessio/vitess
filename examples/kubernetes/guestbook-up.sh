#!/bin/bash

# This is an example script that starts a guestbook replicationController.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

set -e

echo "Creating guestbook service..."
kubectl.sh create -f guestbook-service.yaml

echo "Creating guestbook replicationController..."
kubectl.sh create -f guestbook-controller.yaml
