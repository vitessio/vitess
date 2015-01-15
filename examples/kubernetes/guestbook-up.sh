#!/bin/bash

# This is an example script that starts a guestbook replicationController.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

set -e

echo "Creating guestbook service..."
kubecfg.sh -c guestbook-service.yaml create services

echo "Creating guestbook replicationController..."
kubecfg.sh -c guestbook-controller.yaml create replicationControllers
