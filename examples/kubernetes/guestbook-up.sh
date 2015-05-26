#!/bin/bash

# This is an example script that starts a guestbook replicationController.

set -e

echo "Creating guestbook service..."
kubectl create -f guestbook-service.yaml

echo "Creating guestbook replicationController..."
kubectl create -f guestbook-controller.yaml
