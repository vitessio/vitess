#!/bin/bash

# This is an example script that stops guestbook.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

echo "Stopping guestbook replicationController..."
kubectl.sh stop replicationController guestbook

echo "Deleting guestbook service..."
kubectl.sh delete service guestbook
