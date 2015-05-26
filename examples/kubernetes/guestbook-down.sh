#!/bin/bash

# This is an example script that stops guestbook.

set -e

echo "Stopping guestbook replicationController..."
kubectl stop replicationController guestbook

echo "Deleting guestbook service..."
kubectl delete service guestbook
