#!/bin/bash

# This is an example script that stops vtgate.

set -e

echo "Stopping vtgate replicationController..."
kubectl stop replicationController vtgate

echo "Deleting vtgate service..."
kubectl delete service vtgate
