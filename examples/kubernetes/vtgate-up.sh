#!/bin/bash

# This is an example script that starts a vtgate replicationController.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating vtgate service..."
$KUBECTL create -f vtgate-service.yaml

echo "Creating vtgate replicationController..."
$KUBECTL create -f vtgate-controller.yaml
