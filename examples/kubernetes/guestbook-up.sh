#!/bin/bash

# This is an example script that starts a guestbook replicationcontroller.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating guestbook service..."
$KUBECTL --server=$KUBERENETAPI create --namespace=$VITESS_NAME -f guestbook-service.yaml

echo "Creating guestbook replicationcontroller..."
$KUBECTL --server=$KUBERENETAPI create --namespace=$VITESS_NAME -f guestbook-controller.yaml
