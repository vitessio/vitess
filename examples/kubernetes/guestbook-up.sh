#!/bin/bash

# This is an example script that starts a guestbook replicationController.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating guestbook service..."
$KUBECTL create -f guestbook-service.yaml

echo "Creating guestbook replicationController..."
$KUBECTL create -f guestbook-controller.yaml
