#!/bin/bash

# This is an example script that stops guestbook.

set -e

VITESS_NAME=${VITESS_NAME:-'default'}

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping guestbook replicationcontroller..."
$KUBECTL delete replicationcontroller guestbook --namespace=$VITESS_NAME

echo "Deleting guestbook service..."
$KUBECTL delete service guestbook --namespace=$VITESS_NAME
