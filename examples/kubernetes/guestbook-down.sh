#!/bin/bash

# This is an example script that stops guestbook.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping guestbook replicationcontroller..."
$KUBECTL --server=$KUBERENETAPI delete replicationcontroller guestbook --namespace=$VITESS_NAME

echo "Deleting guestbook service..."
$KUBECTL --server=$KUBERENETAPI delete service guestbook --namespace=$VITESS_NAME
