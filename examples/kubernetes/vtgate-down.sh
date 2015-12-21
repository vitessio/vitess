#!/bin/bash

# This is an example script that stops vtgate.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping vtgate replicationcontroller..."
$KUBECTL stop replicationcontroller vtgate

echo "Deleting vtgate service..."
$KUBECTL delete service vtgate
