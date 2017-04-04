#!/bin/bash

# This is an example script that stops vtworker.

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping vtworker replicationcontroller..."
$KUBECTL $KUBECTL_OPTIONS delete replicationcontroller vtworker

echo "Deleting vtworker service..."
$KUBECTL $KUBECTL_OPTIONS delete service vtworker
