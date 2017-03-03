#!/bin/bash

# This is an example script that stops orchestrator.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping orchestrator replicationcontroller..."
$KUBECTL $KUBECTL_OPTIONS delete replicationcontroller orchestrator

echo "Deleting orchestrator service..."
$KUBECTL $KUBECTL_OPTIONS delete service orchestrator

echo "Deleting orchestrator configmap..."
$KUBECTL $KUBECTL_OPTIONS delete configmap orchestrator-conf
