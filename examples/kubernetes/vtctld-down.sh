#!/bin/bash

# This is an example script that stops vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Stopping vtctld replicationcontroller..."
$KUBECTL $KUBECTL_OPTIONS delete replicationcontroller vtctld

echo "Deleting vtctld service..."
$KUBECTL $KUBECTL_OPTIONS delete service vtctld
