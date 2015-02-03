#!/bin/bash

# This is an example script that stops vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Deleting vtctld pod..."
$KUBECTL delete pod vtctld

echo "Deleting vtctld service..."
$KUBECTL delete service vtctld
