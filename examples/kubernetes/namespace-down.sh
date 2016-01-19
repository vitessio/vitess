#!/bin/bash

# This is an example script that stops vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

namespace=${VITESS_NAME:-'vitess'}

echo "Deleting namespace $namespace..."
$KUBECTL delete namespace $namespace
