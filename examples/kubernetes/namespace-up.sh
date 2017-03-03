#!/bin/bash

# This is an example script that creates a namespace.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

namespace=${VITESS_NAME:-'vitess'}

echo "Creating namespace $namespace..."
sed_script=""
for var in namespace; do
  sed_script+="s,{{$var}},${!var},g;"
done
cat namespace-template.yaml | sed -e "$sed_script" | $KUBECTL $KUBECTL_OPTIONS create -f -

