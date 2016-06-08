#!/bin/bash

# This is an example script that starts orchestrator.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VITESS_NAME=${VITESS_NAME:-'default'}

echo "Creating orchestrator service and replicationcontroller..."
sed_script=""
for var in service_type; do
  sed_script+="s,{{$var}},${!var},g;"
done
cat orchestrator-template.yaml | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

echo
echo "To access orchestrator web UI, start kubectl proxy in another terminal:"
echo "  kubectl proxy --port=8001"
echo "Then visit http://localhost:8001/api/v1/proxy/namespaces/$VITESS_NAME/services/orchestrator/"

