#!/bin/bash

# This is an example script that starts orchestrator.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating orchestrator service, configmap, and replicationcontroller..."
sed_script=""
for var in service_type; do
  sed_script+="s,{{$var}},${!var},g;"
done

# Create configmap from orchestrator docker config file
$KUBECTL $KUBECTL_OPTIONS create configmap orchestrator-conf --from-file="${script_root}/../../docker/orchestrator/orchestrator.conf.json"

cat orchestrator-template.yaml | sed -e "$sed_script" | $KUBECTL $KUBECTL_OPTIONS create -f -

echo
echo "To access orchestrator web UI, start kubectl proxy in another terminal:"
echo "  kubectl proxy --port=8001"
echo "Then visit http://localhost:8001/api/v1/proxy/namespaces/$VITESS_NAME/services/orchestrator/"
