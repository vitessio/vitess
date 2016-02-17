#!/bin/bash

# This is an example script that starts vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

service_type=${VTCTLD_SERVICE_TYPE:-'ClusterIP'}
VITESS_NAME=${VITESS_NAME:-'default'}

echo "Creating vtctld $service_type service..."
sed_script=""
for var in service_type; do
  sed_script+="s,{{$var}},${!var},g;"
done
cat vtctld-service-template.yaml | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

echo "Creating vtctld replicationcontroller..."
# Expand template variables
sed_script=""
for var in backup_flags; do
  sed_script+="s,{{$var}},${!var},g;"
done

# Instantiate template and send to kubectl.
cat vtctld-controller-template.yaml | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

echo
echo "To access vtctld web UI, start kubectl proxy in another terminal:"
echo "  kubectl proxy --port=8001"
echo "Then visit http://localhost:8001/api/v1/proxy/namespaces/$VITESS_NAME/services/vtctld:web/"

