#!/bin/bash

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

cell=test
port=15032
grpc_port=15033

sed_script=""
for var in vitess_image cell port grpc_port; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating vtworker pod in cell $cell..."
cat vtworker-pod-interactive-template.yaml | sed -e "$sed_script" | $KUBECTL $KUBECTL_OPTIONS create -f -

set +e

until [ $($KUBECTL $KUBECTL_OPTIONS get pod -o template --template '{{.status.phase}}' vtworker 2> /dev/null) = "Running" ]; do
  echo "Waiting for vtworker pod to be created..."
        sleep 1
done

service_type='LoadBalancer'
echo "Creating vtworker $service_type service..."
sed_script=""
for var in service_type port grpc_port; do
  sed_script+="s,{{$var}},${!var},g;"
done
cat vtworker-service-template.yaml | sed -e "$sed_script" | $KUBECTL $KUBECTL_OPTIONS create -f -
