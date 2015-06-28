#!/bin/bash

# This is an example script that runs vtworker.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

cell=test
port=15003
vtworker_command="$*"

# Expand template variables
sed_script=""
for var in cell port vtworker_command; do
  sed_script+="s,{{$var}},${!var},g;"
done

# Instantiate template and send to kubectl.
echo "Creating vtworker pod in cell $cell..."
cat vtworker-pod-template.yaml | sed -e "$sed_script" | $KUBECTL create -f -

set +e

# Wait for vtworker pod to show up.
until $KUBECTL get pod vtworker &> /dev/null ; do
  echo "Waiting for vtworker pod to be created..."
	sleep 1
done

echo "Following vtworker logs until termination..."
$KUBECTL logs -f vtworker

echo "Deleting vtworker pod..."
$KUBECTL delete pod vtworker
