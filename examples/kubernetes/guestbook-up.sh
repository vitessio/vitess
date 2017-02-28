#!/bin/bash

# This is an example script that starts a guestbook replicationcontroller.

set -e

port=${GUESTBOOK_PORT:-8080}
cell=${GUESTBOOK_CELL:-"test"}
vtgate_port=${VTGATE_PORT:-15991}

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating guestbook service..."
$KUBECTL create --namespace=$VITESS_NAME -f guestbook-service.yaml

sed_script=""
for var in port cell vtgate_port; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating guestbook replicationcontroller..."
cat guestbook-controller-template.yaml | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -
