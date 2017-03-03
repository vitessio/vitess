#!/bin/bash

# This is an example script that starts a guestbook replicationcontroller.

set -e

port=${GUESTBOOK_PORT:-8080}
cell=${GUESTBOOK_CELL:-"test"}
keyspace=${GUESTBOOK_KEYSPACE:-"test_keyspace"}
vtgate_port=${VTGATE_PORT:-15991}

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Creating guestbook service..."
$KUBECTL $KUBECTL_OPTIONS create -f guestbook-service.yaml

sed_script=""
for var in port cell keyspace vtgate_port; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating guestbook replicationcontroller..."
sed -e "$sed_script" guestbook-controller-template.yaml | $KUBECTL $KUBECTL_OPTIONS create -f -
