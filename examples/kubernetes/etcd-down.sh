#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VITESS_NAME=${VITESS_NAME=-'default'}
CELLS=${CELLS:-'test'}
cells=`echo $CELLS | tr ',' ' '`

# Delete replication controllers
for cell in 'global' $cells; do
  echo "Stopping etcd replicationcontroller for $cell cell..."
  $KUBECTL delete replicationcontroller etcd-$cell --namespace=$VITESS_NAME

  echo "Deleting etcd service for $cell cell..."
  $KUBECTL delete service etcd-$cell --namespace=$VITESS_NAME
  $KUBECTL delete service etcd-$cell-srv --namespace=$VITESS_NAME
done

