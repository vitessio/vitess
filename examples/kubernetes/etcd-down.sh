#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

cells=`echo $CELLS | tr ',' ' '`

# Delete replication controllers
for cell in 'global' $cells; do
  echo "Stopping etcd replicationcontroller for $cell cell..."
  $KUBECTL $KUBECTL_OPTIONS delete replicationcontroller etcd-$cell

  echo "Deleting etcd service for $cell cell..."
  $KUBECTL $KUBECTL_OPTIONS delete service etcd-$cell
  $KUBECTL $KUBECTL_OPTIONS delete service etcd-$cell-srv
done

