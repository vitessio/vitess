#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Delete replication controllers
for cell in 'global' 'test'; do
  echo "Stopping etcd replicationController for $cell cell..."
  $KUBECTL stop replicationController etcd-$cell

  echo "Deleting etcd service for $cell cell..."
  $KUBECTL delete service etcd-$cell
done

