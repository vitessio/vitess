#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh.

set -e

# Delete replication controllers
for cell in 'global' 'test'; do
  echo "Stopping etcd replicationController for $cell cell..."
  kubectl stop replicationController etcd-$cell

  echo "Deleting etcd service for $cell cell..."
  kubectl delete service etcd-$cell
done

