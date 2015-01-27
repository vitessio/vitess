#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh. It assumes that kubernetes/cluster/kubectl.sh is in the path.

# Delete replication controllers
for cell in 'global' 'test'; do
  echo "Stopping etcd replicationController for $cell cell..."
  kubectl.sh stop replicationController etcd-$cell

  echo "Deleting etcd service for $cell cell..."
  kubectl.sh delete service etcd-$cell
done

