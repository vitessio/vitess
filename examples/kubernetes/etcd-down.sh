#!/bin/bash

# This is an example script that tears down the etcd servers started by
# etcd-up.sh. It assumes that kubernetes/cluster/kubecfg.sh is in the path.

# Delete replication controllers
for cell in 'global' 'test'; do
  echo "Deleting pods created by etcd replicationController for $cell cell..."
  kubecfg.sh stop etcd-$cell

  echo "Deleting etcd replicationController for $cell cell..."
  kubecfg.sh delete replicationControllers/etcd-$cell

  echo "Deleting etcd service for $cell cell..."
  kubecfg.sh delete services/etcd-$cell
done

