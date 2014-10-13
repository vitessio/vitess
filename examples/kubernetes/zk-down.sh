#!/bin/bash

# This is an example script that tears down the ZooKeeper servers started by
# zk-up.sh. It assumes that kubernetes/cluster/kubecfg.sh is in the path.

# Delete pods.
for zkid in 1 2 3; do
  echo "Deleting zk$zkid pod..."
  kubecfg.sh delete pods/zk$zkid
done

# Delete client service.
echo "Deleting zk-client service..."
kubecfg.sh delete services/zk-client

# Delete leader and election services.
for zkid in 1 2 3; do
  echo "Deleting zk$zkid-leader service..."
  kubecfg.sh delete services/zk$zkid-leader

  echo "Deleting zk$zkid-election service..."
  kubecfg.sh delete services/zk$zkid-election
done
