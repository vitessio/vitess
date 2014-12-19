#!/bin/bash

# This is an example script that tears down the vttablet pods started by
# vttablet-up.sh. It assumes that kubernetes/cluster/kubecfg.sh is in the path.

# Delete the pods for shard-0
cell='test'
keyspace='test_keyspace'
shard=0
uid_base=100

for uid_index in 0 1 2; do
  uid=$[$uid_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid

  echo "Deleting pod for tablet $alias..."
  kubecfg.sh delete pods/vttablet-$uid
done
