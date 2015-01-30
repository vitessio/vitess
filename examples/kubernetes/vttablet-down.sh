#!/bin/bash

# This is an example script that tears down the vttablet pods started by
# vttablet-up.sh.

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Delete the pods for shard-0
cell='test'
keyspace='test_keyspace'
shard=0
uid_base=100

for uid_index in 0 1 2; do
  uid=$[$uid_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid

  echo "Deleting pod for tablet $alias..."
  $KUBECTL delete pod vttablet-$uid
done
