#!/bin/bash

# This is an example script that tears down the vttablet pods started by
# vttablet-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Delete the pods for all shards
cell='test'
keyspace='test_keyspace'
SHARDS=${SHARDS:-'0'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
num_shards=`echo $SHARDS | tr "," " " | wc -w`

for shard in `seq 1 $num_shards`; do
  uid_base=$((100*$shard))
  for uid_index in `seq 0 $(($TABLETS_PER_SHARD-1))`; do
    uid=$[$uid_base + $uid_index]
    printf -v alias '%s-%010d' $cell $uid

    echo "Deleting pod for tablet $alias..."
    $KUBECTL delete pod vttablet-$uid
  done
done
