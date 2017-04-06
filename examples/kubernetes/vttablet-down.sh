#!/bin/bash

# This is an example script that tears down the vttablet pods started by
# vttablet-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting port forwarding to vtctld..."
start_vtctld_forward
trap stop_vtctld_forward EXIT
VTCTLD_ADDR="localhost:$vtctld_forward_port"

# Delete the pods for all shards
keyspace='test_keyspace'
SHARDS=${SHARDS:-'0'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-5}
UID_BASE=${UID_BASE:-100}

num_shards=`echo $SHARDS | tr "," " " | wc -w`
uid_base=$UID_BASE
cells=`echo $CELLS | tr ',' ' '`
num_cells=`echo $cells | wc -w`

for shard in `seq 1 $num_shards`; do
  [[ $num_cells -gt 1 ]] && cell_index=100000000 || cell_index=0
  for cell in $cells; do
    for uid_index in `seq 0 $(($TABLETS_PER_SHARD-1))`; do
      uid=$[$uid_base + $uid_index + $cell_index]
      printf -v alias '%s-%010d' $cell $uid

      echo "Deleting pod for tablet $alias..."
      $KUBECTL $KUBECTL_OPTIONS delete pod vttablet-$uid
    done
    let cell_index=cell_index+100000000
  done
  let uid_base=uid_base+100
done
