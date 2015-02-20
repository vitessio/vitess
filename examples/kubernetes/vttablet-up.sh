#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Create the pods for shard-0
cell='test'
keyspace='test_keyspace'
NUM_SHARDS=${NUM_SHARDS:-1}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
port=15002

for shard in `seq 0 $(($NUM_SHARDS-1))`; do
  uid_base=$((100*($shard+1)))
  echo "Creating $keyspace.shard-$shard pods in cell $cell..."
  for uid_index in `seq 0 $(($TABLETS_PER_SHARD-1))`; do
    uid=$[$uid_base + $uid_index]
    printf -v alias '%s-%010d' $cell $uid
    printf -v tablet_subdir 'vt_%010d' $uid

    echo "Creating pod for tablet $alias..."

    # Expand template variables
    sed_script=""
    for var in alias cell uid keyspace shard port tablet_subdir; do
      sed_script+="s/{{$var}}/${!var}/g;"
    done

    # Instantiate template and send to kubectl.
    cat vttablet-pod-template.yaml | \
      sed -e "$sed_script" | \
      $KUBECTL create -f -
  done
done
