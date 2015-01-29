#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.
# It assumes that kubernetes/cluster/kubectl.sh is in the path.

set -e

# Create the pods for shard-0
cell='test'
keyspace='test_keyspace'
shard=0
uid_base=100
port=15002

echo "Creating $keyspace.shard-$shard pods in cell $cell..."
for uid_index in 0 1 2; do
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
    kubectl.sh create -f -
done
