#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

set -e

# Create the pods for shard-0
cell=test_cell
keyspace=test_keyspace
shard=0
uid_base=100
port_base=15000

echo "Creating $keyspace.shard-$shard pods in cell $cell..."
for uid_index in 0 1 2; do
  uid=$[$uid_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid

  # It's not strictly necessary to assign a unique port to every tablet since
  # Kubernetes gives every pod its own IP address. However, Kubernetes currently
  # doesn't provide routing from the internet into a particular pod, so we have
  # to publish a port to the host if we want to access each tablet's status page
  # from a workstation. As a result, we need tablets to have unique ports or
  # else Kubernetes will be unable to schedule more than one tablet per host.
  port=$[$port_base + $uid]

  if [ "$uid_index" == "0" ]; then
    type=master
  else
    type=replica
  fi

  echo "Creating pod for tablet $alias..."

  # Expand template variables
  sed_script=""
  for var in alias cell uid keyspace shard type port; do
    sed_script+="s/{{$var}}/${!var}/g;"
  done

  # Instantiate template and send to kubecfg.
  cat vttablet-pod-template.yaml | \
    sed -e "$sed_script" | \
    kubecfg.sh -c - create pods
done
