#!/bin/bash

# This is an example script that creates etcd clusters.
# Vitess requires a global cluster, as well as one for each cell.
#
# For automatic discovery, an etcd cluster can be bootstrapped from an
# existing cluster. In this example, we use an externally-run discovery
# service, but you can use your own. See the etcd docs for more:
# https://github.com/coreos/etcd/blob/v0.4.6/Documentation/cluster-discovery.md

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

for cell in 'global' 'test'; do
  # Generate a discovery token.
  echo "Generating discovery token for $cell cell..."
  discovery=$(curl -sL https://discovery.etcd.io/new)

  # Create the client service, which will load-balance across all replicas.
  echo "Creating etcd service for $cell cell..."
  cat etcd-service-template.yaml | \
    sed -e "s/{{cell}}/$cell/g" | \
    $KUBECTL create -f -

  # Create the replication controller.
  echo "Creating etcd replicationController for $cell cell..."
  cat etcd-controller-template.yaml | \
    sed -e "s/{{cell}}/$cell/g" -e "s,{{discovery}},$discovery,g" | \
    $KUBECTL create -f -
done

