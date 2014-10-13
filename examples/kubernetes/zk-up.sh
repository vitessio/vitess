#!/bin/bash

# This is an example script that creates a quorum of ZooKeeper servers.
# It assumes that kubernetes/cluster/kubecfg.sh is in the path.

set -e

# List of all servers in the quorum.
zkcfg=(\
    '1@$SERVICE_HOST:28881:38881:2181' \
    '2@$SERVICE_HOST:28882:38882:2181' \
    '3@$SERVICE_HOST:28883:38883:2181' \
    )
printf -v zkcfg ",%s" "${zkcfg[@]}"
zkcfg=${zkcfg:1}

# Create the pods.
echo "Creating zk pods..."
for zkid in 1 2 3; do
  cat zk-pod-template.yaml | \
    sed -e "s/{{zkid}}/$zkid/g" -e "s/{{zkcfg}}/$zkcfg/g" | \
    kubecfg.sh -c - create pods
done

# Create the client service, which will load-balance across all replicas.
echo "Creating zk services..."
kubecfg.sh -c zk-client-service.yaml create services

# Create a service for the leader and election ports of each replica.
# This is necessary because ZooKeeper servers need to know how to specifically
# contact replica N (not just "any replica") in order to create a quorum.
# We also have to append the zkid of each server to the port number, because
# every service in Kubernetes needs a unique port number (for now).

ports=( 2888 3888 )
svcs=( leader election )

for zkid in 1 2 3; do
  for i in 0 1; do
    port=${ports[$i]}
    svc=${svcs[$i]}

    cat zk-service-template.yaml | \
      sed -e "s/{{zkid}}/$zkid/g" -e "s/{{port}}/$port/g" -e "s/{{svc}}/$svc/g" | \
      kubecfg.sh -c - create services
  done
done
