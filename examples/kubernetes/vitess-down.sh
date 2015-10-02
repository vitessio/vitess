#!/bin/bash

GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
CELLS=${CELLS:-'test'}

./vtgate-down.sh
SHARDS=$SHARDS CELLS=$CELLS TABLETS_PER_SHARD=$TABLETS_PER_SHARD ./vttablet-down.sh
./vtctld-down.sh
./etcd-down.sh

gcloud compute firewall-rules delete ${GKE_CLUSTER_NAME}-vtctld ${GKE_CLUSTER_NAME}-vtgate -q
