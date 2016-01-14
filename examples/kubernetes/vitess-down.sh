#!/bin/bash

GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
CELLS=${CELLS:-'test'}
TEST_MODE=${TEST_MODE:-'0'}

./vtgate-down.sh
SHARDS=$SHARDS CELLS=$CELLS TABLETS_PER_SHARD=$TABLETS_PER_SHARD ./vttablet-down.sh
./vtctld-down.sh
./etcd-down.sh

if [ $TEST_MODE -gt 0 ]; then
  gcloud compute firewall-rules delete ${GKE_CLUSTER_NAME}-vtctld -q
fi

for cell in `echo $CELLS | tr ',' ' '`; do
  gcloud compute firewall-rules delete ${GKE_CLUSTER_NAME}-vtgate-$cell -q
done
