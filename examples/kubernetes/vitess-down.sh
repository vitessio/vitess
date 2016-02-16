#!/bin/bash

SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
CELLS=${CELLS:-'test'}
TEST_MODE=${TEST_MODE:-'0'}
VITESS_NAME=${VITESS_NAME:-'vitess'}

export VITESS_NAME=$VITESS_NAME

./vtgate-down.sh
SHARDS=$SHARDS CELLS=$CELLS TABLETS_PER_SHARD=$TABLETS_PER_SHARD ./vttablet-down.sh
./vtctld-down.sh
./etcd-down.sh

if [ $TEST_MODE -gt 0 ]; then
  gcloud compute firewall-rules delete ${VITESS_NAME}-vtctld -q
fi

for cell in `echo $CELLS | tr ',' ' '`; do
  gcloud compute firewall-rules delete ${VITESS_NAME}-vtgate-$cell -q
done

./namespace-down.sh
