#!/bin/bash

# This is an example script that starts a vtgate replicationController.

#set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}
STARTING_INDEX=${STARTING_INDEX:--1}

vtdataroot_volume='{emptyDir: {}}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="{hostDir: {path: ${VTDATAROOT_VOLUME}}}"
fi

echo "Creating vtgate service..."
$KUBECTL create -f vtgate-service.yaml

echo "Creating vtgate pods..."
for uid in `seq 1 $VTGATE_REPLICAS`; do
  sed_script=""
  for var in uid vtdataroot_volume; do
    sed_script+="s,{{$var}},${!var},g;"
  done
  if [ "$STARTING_INDEX" -gt -1 ]; then
    vtgate_index=$(($STARTING_INDEX+$uid))
    sed_script+="\$anodeSelector:\n  id: \"$vtgate_index\""
  fi
  cat vtgate-pod-benchmarking-template.yaml | sed -e "$sed_script" | $KUBECTL create -f -
done
