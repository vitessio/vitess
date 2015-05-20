#!/bin/bash

# This is an example script that starts a vtgate replicationController.

set -e

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}

vtdataroot_volume='{emptyDir: {}}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="{hostDir: {path: ${VTDATAROOT_VOLUME}}}"
fi

replicas=$VTGATE_REPLICAS

echo "Creating vtgate service..."
kubectl create -f vtgate-service.yaml

sed_script=""
for var in replicas vtdataroot_volume; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating vtgate replicationController..."
cat vtgate-controller-template.yaml | sed -e "$sed_script" | kubectl create -f -
