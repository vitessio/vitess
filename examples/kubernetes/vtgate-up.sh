#!/bin/bash

# This is an example script that starts a vtgate replicationController.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}

vtdataroot_volume='{emptyDir: {}}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="{hostDir: {path: ${VTDATAROOT_VOLUME}}}"
fi

replicas=$VTGATE_REPLICAS

echo "Creating vtgate service..."
$KUBECTL create -f vtgate-service.yaml

sed_script=""
for var in replicas vtdataroot_volume; do
  sed_script+="s,{{$var}},${!var},g;"
done

echo "Creating vtgate replicationController..."
cat vtgate-controller-template.yaml | sed -e "$sed_script" | $KUBECTL create -f -
