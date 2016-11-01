#!/bin/bash

# This is an example script that starts a vtgate replicationcontroller.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

VTGATE_REPLICAS=${VTGATE_REPLICAS:-3}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}
VTGATE_TEMPLATE=${VTGATE_TEMPLATE:-'vtgate-controller-template.yaml'}
CELLS=${CELLS:-'test'}
VITESS_NAME=${VITESS_NAME:-'default'}

vtdataroot_volume='emptyDir: {}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="hostPath: {path: ${VTDATAROOT_VOLUME}}"
fi

replicas=$VTGATE_REPLICAS

cells=`echo $CELLS | tr ',' ' '`
for cell in $cells; do
  sed_script=""
  for var in cell; do
    sed_script+="s,{{$var}},${!var},g;"
  done

  echo "Creating vtgate service in cell $cell..."
  cat vtgate-service-template.yaml | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

  sed_script=""
  for var in vitess_image replicas vtdataroot_volume cell; do
    sed_script+="s,{{$var}},${!var},g;"
  done

  echo "Creating vtgate replicationcontroller in cell $cell..."
  cat $VTGATE_TEMPLATE | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -
done
