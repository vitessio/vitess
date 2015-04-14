#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Create the pods for shard-0
cell='test'
keyspace='test_keyspace'
SHARDS=${SHARDS:-'0'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
port=15002
uid_base=100
FORCE_NODE=${FORCE_NODE:-false}
VTTABLET_TEMPLATE=${VTTABLET_TEMPLATE:-'vttablet-pod-template.yaml'}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}

vtdataroot_volume='{emptyDir: {}}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="{hostDir: {path: ${VTDATAROOT_VOLUME}}}"
fi

index=1
for shard in $(echo $SHARDS | tr "," " "); do
  echo "Creating $keyspace.shard-$shard pods in cell $cell..."
  for uid_index in `seq 0 $(($TABLETS_PER_SHARD-1))`; do
    uid=$[$uid_base + $uid_index]
    printf -v alias '%s-%010d' $cell $uid
    printf -v tablet_subdir 'vt_%010d' $uid

    echo "Creating pod for tablet $alias..."

    # Add xx to beginning or end if there is a dash.  K8s does not allow for
    # leading or trailing dashes for labels
    shard_label=`echo $shard | sed s'/[-]$/-xx/' | sed s'/^-/xx-/'`

    # Expand template variables
    sed_script=""
    for var in alias cell uid keyspace shard shard_label port tablet_subdir vtdataroot_volume; do
      sed_script+="s,{{$var}},${!var},g;"
    done

    # Add node selector to the end if a vttablet should be on a specific node.
    # Note: this is a workaround until Kubernetes supports the ability to
    # specify resource constraints.  This method requires nodes to be labeled
    # with an ascending id.
    if [ "$FORCE_NODE" = true ]
    then
      sed_script+="\$anodeSelector:\n  id: \"$index\""
    fi

    # Instantiate template and send to kubectl.
    cat $VTTABLET_TEMPLATE | sed -e "$sed_script" | $KUBECTL create -f -

    let index=index+1
  done
  let uid_base=uid_base+100
done
