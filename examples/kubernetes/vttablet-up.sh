#!/bin/bash

# This is an example script that creates a vttablet deployment.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Create the pods for shard-0
CELLS=${CELLS:-'test'}
keyspace='test_keyspace'
SHARDS=${SHARDS:-'0'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-5}
port=15002
grpc_port=16002
UID_BASE=${UID_BASE:-100}
VTTABLET_TEMPLATE=${VTTABLET_TEMPLATE:-'vttablet-pod-template.yaml'}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}
RDONLY_COUNT=${RDONLY_COUNT:-2}
VITESS_NAME=${VITESS_NAME:-'default'}

vtdataroot_volume='emptyDir: {}'
if [ -n "$VTDATAROOT_VOLUME" ]; then
  vtdataroot_volume="hostPath: {path: ${VTDATAROOT_VOLUME}}"
fi

uid_base=$UID_BASE
indices=${TASKS:-`seq 0 $(($TABLETS_PER_SHARD-1))`}
cells=`echo $CELLS | tr ',' ' '`
num_cells=`echo $cells | wc -w`

for shard in $(echo $SHARDS | tr "," " "); do
  [[ $num_cells -gt 1 ]] && cell_index=100000000 || cell_index=0
  for cell in $cells; do
    echo "Creating $keyspace.shard-$shard pods in cell $cell..."
    for uid_index in $indices; do
      uid=$[$uid_base + $uid_index + $cell_index]
      printf -v alias '%s-%010d' $cell $uid
      printf -v tablet_subdir 'vt_%010d' $uid

      echo "Creating pod for tablet $alias..."

      # Add xx to beginning or end if there is a dash.  K8s does not allow for
      # leading or trailing dashes for labels
      shard_label=`echo $shard | sed s'/[-]$/-xx/' | sed s'/^-/xx-/'`

      tablet_type=replica
      if [ $uid_index -gt $(($TABLETS_PER_SHARD-$RDONLY_COUNT-1)) ]; then
        tablet_type=rdonly
      fi

      # Expand template variables
      sed_script=""
      for var in vitess_image alias cell uid keyspace shard shard_label port grpc_port tablet_subdir vtdataroot_volume tablet_type backup_flags; do
        sed_script+="s,{{$var}},${!var},g;"
      done

      # Instantiate template and send to kubectl.
      cat $VTTABLET_TEMPLATE | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -
    done
    let cell_index=cell_index+100000000
  done
  let uid_base=uid_base+100
done
