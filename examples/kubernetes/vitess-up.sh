#!/bin/bash

# This is an example script that creates a fully functional vitess cluster.
# It performs the following steps:
# 1. Create etcd clusters
# 2. Create vtctld clusters
# 3. Forward vtctld port
# 4. Create vttablet clusters
# 5. Perform vtctl initialization:
#      SetKeyspaceShardingInfo, Rebuild Keyspace, Reparent Shard, Apply Schema
# 6. Create vtgate clusters
# 7. Forward vtgate port

# Customizable parameters
GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
RDONLY_COUNT=${RDONLY_COUNT:-0}
MAX_TASK_WAIT_RETRIES=${MAX_TASK_WAIT_RETRIES:-300}
MAX_VTTABLET_TOPO_WAIT_RETRIES=${MAX_VTTABLET_TOPO_WAIT_RETRIES:-180}
VTTABLET_TEMPLATE=${VTTABLET_TEMPLATE:-'vttablet-pod-benchmarking-template.yaml'}
VTGATE_TEMPLATE=${VTGATE_TEMPLATE:-'vtgate-controller-benchmarking-template.yaml'}
VTGATE_COUNT=${VTGATE_COUNT:-0}
VTDATAROOT_VOLUME=${VTDATAROOT_VOLUME:-''}
CELLS=${CELLS:-'test'}
KEYSPACE=${KEYSPACE:-'test_keyspace'}

cells=`echo $CELLS | tr ',' ' '`
num_cells=`echo $cells | wc -w`

# Get region from zone (everything to last dash)
gke_region=`echo $GKE_ZONE | sed "s/-[^-]*$//"`

num_shards=`echo $SHARDS | tr "," " " | wc -w`
total_tablet_count=$(($num_shards*$TABLETS_PER_SHARD*$num_cells))
vtgate_count=$VTGATE_COUNT
if [ $vtgate_count -eq 0 ]; then
  vtgate_count=$(($total_tablet_count/4>3?$total_tablet_count/4:3))
fi

# export for vttablet scripts
export SHARDS=$SHARDS
export TABLETS_PER_SHARD=$TABLETS_PER_SHARD
export RDONLY_COUNT=$RDONLY_COUNT
export VTDATAROOT_VOLUME=$VTDATAROOT_VOLUME
export VTGATE_TEMPLATE=$VTGATE_TEMPLATE
export VTTABLET_TEMPLATE=$VTTABLET_TEMPLATE
export VTGATE_REPLICAS=$vtgate_count

function update_spinner_value () {
  spinner='-\|/'
  cur_spinner=${spinner:$(($1%${#spinner})):1}
}

function wait_for_running_tasks () {
  # This function waits for pods to be in the "Running" state
  # 1. task_name: Name that the desired task begins with
  # 2. num_tasks: Number of tasks to wait for
  # Returns:
  #   0 if successful, -1 if timed out
  task_name=$1
  num_tasks=$2
  counter=0

  echo "Waiting for ${num_tasks}x $task_name to enter state Running"

  while [ $counter -lt $MAX_TASK_WAIT_RETRIES ]; do
    # Get status column of pods with name starting with $task_name,
    # count how many are in state Running
    num_running=`$KUBECTL get pods | grep ^$task_name | grep Running | wc -l`

    echo -en "\r$task_name: $num_running out of $num_tasks in state Running..."
    if [ $num_running -eq $num_tasks ]
    then
      echo Complete
      return 0
    fi
    update_spinner_value $counter
    echo -n $cur_spinner
    let counter=counter+1
    sleep 1
  done
  echo Timed out
  return -1
}

if [ -z "$GOPATH" ]; then
  echo "ERROR: GOPATH undefined, can't obtain vtctlclient"
  exit -1
fi

export KUBECTL='kubectl'
go get github.com/youtube/vitess/go/cmd/vtctlclient

echo "****************************"
echo "*Creating vitess cluster:"
echo "*  Zone: $GKE_ZONE"
echo "*  Shards: $SHARDS"
echo "*  Tablets per shard: $TABLETS_PER_SHARD"
echo "*  Rdonly per shard: $RDONLY_COUNT"
echo "*  VTGate count: $vtgate_count"
echo "*  Cells: $cells"
echo "****************************"

echo 'Running etcd-up.sh' && CELLS=$CELLS ./etcd-up.sh
wait_for_running_tasks etcd-global 3
for cell in $cells; do
  wait_for_running_tasks etcd-$cell 3
done

echo 'Running vtctld-up.sh' && ./vtctld-up.sh

wait_for_running_tasks vtctld 1
echo Creating firewall rule for vtctld...
vtctld_port=30001
gcloud compute firewall-rules create ${GKE_CLUSTER_NAME}-vtctld --allow tcp:$vtctld_port
kvtctl="./kvtctl.sh"

if [ $num_shards -gt 0 ]
then
  echo Calling CreateKeyspace and SetKeyspaceShardingInfo
  $kvtctl CreateKeyspace -force $KEYSPACE
  $kvtctl SetKeyspaceShardingInfo -force -split_shard_count $num_shards $KEYSPACE keyspace_id uint64
fi

echo 'Running vttablet-up.sh' && CELLS=$CELLS ./vttablet-up.sh
echo 'Running vtgate-up.sh' && ./vtgate-up.sh
wait_for_running_tasks vttablet $total_tablet_count
wait_for_running_tasks vtgate $vtgate_count


echo Waiting for tablets to be visible in the topology
counter=0
while [ $counter -lt $MAX_VTTABLET_TOPO_WAIT_RETRIES ]; do
  num_tablets=0
  for cell in $cells; do
    num_tablets=$(($num_tablets+`$kvtctl ListAllTablets $cell | grep $KEYSPACE | wc -l`))
  done
  echo -en "\r$num_tablets out of $total_tablet_count in topology..."
  if [ $num_tablets -eq $total_tablet_count ]
  then
    echo Complete
    break
  fi
  update_spinner_value $counter
  echo -n $cur_spinner
  let counter=counter+1
  sleep 1
  if [ $counter -eq $MAX_VTTABLET_TOPO_WAIT_RETRIES ]
  then
    echo Timed out
  fi
done

# split_shard_count = num_shards for sharded keyspace, 0 for unsharded
split_shard_count=$num_shards
if [ $split_shard_count -eq 1 ]; then
  split_shard_count=0
fi

echo -n Setting Keyspace Sharding Info...
$kvtctl SetKeyspaceShardingInfo -force -split_shard_count $split_shard_count $KEYSPACE keyspace_id uint64
echo Done
echo -n Rebuilding Keyspace Graph...
$kvtctl RebuildKeyspaceGraph $KEYSPACE
echo Done
echo -n Reparenting...
shard_num=1
master_cell=`echo $cells | awk '{print $1}'`
for shard in $(echo $SHARDS | tr "," " "); do
  [[ $num_cells -gt 1 ]] && cell_id=10 || cell_id=00
  printf -v master_tablet_id '%s-%02d0000%02d00' $master_cell $cell_id $shard_num
  $kvtctl InitShardMaster -force $KEYSPACE/$shard $master_tablet_id
  let shard_num=shard_num+1
done
echo Done
echo -n Applying Schema...
$kvtctl ApplySchema -sql "$(cat create_test_table.sql)" $KEYSPACE
echo Done

echo Creating firewall rule for vtgate
vtgate_port=15001
gcloud compute firewall-rules create ${GKE_CLUSTER_NAME}-vtgate --allow tcp:$vtgate_port
vtgate_pool=`util/get_forwarded_pool.sh $GKE_CLUSTER_NAME $gke_region $vtgate_port`
vtgate_ip=`gcloud compute forwarding-rules list | grep $vtgate_pool | awk '{print $3}'`
vtgate_server="$vtgate_ip:$vtgate_port"

if [ -n "$NEWRELIC_LICENSE_KEY" ]; then
  echo Setting up Newrelic monitoring
  i=1
  for nodename in `$KUBECTL get nodes --no-headers | awk '{print $1}'`; do
    gcloud compute copy-files newrelic.sh $nodename:~/
    gcloud compute copy-files newrelic_start_agent.sh $nodename:~/
    gcloud compute copy-files newrelic_start_mysql_plugin.sh $nodename:~/
    gcloud compute ssh $nodename --command "bash -c '~/newrelic.sh ${NEWRELIC_LICENSE_KEY} ${VTDATAROOT}'"
    let i=i+1
  done
fi

echo "****************************"
echo "* Complete!"
echo "* Access the vtctld web UI by performing the following steps:"
echo "*   $ kubectl proxy --port=8001"
echo "*   Visit http://localhost:8001/api/v1/proxy/namespaces/default/services/vtctld:web/"
echo "* vtgate: $vtgate_server"
echo "****************************"
