#!/bin/bash

# This is an example script that creates a fully functional vitess cluster.
# It performs the following steps:
# 1. Create a container engine cluster
# 2. Create etcd clusters
# 3. Create vtctld clusters
# 4. Forward vtctld port
# 5. Create vttablet clusters
# 6. Perform vtctl initialization:
#      Rebuild Keyspace, Reparent Shard, Apply Schema
# 7. Create vtgate clusters
# 8. Forward vtgate port

# Customizable parameters
GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_MACHINE_TYPE=${GKE_MACHINE_TYPE:-'n1-standard-1'}
GKE_NUM_NODES=${GKE_NUM_NODES:-3}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
GKE_SSD_SIZE_GB=${GKE_SSD_SIZE_GB:-0}
SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
MAX_TASK_WAIT_RETRIES=${MAX_TASK_WAIT_RETRIES:-300}
MAX_VTTABLET_TOPO_WAIT_RETRIES=${MAX_VTTABLET_TOPO_WAIT_RETRIES:-180}

# export for vttablet scripts
export SHARDS=$SHARDS
export TABLETS_PER_SHARD=$TABLETS_PER_SHARD

function update_spinner_value () {
  spinner='-\|/'
  cur_spinner=${spinner:$(($1%${#spinner})):1}
}

function run_script () {
  # Parameters:
  # 1. script: Name of the script to execute
  script=$1
  echo "Running ${script}..."
  ./$script
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
    statuses=`$KUBECTL get pods | awk '$1 ~ /^'"$task_name"'/ {print $7}'`
    num_running=`grep -o "Running" <<< "$statuses" | wc -l`

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

export KUBECTL='gcloud preview container kubectl'
go get github.com/youtube/vitess/go/cmd/vtctlclient
gcloud config set compute/zone $GKE_ZONE
project_id=`gcloud config list project | sed -n 2p | cut -d " " -f 3`

echo "****************************"
echo "*Creating cluster:"
echo "*  Zone: $GKE_ZONE"
echo "*  Machine type: $GKE_MACHINE_TYPE"
echo "*  Num nodes: $GKE_NUM_NODES"
echo "*  SSD Size: $GKE_SSD_SIZE_GB"
echo "*  Shards: $SHARDS"
echo "*  Tablets per shard: $TABLETS_PER_SHARD"
echo "*  Cluster name: $GKE_CLUSTER_NAME"
echo "*  Project ID: $project_id"
echo "****************************"
gcloud preview container clusters create $GKE_CLUSTER_NAME --machine-type $GKE_MACHINE_TYPE --num-nodes $GKE_NUM_NODES

if [ $GKE_SSD_SIZE_GB -gt 0 ]
then
  echo Creating SSDs and attaching to container engine nodes
  for i in `seq 1 $GKE_NUM_NODES`; do
    diskname=$GKE_CLUSTER_NAME-vt-ssd-$i
    gcutil adddisk --zone $GKE_ZONE --disk_type=pd-ssd --size_gb $GKE_SSD_SIZE_GB $diskname
    gcutil attachdisk --disk $diskname k8s-$GKE_CLUSTER_NAME-node-$i
  done
fi

run_script etcd-up.sh
wait_for_running_tasks etcd 6

run_script vtctld-up.sh
run_script vttablet-up.sh
run_script vtgate-up.sh

num_shards=`echo $SHARDS | tr "," " " | wc -w`
total_tablets=$(($num_shards*$TABLETS_PER_SHARD))

wait_for_running_tasks vtctld 1
wait_for_running_tasks vttablet $total_tablets
wait_for_running_tasks vtgate 3

echo Creating firewall rule for vtctld...
vtctl_port=15000
gcloud compute firewall-rules create vtctld --allow tcp:$vtctl_port
vtctl_ip=`gcloud compute forwarding-rules list | awk '$1=="vtctld" {print $3}'`
vtctl_server="$vtctl_ip:$vtctl_port"
kvtctl="$GOPATH/bin/vtctlclient -server $vtctl_server"

echo Waiting for tablets to be visible in the topology
counter=0
while [ $counter -lt $MAX_VTTABLET_TOPO_WAIT_RETRIES ]; do
  num_tablets=`$kvtctl ListAllTablets test | wc -l`
  echo -en "\r$num_tablets out of $total_tablets in topology..."
  if [ $num_tablets -eq $total_tablets ]
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
$kvtctl SetKeyspaceShardingInfo -force -split_shard_count $split_shard_count test_keyspace keyspace_id uint64
echo Done
echo -n Rebuilding Keyspace Graph...
$kvtctl RebuildKeyspaceGraph test_keyspace
echo Done
echo -n Reparenting...
shard_num=1
for shard in $(echo $SHARDS | tr "," " "); do
  $kvtctl ReparentShard -force test_keyspace/$shard test-0000000${shard_num}00
  let shard_num=shard_num+1
done
echo Done
echo -n Applying Schema...
$kvtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
echo Done

echo Creating firewall rule for vtgate
vtgate_port=15001
gcloud compute firewall-rules create vtgate --allow tcp:$vtgate_port
vtgate_ip=`gcloud compute forwarding-rules list | awk '$1=="vtgate" {print $3}'`
if [ -z "$vtgate_ip" ]
then
  vtgate_server="No firewall rules created for vtgate. Add createExternalLoadBalancer: true if access to vtgate is desired"
else
  vtgate_server="$vtgate_ip:$vtgate_port"
fi

echo "****************************"
echo "* Complete!"
echo "* Use the following line to make an alias to kvtctl:"
echo "* alias kvtctl='\$GOPATH/bin/vtctlclient -server $vtctl_server'"
echo "* vtctld: [http://${vtctl_server}]"
echo "* vtgate: $vtgate_server"
echo "****************************"
