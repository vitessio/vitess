#!/bin/bash

# This is an example script that creates a fully functional vitess cluster.
# It performs the following steps:
# 1. Create a container engine cluster
# 2. Create etcd clusters
# 3. Create vtctld clusters
# 4. Forward vtctld port
# 5. Create vttablet clusters
# 6. Perform vtctl initialization:
#      SetKeyspaceShardingInfo, Rebuild Keyspace, Reparent Shard, Apply Schema
# 7. Create vtgate clusters
# 8. Forward vtgate port

# Customizable parameters
GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_MACHINE_TYPE=${GKE_MACHINE_TYPE:-'n1-standard-1'}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
GKE_SSD_SIZE_GB=${GKE_SSD_SIZE_GB:-0}
SHARDS=${SHARDS:-'-80,80-'}
TABLETS_PER_SHARD=${TABLETS_PER_SHARD:-3}
MAX_TASK_WAIT_RETRIES=${MAX_TASK_WAIT_RETRIES:-300}
MAX_VTTABLET_TOPO_WAIT_RETRIES=${MAX_VTTABLET_TOPO_WAIT_RETRIES:-180}
BENCHMARK_CLUSTER=${BENCHMARK_CLUSTER:-true}
VTGATE_COUNT=${VTGATE_COUNT:-0}

# Get region from zone (everything to last dash)
gke_region=`echo $GKE_ZONE | sed "s/-[^-]*$//"`
vttablet_template='vttablet-pod-template.yaml'
vtgate_script='vtgate-up.sh'
if $BENCHMARK_CLUSTER; then
  vttablet_template='vttablet-pod-benchmarking-template.yaml'
  vtgate_script='vtgate-benchmarking-up.sh'
fi

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
  params="${@:2}"
  echo "Running ${script}..."
  eval $params ./$script
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
gcloud config set compute/zone $GKE_ZONE
project_id=`gcloud config list project | sed -n 2p | cut -d " " -f 3`
num_shards=`echo $SHARDS | tr "," " " | wc -w`
total_tablet_count=$(($num_shards*$TABLETS_PER_SHARD))
vtgate_count=$VTGATE_COUNT
if [ $vtgate_count -eq 0 ]; then
  vtgate_count=$(($total_tablet_count/4>3?$total_tablet_count/4:3))
fi
num_nodes=$(($total_tablet_count+$vtgate_count))

echo "****************************"
echo "*Creating cluster:"
echo "*  Zone: $GKE_ZONE"
echo "*  Machine type: $GKE_MACHINE_TYPE"
echo "*  Num nodes: $num_nodes"
echo "*  SSD Size: $GKE_SSD_SIZE_GB"
echo "*  Shards: $SHARDS"
echo "*  Tablets per shard: $TABLETS_PER_SHARD"
echo "*  VTGate count: $vtgate_count"
echo "*  Cluster name: $GKE_CLUSTER_NAME"
echo "*  Project ID: $project_id"
echo "****************************"
gcloud alpha container clusters create $GKE_CLUSTER_NAME --machine-type $GKE_MACHINE_TYPE --num-nodes $num_nodes
gcloud config set container/cluster $GKE_CLUSTER_NAME

# We label the nodes so that we can force a 1:1 relationship between vttablets and nodes
for i in `seq 1 $num_nodes`; do
  for j in `seq 0 2`; do
    $KUBECTL label nodes k8s-$GKE_CLUSTER_NAME-node-${i} id=$i
    result=`$KUBECTL get nodes | grep id=$i`
    if [ -n "$result" ]; then
      break
    fi
    sleep 1
  done
done

if [ $GKE_SSD_SIZE_GB -gt 0 ]
then
  export VTDATAROOT_VOLUME='/ssd'
  echo Creating SSDs and attaching to container engine nodes
  for i in `seq 1 $num_nodes`; do
    diskname=$GKE_CLUSTER_NAME-vt-ssd-$i
    nodename=k8s-$GKE_CLUSTER_NAME-node-$i
    gcloud compute disks create $diskname --type=pd-ssd --size=${GKE_SSD_SIZE_GB}GB
    gcloud compute instances attach-disk $nodename --disk $diskname
    gcloud compute ssh $nodename --zone=$GKE_ZONE --command "sudo mkdir /ssd; sudo /usr/share/google/safe_format_and_mount -m \"mkfs.ext4 -F\" /dev/disk/by-id/google-persistent-disk-1 ${VTDATAROOT_VOLUME}" &
  done
  wait
fi

run_script etcd-up.sh
wait_for_running_tasks etcd-global 3
wait_for_running_tasks etcd-test 3

run_script vtctld-up.sh
run_script vttablet-up.sh FORCE_NODE=true VTTABLET_TEMPLATE=$vttablet_template
run_script $vtgate_script STARTING_INDEX=$total_tablet_count VTGATE_REPLICAS=$vtgate_count

wait_for_running_tasks vtctld 1
wait_for_running_tasks vttablet $total_tablet_count
wait_for_running_tasks vtgate $vtgate_count

echo Creating firewall rule for vtctld...
vtctld_port=15000
gcloud compute firewall-rules create ${GKE_CLUSTER_NAME}-vtctld --allow tcp:$vtctld_port
vtctld_pool=`util/get_forwarded_pool.sh $GKE_CLUSTER_NAME $gke_region $vtctld_port`
vtctld_ip=`gcloud compute forwarding-rules list | grep $vtctld_pool | awk '{print $3}'`
vtctl_server="$vtctld_ip:$vtctld_port"
kvtctl="$GOPATH/bin/vtctlclient -server $vtctl_server"

echo Waiting for tablets to be visible in the topology
counter=0
while [ $counter -lt $MAX_VTTABLET_TOPO_WAIT_RETRIES ]; do
  num_tablets=`$kvtctl ListAllTablets test | wc -l`
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
$kvtctl SetKeyspaceShardingInfo -force -split_shard_count $split_shard_count test_keyspace keyspace_id uint64
echo Done
echo -n Rebuilding Keyspace Graph...
$kvtctl RebuildKeyspaceGraph test_keyspace
echo Done
echo -n Reparenting...
shard_num=1
for shard in $(echo $SHARDS | tr "," " "); do
  $kvtctl InitShardMaster -force test_keyspace/$shard test-0000000${shard_num}00
  let shard_num=shard_num+1
done
echo Done
echo -n Applying Schema...
$kvtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
echo Done

echo Creating firewall rule for vtgate
vtgate_port=15001
gcloud compute firewall-rules create ${GKE_CLUSTER_NAME}-vtgate --allow tcp:$vtgate_port
vtgate_pool=`util/get_forwarded_pool.sh $GKE_CLUSTER_NAME $gke_region $vtgate_port`
vtgate_ip=`gcloud compute forwarding-rules list | grep $vtgate_pool | awk '{print $3}'`
vtgate_server="$vtgate_ip:$vtgate_port"

if [ -n "$NEWRELIC_LICENSE_KEY" -a $GKE_SSD_SIZE_GB -gt 0 ]; then
  for i in `seq 1 $num_nodes`; do
    nodename=k8s-$GKE_CLUSTER_NAME-node-${i}
    gcloud compute copy-files newrelic.sh $nodename:~/
    gcloud compute copy-files newrelic_start_agent.sh $nodename:~/
    gcloud compute copy-files newrelic_start_mysql_plugin.sh $nodename:~/
    gcloud compute ssh $nodename --command "bash -c '~/newrelic.sh ${NEWRELIC_LICENSE_KEY}'"
  done
fi

echo "****************************"
echo "* Complete!"
echo "* Use the following line to make an alias to kvtctl:"
echo "* alias kvtctl='\$GOPATH/bin/vtctlclient -server $vtctl_server'"
echo "* vtctld: [http://${vtctl_server}]"
echo "* vtgate: $vtgate_server"
echo "****************************"
