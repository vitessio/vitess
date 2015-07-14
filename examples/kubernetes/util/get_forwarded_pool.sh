#!/bin/bash

cluster_name=$1
region=$2
port=$3

target_pools=`gcloud compute target-pools list | awk 'NR>1 {print $1}'`
for pool in $target_pools; do
  if [ -n "`gcloud compute target-pools describe $pool --region $region | grep gke-$cluster_name`" ]; then
    if [ -n "`gcloud compute forwarding-rules describe $pool --region $region | grep "portRange: $port"`" ]; then
      echo $pool
      exit 0
    fi
  fi
done
exit -1
