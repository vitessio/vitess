#!/bin/bash

# Tears down the container engine cluster, removes rules/pools

GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}

base_ssd_name="$GKE_CLUSTER_NAME-vt-ssd-"

gcloud container clusters delete $GKE_CLUSTER_NAME -z $GKE_ZONE -q

num_ssds=`gcloud compute disks list | awk -v name="$base_ssd_name" -v zone=$GKE_ZONE '$1~name && $2==zone' | wc -l`
for i in `seq 1 $num_ssds`; do
  gcloud compute disks delete $base_ssd_name$i --zone $GKE_ZONE -q
done

