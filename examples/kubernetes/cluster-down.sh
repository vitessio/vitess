#!/bin/bash

# Tears down the container engine cluster, removes rules/pools
GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
GKE_NUM_NODES=${GKE_NUM_NODES:-3}
GKE_SSD_SIZE_GB=${GKE_SSD_SIZE_GB:-0}

# Get the region from the zone (everything up to last dash)
gke_region=`echo $GKE_ZONE | sed "s/-[^-]*$//"`

gcloud preview container clusters delete $GKE_CLUSTER_NAME

if [ $GKE_SSD_SIZE_GB -gt 0 ]
then
  for i in `seq 1 $GKE_NUM_NODES`; do
    gcutil deletedisk -f $GKE_CLUSTER_NAME-vt-ssd-$i
  done
fi

gcloud compute forwarding-rules delete vtctld -q --region=$gke_region
gcloud compute forwarding-rules delete vtgate -q --region=$gke_region
gcloud compute firewall-rules delete vtctld -q
gcloud compute firewall-rules delete vtgate -q
gcloud compute target-pools delete vtctld -q --region=$gke_region
gcloud compute target-pools delete vtgate -q --region=$gke_region
