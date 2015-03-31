#!/bin/bash

# Tears down the container engine cluster, removes rules/pools

GKE_ZONE=${GKE_ZONE:-'us-central1-b'}
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}

# Get the region from the zone (everything up to last dash)
gke_region=`echo $GKE_ZONE | sed "s/-[^-]*$//"`
base_ssd_name="$GKE_CLUSTER_NAME-vt-ssd-"

gcloud preview container clusters delete $GKE_CLUSTER_NAME

num_ssds=`gcloud compute disks list | awk -v name="$base_ssd_name" -v zone=$GKE_ZONE '$1~name && $2==zone' | wc -l`
for i in `seq 1 $num_ssds`; do
  gcloud compute disks delete $base_ssd_name$i --zone $GKE_ZONE -q
done

vtctld="k8s-${GKE_CLUSTER_NAME}-default-vtctld"
vtgate="k8s-${GKE_CLUSTER_NAME}-default-vtgate"

gcloud compute forwarding-rules delete $vtctld -q --region=$gke_region
gcloud compute forwarding-rules delete $vtgate -q --region=$gke_region
gcloud compute firewall-rules delete vtctld -q
gcloud compute firewall-rules delete vtgate -q
gcloud compute target-pools delete $vtctld -q --region=$gke_region
gcloud compute target-pools delete $vtgate -q --region=$gke_region
