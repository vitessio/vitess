#!/bin/bash

# Tears down the container engine cluster, removes rules/pools
GKE_CLUSTER_NAME=${GKE_CLUSTER_NAME:-'example'}
gcloud preview container clusters delete $GKE_CLUSTER_NAME
gcloud compute forwarding-rules delete vtctld -q --region=us-central1
gcloud compute forwarding-rules delete vtgate -q --region=us-central1
gcloud compute firewall-rules delete vtctld -q
gcloud compute firewall-rules delete vtgate -q
gcloud compute target-pools delete vtctld -q --region=us-central1
gcloud compute target-pools delete vtgate -q --region=us-central1
