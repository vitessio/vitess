#!/bin/bash

KUBECTL=${KUBECTL:-kubectl}

$KUBECTL delete replicationcontroller keytar
$KUBECTL delete service keytar
$KUBECTL delete configmap config
gcloud container clusters delete keytar -z us-central1-b -q
gcloud compute firewall-rules delete keytar -q
