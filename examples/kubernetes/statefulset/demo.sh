#!/bin/bash

. $(dirname ${BASH_SOURCE})/util.sh

release=vitess
bucket=enisoc-vitess-backups

desc 'Install Vitess Helm chart'
run "helm install -n $release -f site-values.yaml ./helm/vitess"

desc 'Wait for StatefulSets... (check out vtctld URL above)'
run 'kubectl get statefulsets -l app=vitess --watch'

desc 'Initialize Vitess shards'
run './init-vitess.sh'

desc 'Start Vitess load test'
run 'kubectl create -f loadtest.yaml'

desc 'Scale StatefulSet'
run 'kubectl get statefulsets'

desc 'Scale up to 3'
run 'kubectl scale statefulset zone1-main-80-x-replica --replicas=3'

desc 'Go check out traffic graphs on tablet status pages...'
run 'kubectl get pod zone1-main-80-x-replica-2 --watch'

desc 'Disable StatefulSet to prevent Pod recreation'
run 'kubectl delete statefulset zone1-main-80-x-replica --cascade=false'

desc 'Force-delete MySQL master Pod'
run 'kubectl delete pod zone1-main-80-x-replica-0 --force --grace-period=0'

desc 'Clean up'
run 'kubectl delete -f loadtest.yaml'
release=$(helm list -q | grep '^vitess')
run "helm delete --purge $release"
run 'kubectl delete pod,pvc -l app=vitess'
run "gsutil -m rm gs://$bucket/**"

desc 'Have a nice day!'

