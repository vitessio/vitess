#!/bin/bash

set -e

KUBECTL=${KUBECTL:-kubectl}

config_path=${KEYTAR_CONFIG_PATH:-"./config"}
port=${KEYTAR_PORT:-8080}
password=${KEYTAR_PASSWORD:-"defaultkey"}
config=${KEYTAR_CONFIG:-"/config/vitess_config.yaml"}

sed_script=""
for var in config_path port config password; do
  sed_script+="s,{{$var}},${!var},g;"
done

gcloud container clusters create keytar --machine-type n1-standard-4 --num-nodes 1 --scopes cloud-platform --zone us-central1-b

echo "Creating keytar configmap"
$KUBECTL create configmap --from-file=$config_path config

echo "Creating keytar service"
$KUBECTL create -f keytar-service.yaml

echo "Creating keytar controller"
cat keytar-controller-template.yaml | sed -e "$sed_script" | $KUBECTL create -f -

echo "Creating firewall-rule"
gcloud compute firewall-rules create keytar --allow tcp:80

for i in `seq 1 20`; do
  ip=`$KUBECTL get service keytar -o template --template '{{if ge (len .status.loadBalancer) 1}}{{index (index .status.loadBalancer.ingress 0) "ip"}}{{end}}'`
  if [[ -n "$ip" ]]; then
    echo "Keytar address: http://${ip}:80"
    break
  fi
  echo "Waiting for keytar external IP"
  sleep 10
done
