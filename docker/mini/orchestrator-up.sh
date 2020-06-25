#!/bin/bash

source ./env.sh

echo "- Configuring orchestrator with given topology server and credentials..."
cp /etc/orchestrator.conf.json /tmp/
sed -i /tmp/orchestrator.conf.json -e "s/DISCOVERY_SEED_PLACEHOLDER/$TOPOLOGY_SERVER/g"
sed -i /tmp/orchestrator.conf.json -e "s/MYSQL_TOPOLOGY_USER_PLACEHOLDER/$TOPOLOGY_USER/g"
sed -i /tmp/orchestrator.conf.json -e "s/MYSQL_TOPOLOGY_PASSWORD_PLACEHOLDER/$TOPOLOGY_PASSWORD/g"

cat /tmp/orchestrator.conf.json > /etc/orchestrator.conf.json
rm /tmp/orchestrator.conf.json

ORCHESTRATOR_LOG="${VTDATAROOT}/tmp/orchestrator.out"

echo "- Starting orchestrator... Logfile is $ORCHESTRATOR_LOG"

cd /usr/local/orchestrator
./orchestrator http > $ORCHESTRATOR_LOG 2>&1 &
