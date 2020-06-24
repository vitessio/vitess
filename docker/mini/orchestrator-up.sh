#!/bin/bash

source ./env.sh

if [ -z "$TOPOLOGY_SERVER" ] ; then
  echo "TOPOLOGY_SERVER environment variable not found. You must specify a MySQL server within your topology."
  echo "This mini-vitess setup will attempt to discover your topology using that server and will bootstrap to match your topology"
  exit 1
fi
if [ -z "$TOPOLOGY_USER" ] ; then
  echo "TOPOLOGY_USER environment variable not found. You must specify a MySQL username accessible to vitess."
  exit 1
fi
if [ -z "$TOPOLOGY_PASSWORD" ] ; then
  echo "TOPOLOGY_PASSWORD environment variable not found. You must specify a MySQL password accessible to vitess."
  exit 1
fi

cp /etc/orchestrator.conf.json /tmp/
sed -i /tmp/orchestrator.conf.json -e "s/DISCOVERY_SEED_PLACEHOLDER/$TOPOLOGY_SERVER/g"
sed -i /tmp/orchestrator.conf.json -e "s/MYSQL_TOPOLOGY_USER_PLACEHOLDER/$TOPOLOGY_USER/g"
sed -i /tmp/orchestrator.conf.json -e "s/MYSQL_TOPOLOGY_PASSWORD_PLACEHOLDER/$TOPOLOGY_PASSWORD/g"

cat /tmp/orchestrator.conf.json > /etc/orchestrator.conf.json
rm /tmp/orchestrator.conf.json

ORCHESTRATOR_LOG="${VTDATAROOT}/tmp/orchestrator.out"

echo "Starting orchestrator... Logfile is $ORCHESTRATOR_LOG"

cd /usr/local/orchestrator
./orchestrator http > $ORCHESTRATOR_LOG 2>&1 &
