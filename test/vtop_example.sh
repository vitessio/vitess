#!/bin/bash

# Copyright 2022 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This test runs through the scripts in examples/operator to make sure they work.
# It should be kept in sync with the steps in https://vitess.io/docs/get-started/operator/
# So we can detect if a regression affecting a tutorial is introduced.


## This script should be called with kind and kubectl already installed

source build.env

# Use this to debug issues. It will print the commands as they run
# set -x
shopt -s expand_aliases
alias vtctlclient="vtctlclient --server=localhost:15999"
alias vtctldclient="vtctldclient --server=localhost:15999"
alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"

cd "$VTROOT"
unset VTROOT # ensure that the examples can run without VTROOT now.

function checkSemiSyncSetup() {
  for vttablet in $(kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "vttablet") ; do
    echo "Checking semi-sync in $vttablet"
    kubectl exec "$vttablet" -c mysqld -- mysql -S "/vt/socket/mysql.sock" -u root -e "show variables like 'rpl_semi_sync_slave_enabled'" | grep "OFF"
    if [ $? -ne 0 ]; then
      echo "Semi Sync setup on $vttablet"
      exit 1
    fi
  done
}

# checkPodStatusWithTimeout:
# $1: regex used to match pod names
# $2: number of pods to match (default: 1)
function checkPodStatusWithTimeout() {
  regex=$1
  nb=$2

  # Number of pods to match defaults to one
  if [ -z "$nb" ]; then
    nb=1
  fi

  # We use this for loop instead of `kubectl wait` because we don't have access to the full pod name
  # and `kubectl wait` does not support regex to match resource name.
  for i in {1..1200} ; do
    out=$(kubectl get pods)
    echo "$out" | grep -E "$regex" | wc -l | grep "$nb" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
      echo "$regex found"
      return
    fi
    sleep 1
  done
  echo -e "ERROR: checkPodStatusWithTimeout timeout to find pod matching:\ngot:\n$out\nfor regex: $regex"
  exit 1
}

# changeImageAndApply changes the vitess images in the yaml configuration
# to the ones produced down below before applying them
function changeImageAndApply() {
  file=$1
  sed 's/vitess\/lite:.*/vitess\/lite:pr/g' "$file" | sed 's/vitess\/vtadmin:.*/vitess\/vtadmin:pr/g' > temp.yaml
  kubectl apply -f temp.yaml
  rm temp.yaml
}

function waitForKeyspaceToBeServing() {
  ks=$1
  shard=$2
  nb_of_replica=$3
  for i in {1..600} ; do
    out=$(mysql --table --execute="show vitess_tablets")
    echo "$out" | grep -E "$ks(.*)$shard(.*)PRIMARY(.*)SERVING|$ks(.*)$shard(.*)REPLICA(.*)SERVING" | wc -l | grep "$((nb_of_replica+1))"
    if [ $? -eq 0 ]; then
      echo "Shard $ks/$shard is serving"
      return
    fi
    echo "Shard $ks/$shard is not fully serving, retrying (attempt #$i) ..."
    sleep 10
  done
}

function applySchemaWithRetry() {
  schema=$1
  ks=$2
  drop_sql=$3
  for i in {1..600} ; do
    vtctldclient ApplySchema --sql-file="$schema" $ks
    if [ $? -eq 0 ]; then
      return
    fi
    if [ -n "$drop_sql" ]; then
      mysql --table < $drop_sql
    fi
    echo "failed to apply schema $schema, retrying (attempt #$i) ..."
    sleep 1
  done
}

function printMysqlErrorFiles() {
  for vttablet in $(kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep "vttablet") ; do
    echo "Finding error.log file in $vttablet"
    kubectl logs "$vttablet" -c mysqld
    kubectl logs "$vttablet" -c vttablet
  done
}

function insertWithRetry() {
  for i in {1..600} ; do
    mysql --table < ./delete_commerce_data.sql && mysql --table < ../common/insert_commerce_data.sql
    if [ $? -eq 0 ]; then
      return
    fi
    echo "failed to insert commerce data, retrying (attempt #$i) ..."
    sleep 1
  done
}

function assertSelect() {
  sql=$1
  shard=$2
  expected=$3
  data=$(mysql --table < $sql)
  echo "$data" | grep "$expected" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo -e "The data in $shard's tables is incorrect, got:\n$data"
    exit 1
  fi
}

# get_started:
function get_started() {
    echo "Apply operator.yaml"
    changeImageAndApply "operator.yaml"
    checkPodStatusWithTimeout "vitess-operator(.*)1/1(.*)Running(.*)"

    echo "Apply 101_initial_cluster.yaml"
    changeImageAndApply "101_initial_cluster.yaml"
    checkPodStatusWithTimeout "example-zone1-vtctld(.*)1/1(.*)Running(.*)"
    checkPodStatusWithTimeout "example-zone1-vtgate(.*)1/1(.*)Running(.*)"
    checkPodStatusWithTimeout "example-etcd(.*)1/1(.*)Running(.*)" 3
    checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 2
    checkPodStatusWithTimeout "example-zone1-vtadmin(.*)2/2(.*)Running(.*)"
    checkPodStatusWithTimeout "example-commerce-x-x-zone1-vtorc(.*)1/1(.*)Running(.*)"

    sleep 10
    echo "Creating vschema and commerce SQL schema"

    ./pf.sh > /dev/null 2>&1 &
    sleep 5

    waitForKeyspaceToBeServing commerce - 1
    sleep 5

    applySchemaWithRetry create_commerce_schema.sql commerce drop_all_commerce_tables.sql
    vtctldclient ApplyVSchema --vschema-file="vschema_commerce_initial.json" commerce
    if [ $? -ne 0 ]; then
      echo "ApplySchema failed for initial commerce"
      printMysqlErrorFiles
      exit 1
    fi
    sleep 5

    echo "show databases;" | mysql | grep "commerce" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Could not find commerce database"
      printMysqlErrorFiles
      exit 1
    fi

    echo "show tables;" | mysql commerce | grep -E 'corder|customer|product' | wc -l | grep 3 > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Could not find commerce's tables"
      printMysqlErrorFiles
      exit 1
    fi

    insertWithRetry

    assertSelect ../common/select_commerce_data.sql "commerce" <<EOF
Using commerce
Customer
+-------------+--------------------+
| customer_id | email              |
+-------------+--------------------+
|           1 | alice@domain.com   |
|           2 | bob@domain.com     |
|           3 | charlie@domain.com |
|           4 | dan@domain.com     |
|           5 | eve@domain.com     |
+-------------+--------------------+
Product
+----------+-------------+-------+
| sku      | description | price |
+----------+-------------+-------+
| SKU-1001 | Monitor     |   100 |
| SKU-1002 | Keyboard    |    30 |
+----------+-------------+-------+
COrder
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        1 |           1 | SKU-1001 |   100 |
|        2 |           2 | SKU-1002 |    30 |
|        3 |           3 | SKU-1002 |    30 |
|        4 |           4 | SKU-1002 |    30 |
|        5 |           5 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
EOF
}

# verifyVtadminSetup verifies that we can query the vtadmin api end point
function verifyVtadminSetup() {
  # Verify the debug/env page can be curled and it contains the kubernetes environment variables like HOSTNAME
  curlGetRequestWithRetry "localhost:14001/debug/env" "HOSTNAME=example-zone1-vtadmin"
  # Verify the api/keyspaces page can be curled and it contains the name of the keyspace created
  curlGetRequestWithRetry "localhost:14001/api/keyspaces" "commerce"
  # Verify the other APIs work as well
  curlGetRequestWithRetry "localhost:14001/api/tablets" '"tablets":\[{"cluster":{"id":"example","name":"example"},"tablet":{"alias":{"cell":"zone1"'
  curlGetRequestWithRetry "localhost:14001/api/schemas" '"keyspace":"commerce","table_definitions":\[{"name":"corder","schema":"CREATE TABLE `corder` (\\n  `order_id` bigint NOT NULL AUTO_INCREMENT'
  # Verify that we are able to create a keyspace
  curlPostRequest "localhost:14001/api/keyspace/example" '{"name":"testKeyspace"}'
  # List the keyspaces and check that we have them both
  curlGetRequestWithRetry "localhost:14001/api/keyspaces" "commerce.*testKeyspace"
  # Try and delete the keyspace but this should fail because of the rbac rules
  curlDeleteRequest "localhost:14001/api/keyspace/example/testKeyspace" "unauthorized.*cannot.*delete.*keyspace"
  # We should still have both the keyspaces
  curlGetRequestWithRetry "localhost:14001/api/keyspaces" "commerce.*testKeyspace"
  # Delete the keyspace by using the vtctlclient
  vtctldclient DeleteKeyspace testKeyspace
  # Verify we still have the commerce keyspace and no other keyspace
  curlGetRequestWithRetry "localhost:14001/api/keyspaces" "commerce.*}}}}]"
}

# verifyVTOrcSetup verifies that VTOrc is running and repairing things that we mess up
function verifyVTOrcSetup() {
  # Set the primary tablet to readOnly using the vtctld and wait for VTOrc to repair
  primaryTablet=$(getPrimaryTablet)
  vtctldclient SetReadOnly "$primaryTablet"

  # Now that we have set the primary tablet to read only, we know that this will
  # only succeed if VTOrc is able to fix it
  tryInsert
  # We now delete the row because the move tables workflow assertions are not expecting this row to be present
  mysql -e "delete from customer where email = 'newemail@domain.com';"
}

function tryInsert() {
  for i in {1..600} ; do
    mysql -e "insert into customer(email) values('newemail@domain.com');"
    if [ $? -eq 0 ]; then
      return
    fi
    echo "failed to insert data, retrying (attempt #$i) ..."
    sleep 1
  done
}

# getPrimaryTablet returns the primary tablet
function getPrimaryTablet() {
  vtctlclient ListAllTablets | grep "primary" | awk '{print $1}'
}

function curlGetRequestWithRetry() {
  url=$1
  dataToAssert=$2
  for i in {1..600} ; do
    res=$(curl "$url")
    if [ $? -eq 0 ]; then
      echo "$res" | grep "$dataToAssert" > /dev/null 2>&1
      if [ $? -ne 0 ]; then
        echo -e "The data in $url is incorrect, got:\n$res"
        exit 1
      fi
      return
    fi
    echo "failed to query url $url, retrying (attempt #$i) ..."
    sleep 1
  done
}

function curlDeleteRequest() {
  url=$1
  dataToAssert=$2
  res=$(curl -X DELETE "$url")
  if [ $? -ne 0 ]; then
    echo -e "The DELETE request to $url failed\n"
    exit 1
  fi
  echo "$res" | grep "$dataToAssert" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo -e "The data in delete request to $url is incorrect, got:\n$res"
    exit 1
  fi
}

function curlPostRequest() {
  url=$1
  data=$2
  curl -X POST -d "$data" "$url"
  if [ $? -ne 0 ]; then
    echo -e "The POST request to $url with data $data failed\n"
    exit 1
  fi
}

function move_tables() {
  echo "Apply 201_customer_tablets.yaml"
  changeImageAndApply 201_customer_tablets.yaml
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 4

  killall kubectl
  ./pf.sh > /dev/null 2>&1 &

  waitForKeyspaceToBeServing customer - 1

  sleep 10

  vtctldclient LegacyVtctlCommand -- MoveTables --source commerce --tables 'customer,corder' Create customer.commerce2customer
  if [ $? -ne 0 ]; then
    echo "MoveTables failed"
    printMysqlErrorFiles
    exit 1
  fi

  sleep 10

  vdiff_out=$(vtctldclient LegacyVtctlCommand -- VDiff customer.commerce2customer)
    echo "$vdiff_out" | grep "ProcessedRows: 5" | wc -l | grep "2" > /dev/null
    if [ $? -ne 0 ]; then
      echo -e "VDiff output is invalid, got:\n$vdiff_out"
      # Allow failure
    fi

  vtctldclient LegacyVtctlCommand -- MoveTables --tablet_types='rdonly,replica' SwitchTraffic customer.commerce2customer
  if [ $? -ne 0 ]; then
    echo "SwitchTraffic for rdonly and replica failed"
    printMysqlErrorFiles
    exit 1
  fi

  vtctldclient LegacyVtctlCommand -- MoveTables --tablet_types='primary' SwitchTraffic customer.commerce2customer
  if [ $? -ne 0 ]; then
    echo "SwitchTraffic for primary failed"
    printMysqlErrorFiles
    exit 1
  fi

  vtctldclient LegacyVtctlCommand -- MoveTables Complete customer.commerce2customer
  if [ $? -ne 0 ]; then
    echo "MoveTables Complete failed"
    printMysqlErrorFiles
    exit 1
  fi

  sleep 10
}

function resharding() {
  echo "Create new schemas for new shards"
  applySchemaWithRetry create_commerce_seq.sql commerce
  sleep 4
  vtctldclient ApplyVSchema --vschema-file="vschema_commerce_seq.json" commerce
  if [ $? -ne 0 ]; then
    echo "ApplyVschema commerce_seq during resharding failed"
    printMysqlErrorFiles
    exit 1
  fi
  sleep 4
  vtctldclient ApplyVSchema --vschema-file="vschema_customer_sharded.json" customer
  if [ $? -ne 0 ]; then
    echo "ApplyVschema customer_sharded during resharding failed"
    printMysqlErrorFiles
    exit 1
  fi
  sleep 4
  applySchemaWithRetry create_customer_sharded.sql customer
  sleep 4

  echo "Apply 302_new_shards.yaml"
  changeImageAndApply 302_new_shards.yaml
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 8

  killall kubectl
  ./pf.sh > /dev/null 2>&1 &
  sleep 5

  waitForKeyspaceToBeServing customer -80 1
  waitForKeyspaceToBeServing customer 80- 1

  echo "Ready to reshard ..."
  sleep 15

  vtctldclient LegacyVtctlCommand -- Reshard --source_shards '-' --target_shards '-80,80-' Create customer.cust2cust
  if [ $? -ne 0 ]; then
    echo "Reshard Create failed"
    printMysqlErrorFiles
    exit 1
  fi

  sleep 15

  vdiff_out=$(vtctldclient LegacyVtctlCommand -- VDiff customer.cust2cust)
  echo "$vdiff_out" | grep "ProcessedRows: 5" | wc -l | grep "2" > /dev/null
  if [ $? -ne 0 ]; then
    echo -e "VDiff output is invalid, got:\n$vdiff_out"
    # Allow failure
  fi

  vtctldclient LegacyVtctlCommand -- Reshard --tablet_types='rdonly,replica' SwitchTraffic customer.cust2cust
  if [ $? -ne 0 ]; then
    echo "Reshard SwitchTraffic for replica,rdonly failed"
    printMysqlErrorFiles
    exit 1
  fi
  vtctldclient LegacyVtctlCommand -- Reshard --tablet_types='primary' SwitchTraffic customer.cust2cust
  if [ $? -ne 0 ]; then
    echo "Reshard SwitchTraffic for primary failed"
    printMysqlErrorFiles
    exit 1
  fi

  sleep 10

  assertSelect ../common/select_customer-80_data.sql "customer/-80" << EOF
Using customer/-80
Customer
+-------------+--------------------+
| customer_id | email              |
+-------------+--------------------+
|           1 | alice@domain.com   |
|           2 | bob@domain.com     |
|           3 | charlie@domain.com |
|           5 | eve@domain.com     |
+-------------+--------------------+
COrder
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        1 |           1 | SKU-1001 |   100 |
|        2 |           2 | SKU-1002 |    30 |
|        3 |           3 | SKU-1002 |    30 |
|        5 |           5 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
EOF

  assertSelect ../common/select_customer80-_data.sql "customer/80-" << EOF
Using customer/80-
Customer
+-------------+----------------+
| customer_id | email          |
+-------------+----------------+
|           4 | dan@domain.com |
+-------------+----------------+
COrder
+----------+-------------+----------+-------+
| order_id | customer_id | sku      | price |
+----------+-------------+----------+-------+
|        4 |           4 | SKU-1002 |    30 |
+----------+-------------+----------+-------+
EOF

  changeImageAndApply 306_down_shard_0.yaml
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 6
  waitForKeyspaceToBeServing customer -80 1
  waitForKeyspaceToBeServing customer 80- 1
}


# Build the docker image for vitess/lite using the local code
docker build -f docker/lite/Dockerfile -t vitess/lite:pr .
# Build the docker image for vitess/vtadmin using the local code
docker build -f docker/base/Dockerfile -t vitess/base:pr .
docker build -f docker/k8s/Dockerfile --build-arg VT_BASE_VER=pr -t vitess/k8s:pr .
docker build -f docker/k8s/vtadmin/Dockerfile --build-arg VT_BASE_VER=pr -t vitess/vtadmin:pr .

# Print the docker images available
docker image ls

echo "Creating Kind cluster"
kind create cluster --wait 30s --name kind
echo "Loading docker images into Kind cluster"
kind load docker-image vitess/lite:pr --name kind
kind load docker-image vitess/vtadmin:pr --name kind

cd "./examples/operator"
killall kubectl

# Start all the vitess components
get_started
checkSemiSyncSetup

# Check Vtadmin is setup
# In get_started we verify that the pod for vtadmin exists and is healthy
# We now try and query the vtadmin api
verifyVtadminSetup
# Next we check that VTOrc is running properly and is able to fix issues as they come up
verifyVTOrcSetup
move_tables
resharding

# Teardown
echo "Deleting Kind cluster. This also deletes the volume associated with it"
kind delete cluster --name kind
