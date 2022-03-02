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

source build.env

alias vtctlclient="vtctlclient -server=localhost:15999"
alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"
shopt -s expand_aliases

# checkPodStatusWithTimeout:
#   $1: regex used to match pod names
#   $2: number of pods to match (default: 1)
function checkPodStatusWithTimeout() {
  regex=$1
  nb=$2

  # Number of pods to match defaults to zero
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

function insertWithRetry() {
  for i in {1..600} ; do
    mysql --table < ../common/delete_commerce_data.sql && mysql --table < ../common/insert_commerce_data.sql
    if [ $? -eq 0 ]; then
      return
    fi
    echo "failed to insert commerce data, retrying (attempt #$i) ..."
    sleep 1
  done
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
    vtctlclient ApplySchema -sql="$(cat $schema)" $ks
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

function get_started() {
    echo "Apply operator.yaml"
    kubectl apply -f operator.yaml > /dev/null
    checkPodStatusWithTimeout "vitess-operator(.*)1/1(.*)Running(.*)"

    echo "Apply 101_initial_cluster.yaml"
    kubectl apply -f 101_initial_cluster.yaml > /dev/null
    checkPodStatusWithTimeout "example-zone1-vtctld(.*)1/1(.*)Running(.*)"
    checkPodStatusWithTimeout "example-zone1-vtgate(.*)1/1(.*)Running(.*)"
    checkPodStatusWithTimeout "example-etcd(.*)1/1(.*)Running(.*)" 3
    checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 2

    sleep 10
    echo "Creating vschema and commerce SQL schema"

    ./pf.sh > /dev/null 2>&1 &
    sleep 5

    waitForKeyspaceToBeServing commerce - 1
    sleep 5

    applySchemaWithRetry create_commerce_schema.sql commerce drop_all_commerce_tables.sql
    vtctlclient ApplyVSchema -vschema="$(cat vschema_commerce_initial.json)" commerce
    sleep 5

    echo "show databases;" | mysql | grep "commerce" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Could not find commerce database"
      exit 1
    fi

    echo "show tables;" | mysql commerce | grep -E 'corder|customer|product' | wc -l | grep 3 > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Could not find commerce's tables"
      exit 1
    fi

    insertWithRetry

    assertSelect ../common/select_commerce_data.sql "commerce" << EOF
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

function move_tables() {
  echo "Apply 201_customer_tablets.yaml"
  kubectl apply -f 201_customer_tablets.yaml > /dev/null
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 4

  killall kubectl
  ./pf.sh > /dev/null 2>&1 &

  waitForKeyspaceToBeServing customer - 1

  sleep 10

  vtctlclient MoveTables -source commerce -tables 'customer,corder' Create customer.commerce2customer

  vdiff_out=$(vtctlclient VDiff customer.commerce2customer)
  echo "$vdiff_out" | grep "ProcessedRows: 5" | wc -l | grep "2" > /dev/null
  if [ $? -ne 0 ]; then
    echo -e "VDiff output is invalid, got:\n$vdiff_out"
    exit 1
  fi

  vtctlclient MoveTables -tablet_types=rdonly,replica SwitchTraffic customer.commerce2customer
  vtctlclient MoveTables -tablet_types=primary SwitchTraffic customer.commerce2customer
  vtctlclient MoveTables Complete customer.commerce2customer

  sleep 10
}

function resharding() {
  echo "Create new schemas for new shards"
  applySchemaWithRetry create_commerce_seq.sql commerce
  sleep 4
  vtctlclient ApplyVSchema -vschema="$(cat vschema_commerce_seq.json)" commerce
  sleep 4
  vtctlclient ApplyVSchema -vschema="$(cat vschema_customer_sharded.json)" customer
  sleep 4
  applySchemaWithRetry create_customer_sharded.sql customer
  sleep 4

  echo "Apply 302_new_shards.yaml"
  kubectl apply -f 302_new_shards.yaml
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 8

  killall kubectl
  ./pf.sh > /dev/null 2>&1 &
  sleep 5

  waitForKeyspaceToBeServing customer -80 1
  waitForKeyspaceToBeServing customer 80- 1

  echo "Ready to reshard ..."
  sleep 15

  vtctlclient Reshard -source_shards '-' -target_shards '-80,80-' Create customer.cust2cust

  sleep 5

  vdiff_out=$(vtctlclient VDiff customer.cust2cust)
  echo "$vdiff_out" | grep "ProcessedRows: 5" | wc -l | grep "2" > /dev/null
  if [ $? -ne 0 ]; then
    echo -e "VDiff output is invalid, got:\n$vdiff_out"
    # Allow failure
  fi

  vtctlclient Reshard -tablet_types=rdonly,replica SwitchTraffic customer.cust2cust
  vtctlclient Reshard -tablet_types=primary SwitchTraffic customer.cust2cust

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

  kubectl apply -f 306_down_shard_0.yaml
  checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 6
  waitForKeyspaceToBeServing customer -80 1
  waitForKeyspaceToBeServing customer 80- 1
}


cd "$VTROOT/examples/operator"
killall kubectl
kubectl delete -f 101_initial_cluster.yaml

get_started
move_tables
resharding
