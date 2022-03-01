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
  for (( x=0 ; x<60 ; x+=1 )); do
    kubectl get pods | grep -E "$regex" | wc -l | grep "$nb" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
      echo "$regex found"
      return
    fi
    sleep 1
  done
  echo "ERROR: checkPodStatusWithTimeout timeout to find pod matching: $regex"
  exit 1
}

function insertWithRetry() {
  for i in {1..3} ; do
    mysql --table < ../common/insert_commerce_data.sql
    if [ $? -eq 0 ]; then
      return
    fi
    echo "failed to insert commerce data, retrying (attempt #$i) ..."
    mysql --table < ../common/delete_commerce_data.sql
    sleep 2
  done
}

function waitForKeyspaceToBeServing() {
  ks=$1
  shard=$2
  for i in {1..20} ; do
    out=$(mysql --table --execute="show vitess_tablets")
    echo "$out" | grep -E "$ks(.*)$shard(.*)PRIMARY(.*)SERVING"
    if [ $? -eq 0 ]; then
      echo "Shard $ks/$shard is serving"
      return
    fi
    echo "Shard $ks/$shard does not have a serving primary, retrying (attempt #$i) ..."
    sleep 10
  done
}

killall kubectl

cd "$VTROOT/examples/operator"

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

waitForKeyspaceToBeServing commerce -

vtctlclient ApplySchema -sql="$(cat create_commerce_schema.sql)" commerce
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

# Insert data
insertWithRetry

commerce_data=$(mysql --table < ../common/select_commerce_data.sql)
grep << EOF "$commerce_data" > /dev/null 2>&1
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
if [ $? -ne 0 ]; then
  echo -e "The data in commerce's tables is incorrect, got:\n$commerce_data"
  exit 1
fi

echo "Apply 201_customer_tablets.yaml"
kubectl apply -f 201_customer_tablets.yaml > /dev/null
checkPodStatusWithTimeout "example-vttablet-zone1(.*)3/3(.*)Running(.*)" 4

killall kubectl
./pf.sh > /dev/null 2>&1 &

waitForKeyspaceToBeServing customer -

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