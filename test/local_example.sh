#!/bin/bash

# Copyright 2017 Google Inc.
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

# This test runs through the scripts in examples/local to make sure they work.

# timeout in seconds (for each step, not overall)
timeout=30
export TOPO=ectd2
cd $VTTOP/examples/local

exitcode=1

# Use a minimal number of tablets for the test.
# This helps with staying under CI resource limits.
num_tablets=2
uid_base=100
cell=test
TABLETS_UIDS="$(seq 0 $((num_tablets - 1)))"
export TABLETS_UIDS

teardown() {
  ./vtgate-down.sh &
  ./vttablet-down.sh "$TABLETS_UIDS" &
  ./vtctld-down.sh &
  ./etcd-down.sh &
  wait
  exit $exitcode
}
trap teardown SIGTERM SIGINT EXIT

# Set up servers.
timeout $timeout ./etcd-up.sh || teardown
timeout $timeout ./vtctld-up.sh || teardown
timeout $timeout ./vttablet-up.sh || teardown

# Retry loop function
retry_with_timeout() {
  if [ `date +%s` -gt $[$start + $timeout] ]; then
    echo "Timeout ($timeout) exceeded"
    teardown
  fi

  echo "Waiting 2 seconds to try again..."
  sleep 2
}

# Wait for vttablets to show up in topology.
# If we don't do this, then vtgate might take up to a minute
# to notice the new tablets, which is normally fine, but not
# when we're trying to get through the test ASAP.
echo "Waiting for $num_tablets tablets to appear in topology..."
start=`date +%s`
until [[ $(./lvtctl.sh ListAllTablets test | wc -l) -eq $num_tablets ]]; do
  retry_with_timeout
done

timeout $timeout ./vtgate-up.sh || teardown

echo "Initialize shard..."
start=`date +%s`
until ./lvtctl.sh InitShardMaster -force test_keyspace/0 test-100; do
  retry_with_timeout
done

# Run manual health check on each tablet.
# This is not necessary, but it helps make this test more representative of
# what a human would do. It simulates the case where a periodic health check
# occurs before the user gets around to running the next command. For example,
# in that case the health check will make the tablet begin serving prior to the
# ApplySchema command below. Otherwise, ApplySchema races with the periodic
# health check.
echo "Running health check on tablets..."
start=`date +%s`
for uid_index in $TABLETS_UIDS; do
  uid=$[$uid_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  ./lvtctl.sh RunHealthCheck $alias
done

echo "Create table..."
start=`date +%s`
until ./lvtctl.sh ApplySchema -sql "$(cat create_test_table.sql)" test_keyspace; do
  retry_with_timeout
done

echo "Rebuild vschema..."
start=`date +%s`
until ./lvtctl.sh RebuildVSchemaGraph; do
  retry_with_timeout
done

echo "Run Python client script..."
# Retry until vtgate is ready.
start=`date +%s`
# Test that the client.sh script works with no --server arg,
# because that's how the tutorial says to run it.
until ./client.sh ; do
  retry_with_timeout
done

echo "Run Go client script..."
go run client.go -server=localhost:15991 || teardown

echo "Run Java client script..."
./client_java.sh || teardown

echo "Run JDBC client script..."
./client_jdbc.sh || teardown

exitcode=0
teardown
