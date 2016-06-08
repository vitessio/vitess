#!/bin/bash

# This test runs through the scripts in examples/local to make sure they work.

# timeout in seconds (for each step, not overall)
timeout=30

cd $VTTOP/examples/local

exitcode=1

# Use a minimal number of tablets for the test.
# This helps with staying under CI resource limits.
num_tablets=2
tablet_tasks=`seq 0 $[$num_tablets - 1]`

teardown() {
  ./vtgate-down.sh &
  ./vttablet-down.sh $tablet_tasks &
  ./vtctld-down.sh &
  ./zk-down.sh &
  wait
  exit $exitcode
}
trap teardown SIGTERM SIGINT

# Set up servers.
timeout $timeout ./zk-up.sh || teardown
timeout $timeout ./vtctld-up.sh || teardown
timeout $timeout ./vttablet-up.sh $tablet_tasks || teardown

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

echo "Run PHP client script..."
# We don't need to retry anymore, because we've established that vtgate is ready.
php client.php --server=localhost:15991 || teardown

echo "Run Go client script..."
go run client.go -server=localhost:15991 || teardown

echo "Run Java client script..."
./client_java.sh || teardown

echo "Run JDBC client script..."
./client_jdbc.sh || teardown

exitcode=0
teardown
