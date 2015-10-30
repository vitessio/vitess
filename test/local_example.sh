#!/bin/bash

# This test runs through the scripts in examples/local to make sure they work.

# timeout in seconds (for each step, not overall)
timeout=30

cd $VTTOP/examples/local

exitcode=1

teardown() {
  ./vtgate-down.sh &
  ./vttablet-down.sh &
  ./vtctld-down.sh &
  ./zk-down.sh &
  wait
  exit $exitcode
}
trap teardown SIGTERM SIGINT

# Set up servers.
timeout $timeout ./zk-up.sh || teardown
timeout $timeout ./vtctld-up.sh || teardown
timeout $timeout ./vtgate-up.sh || teardown
timeout $timeout ./vttablet-up.sh || teardown

# Retry loop function
retry_with_timeout() {
  if [ `date +%s` -gt $[$start + $timeout] ]; then
    echo "Timeout ($timeout) exceeded"
    teardown
  fi

  echo "Waiting 5 seconds to try again..."
  sleep 5
}

echo "Rebuild keyspace..."
start=`date +%s`
until vtctlclient -server localhost:15999 RebuildKeyspaceGraph test_keyspace; do
  retry_with_timeout
done

echo "Initialize shard..."
start=`date +%s`
until vtctlclient -server localhost:15999 InitShardMaster -force test_keyspace/0 test-0000000100; do
  retry_with_timeout
done

echo "Create table..."
start=`date +%s`
until vtctlclient -server localhost:15999 ApplySchema -sql "$(cat create_test_table.sql)" test_keyspace; do
  retry_with_timeout
done

echo "Run client script..."
start=`date +%s`
until ./client.sh; do
  retry_with_timeout
done

exitcode=0
teardown
