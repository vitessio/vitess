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

echo "Run Python client script..."
# Retry until vtgate is ready.
start=`date +%s`
until ./client.sh; do
  retry_with_timeout
done

echo "Run PHP client script..."
# We don't need to retry anymore, because we've established that vtgate is ready.
php client.php --server=localhost:15991 || teardown

echo "Run Go client script..."
go run client.go -server=localhost:15991 || teardown

echo "Run Java client script..."
# We have to install the "example" module first because Maven cannot resolve
# them when we run "exec:java". See also: http://stackoverflow.com/questions/11091311/maven-execjava-goal-on-a-multi-module-project
# Install only "example". See also: http://stackoverflow.com/questions/1114026/maven-modules-building-a-single-specific-module
mvn -f ../../java/pom.xml -pl example -am install -DskipTests
mvn -f ../../java/example/pom.xml exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="com.youtube.vitess.example.VitessClientExample" -Dexec.args="localhost:15991" || teardown

exitcode=0
teardown
