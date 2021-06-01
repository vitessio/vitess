#!/bin/bash

# Local integration tests. To be used by CI.
# See https://github.com/github/gh-ost/tree/doc/local-tests.md
#

# Usage: localtests/test/sh [filter]
# By default, runs all tests. Given filter, will only run tests matching given regep

tests_path=$(dirname $0)
test_logfile=/tmp/gh-ost-test.log
default_ghost_binary=/tmp/gh-ost-test
ghost_binary=""
exec_command_file=/tmp/gh-ost-test.bash
ghost_structure_output_file=/tmp/gh-ost-test.ghost.structure.sql
orig_content_output_file=/tmp/gh-ost-test.orig.content.csv
ghost_content_output_file=/tmp/gh-ost-test.ghost.content.csv
throttle_flag_file=/tmp/gh-ost-test.ghost.throttle.flag

master_host=
master_port=
replica_host=
replica_port=
original_sql_mode=

OPTIND=1
while getopts "b:" OPTION
do
  case $OPTION in
    b)
      ghost_binary="$OPTARG"
    ;;
  esac
done
shift $((OPTIND-1))

test_pattern="${1:-.}"

verify_master_and_replica() {
  if [ "$(gh-ost-test-mysql-master -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost-test-mysql-master"
    exit 1
  fi
  read master_host master_port <<< $(gh-ost-test-mysql-master -e "select @@hostname, @@port" -ss)
  [ "$master_host" == "$(hostname)" ] && master_host="127.0.0.1"
  echo "# master verified at $master_host:$master_port"
  if ! gh-ost-test-mysql-master -e "set global event_scheduler := 1" ; then
    echo "Cannot enable event_scheduler on master"
    exit 1
  fi
  original_sql_mode="$(gh-ost-test-mysql-master -e "select @@global.sql_mode" -s -s)"
  echo "sql_mode on master is ${original_sql_mode}"

  echo "Gracefully sleeping for 3 seconds while replica is setting up..."
  sleep 3

  if [ "$(gh-ost-test-mysql-replica -e "select 1" -ss)" != "1" ] ; then
    echo "Cannot verify gh-ost-test-mysql-replica"
    exit 1
  fi
  if [ "$(gh-ost-test-mysql-replica -e "select @@global.binlog_format" -ss)" != "ROW" ] ; then
    echo "Expecting test replica to have binlog_format=ROW"
    exit 1
  fi
  read replica_host replica_port <<< $(gh-ost-test-mysql-replica -e "select @@hostname, @@port" -ss)
  [ "$replica_host" == "$(hostname)" ] && replica_host="127.0.0.1"
  echo "# replica verified at $replica_host:$replica_port"
}

exec_cmd() {
  echo "$@"
  command "$@" 1> $test_logfile 2>&1
  return $?
}

echo_dot() {
  echo -n "."
}

start_replication() {
  gh-ost-test-mysql-replica -e "stop slave; start slave;"
  num_attempts=0
  while gh-ost-test-mysql-replica -e "show slave status\G" | grep Seconds_Behind_Master | grep -q NULL ; do
    ((num_attempts=num_attempts+1))
    if [ $num_attempts -gt 10 ] ; then
      echo
      echo "ERROR replication failure"
      exit 1
    fi
    echo_dot
    sleep 1
  done
}

test_single() {
  local test_name
  test_name="$1"

  if [ -f $tests_path/$test_name/ignore_versions ] ; then
    ignore_versions=$(cat $tests_path/$test_name/ignore_versions)
    mysql_version=$(gh-ost-test-mysql-master -s -s -e "select @@version")
    if echo "$mysql_version" | egrep -q "^${ignore_versions}" ; then
      echo -n "Skipping: $test_name"
      return 0
    fi
  fi

  echo -n "Testing: $test_name"

  echo_dot
  start_replication
  echo_dot

  if [ -f $tests_path/$test_name/sql_mode ] ; then
    gh-ost-test-mysql-master --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='$(cat $tests_path/$test_name/sql_mode)'"
  fi

  gh-ost-test-mysql-master --default-character-set=utf8mb4 test < $tests_path/$test_name/create.sql

  extra_args=""
  if [ -f $tests_path/$test_name/extra_args ] ; then
    extra_args=$(cat $tests_path/$test_name/extra_args)
  fi
  orig_columns="*"
  ghost_columns="*"
  order_by=""
  if [ -f $tests_path/$test_name/orig_columns ] ; then
    orig_columns=$(cat $tests_path/$test_name/orig_columns)
  fi
  if [ -f $tests_path/$test_name/ghost_columns ] ; then
    ghost_columns=$(cat $tests_path/$test_name/ghost_columns)
  fi
  if [ -f $tests_path/$test_name/order_by ] ; then
    order_by="order by $(cat $tests_path/$test_name/order_by)"
  fi
  # graceful sleep for replica to catch up
  echo_dot
  sleep 1
  #
  cmd="$ghost_binary \
    --user=gh-ost \
    --password=gh-ost \
    --host=$replica_host \
    --port=$replica_port \
    --assume-master-host=${master_host}:${master_port}
    --database=test \
    --table=onlineddl_test \
    --alter='engine=innodb' \
    --exact-rowcount \
    --assume-rbr \
    --initially-drop-old-table \
    --initially-drop-ghost-table \
    --throttle-query='select timestampdiff(second, min(last_update), now()) < 5 from _gh_ost_test_ghc' \
    --throttle-flag-file=$throttle_flag_file \
    --serve-socket-file=/tmp/gh-ost.test.sock \
    --initially-drop-socket-file \
    --test-on-replica \
    --default-retries=3 \
    --chunk-size=10 \
    --verbose \
    --debug \
    --stack \
    --execute ${extra_args[@]}"
  echo_dot
  echo $cmd > $exec_command_file
  echo_dot
  bash $exec_command_file 1> $test_logfile 2>&1

  execution_result=$?

  if [ -f $tests_path/$test_name/sql_mode ] ; then
    gh-ost-test-mysql-master --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
    gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "set @@global.sql_mode='${original_sql_mode}'"
  fi

  if [ -f $tests_path/$test_name/destroy.sql ] ; then
    gh-ost-test-mysql-master --default-character-set=utf8mb4 test < $tests_path/$test_name/destroy.sql
  fi

  if [ -f $tests_path/$test_name/expect_failure ] ; then
    if [ $execution_result -eq 0 ] ; then
      echo
      echo "ERROR $test_name execution was expected to exit on error but did not. cat $test_logfile"
      return 1
    fi
    if [ -s $tests_path/$test_name/expect_failure ] ; then
      # 'expect_failure' file has content. We expect to find this content in the log.
      expected_error_message="$(cat $tests_path/$test_name/expect_failure)"
      if grep -q "$expected_error_message" $test_logfile ; then
          return 0
      fi
      echo
      echo "ERROR $test_name execution was expected to exit with error message '${expected_error_message}' but did not. cat $test_logfile"
      return 1
    fi
    # 'expect_failure' file has no content. We generally agree that the failure is correct
    return 0
  fi

  if [ $execution_result -ne 0 ] ; then
    echo
    echo "ERROR $test_name execution failure. cat $test_logfile:"
    cat $test_logfile
    return 1
  fi

  gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "show create table _gh_ost_test_gho\G" -ss > $ghost_structure_output_file

  if [ -f $tests_path/$test_name/expect_table_structure ] ; then
    expected_table_structure="$(cat $tests_path/$test_name/expect_table_structure)"
    if ! grep -q "$expected_table_structure" $ghost_structure_output_file ; then
      echo
      echo "ERROR $test_name: table structure was expected to include ${expected_table_structure} but did not. cat $ghost_structure_output_file:"
      cat $ghost_structure_output_file
      return 1
    fi
  fi

  echo_dot
  gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${orig_columns} from onlineddl_test ${order_by}" -ss > $orig_content_output_file
  gh-ost-test-mysql-replica --default-character-set=utf8mb4 test -e "select ${ghost_columns} from _gh_ost_test_gho ${order_by}" -ss > $ghost_content_output_file
  orig_checksum=$(cat $orig_content_output_file | md5sum)
  ghost_checksum=$(cat $ghost_content_output_file | md5sum)

  if [ "$orig_checksum" != "$ghost_checksum" ] ; then
    echo "ERROR $test_name: checksum mismatch"
    echo "---"
    diff $orig_content_output_file $ghost_content_output_file

    echo "diff $orig_content_output_file $ghost_content_output_file"

    return 1
  fi
}

build_binary() {
  echo "Building"
  rm -f $default_ghost_binary
  [ "$ghost_binary" == "" ] && ghost_binary="$default_ghost_binary"
  if [ -f "$ghost_binary" ] ; then
    echo "Using binary: $ghost_binary"
    return 0
  fi
  go build -o $ghost_binary go/cmd/gh-ost/main.go
  if [ $? -ne 0 ] ; then
    echo "Build failure"
    exit 1
  fi
}

test_all() {
  build_binary
  find $tests_path ! -path . -type d -mindepth 1 -maxdepth 1 | cut -d "/" -f 3 | egrep "$test_pattern" | while read test_name ; do
    test_single "$test_name"
    if [ $? -ne 0 ] ; then
      create_statement=$(gh-ost-test-mysql-replica test -t -e "show create table _gh_ost_test_gho \G")
      echo "$create_statement" >> $test_logfile
      echo "+ FAIL"
      return 1
    else
      echo
      echo "+ pass"
    fi
    gh-ost-test-mysql-replica -e "start slave"
  done
}

verify_master_and_replica
test_all
