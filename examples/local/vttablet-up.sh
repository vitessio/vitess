#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.

set -e

cell='test'
keyspace='test_keyspace'
shard=0
uid_base=100
port_base=15100
mysql_port_base=33100

hostname=`hostname -f`

# We expect to find zk-client-conf.json in the same folder as this script.
script_root=`dirname "${BASH_SOURCE}"`

dbconfig_flags="\
    -db-config-app-uname vt_app \
    -db-config-app-dbname vt_$keyspace \
    -db-config-app-charset utf8 \
    -db-config-dba-uname vt_dba \
    -db-config-dba-charset utf8 \
    -db-config-repl-uname vt_repl \
    -db-config-repl-dbname vt_$keyspace \
    -db-config-repl-charset utf8 \
    -db-config-filtered-uname vt_filtered \
    -db-config-filtered-dbname vt_$keyspace \
    -db-config-filtered-charset utf8"

# Set up environment.
export LD_LIBRARY_PATH=$VTROOT/dist/vt-zookeeper-3.3.5/lib:$LD_LIBRARY_PATH
export ZK_CLIENT_CONFIG=$script_root/zk-client-conf.json
export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mariadb.cnf
mkdir -p $VTDATAROOT/tmp

# Try to find mysqld_safe on PATH.
if [ -z "$VT_MYSQL_ROOT" ]; then
  mysql_path=`which mysqld_safe`
  if [ -z "$mysql_path" ]; then
    echo "Can't guess location of mysqld_safe. Please set VT_MYSQL_ROOT so it can be found at \$VT_MYSQL_ROOT/bin/mysqld_safe."
    exit 1
  fi
  export VT_MYSQL_ROOT=$(dirname `dirname $mysql_path`)
fi

# Look for memcached.
memcached_path=`which memcached`
if [ -z "$memcached_path" ]; then
  echo "Can't find memcached. Please make sure it is available in PATH."
  exit 1
fi

# Start 3 vttablets.
for uid_index in 0 1 2; do
  uid=$[$uid_base + $uid_index]
  port=$[$port_base + $uid_index]
  mysql_port=$[$mysql_port_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid

  echo "Starting MySQL for tablet $alias..."
  $VTROOT/bin/mysqlctl -log_dir $VTDATAROOT/tmp -tablet_uid $uid $dbconfig_flags \
    -mysql_port $mysql_port \
    init -bootstrap_archive mysql-db-dir_10.0.13-MariaDB.tbz

  $VT_MYSQL_ROOT/bin/mysql -u vt_dba -S $VTDATAROOT/$tablet_dir/mysql.sock \
    -e "CREATE DATABASE IF NOT EXISTS vt_$keyspace"

  echo "Starting vttablet for $alias..."
  $VTROOT/bin/vttablet -log_dir $VTDATAROOT/tmp -port $port $dbconfig_flags \
    -tablet-path $alias \
    -init_keyspace $keyspace \
    -init_shard $shard \
    -target_tablet_type replica \
    -enable-rowcache \
    -rowcache-bin $memcached_path \
    -rowcache-socket $VTDATAROOT/$tablet_dir/memcache.sock \
    > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

  echo "Access tablet $alias at http://$hostname:$port/debug/status"
done

disown -a
