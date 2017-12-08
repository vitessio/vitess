#!/bin/bash

set -u

keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
uid=$1

printf -v alias '%s-%010d' $CELL $uid
printf -v tablet_dir 'vt_%010d' $uid

tablet_role='replica'
if [ "$uid" = "1" ]; then
    tablet_role='master'
fi

dbconfig_dba_flags="\
    -db-config-dba-uname root \
    -db-config-dba-charset utf8"

dbconfig_flags="$dbconfig_dba_flags \
    -db-config-app-uname root \
    -db-config-app-charset utf8 \
    -db-config-appdebug-uname root \
    -db-config-appdebug-charset utf8 \
    -db-config-allprivs-uname root \
    -db-config-allprivs-charset utf8 \
    -db-config-repl-uname root \
    -db-config-repl-charset utf8 \
    -db-config-filtered-uname root \
    -db-config-filtered-charset utf8"

init_db_sql_file="$VTROOT/init_db.sql"
echo "GRANT ALL ON *.* TO 'root'@'%';" > $init_db_sql_file

if [ "$tablet_role" = "master" ]; then
    echo "CREATE DATABASE vt_$keyspace;" >> $init_db_sql_file
fi

export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mysql56.cnf

mkdir -p $VTDATAROOT/backups

echo "Starting MySQL for tablet..."
action="init -init_db_sql_file $init_db_sql_file"
if [ -d $VTDATAROOT/$tablet_dir ]; then
  echo "Resuming from existing vttablet dir:"
  echo "    $VTDATAROOT/$tablet_dir"
  action='start'
fi

$VTROOT/bin/mysqlctl \
  -log_dir $VTDATAROOT/tmp \
  -tablet_uid $uid \
  $dbconfig_dba_flags \
  -mysql_port 3306 \
  $action &

wait

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo -root vitess/$CELL -server_address consul1:8500 $CELL

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateKeyspace $keyspace
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateShard $keyspace/$shard

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS InitTablet -shard $shard -keyspace $keyspace -allow_master_override $alias $tablet_role

echo "Starting vttablet..."
exec $VTROOT/bin/vttablet \
  $TOPOLOGY_FLAGS \
  -logtostderr=true \
  -tablet-path $alias \
  -tablet_hostname "" \
  -health_check_interval 5s \
  -enable_semi_sync \
  -enable_replication_reporter \
  -port $WEB_PORT \
  -grpc_port $GRPC_PORT \
  -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
  -vtctld_addr http://vtctld:$WEB_PORT/ \
  $dbconfig_flags
