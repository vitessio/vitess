#!/bin/bash

set -uex

keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
role=${ROLE:-'replica'}
vthost=${VTHOST:-'localhost'}
sleeptime=${SLEEPTIME:-'0'}
uid=$1

printf -v alias '%s-%010d' $CELL $uid
printf -v tablet_dir 'vt_%010d' $uid

tablet_role=$role

init_db_sql_file="$VTROOT/init_db.sql"
echo "GRANT ALL ON *.* TO 'root'@'%';" > $init_db_sql_file
echo "GRANT ALL ON *.* TO 'vt_dba'@'%';" >> $init_db_sql_file
echo "GRANT ALL ON *.* TO 'vt_app'@'%';" >> $init_db_sql_file
echo "GRANT ALL ON *.* TO 'vt_repl'@'%';" >> $init_db_sql_file
echo "GRANT ALL ON *.* TO 'vt_filtered'@'%';" >> $init_db_sql_file

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
  -mysql_port 3306 \
  $action &

wait

sleep $sleeptime

if [ "$role" != "master" ]; then

    master_uid=${uid:0:1}01
    master_vttablet=vttablet${master_uid}
    until mysql -h ${master_vttablet} -u root -e "select 0;"; do echo "Polling master mysql at ${master_vttablet}..." && sleep 1; done

    echo "Restoring mysql dump from ${master_vttablet}..."
    mysql -S $VTDATAROOT/$tablet_dir/mysql.sock -u root -e "FLUSH LOGS; RESET SLAVE;RESET MASTER;"
    mysqldump -h ${master_vttablet} -u root --all-databases --triggers --routines --events --single-transaction --set-gtid-purged=AUTO --default-character-set=utf8mb4 | mysql -S $VTDATAROOT/$tablet_dir/mysql.sock -u root

fi

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo -root vitess/$CELL -server_address consul1:8500 $CELL || true

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateKeyspace $keyspace || true
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateShard $keyspace/$shard || true


$VTROOT/bin/vtctl $TOPOLOGY_FLAGS InitTablet -shard $shard -keyspace $keyspace -hostname $vthost -grpc_port $GRPC_PORT -port $WEB_PORT -allow_master_override $alias $tablet_role

echo "Starting vttablet..."
exec $VTROOT/bin/vttablet \
  $TOPOLOGY_FLAGS \
  -logtostderr=true \
  -tablet-path $alias \
  -tablet_hostname $vthost \
  -health_check_interval 5s \
  -enable_semi_sync \
  -enable_replication_reporter \
  -port $WEB_PORT \
  -grpc_port $GRPC_PORT \
  -binlog_use_v3_resharding_mode=true \
  -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
  -vtctld_addr "http://vtctld:$WEB_PORT/"