#!/bin/bash

# This is an example script that creates a single shard vttablet deployment.

set -e

cell='test'
keyspace='test_keyspace'
shard=${SHARD:-'0'}
uid_base=${UID_BASE:-'100'}
port_base=$[15000 + $uid_base]
grpc_port_base=$[16000 + $uid_base]
mysql_port_base=$[17000 + $uid_base]
tablet_hostname=''

# Travis hostnames are too long for MySQL, so we use IP.
# Otherwise, blank hostname means the tablet auto-detects FQDN.
if [ "$TRAVIS" == true ]; then
  tablet_hostname=`hostname -i`
fi

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

dbconfig_dba_flags="\
    -db-config-dba-uname vt_dba \
    -db-config-dba-charset utf8"
dbconfig_flags="$dbconfig_dba_flags \
    -db-config-app-uname vt_app \
    -db-config-app-dbname vt_$keyspace \
    -db-config-app-charset utf8 \
    -db-config-allprivs-uname vt_allprivs \
    -db-config-allprivs-dbname vt_$keyspace \
    -db-config-allprivs-charset utf8 \
    -db-config-repl-uname vt_repl \
    -db-config-repl-dbname vt_$keyspace \
    -db-config-repl-charset utf8 \
    -db-config-filtered-uname vt_filtered \
    -db-config-filtered-dbname vt_$keyspace \
    -db-config-filtered-charset utf8"
init_db_sql_file="$VTROOT/config/init_db.sql"

case "$MYSQL_FLAVOR" in
  "MySQL56")
    export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mysql56.cnf
    ;;
  "MariaDB")
    export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mariadb.cnf
    ;;
  *)
    echo "Please set MYSQL_FLAVOR to MySQL56 or MariaDB."
    exit 1
    ;;
esac

mkdir -p $VTDATAROOT/backups

# Start 5 vttablets by default.
# Pass a list of UID indices on the command line to override.
uids=${@:-'0 1 2 3 4'}

# Start all mysqlds in background.
for uid_index in $uids; do
  uid=$[$uid_base + $uid_index]
  mysql_port=$[$mysql_port_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid

  echo "Starting MySQL for tablet $alias..."
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
    -mysql_port $mysql_port \
    $action &
done

# Wait for all mysqld to start up.
wait

# Start all vttablets in background.
for uid_index in $uids; do
  uid=$[$uid_base + $uid_index]
  port=$[$port_base + $uid_index]
  grpc_port=$[$grpc_port_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid
  tablet_type=replica
  if [[ $uid_index -gt 2 ]]; then
    tablet_type=rdonly
  fi

  echo "Starting vttablet for $alias..."
  $VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -tablet-path $alias \
    -tablet_hostname "$tablet_hostname" \
    -init_keyspace $keyspace \
    -init_shard $shard \
    -init_tablet_type $tablet_type \
    -health_check_interval 5s \
    -enable_semi_sync \
    -enable_replication_reporter \
    -backup_storage_implementation file \
    -file_backup_storage_root $VTDATAROOT/backups \
    -restore_from_backup \
    -port $port \
    -grpc_port $grpc_port \
    -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
    -vtctld_addr http://$hostname:$vtctld_web_port/ \
    $dbconfig_flags \
    > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

  echo "Access tablet $alias at http://$hostname:$port/debug/status"
done

disown -a
