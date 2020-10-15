#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

set -u

keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
grpc_port=${GRPC_PORT:-'15999'}
web_port=${WEB_PORT:-'8080'}
role=${ROLE:-'replica'}
vthost=${VTHOST:-''}
sleeptime=${SLEEPTIME:-'0'}
uid=$1
external=${EXTERNAL_DB:-0}
[ $external = 0 ] && db_name=${DB:-"vt_$keyspace"} ||  db_name=${DB:-"$keyspace"}
db_charset=${DB_CHARSET:-''}
tablet_hostname=''

# Use IPs to simplify connections when testing in docker.
# Otherwise, blank hostname means the tablet auto-detects FQDN.
if [ $external = 1 ]; then
  vthost=`hostname -i`
fi

printf -v alias '%s-%010d' $CELL $uid
printf -v tablet_dir 'vt_%010d' $uid

tablet_role=$role
tablet_type='replica'

# Make every 3rd tablet rdonly
if (( $uid % 100 % 3 == 0 )) ; then
    tablet_type='rdonly'
fi

init_db_sql_file="$VTROOT/config/init_db.sql"

# Create database on master
if [ $tablet_role = "master" ]; then
    echo "CREATE DATABASE IF NOT EXISTS $db_name;" >> $init_db_sql_file
fi
# Create database on replicas
if [ $tablet_role != "master" ]; then
    echo "Add create database statement for replicas tablet:  $uid..."
    if [ "$external" = "1" ]; then
        # Add master character set and collation to avoid replication errors
        # Example:CREATE DATABASE IF NOT EXISTS $keyspace CHARACTER SET latin1 COLLATE latin1_swedish_ci
        echo "CREATE DATABASE IF NOT EXISTS $db_name $db_charset;" >> $init_db_sql_file
        echo "Creating matching user for replicas..."
        echo "CREATE USER IF NOT EXISTS '$DB_USER'@'%' IDENTIFIED BY '$DB_PASS';" >> $init_db_sql_file
        echo "GRANT ALL ON *.* TO '$DB_USER'@'%';FLUSH PRIVILEGES;" >> $init_db_sql_file
        # Prevent replication failures in case external db server has multiple databases which have not been created here
    else
        echo "CREATE DATABASE IF NOT EXISTS $db_name;" >> $init_db_sql_file
    fi
fi

mkdir -p $VTDATAROOT/backups

echo "Starting MySQL for tablet..."
action="init -init_db_sql_file $init_db_sql_file"
if [ -d $VTDATAROOT/$tablet_dir ]; then
  echo "Resuming from existing vttablet dir:"
  echo "    $VTDATAROOT/$tablet_dir"
  action='start'
fi

export KEYSPACE=$keyspace
export SHARD=$shard
export TABLET_ID=$alias
export TABLET_DIR=$tablet_dir
export MYSQL_PORT=3306
export TABLET_TYPE=$tablet_role
export DB_PORT=${DB_PORT:-3306}
export DB_HOST=${DB_HOST:-""}
export DB_NAME=$db_name

# Create mysql instances
# Do not create mysql instance for master if connecting to external mysql database
if [[ $role != "master" || $external = 0 ]]; then
  echo "Initing mysql for tablet: $uid.. "
  $VTROOT/bin/mysqlctl \
  -log_dir $VTDATAROOT/tmp \
  -tablet_uid $uid \
  -mysql_port 3306 \
  $action &

  wait
fi

sleep $sleeptime

# if [ $role != "master" ]; then

    # master_uid=${uid:0:1}01
    # master_vttablet=vttablet${master_uid}
    # until mysql -h ${master_vttablet} -u root -e "select 0;"; do echo "Polling master mysql at ${master_vttablet}..." && sleep 1; done

    # echo "Restoring mysql dump from ${master_vttablet}..."
    # mysql -S $VTDATAROOT/$tablet_dir/mysql.sock -u root -e "FLUSH LOGS; RESET SLAVE;RESET MASTER;"
    # mysqldump -h ${master_vttablet} -u root --all-databases --triggers --routines --events --single-transaction --set-gtid-purged=AUTO --default-character-set=utf8mb4 | mysql -S $VTDATAROOT/$tablet_dir/mysql.sock -u root

# fi

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS AddCellInfo -root vitess/$CELL -server_address consul1:8500 $CELL || true

$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateKeyspace $keyspace || true
$VTROOT/bin/vtctl $TOPOLOGY_FLAGS CreateShard $keyspace/$shard || true

#Populate external db conditional args
if [ "$external" = "1" ]; then
    if [ $role = "master" ]; then
        echo "Setting external db args for master: $DB_NAME"
        external_db_args="-db_host $DB_HOST \
                          -db_port $DB_PORT \
                          -init_db_name_override $DB_NAME \
                          -mycnf_server_id $uid \
                          -db_app_user $DB_USER \
                          -db_app_password $DB_PASS \
                          -db_allprivs_user $DB_USER \
                          -db_allprivs_password $DB_PASS \
                          -db_appdebug_user $DB_USER \
                          -db_appdebug_password $DB_PASS \
                          -db_dba_user $DB_USER \
                          -db_dba_password $DB_PASS \
                          -db_filtered_user $DB_USER \
                          -db_filtered_password $DB_PASS \
                          -db_repl_user $DB_USER \
                          -db_repl_password $DB_PASS"
    else
        echo "Setting external db args for replicas"
        external_db_args="-init_db_name_override $DB_NAME \
                          -db_filtered_user $DB_USER \
                          -db_filtered_password $DB_PASS \
                          -db_repl_user $DB_USER \
                          -db_repl_password $DB_PASS \
                          -restore_from_backup"
    fi
else
    external_db_args="-init_db_name_override $DB_NAME \
                      -restore_from_backup"
fi


echo "Starting vttablet..."
exec $VTROOT/bin/vttablet \
  $TOPOLOGY_FLAGS \
  -logtostderr=true \
  -tablet-path $alias \
  -tablet_hostname "$vthost" \
  -health_check_interval 5s \
  -enable_semi_sync \
  -enable_replication_reporter \
  -heartbeat_enable \
  -heartbeat_interval 250ms \
  -port $web_port \
  -grpc_port $grpc_port \
  -binlog_use_v3_resharding_mode=true \
  -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
  -vtctld_addr "http://vtctld:$WEB_PORT/" \
  -init_keyspace $keyspace \
  -init_shard $shard \
  -init_tablet_type $tablet_type \
  -backup_storage_implementation file \
  -file_backup_storage_root $VTDATAROOT/backups \
  -queryserver-config-schema-reload-time 60 \
  $external_db_args
