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

# This is an example script that creates a single shard vttablet deployment.

set -e

cell='test'
keyspace=${KEYSPACE:-'test_keyspace'}
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
# Pass TABLETS_UIDS indices as env variable to change
uids=${TABLETS_UIDS:-'0 1 2 3 4'}

# Start all mysqlds in background.
for uid_index in $uids; do
  uid=$[$uid_base + $uid_index]
  mysql_port=$[$mysql_port_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid

  export KEYSPACE=$keyspace
  export SHARD=$shard
  export TABLET_ID=$alias
  export TABLET_DIR=$tablet_dir
  export MYSQL_PORT=$mysql_port

  tablet_type=replica
  if [[ $uid_index -gt 2 ]]; then
    tablet_type=rdonly
  fi

  export TABLET_TYPE=$tablet_type

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
    -mysql_port $mysql_port \
    $action &
done

# Wait for all mysqld to start up.
wait

optional_auth_args=''
if [ "$1" = "--enable-grpc-static-auth" ];
then
	  echo "Enabling Auth with static authentication in grpc"
    optional_auth_args='-grpc_auth_mode static -grpc_auth_static_password_file ./grpc_static_auth.json'
fi

# Start all vttablets in background.
for uid_index in $uids; do
  uid=$[$uid_base + $uid_index]
  port=$[$port_base + $uid_index]
  grpc_port=$[$grpc_port_base + $uid_index]
  printf -v alias '%s-%010d' $cell $uid
  printf -v tablet_dir 'vt_%010d' $uid
  printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid
  tablet_type=replica
  if [[ $uid_index -gt 2 ]]; then
    tablet_type=rdonly
  fi

  echo "Starting vttablet for $alias..."
  # shellcheck disable=SC2086
  $VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
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
    $optional_auth_args \
    > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

  echo "Access tablet $alias at http://$hostname:$port/debug/status"
done

disown -a
