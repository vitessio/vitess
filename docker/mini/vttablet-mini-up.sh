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

mysql_host="$1"
mysql_port="$2"
is_master="$3"

source ./env.sh

cell=${CELL:-'test'}
keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
uid=$TABLET_UID
port=$[15000 + $TABLET_UID]
grpc_port=$[16000 + $uid]
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
tablet_hostname="$mysql_host"
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

mkdir -p "$VTDATAROOT/$tablet_dir"

tablet_type=replica

echo "> Starting vttablet for server $mysql_host:$mysql_port"
echo "  - Tablet alias is $alias"
echo "  - Tablet listens on http://$hostname:$port"
# shellcheck disable=SC2086
vttablet \
 $TOPOLOGY_FLAGS \
 -log_dir $VTDATAROOT/tmp \
 -log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
 -tablet-path $alias \
 -tablet_hostname "$hostname" \
 -init_db_name_override "$keyspace" \
 -init_keyspace $keyspace \
 -init_shard $shard \
 -init_tablet_type $tablet_type \
 -health_check_interval 5s \
 -enable_semi_sync \
 -enable_replication_reporter \
 -backup_storage_implementation file \
 -file_backup_storage_root $VTDATAROOT/backups \
 -port $port \
 -grpc_port $grpc_port \
 -db_host $mysql_host \
 -db_port $mysql_port \
 -db_app_user $TOPOLOGY_USER \
 -db_app_password $TOPOLOGY_PASSWORD \
 -db_dba_user $TOPOLOGY_USER \
 -db_dba_password $TOPOLOGY_PASSWORD \
 -mycnf_mysql_port $mysql_port \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
 -vtctld_addr http://$hostname:$vtctld_web_port/ \
 > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

# Block waiting for the tablet to be listening
# Not the same as healthy

for i in $(seq 0 300); do
 curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
 sleep 0.1
done

# check one last time
curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 || fail "tablet could not be started!"

echo "  + vttablet started"

if [ "$is_master" == "true" ] ; then
  echo "  > Setting this tablet as master"
  vtctlclient TabletExternallyReparented "$alias" &&
    echo "  + done"
fi
