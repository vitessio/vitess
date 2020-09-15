#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

source ./env.sh

cell=${CELL:-'test'}
keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
uid=$TABLET_UID
mysql_port=$[17000 + $uid]
port=$[15000 + $uid]
grpc_port=$[16000 + $uid]
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
tablet_hostname=''
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

tablet_type=replica
if [[ "${uid: -1}" -gt 1 ]]; then
 tablet_type=rdonly
fi

echo "Starting vttablet for $alias..."
# shellcheck disable=SC2086
vttablet \
 $TOPOLOGY_FLAGS \
 -log_dir $VTDATAROOT/tmp \
 -log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
 -tablet-path $alias \
 -tablet_hostname "$tablet_hostname" \
 -init_keyspace $keyspace \
 -init_shard $shard \
 -init_tablet_type $tablet_type \
 -health_check_interval 5s \
 -enable_replication_reporter \
 -backup_storage_implementation file \
 -file_backup_storage_root $VTDATAROOT/backups \
 -restore_from_backup \
 -port $port \
 -grpc_port $grpc_port \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
 -vtctld_addr http://$hostname:$vtctld_web_port/ \
 -disable_active_reparents \
 > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

# Block waiting for the tablet to be listening
# Not the same as healthy

for i in $(seq 0 300); do
 curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
 sleep 0.1
done

# check one last time
curl -I "http://$hostname:$port/debug/status" || fail "tablet could not be started!"
