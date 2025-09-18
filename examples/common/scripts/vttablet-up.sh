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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

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

echo "Starting backup script for vttablet for $alias..."

# shellcheck disable=SC2086
vttablet \
 $TOPOLOGY_FLAGS \
 --log_dir $VTDATAROOT/tmp \
 --log-queries-to-file $VTDATAROOT/tmp/$tablet_logfile \
 --tablet-path $alias \
 --tablet-hostname "$tablet_hostname" \
 --init-keyspace $keyspace \
 --init-shard $shard \
 --init-tablet-type $tablet_type \
 --health-check-interval 5s \
 --backup-storage-implementation file \
 --file-backup-storage-root $VTDATAROOT/backups \
 --restore-from-backup \
 --port $port \
 --grpc-port $grpc_port \
 --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 --pid-file $VTDATAROOT/$tablet_dir/vttablet.pid \
 --heartbeat-on-demand-duration=5s \
 --pprof-http \
 > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

# Block waiting for the tablet to be listening
# Not the same as healthy

for i in $(seq 0 300); do
 curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
 sleep 0.1
done

# check one last time
curl -I "http://$hostname:$port/debug/status" || fail "tablet could not be started!"

echo -e "vttablet for $alias is running!"
