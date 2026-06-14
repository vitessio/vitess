#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

set -euo pipefail

cell=${CELL:-'zone1'}
keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
uid=${TABLET_UID}
mysql_port=17000
port=15000
grpc_port=16000

printf -v alias '%s-%010d' "${cell}" "${uid}"
printf -v tablet_dir 'vt_%010d' "${uid}"

tablet_type=replica
last_digit="${uid: -1}"
if [[ "${last_digit}" -gt 1 ]]; then
  tablet_type=rdonly
fi

mkdir -p /vt/vtdataroot/backups

echo "Starting MySQL for tablet ${alias}..."
action="init"
if [ -d "/vt/vtdataroot/${tablet_dir}" ]; then
  echo "Resuming from existing vttablet dir: /vt/vtdataroot/${tablet_dir}"
  action='start'
fi

mysqlctl \
  --tablet-uid "${uid}" \
  --mysql-port ${mysql_port} \
  --log-format text \
  ${action}

echo "MySQL for tablet ${alias} is running!"

echo "Starting vttablet for ${alias}..."
# shellcheck disable=SC2086
exec vttablet \
  ${TOPOLOGY_FLAGS} \
  --tablet-path "${alias}" \
  --tablet-hostname "vttablet${uid}" \
  --init-keyspace "${keyspace}" \
  --init-shard "${shard}" \
  --init-tablet-type "${tablet_type}" \
  --health-check-interval 5s \
  --backup-storage-implementation file \
  --file-backup-storage-root /vt/vtdataroot/backups \
  --restore-from-backup \
  --port ${port} \
  --grpc-port ${grpc_port} \
  --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  --heartbeat-on-demand-duration=5s \
  --pprof-http \
  --log-format text \
  --config-file-not-found-handling=ignore
