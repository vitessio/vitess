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

# Starts a vttestserver with two unsharded (u0, u1) and two sharded (s0, s1) keyspaces for SQLancer, and gives
# the sharded keyspaces a vschema with the vindexes that run.sh can use. Run it as a non-root user (mysqlctl
# refuses to run as root) with the Vitess binaries in $VTROOT/bin and mysqld on the PATH.
#
# The vtgate MySQL port is VTGATE_PORT (default 33573, i.e. --port + 3).

set -euo pipefail

BASE_PORT=${BASE_PORT:-33570}
VTGATE_PORT=$((BASE_PORT + 3))
VTCTLD_GRPC_PORT=$((BASE_PORT + 1))
: "${VTROOT:?set VTROOT to a directory that contains bin/ (the Vitess binaries) and config/}"
: "${VTDATAROOT:?set VTDATAROOT to a scratch directory}"

"$VTROOT/bin/vttestserver" \
  --keyspaces u0,u1,s0,s1 --num-shards 1,1,2,2 \
  --replica-count 1 --rdonly-count 0 \
  --port "$BASE_PORT" --mysql-bind-host 127.0.0.1 \
  --vschema-ddl-authorized-users '%' --initialize-with-vt-dba-tcp &
pid=$!
trap 'kill $pid' INT TERM

until mysql -h127.0.0.1 -P"$VTGATE_PORT" -uroot -e 'select 1' >/dev/null 2>&1; do
  kill -0 $pid
  sleep 2
done

for ks in s0 s1; do
  "$VTROOT/bin/vtctldclient" --server "127.0.0.1:$VTCTLD_GRPC_PORT" ApplyVSchema \
    --vschema '{"sharded":true,"vindexes":{"hash":{"type":"hash"},"xxhash":{"type":"xxhash"}},"tables":{}}' "$ks" >/dev/null
done
echo "vtgate is listening on 127.0.0.1:$VTGATE_PORT"
wait $pid
