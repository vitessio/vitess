#!/bin/bash

# Copyright 2021 The Vitess Authors.
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

# Setup the Vschema Folder
/vt/setup_vschema_folder.sh "$KEYSPACES" "$NUM_SHARDS"

# Set the maximum connections in the cnf file
# use 1000 as the default if it is unspecified
if [[ -z $MYSQL_MAX_CONNECTIONS ]]; then
  MYSQL_MAX_CONNECTIONS=1000
fi
echo "max_connections = $MYSQL_MAX_CONNECTIONS" >> /vt/config/mycnf/test-suite.cnf

# Delete socket files before running mysqlctld if exists.
# This is the primary reason for unhealthy state on restart.
# https://github.com/vitessio/vitess/pull/5115/files
rm -vf "$VTDATAROOT"/"$tablet_dir"/{mysql.sock,mysql.sock.lock}

# Run the vttestserver binary
/vt/bin/vttestserver \
	--port "$PORT" \
	--keyspaces "$KEYSPACES" \
	--num-shards "$NUM_SHARDS" \
	--mysql-bind-host "${MYSQL_BIND_HOST:-127.0.0.1}" \
	--vtcombo-bind-host "${VTCOMBO_BIND_HOST:-127.0.0.1}" \
	--mysql-server-version "${MYSQL_SERVER_VERSION:-$1}" \
	--charset "${CHARSET:-utf8mb4}" \
	--foreign-key-mode "${FOREIGN_KEY_MODE:-allow}" \
	--enable-online-ddl="${ENABLE_ONLINE_DDL:-true}" \
	--enable-direct-ddl="${ENABLE_DIRECT_DDL:-true}" \
	--planner-version="${PLANNER_VERSION:-gen4}" \
	--vschema-ddl-authorized-users=% \
	--tablet-refresh-interval "${TABLET_REFRESH_INTERVAL:-10s}" \
	--schema-dir="/vt/schema/"

