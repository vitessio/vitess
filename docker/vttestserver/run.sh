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
# Run the vttestserver binary
/vt/bin/vttestserver -port "$PORT" -keyspaces "$KEYSPACES" -num_shards "$NUM_SHARDS" -mysql_bind_host "${MYSQL_BIND_HOST:-127.0.0.1}" -mysql_server_version "${MYSQL_SERVER_VERSION:-$1}" -vschema_ddl_authorized_users=% -schema_dir="/vt/schema/"