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

# Run the vttestserver binary
bin/vttestserver \
	--port "33804" \
	--keyspaces "unsharded" \
	--num_shards "1" \
	--mysql_bind_host "127.0.0.1" \
	--charset "utf8mb4" \
	--foreign_key_mode "allow" \
	--enable_online_ddl="true" \
	--enable_direct_ddl="true" \
	--planner-version="v3" \
	--vschema_ddl_authorized_users=% \
	--tablet_refresh_interval "11s"
	--schema_dir="/vtdataroot/vt/schema/"

