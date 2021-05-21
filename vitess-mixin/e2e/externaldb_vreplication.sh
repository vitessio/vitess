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

set -ex

VTCTLD_SERVER=${VTCTLD_SERVER:-'vtctld:15999'}

# Wait until source and destination masters are available
until (/vt/bin/vtctlclient --server $VTCTLD_SERVER ListAllTablets | grep "ext_" | grep "master" ); do
  echo 'waiting for external master..';
  sleep 1;
done

until (/vt/bin/vtctlclient --server $VTCTLD_SERVER ListAllTablets | grep -v "ext_" | grep "master" ); do
  echo 'waiting for managed master..';
  sleep 1;
done


# Get source and destination tablet and shard information
TABLET_INFO=$(/vt/bin/vtctlclient --server $VTCTLD_SERVER ListAllTablets)
source_alias=$(echo "$TABLET_INFO "| grep "ext_" | grep "master" | awk '{ print $1 }')
dest_alias=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "master" | awk '{ print $1 }')
source_keyspace=$(echo "$TABLET_INFO "| grep "ext_" | grep "master" | awk '{ print $2 }')
dest_keyspace=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "master" | awk '{ print $2 }')
source_shard=$(echo "$TABLET_INFO "| grep "ext_" | grep "master" | awk '{ print $3 }')
dest_shard=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "master" | awk '{ print $3 }')
source_tablet=$(echo "$TABLET_INFO "| grep "ext_" | grep "master" | awk '{ print $2 "/" $3}')
dest_tablet=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "master" | awk '{ print $2 "/" $3}')


# Disable foreign_key checks on destination
/vt/bin/vtctlclient --server $VTCTLD_SERVER ExecuteFetchAsDba $dest_alias 'SET GLOBAL FOREIGN_KEY_CHECKS=0;'

# Get source_sql mode
source_sql_mode=$(/vt/bin/vtctlclient --server $VTCTLD_SERVER ExecuteFetchAsDba $source_alias 'SELECT @@GLOBAL.sql_mode' | awk 'NR==4 {print $2}')

# Apply source sql_mode to destination
# The intention is to avoid replication errors
/vt/bin/vtctlclient --server $VTCTLD_SERVER ExecuteFetchAsDba $dest_alias "SET GLOBAL sql_mode='$source_sql_mode';"

# Verify sql_mode matches
[ $source_sql_mode == $(/vt/bin/vtctlclient --server $VTCTLD_SERVER ExecuteFetchAsDba $dest_alias 'SELECT @@GLOBAL.sql_mode' | awk 'NR==4 {print $2}') ] && \
echo "Source and Destination sql_mode Match." || echo "sql_mode MisMatch"

until /vt/bin/vtctlclient --server $VTCTLD_SERVER GetSchema $dest_alias; do
  echo "Waiting for destination schema to be ready..";
  sleep 3;
done

# Copy schema from source to destination shard
/vt/bin/vtctlclient --server $VTCTLD_SERVER CopySchemaShard $source_tablet $dest_tablet || true

# Verify schema
/vt/bin/vtctlclient --server $VTCTLD_SERVER GetSchema $dest_alias

# Start vreplication
/vt/bin/vtctlclient --server $VTCTLD_SERVER VReplicationExec $dest_alias 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"''"$dest_keyspace"''"'"', '"'"'keyspace:\"'"$source_keyspace"'\" shard:\"'"$source_shard"'\" filter:<rules:<match:\"/.*\" > > on_ddl:EXEC_IGNORE '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'

# Check vreplication status
/vt/bin/vtctlclient --server $VTCTLD_SERVER VReplicationExec $dest_alias 'select * from _vt.vreplication'

