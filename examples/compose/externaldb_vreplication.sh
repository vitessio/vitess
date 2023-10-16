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

# Wait until source and destination primaries are available
until (/vt/bin/vtctldclient --server $VTCTLD_SERVER GetTablets | grep "ext_" | grep "primary" ); do
  echo 'waiting for external primary..';
  sleep 1;
done

until (/vt/bin/vtctldclient --server $VTCTLD_SERVER GetTablets | grep -v "ext_" | grep "primary" ); do
  echo 'waiting for managed primary..';
  sleep 1;
done


# Get source and destination tablet and shard information
TABLET_INFO=$(/vt/bin/vtctldclient --server $VTCTLD_SERVER GetTablets)
source_alias=$(echo "$TABLET_INFO "| grep "ext_" | grep "primary" | awk '{ print $1 }')
dest_alias=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "primary" | awk '{ print $1 }')
source_keyspace=$(echo "$TABLET_INFO "| grep "ext_" | grep "primary" | awk '{ print $2 }')
dest_keyspace=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "primary" | awk '{ print $2 }')
source_shard=$(echo "$TABLET_INFO "| grep "ext_" | grep "primary" | awk '{ print $3 }')
dest_shard=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "primary" | awk '{ print $3 }')
source_tablet=$(echo "$TABLET_INFO "| grep "ext_" | grep "primary" | awk '{ print $2 "/" $3}')
dest_tablet=$(echo "$TABLET_INFO "| grep -v "ext_" | grep "primary" | awk '{ print $2 "/" $3}')


# Disable foreign_key checks on destination
/vt/bin/vtctldclient --server $VTCTLD_SERVER ExecuteFetchAsDBA $dest_alias 'SET GLOBAL FOREIGN_KEY_CHECKS=0;'

# Get source_sql mode
source_sql_mode=$(/vt/bin/vtctldclient --server $VTCTLD_SERVER ExecuteFetchAsDBA $source_alias 'SELECT @@GLOBAL.sql_mode' | awk 'NR==4 {print $2}')

# Apply source sql_mode to destination
# The intention is to avoid replication errors
/vt/bin/vtctldclient --server $VTCTLD_SERVER ExecuteFetchAsDBA $dest_alias "SET GLOBAL sql_mode='$source_sql_mode';"

# Verify sql_mode matches
[ $source_sql_mode == $(/vt/bin/vtctldclient --server $VTCTLD_SERVER ExecuteFetchAsDBA $dest_alias 'SELECT @@GLOBAL.sql_mode' | awk 'NR==4 {print $2}') ] && \
echo "Source and Destination sql_mode Match." || echo "sql_mode MisMatch"

until /vt/bin/vtctldclient --server $VTCTLD_SERVER GetSchema $dest_alias; do
  echo "Waiting for destination schema to be ready..";
  sleep 3;
done

# Start vreplication workflow
/vt/bin/vtctldclient --server $VTCTLD_SERVER MoveTables --workflow ext_commerce2commerce --target-keyspace $dest_keyspace create --source-keyspace $source_keyspace --all-tables

# Check vreplication status
/vt/bin/vtctldclient --server $VTCTLD_SERVER MoveTables --workflow ext_commerce2commerce --target-keyspace $dest_keyspace show

