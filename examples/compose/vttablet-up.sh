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

set -u
export VTROOT=/vt
export VTDATAROOT=/vt/vtdataroot

keyspace=${KEYSPACE:-'test_keyspace'}
shard=${SHARD:-'0'}
grpc_port=${GRPC_PORT:-'15999'}
web_port=${WEB_PORT:-'8080'}
role=${ROLE:-'replica'}
vthost=${VTHOST:-`hostname -i`}
sleeptime=${SLEEPTIME:-'0'}
uid=$1
external=${EXTERNAL_DB:-0}

# If DB is not explicitly set, we default to behaviour of prefixing with vt_
# If there is an external db, the db_nmae will always match the keyspace name
[ $external = 0 ] && db_name=${DB:-"vt_$keyspace"} ||  db_name=${DB:-"$keyspace"}
db_charset=${DB_CHARSET:-''}
tablet_hostname=''

# Use IPs to simplify connections when testing in docker.
# Otherwise, blank hostname means the tablet auto-detects FQDN.
# This is now set further up

printf -v alias '%s-%010d' $CELL $uid
printf -v tablet_dir 'vt_%010d' $uid

tablet_role=$role
tablet_type='replica'

# Make every 3rd tablet rdonly
if (( $uid % 100 % 3 == 0 )) ; then
    tablet_type='rdonly'
fi

# Consider every tablet with %d00 as external primary
if [ $external = 1 ] && (( $uid % 100 == 0 )) ; then
    tablet_type='replica'
    tablet_role='externalprimary'
    keyspace="ext_$keyspace"
fi

# Copy config directory
cp -R /script/config $VTROOT
init_db_sql_file="$VTROOT/config/init_db.sql"
# Clear in-place edits of init_db_sql_file if any exist
sed -i '/##\[CUSTOM_SQL/{:a;N;/END\]##/!ba};//d' $init_db_sql_file

echo "##[CUSTOM_SQL_START]##" >> $init_db_sql_file

if [ "$external" = "1" ]; then
  # We need a common user for the unmanaged and managed tablets else tools like orchestrator will not function correctly
  echo "Creating matching user for managed tablets..."
  echo "CREATE USER IF NOT EXISTS '$DB_USER'@'%' IDENTIFIED BY '$DB_PASS';" >> $init_db_sql_file
  echo "GRANT ALL ON *.* TO '$DB_USER'@'%';" >> $init_db_sql_file
fi
echo "##[CUSTOM_SQL_END]##" >> $init_db_sql_file

echo "##[CUSTOM_SQL_END]##" >> $init_db_sql_file

mkdir -p $VTDATAROOT/backups


export KEYSPACE=$keyspace
export SHARD=$shard
export TABLET_ID=$alias
export TABLET_DIR=$tablet_dir
export MYSQL_PORT=3306
export TABLET_ROLE=$tablet_role
export DB_PORT=${DB_PORT:-3306}
export DB_HOST=${DB_HOST:-""}
export DB_NAME=$db_name

# Delete socket files before running mysqlctld if exists.
# This is the primary reason for unhealthy state on restart.
# https://github.com/vitessio/vitess/pull/5115/files
echo "Removing $VTDATAROOT/$tablet_dir/{mysql.sock,mysql.sock.lock}..."
rm -rf $VTDATAROOT/$tablet_dir/{mysql.sock,mysql.sock.lock}

# Create mysql instances
# Do not create mysql instance for primary if connecting to external mysql database
#TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
if [[ $tablet_role != "externalprimary" ]]; then
  echo "Initing mysql for tablet: $uid role: $role external: $external.. "
  $VTROOT/bin/mysqlctld \
  --init-db-sql-file=$init_db_sql_file \
  --logtostderr=true \
  --tablet-uid=$uid \
  &
fi

sleep $sleeptime

# Create the cell
# https://vitess.io/blog/2020-04-27-life-of-a-cluster/
$VTROOT/bin/vtctldclient --server vtctld:$GRPC_PORT AddCellInfo --root vitess/$CELL --server-address consul1:8500 $CELL || true

#Populate external db conditional args
if [ $tablet_role = "externalprimary" ]; then
    echo "Setting external db args for primary: $DB_NAME"
    external_db_args="--db-host $DB_HOST \
                      --db-port $DB_PORT \
                      --init-db-name-override $DB_NAME \
                      --init-tablet-type $tablet_type \
                      --mycnf-server-id $uid \
                      --db-app-user $DB_USER \
                      --db-app-password $DB_PASS \
                      --db-allprivs-user $DB_USER \
                      --db-allprivs-password $DB_PASS \
                      --db-appdebug-user $DB_USER \
                      --db-appdebug-password $DB_PASS \
                      --db-dba-user $DB_USER \
                      --db-dba-password $DB_PASS \
                      --db-filtered-user $DB_USER \
                      --db-filtered-password $DB_PASS \
                      --db-repl-user $DB_USER \
                      --db-repl-password $DB_PASS \
                      --enable-replication-reporter=false \
                      --enforce-strict-trans-tables=false \
                      --track-schema-versions=true \
                      --vreplication-tablet-type=primary \
                      --watch-replication-stream=true"
else
    external_db_args="--init-db-name-override $DB_NAME \
                      --init-tablet-type $tablet_type \
                      --enable-replication-reporter=true \
                      --restore-from-backup"
fi

#TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
echo "Starting vttablet..."
exec $VTROOT/bin/vttablet \
  $TOPOLOGY_FLAGS \
  --logtostderr=true \
  --tablet-path $alias \
  --tablet-hostname "$vthost" \
  --health-check-interval 5s \
  --port $web_port \
  --grpc-port $grpc_port \
  --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  --init-keyspace $keyspace \
  --init-shard $shard \
  --backup-storage-implementation file \
  --file-backup-storage-root $VTDATAROOT/backups \
  --queryserver-config-schema-reload-time 60s \
  $external_db_args
