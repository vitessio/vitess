#!/bin/bash -e

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

sleeptime=${SLEEPTIME:-0}
targettab=${TARGETTAB:-"${CELL}-0000000101"}
schema_files=${SCHEMA_FILES:-'create_messages.sql create_tokens.sql'}
vschema_file=${VSCHEMA_FILE:-'default_vschema.json'}
load_file=${POST_LOAD_FILE:-''}
external_db=${EXTERNAL_DB:-'0'}
export PATH=/vt/bin:$PATH

sleep $sleeptime

if [ ! -f schema_run ]; then
  while true; do
    vtctlclient -server vtctld:$GRPC_PORT GetTablet $targettab && break
    sleep 1
  done
  if [ "$external_db" = "0" ]; then
    for schema_file in $schema_files; do
      echo "Applying Schema ${schema_file} to ${KEYSPACE}"
      vtctlclient -server vtctld:$GRPC_PORT ApplySchema -sql-file /script/tables/${schema_file} $KEYSPACE || \
      vtctlclient -server vtctld:$GRPC_PORT ApplySchema -sql "$(cat /script/tables/${schema_file})" $KEYSPACE || true
    done
  fi
  echo "Applying VSchema ${vschema_file} to ${KEYSPACE}"
  
  vtctlclient -server vtctld:$GRPC_PORT ApplyVSchema -vschema_file /script/${vschema_file} $KEYSPACE || \
  vtctlclient -server vtctld:$GRPC_PORT ApplyVSchema -vschema "$(cat /script/${vschema_file})" $KEYSPACE
  
  echo "List All Tablets"
  vtctlclient -server vtctld:$GRPC_PORT ListAllTablets
    
  if [ -n "$load_file" ]; then
    # vtgate can take a REALLY long time to come up fully
    sleep 60
    mysql --port=15306 --host=vtgate < /script/$load_file
  fi

  touch /vt/schema_run
  echo "Time: $(date). SchemaLoad completed at $(date "+%FT%T") " >> /vt/schema_run
  echo "Done Loading Schema at $(date "+%FT%T")"
fi
