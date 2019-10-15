#!/bin/bash -e

sleeptime=${SLEEPTIME:-0}
targettab=${TARGETTAB:-'test-0000000101'}
schema_files=${SCHEMA_FILES:-'create_messages.sql create_tokens.sql'}
vschema_file=${VSCHEMA_FILE:-'base_vschema.json'}
load_file=${POST_LOAD_FILE:-''}
external_db=${EXTERNAL_DB:-'0'}

sleep $sleeptime

if [ ! -f schema_run ]; then
  while true; do
    vtctlclient -server vtctld:$GRPC_PORT GetTablet $targettab && break
    sleep 1
  done
  if [ "$external_db" = "0" ]; then
    for schema_file in $schema_files; do
      vtctlclient -server vtctld:$GRPC_PORT ApplySchema -sql-file /script/tables/${schema_file} $KEYSPACE
    done
  fi
  vtctlclient -server vtctld:$GRPC_PORT ApplyVSchema -vschema_file /script/${vschema_file} $KEYSPACE

  if [ -n "$load_file" ]; then
    # vtgate can take a REALLY long time to come up fully
    sleep 60
    mysql --port=15306 --host=vtgate < /script/$load_file
  fi

  touch schema_run
fi
