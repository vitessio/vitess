#!/bin/bash

usage () {
  echo "Starts a session on a sideloaded vttablet."
  echo "Note that this is a direct MySQL connection; if you actually want to work with Vitess, connect via the vtgate with:"
  echo "  mysql --port=15306 --host=127.0.0.1"
  echo
  echo "Usage: $0 <tablet alias> [<keyspace>]"
  echo "  Don't forget the 'vt_' before the keyspace!"
}

if [ $# -lt 1 ]; then
  usage
  exit -1
fi

keyspace=${2:-vt_test_keyspace}
long_alias=`printf "%010d" $1`
docker-compose exec vttablet$1 mysql -uvt_dba -S /vt/vtdataroot/vt_${long_alias}/mysql.sock $keyspace
