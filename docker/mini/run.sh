#!/bin/bash
#
# Run a MiniVitess docker image, with details and credentials for a MySQL replication topology
#

TOPOLOGY_SERVER=""
MYSQL_SCHEMA=""
TOPOLOGY_USER=""
TOPOLOGY_PASSWORD=""

help() {
cat <<- "EOF"
Usage:
  docker/mini/run.sh <options>

Mandatory options:
 -s <topology server>: in hostname[:port] format; a single MySQL server in your replication
                       topology from which MiniVitess will discover your entire topology
 -d <database name>:   database/schema name on your MySQL server. A single schema is supported.
 -u <mysql user>:      MySQL username with enough privileges for Vitess to run DML, and for
                       orchestrator to probe replication
 -p <mysql password>:  password for given user
EOF
  exit 1
}

error_help() {
  local message="$1"
  echo "ERROR: $message"
  help
}

while getopts "s:d:u:p:h" OPTION
do
  case $OPTION in
    h) help ;;
    s) TOPOLOGY_SERVER="$OPTARG" ;;
    d) MYSQL_SCHEMA="$OPTARG" ;;
    u) TOPOLOGY_USER="$OPTARG" ;;
    p) TOPOLOGY_PASSWORD="$OPTARG" ;;
    *) help ;;
  esac
done

if [ -z "$TOPOLOGY_SERVER" ] ; then
  error_help "Expected topology server"
fi
if [ -z "$MYSQL_SCHEMA" ] ; then
  error_help "Expected MySQL schema/database name"
fi
if [ -z "$TOPOLOGY_USER" ] ; then
  error_help "Expected MySQL user"
fi
if [ -z "$TOPOLOGY_PASSWORD" ] ; then
  error_help "Expected MySQL password"
fi

docker run --rm -it -p 3000:3000 -p 15000:15000 -p 15001:15001 -e "TOPOLOGY_SERVER=$TOPOLOGY_SERVER" -e "TOPOLOGY_USER=$TOPOLOGY_USER" -e "TOPOLOGY_PASSWORD=$TOPOLOGY_PASSWORD" -e "MYSQL_SCHEMA=$MYSQL_SCHEMA" --network=host vitess/mini:latest
