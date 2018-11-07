#!/bin/bash
set -ex

# SIGTERM-handler
term_handler() {
  # block shutting down log tailers until mysql shuts down first
  until [ "$MYSQL_GONE" ]; do
    sleep 5

    # poll every 5 seconds to see if mysql is still running
    if ! mysqladmin ping -uroot --socket=/vtdataroot/tabletdata/mysql.sock; then
      MYSQL_GONE=true
    fi
  done
  
  exit;
}

# setup handlers
# on callback, kill the last background process and execute the specified handler
trap 'kill ${!}; term_handler' SIGINT SIGTERM SIGHUP

# wait forever
while true
do
  tail -n+1 -F "$TAIL_FILEPATH" & wait ${!}
done