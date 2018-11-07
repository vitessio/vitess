#!/bin/bash
set -ex

# SIGTERM-handler
term_handler() {
  # block shutting down mysqlctld until vttablet shuts down first
  until [ $MYSQL_GONE ]; do

    # poll every 5 seconds to see if vttablet is still running
    mysqladmin ping -uroot --socket=/vtdataroot/tabletdata/mysql.sock

    if [ $? -ne 0 ]; then
      MYSQL_GONE=true
    fi

    sleep 5
  done
  
  exit;
}

# setup handlers
# on callback, kill the last background process, which is `tail -f /dev/null` and execute the specified handler
trap 'kill ${!}; term_handler' SIGINT SIGTERM SIGHUP

# wait forever
while true
do
  tail -n+1 -F "$TAIL_FILEPATH" & wait ${!}
done