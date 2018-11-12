#!/bin/bash
set -ex

# SIGTERM-handler
term_handler() {
  exit;
}

# setup handlers
# on callback, kill the last background process, which is `tail -f /dev/null` and execute the specified handler
trap 'kill ${!}; term_handler' SIGINT SIGTERM SIGHUP

# wait forever
while true
do
  /usr/sbin/logrotate -s /vt/logrotate.status /vt/logrotate.conf
  # run once an hour
  sleep 3600 & wait ${!}
done