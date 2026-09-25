#!/bin/bash
# Poll processlist of mysqld 101 every 100ms for N seconds.
for i in $(seq 1 ${N:-80}); do
  echo "--- $(date +%T.%3N)"
  mysql -S /home/vt/perf/c40000/vt_0000000101/mysql.sock -u vt_dba -N -e "select id,user,db,command,time,state,left(info,50) from information_schema.processlist where user not in ('event_scheduler','system user') and id <> connection_id()" 2>/dev/null
  sleep 0.1
done
