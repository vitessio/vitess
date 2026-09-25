#!/bin/bash
# Time SET GLOBAL rpl_semi_sync_source_enabled=0 on a (non-primary) mysqld.
SOCK=${1:-/home/vt/perf/c40000/vt_0000000100/mysql.sock}
m() { mysql -S "$SOCK" -u vt_dba "$@"; }
for i in 1 2 3 4 5; do
  m -e "SET GLOBAL rpl_semi_sync_source_enabled=1"
  sleep ${WAIT:-0.3}
  s=$(date +%s%N)
  m -e "SET GLOBAL rpl_semi_sync_source_enabled=0"
  e=$(date +%s%N)
  echo "disable took $(( (e-s)/1000000 )) ms"
done
s=$(date +%s%N); m -e "SELECT 1" >/dev/null; e=$(date +%s%N); echo "noop client roundtrip $(( (e-s)/1000000 )) ms"
