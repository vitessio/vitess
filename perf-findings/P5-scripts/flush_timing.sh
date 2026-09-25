#!/bin/bash
# Time FLUSH BINARY LOGS on a mysqld.
SOCK=${1:-/home/vt/perf/c40000/vt_0000000100/mysql.sock}
m() { mysql -S "$SOCK" -u vt_dba "$@"; }
for i in 1 2 3 4 5; do
  s=$(date +%s%N)
  m -e "${Q:-FLUSH BINARY LOGS}"
  e=$(date +%s%N)
  echo "took $(( (e-s)/1000000 )) ms"
  sleep 0.2
done
m -e "show variables like 'rpl_semi_sync%enabled'; show binary logs" | tail -3
