#!/bin/bash
# Print the vreplication streams of every tablet of the P4 cluster.
for d in /home/vt/perf/c40000/vt_*; do
  echo "== $d"
  mysql -N -S "$d/mysql.sock" -u vt_dba -e "select id, workflow, state, left(message, 200), from_unixtime(time_updated), left(pos, 80) from _vt.vreplication" 2>&1
done
date
/home/vt/perf/P4/c.sh vtctldclient GetTablets
