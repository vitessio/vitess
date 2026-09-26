#!/bin/bash
# rotate and purge binary logs on all mysqlds of the P7 cluster (no replicas; the disk is shared)
for s in /home/vt/perf/c40000/vt_*/mysql.sock; do
  mysql -S "$s" -u vt_dba -e "flush binary logs; purge binary logs before now() + interval 1 second" 2>/dev/null
done
