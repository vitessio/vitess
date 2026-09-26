#!/bin/bash
# purge.sh: rotate and purge binary logs on all mysqlds of the P2 cluster (no replicas, disk is shared and small)
for s in /home/vt/perf/c30000/vt_*/mysql.sock; do
  mysql -S "$s" -u vt_dba -e "flush binary logs; purge binary logs before now() + interval 1 second" 2>/dev/null
done
