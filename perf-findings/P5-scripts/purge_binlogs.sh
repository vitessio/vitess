#!/bin/bash
# Rotate and purge binlogs on every mysqld of the P5 cluster to save disk.
for d in /home/vt/perf/c40000/vt_*; do
  mysql -S $d/mysql.sock -u vt_dba -e "FLUSH BINARY LOGS; PURGE BINARY LOGS BEFORE NOW() + INTERVAL 1 SECOND;" 2>&1 | grep -v Warning
done
df -h / | tail -1
