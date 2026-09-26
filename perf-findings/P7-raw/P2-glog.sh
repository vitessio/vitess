#!/bin/bash
# glog.sh "sysbench args..." : run a short sysbench (1 thread, N events) with the general log on for mysqld uid 100; print log
S=/home/vt/perf/c30000/vt_0000000100/mysql.sock
L=/home/vt/perf/c30000/glog100.log
M="mysql -S $S -u vt_dba"
runuser -u vt -- $M -e "set global general_log_file='$L'; set global general_log=1"
/home/vt/perf/P2/c.sh sb "$@" >/dev/null 2>&1
runuser -u vt -- $M -e "set global general_log=0"
cat $L; rm -f $L
