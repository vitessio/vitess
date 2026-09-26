#!/bin/bash
# Turn the general log of tablets 103 and 104 on or off.  Usage: gl.sh on|off
v=0
[[ $1 == on ]] && v=1
for u in 103 104; do
  mysql -S "/home/vt/perf/c40000/vt_0000000$u/mysql.sock" -u vt_dba \
    -e "SET GLOBAL general_log_file='/home/vt/perf/c40000/gl_$u.log'; SET GLOBAL general_log=$v"
done
