#!/bin/bash
B=/tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog
cmd=$1; n=$2; d=$B/$n
case $cmd in
init) /usr/sbin/mysqld --defaults-file=$d/my.cnf --initialize-insecure --user=root ;;
start) nohup /usr/sbin/mysqld --defaults-file=$d/my.cnf --user=root >/dev/null 2>&1 &
  for i in $(seq 60); do mysqladmin -uroot -S $d/mysql.sock ping >/dev/null 2>&1 && exit 0; sleep 0.5; done; echo "start failed"; tail -20 $d/logs/error.log; exit 1;;
stop) mysqladmin -uroot -S $d/mysql.sock shutdown; for i in $(seq 120); do [ -e $d/mysqld.pid ] || exit 0; sleep 0.5; done ;;
kill9) p=$(cat $d/mysqld.pid); kill -9 $p; for i in $(seq 100); do kill -0 $p 2>/dev/null || break; sleep 0.2; done; rm -f $d/mysqld.pid ;;
sql) shift 2; mysql -uroot -S $d/mysql.sock -N -e "$*" ;;
sqlv) shift 2; mysql -uroot -S $d/mysql.sock -e "$*" ;;
esac
