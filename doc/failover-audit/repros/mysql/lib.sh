B=/tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog
cd $B
C=$B/ctl.sh
q(){ n=$1; shift; echo "[$n]> $*" >&2; mysql -uroot -S $B/$n/mysql.sock -N -e "$*"; }
qv(){ n=$1; shift; echo "[$n]> $*" >&2; mysql -uroot -S $B/$n/mysql.sock -e "$*"; }
killall_ours(){ for n in P R1 R2; do [ -e $B/$n/mysqld.pid ] && kill -9 $(cat $B/$n/mysqld.pid) 2>/dev/null; rm -f $B/$n/mysqld.pid; done; pkill -9 -f "$B/.*/my.cnf" 2>/dev/null; sleep 1; }
changesrc(){ # node port hb
  echo "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$2, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=$3, SOURCE_AUTO_POSITION=1"; }
reset_env(){ # $@ = extra cnf lines for R1
  killall_ours
  for n in P R1 R2; do rm -rf $B/$n/data $B/$n/logs; tar xzf clean_$n.tgz; done
  ./mkcnf.sh P 45001 101; ./mkcnf.sh R1 45002 102 "$@"; ./mkcnf.sh R2 45003 103
  for n in P R1 R2; do $C start $n || return 1; done
  q P "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; SET GLOBAL rpl_semi_sync_source_enabled=ON"
  for n in R1 R2; do
    q $n "SET GLOBAL rpl_semi_sync_replica_enabled=ON"
    q $n "STOP REPLICA; $(changesrc $n 45001 4); START REPLICA"
  done
  q R1 "SET GLOBAL innodb_lock_wait_timeout=100000; SET GLOBAL replica_transaction_retries=1000000; STOP REPLICA SQL_THREAD; START REPLICA SQL_THREAD"
  q P "CREATE DATABASE t; CREATE TABLE t.t(id INT PRIMARY KEY, v INT, pad VARCHAR(255)); INSERT INTO t.t(id,v) VALUES (1,0),(2,0),(3,0)"
  sleep 2
  for n in R1 R2; do q $n "SELECT COUNT(*) FROM t.t"; done
}
LOCKPID=
hold_lock(){ # hold row lock id=1 on R1 for $1 seconds
  mysql -uroot -S $B/R1/mysql.sock -N -e "BEGIN; SELECT id FROM t.t WHERE id=1 FOR SHARE; DO SLEEP($1); ROLLBACK;" > $B/lock.out 2>&1 &
  LOCKPID=$!; sleep 1; }
semistat(){ q P "SHOW GLOBAL STATUS WHERE Variable_name IN ('Rpl_semi_sync_source_yes_tx','Rpl_semi_sync_source_no_tx','Rpl_semi_sync_source_clients','Rpl_semi_sync_source_status')" | tr '\n' ' '; echo; }
# create acked-but-unapplied T on R1 (R2 IO stopped, applier blocked by lock)
make_T(){ # $1 = id of inserted row marker
  q R2 "STOP REPLICA IO_THREAD"
  if [ "${MODE:-lock}" = sqlstop ]; then q R1 "STOP REPLICA SQL_THREAD"; else hold_lock 100000; fi
  for i in $(seq 40); do [ "$(q P "SHOW GLOBAL STATUS LIKE 'Rpl_semi_sync_source_clients'" 2>/dev/null | cut -f2)" = 1 ] && break; sleep 0.5; done
  semistat
  local t0=$(date +%s.%N)
  for i in $(seq 0 $(( ${N:-1} - 1 ))); do
  timeout 20 mysql -uroot -S $B/P/mysql.sock -e "BEGIN; UPDATE t.t SET v=v+1 WHERE id=1; INSERT INTO t.t(id,v) VALUES ($(( $1 + i )),1); COMMIT;" && echo "T(id=$(( $1 + i ))) commit returned in $(echo "$(date +%s.%N)-$t0" | bc)s" || echo "T COMMIT TIMED OUT/FAILED"
  done
  semistat
  echo "P gtid_executed: $(q P 'select @@gtid_executed')"
  sleep 1
  ./rs.sh R1
  echo "R2 Retrieved: $(q R2 "select received_transaction_set from performance_schema.replication_connection_status" 2>/dev/null)"
  echo "R1 T rows present (id>=1000): $(q R1 "select count(*) from t.t where id>=1000")"
  mysql -uroot -S $B/R1/mysql.sock -e 'SHOW PROCESSLIST' | grep 'system user' | cut -c1-200
}
final(){ echo "--- FINAL R1"; ./rs.sh R1; echo "R1 T rows present (id>=1000): $(q R1 'select count(*) from t.t where id>=1000' 2>/dev/null) ; ids: $(q R1 'select group_concat(id) from t.t where id>=1000' 2>/dev/null)"; }
prep_shutdown(){ for s in "SET GLOBAL innodb_flush_log_at_trx_commit = 1" "SET GLOBAL sync_binlog = 1" "SET GLOBAL sync_relay_log = 1" "FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS" "FLUSH NO_WRITE_TO_BINLOG BINARY LOGS" "FLUSH NO_WRITE_TO_BINLOG RELAY LOGS" "STOP REPLICA IO_THREAD" "STOP REPLICA SQL_THREAD"; do timeout 10 mysql -uroot -S $B/R1/mysql.sock -e "$s" && echo "[R1]> $s : ok" || echo "[R1]> $s : FAILED/TIMED OUT (10s)"; done; ./rs.sh R1; }
release_lock(){ for id in $(mysql -uroot -S $B/R1/mysql.sock -N -e "SHOW PROCESSLIST" | awk -F'\t' '$8 ~ /^DO SLEEP/ {print $1}'); do q R1 "KILL $id"; done; }
