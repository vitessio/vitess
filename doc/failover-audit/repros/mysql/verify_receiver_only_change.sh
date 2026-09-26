#!/bin/bash
# Independent verification: can we switch replication source with the applier running
# (receiver stopped, receiver-only CHANGE) without losing relay-log content?
source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
recv_only(){ echo "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$1, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=2"; }
brief(){ mysql -uroot -S $B/$1/mysql.sock -e "SHOW REPLICA STATUS\G" | egrep "Source_Port|Replica_IO_Running:|Replica_SQL_Running:|Retrieved_Gtid_Set|Executed_Gtid_Set|Last_SQL_Error:|Last_IO_Error:" | tr -s ' '; }
checksum(){ q $1 "SELECT COUNT(*), COALESCE(SUM(v),0), COALESCE(SUM(LENGTH(pad)),0), COALESCE(SUM(LENGTH(b)),0) FROM t.t" 2>/dev/null; }
rel(){ ls -l $B/$1/logs/ | grep relay-bin | awk '{print $9"("$5")"}' | tr '\n' ' '; echo; }
N=3
case $1 in
V1)
  echo "=== V1 (repeat E4e): R1 applier blocked, R2 lacks T; STOP IO; receiver-only CHANGE to R2; kill P; START IO; release"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000 2>&1 | egrep "commit returned|Retrieved|Executed_Gtid"
  q R1 "STOP REPLICA IO_THREAD"
  q R1 "$(recv_only 45003)" && echo "  CHANGE OK"
  brief R1; rel R1
  $C kill9 P; echo "--- P killed"; q R1 "START REPLICA IO_THREAD"; sleep 2
  release_lock; sleep 4; brief R1; echo "R1 T rows: $(q R1 'select group_concat(id) from t.t where id>=1000')";;
V2)
  echo "=== V2: new source HAS T (R2 caught up); R1 applier blocked; STOP IO; receiver-only CHANGE to R2; START IO; release; compare"
  reset_env >/dev/null 2>&1
  hold_lock 100000
  for i in 0 1 2; do mysql -uroot -S $B/P/mysql.sock -e "BEGIN; UPDATE t.t SET v=v+1 WHERE id=1; INSERT INTO t.t(id,v) VALUES ($((1000+i)),1); COMMIT;"; done
  sleep 1; echo "R2: $(q R2 'select @@gtid_executed')"; brief R1
  q R1 "STOP REPLICA IO_THREAD"; q R1 "$(recv_only 45003)" && echo "  CHANGE OK"; rel R1
  # more writes on P after the switch, which reach R1 only through R2
  for i in 3 4; do mysql -uroot -S $B/P/mysql.sock -e "INSERT INTO t.t(id,v) VALUES ($((1000+i)),1)"; done
  q R1 "START REPLICA IO_THREAD"; sleep 2; release_lock; sleep 4
  brief R1; echo "P  sum: $(checksum P) gtid: $(q P 'select @@gtid_executed')"; echo "R1 sum: $(checksum R1) gtid: $(q R1 'select @@gtid_executed')";;
V3)
  echo "=== V3: applier STOPPED (sqlstop) + IO stopped; receiver-only CHANGE (docs: relay logs deleted when both threads stopped)"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000 2>&1 | egrep "commit returned|Retrieved|Executed_Gtid"
  q R1 "STOP REPLICA IO_THREAD"; rel R1
  q R1 "$(recv_only 45003)" && echo "  CHANGE OK"; brief R1; rel R1;;
V4|V5)
  # big transaction; stop R1's receiver mid-transfer so a partial transaction sits at the relay-log tail
  echo "=== $1: partial transaction at relay-log tail, applier running; switch to R2 ($( [ $1 = V4 ] && echo 'R2 HAS the big trx' || echo 'R2 LACKS it, P killed'))"
  reset_env >/dev/null 2>&1
  q P "ALTER TABLE t.t ADD COLUMN b LONGBLOB; SET GLOBAL max_allowed_packet=1073741824"
  for n in R1 R2; do q $n "SET GLOBAL max_allowed_packet=1073741824; SET GLOBAL replica_max_allowed_packet=1073741824"; done
  sleep 1
  [ $1 = V5 ] && q R2 "STOP REPLICA IO_THREAD"
  hold_lock 100000
  mysql -uroot -S $B/P/mysql.sock -e "SET SESSION cte_max_recursion_depth=100000; BEGIN; INSERT INTO t.t(id,v,b) WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<60) SELECT 5000+n, 1, REPEAT('x', 4*1024*1024) FROM s; COMMIT;" &
  CPID=$!
  # stop the receiver once the relay log has grown but before the transaction finished arriving
  for i in $(seq 12000); do s=$(stat -c %s $B/R1/logs/relay-bin.000002 2>/dev/null || echo 0); [ "$s" -gt 20000000 ] && break; sleep 0.01; done
  q R1 "STOP REPLICA IO_THREAD"; echo "stopped IO at relay size $(stat -c %s $B/R1/logs/relay-bin.000002)"
  brief R1; rel R1
  if [ $1 = V5 ]; then $C kill9 P; echo "--- P killed (big trx never fully acked? commit client: $(wait $CPID; echo rc=$?))"; else for w in $(seq 180); do case "$(q R2 'select @@gtid_executed' 2>/dev/null)" in *1-5) break;; esac; sleep 1; done; echo "P gtid: $(q P 'select @@gtid_executed')  R2 gtid: $(q R2 'select @@gtid_executed')"; brief R2; fi
  q R1 "$(recv_only 45003)" && echo "  CHANGE OK"; rel R1
  q R1 "START REPLICA IO_THREAD"; sleep 3; release_lock; for w in $(seq 60); do [ "$(q R1 "select @@gtid_executed" 2>/dev/null)" = "$(q R1 "select received_transaction_set from performance_schema.replication_connection_status" 2>/dev/null)" ] && break; sleep 1; done
  brief R1; rel R1
  echo "R2 sum: $(checksum R2) gtid: $(q R2 'select @@gtid_executed')"; echo "R1 sum: $(checksum R1) gtid: $(q R1 'select @@gtid_executed')"
  grep -i -E "error|partial|rollback|incomplete" $B/R1/logs/error.log | tail -5;;
esac
