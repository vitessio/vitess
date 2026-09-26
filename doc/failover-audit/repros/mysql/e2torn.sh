# usage: bash e2torn.sh <sync_relay_log> <trials>
source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
sync=$1; trials=$2
echo "=== E2torn: relay_log_recovery=0 sync_relay_log=$sync; kill -9 R1 under heavy load (P up, R2 up)"
reset_env relay_log_recovery=0 sync_relay_log=$sync >/dev/null 2>&1
q R1 "select @@relay_log_recovery, @@sync_relay_log, @@replica_parallel_workers"
q P "SET SESSION cte_max_recursion_depth=100000; CREATE TABLE t.seq(id INT PRIMARY KEY); INSERT INTO t.seq WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<20000) SELECT n FROM s; CREATE TABLE t.big(id BIGINT AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(255))" 
sleep 2
for tr in $(seq $trials); do
  echo "----- trial $tr"
  ( while true; do mysql -uroot -S $B/P/mysql.sock -e "INSERT INTO t.big(pad) SELECT REPEAT(CHAR(65+FLOOR(RAND()*26)),255) FROM t.seq LIMIT 5000" 2>/dev/null || sleep 0.1; done ) &
  LP=$!
  sleep $(( 2 + RANDOM % 3 )).$(( RANDOM % 10 ))
  echo "R1 before kill: $(q R1 "select received_transaction_set from performance_schema.replication_connection_status") exec=$(q R1 'select @@gtid_executed')"
  $C kill9 R1; echo "killed R1"
  ls -l R1/logs | grep relay | awk '{print $9"("$5")"}' | tr '\n' ' '; echo
  last=$(tail -1 R1/logs/relay-bin.index); sz=$(stat -c %s $last)
  echo "tail of $last (size $sz):"; mysqlbinlog $last 2>&1 | egrep "^# at|GTID_NEXT|Xid =|^ERROR|^COMMIT|Warning" | tail -4
  if [ -n "$TRUNC" ]; then newsz=$(( sz - TRUNC )); truncate -s $newsz $last; echo "SIMULATED TORN TAIL: truncated $last from $sz to $newsz bytes"; mysqlbinlog $last 2>&1 | egrep "^# at|GTID_NEXT|Xid =|^ERROR|COMMIT" | tail -3; fi
  $C start R1 || { echo START FAILED; tail -20 R1/logs/error.log; }
  echo "after restart: Retrieved=$(q R1 "select received_transaction_set from performance_schema.replication_connection_status") exec=$(q R1 'select @@gtid_executed')"
  awk '/starting as process/{buf=""} {buf=buf"\n"$0} END{print buf}' R1/logs/error.log | egrep -i "relay|recover|sanit|trunc|partial|Repl|ERROR" | egrep -v "Insecure|XA crash" | cut -c1-500
  q R1 "START REPLICA SQL_THREAD"; sleep 5
  ./rs.sh R1 | egrep "SQL_Running|Last_SQL_Error|Retrieved|Executed|relay files"
  q R1 "START REPLICA IO_THREAD"; sleep 2
  kill $LP; wait $LP 2>/dev/null
  for i in $(seq 120); do [ "$(q R1 'select @@gtid_executed')" = "$(q P 'select @@gtid_executed')" ] && break; sleep 0.5; done
  ./rs.sh R1 | egrep "Running:|Last_SQL_Error|Last_IO_Error|Retrieved|Executed"
  echo "P gtid=$(q P 'select @@gtid_executed') rows=$(q P 'select count(*), sum(crc32(pad)) from t.big')"
  echo "R1 rows=$(q R1 'select count(*), sum(crc32(pad)) from t.big')"
  awk '/starting as process/{buf=""} {buf=buf"\n"$0} END{print buf}' R1/logs/error.log | egrep "ERROR|MY-0105|sanit" | cut -c1-400 | tail -5
done
