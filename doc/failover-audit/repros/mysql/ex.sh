source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
wait_retrieved(){ # poll until R1 retrieved set contains :1-6 ; print elapsed
  local t0=$(date +%s.%N)
  for i in $(seq 400); do r=$(q R1 "select received_transaction_set from performance_schema.replication_connection_status" 2>/dev/null); case "$r" in *:1-6*|*-6) echo "re-fetched ($r) after $(echo "$(date +%s.%N)-$t0"|bc)s"; return;; esac; sleep 0.05; done; echo "NOT re-fetched within 20s ($r)"; }
N=3
case $1 in
E3)
  echo "=== E3: sqlstop, P alive, STOP REPLICA; CHANGE (new heartbeat) without START; then kill P"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA; $(changesrc R1 45001 2)"; ./rs.sh R1
  echo "--- kill -9 P"; $C kill9 P
  q R1 "START REPLICA"; sleep 4; final;;
E3t)
  echo "=== E3t: sqlstop, P alive, STOP REPLICA; CHANGE; START REPLICA immediately - measure refetch"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA; $(changesrc R1 45001 2); START REPLICA IO_THREAD"; wait_retrieved; ./rs.sh R1
  echo "(Note: START REPLICA also starts applier; here IO only to keep T unapplied for inspection)"
  q R1 "STOP REPLICA; $(changesrc R1 45001 3); START REPLICA"; wait_retrieved; sleep 2; final;;
E3lock)
  echo "=== E3lock: lock mode (MTA worker blocked on row lock), Vitess STOP REPLICA with 10s client timeout"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000
  t0=$(date +%s); timeout 10 mysql -uroot -S $B/R1/mysql.sock -e "STOP REPLICA" && echo "STOP REPLICA returned" || echo "STOP REPLICA did not return within 10s"
  ./rs.sh R1; mysql -uroot -S $B/R1/mysql.sock -e 'SHOW PROCESSLIST' | cut -c1-160
  echo "--- releasing lock (kill lock holder client)"; kill $LOCKPID; sleep 3; ./rs.sh R1; final;;
E3same)
  echo "=== E3same: sqlstop, STOP REPLICA; CHANGE with IDENTICAL params (same heartbeat 4)"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA; $(changesrc R1 45001 4)"; ./rs.sh R1;;
E4a_hb)
  echo "=== E4a_hb: lock mode, STOP REPLICA IO_THREAD only, CHANGE SOURCE_HEARTBEAT_PERIOD only (applier running/blocked)"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; ./rs.sh R1
  q R1 "CHANGE REPLICATION SOURCE TO SOURCE_HEARTBEAT_PERIOD=2"; echo "rc=$?"; ./rs.sh R1
  echo "--- kill -9 P, release lock"; $C kill9 P; kill $LOCKPID; sleep 3; q R1 "START REPLICA"; sleep 3; final;;
E4a_full)
  echo "=== E4a_full: lock mode, STOP REPLICA IO_THREAD only, full Vitess CHANGE (host/port/user/pw/retry/hb/autopos) (applier running)"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; q R1 "$(changesrc R1 45001 2)"; echo "rc=$?"; ./rs.sh R1
  echo "--- kill -9 P, release lock"; $C kill9 P; kill $LOCKPID; sleep 3; q R1 "START REPLICA"; sleep 3; final;;
E4a_port)
  echo "=== E4a_port: lock mode, STOP IO only, CHANGE to a DIFFERENT source (R2 port 45003) (applier running)"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; q R1 "$(changesrc R1 45003 4)"; echo "rc=$?"; ./rs.sh R1
  echo "--- kill -9 P, release lock"; $C kill9 P; kill $LOCKPID; sleep 3; ./rs.sh R1; final;;
E4a_sqlstopped_hb)
  echo "=== E4a_sqlstopped_hb: both threads stopped, CHANGE SOURCE_HEARTBEAT_PERIOD only"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_HEARTBEAT_PERIOD=2"; ./rs.sh R1
  echo "--- kill -9 P"; $C kill9 P; q R1 "START REPLICA"; sleep 3; final;;
E4a_sqlstopped_full_iostopfirst)
  echo "=== E4a_sqlstopped_full: both threads stopped, full Vitess CHANGE (no STOP REPLICA cmd since already stopped)"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; q R1 "$(changesrc R1 45001 2)"; ./rs.sh R1;;
E4b)
  echo "=== E4b: both stopped, CHANGE with RELAY_LOG_FILE/POS"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  f=$(q R1 "select RELAY_LOG_NAME from mysql.slave_relay_log_info" ); p=$(q R1 "select RELAY_LOG_POS from mysql.slave_relay_log_info"); echo "applier relay pos: $f $p"
  q R1 "STOP REPLICA"
  q R1 "$(changesrc R1 45001 2), RELAY_LOG_FILE='$(basename $f)', RELAY_LOG_POS=$p"; ./rs.sh R1
  q R1 "CHANGE REPLICATION SOURCE TO SOURCE_HEARTBEAT_PERIOD=2, RELAY_LOG_FILE='$(basename $f)', RELAY_LOG_POS=$p"; ./rs.sh R1
  q R1 "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=45001, SOURCE_HEARTBEAT_PERIOD=3, RELAY_LOG_FILE='$(basename $f)', RELAY_LOG_POS=$p"; ./rs.sh R1
  echo "--- kill -9 P"; $C kill9 P; q R1 "START REPLICA SQL_THREAD"; sleep 3; final;;
E4c)
  echo "=== E4c: RESET REPLICA / RESET REPLICA ALL"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA; RESET REPLICA"; ./rs.sh R1; q R1 "RESET REPLICA ALL"; ./rs.sh R1;;
E5lock)
  echo "=== E5lock: lock mode; STOP IO; WAIT_FOR_EXECUTED_GTID_SET(Retrieved, 60) (lock released after 5s); STOP REPLICA; CHANGE; kill P"
  reset_env >/dev/null 2>&1; MODE=lock make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; r=$(q R1 "select received_transaction_set from performance_schema.replication_connection_status"); echo "Retrieved=$r"
  (sleep 5; kill $LOCKPID) &
  t0=$(date +%s.%N); echo "WAIT result: $(q R1 "SELECT WAIT_FOR_EXECUTED_GTID_SET('$r', 60)") after $(echo "$(date +%s.%N)-$t0"|bc)s"
  q R1 "STOP REPLICA; $(changesrc R1 45001 2)"; ./rs.sh R1
  echo "--- kill -9 P"; $C kill9 P; q R1 "START REPLICA"; sleep 3; final;;
E5sql)
  echo "=== E5sql: sqlstop mode; STOP IO; START SQL_THREAD; WAIT_FOR_EXECUTED_GTID_SET(Retrieved, 60); STOP REPLICA; CHANGE; kill P"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  q R1 "STOP REPLICA IO_THREAD"; r=$(q R1 "select received_transaction_set from performance_schema.replication_connection_status"); echo "Retrieved=$r"
  q R1 "START REPLICA SQL_THREAD"
  t0=$(date +%s.%N); echo "WAIT result: $(q R1 "SELECT WAIT_FOR_EXECUTED_GTID_SET('$r', 60)") after $(echo "$(date +%s.%N)-$t0"|bc)s"
  q R1 "STOP REPLICA; $(changesrc R1 45001 2)"; ./rs.sh R1
  echo "--- kill -9 P"; $C kill9 P; q R1 "START REPLICA"; sleep 3; final;;
E6)
  echo "=== E6: relay_log_recovery=1, sqlstop, P ALIVE, graceful restart R1, START REPLICA"
  reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000
  $C stop R1; $C start R1; ./rs.sh R1
  q R1 "START REPLICA"; wait_retrieved; sleep 2; final;;
esac
