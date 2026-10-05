#!/bin/bash
source "${RELAYLOG_DIR:-$HOME/relaylog-work}/lib.sh"
recv_only(){ echo "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$1, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=2"; }
d(){ mysql -uroot -S $B/R1/mysql.sock -e "SHOW REPLICA STATUS\G" | egrep " Source_Port|SQL_Delay|SQL_Remaining_Delay|Replica_SQL_Running:|Replica_IO_Running:|Retrieved_Gtid|Executed_Gtid" | tr -s ' ' | tr '\n' ';'; echo; }
rows(){ q R1 "select coalesce(group_concat(id),'-') from t.t where id>=1000" 2>/dev/null; }
reset_env >/dev/null 2>&1
echo "=== D1: delayed R1 (SOURCE_DELAY=20), receiver-only switch to R2 with delayed events in the relay log"
q R1 "STOP REPLICA SQL_THREAD; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=20; START REPLICA SQL_THREAD" 2>/dev/null
t0=$(date +%s)
mysql -uroot -S $B/P/mysql.sock -e "INSERT INTO t.t(id,v) VALUES (1000,1)"; sleep 2
echo "t+$(( $(date +%s)-t0 ))s before switch: $(d) rows=$(rows)"
q R1 "STOP REPLICA IO_THREAD" 2>/dev/null; q R1 "$(recv_only 45003)" 2>/dev/null && echo "  receiver-only CHANGE OK"
q R1 "START REPLICA" 2>/dev/null
mysql -uroot -S $B/P/mysql.sock -e "INSERT INTO t.t(id,v) VALUES (1001,1)"   # reaches R1 only via R2 now
echo "t+$(( $(date +%s)-t0 ))s after switch: $(d) rows=$(rows)"
sleep 12; echo "t+$(( $(date +%s)-t0 ))s: rows=$(rows)  (1000 must NOT be applied before ~20s)"
sleep 12; echo "t+$(( $(date +%s)-t0 ))s: rows=$(rows)  (1000 applied after 20s; 1001 around t+22s+)"
sleep 6;  echo "t+$(( $(date +%s)-t0 ))s: rows=$(rows)  $(d)"
echo "=== D2: does RESET REPLICA (self-heal path) reset SOURCE_DELAY?"
q R1 "STOP REPLICA; RESET REPLICA" 2>/dev/null; echo "after RESET REPLICA: $(d)"
q R1 "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=20" 2>/dev/null; q R1 "STOP REPLICA; $(recv_only 45003)" 2>/dev/null; echo "after full STOP+CHANGE (old path) with delay set: $(d)"
killall_ours
