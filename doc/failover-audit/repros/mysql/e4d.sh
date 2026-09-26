source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
N=3
echo "=== E4d: lock mode (applier running, blocked), IO stopped; test individual CHANGE options"
reset_env >/dev/null 2>&1; MODE=lock make_T 1000 >/dev/null 2>&1
q R1 "STOP REPLICA IO_THREAD"; ./rs.sh R1 | egrep "Retrieved|Executed_Gtid|relay files"
for opt in "SOURCE_AUTO_POSITION=1" "SOURCE_HOST='127.0.0.1'" "SOURCE_PORT=45001" "SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw'" "SOURCE_CONNECT_RETRY=10" "GET_SOURCE_PUBLIC_KEY=1" "SOURCE_HEARTBEAT_PERIOD=3" \
  "SOURCE_HOST='127.0.0.1', SOURCE_PORT=45001, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=2"; do
  q R1 "CHANGE REPLICATION SOURCE TO $opt" && echo "  -> OK" || echo "  -> REFUSED"
  ./rs.sh R1 | egrep "Retrieved|relay files" | tr -s ' '
done
echo "--- repoint to R2 (different source, port 45003) with receiver options only, applier still running"
q R1 "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=45003, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=2" && echo "  -> OK" || echo "  -> REFUSED"
./rs.sh R1 | egrep "Source_Port|Retrieved|Executed_Gtid|relay files" | tr -s ' '
echo "--- kill -9 P; start R2 IO so it has 1-3 only; START REPLICA IO_THREAD on R1 (now from R2); release lock"
$C kill9 P; q R1 "START REPLICA IO_THREAD"; sleep 2; ./rs.sh R1 | egrep "IO_Running|Last_IO_Error|Retrieved|relay files" | tr -s ' '
kill $LOCKPID; sleep 3; final
echo "=== E4d2: both threads stopped; CHANGE SOURCE_AUTO_POSITION=0, RELAY_LOG_FILE/POS; then CHANGE SOURCE_AUTO_POSITION=1"
reset_env >/dev/null 2>&1; MODE=sqlstop make_T 1000 >/dev/null 2>&1
f=$(basename $(q R1 "select RELAY_LOG_NAME from mysql.slave_relay_log_info")); p=$(q R1 "select RELAY_LOG_POS from mysql.slave_relay_log_info")
q R1 "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_HEARTBEAT_PERIOD=2, SOURCE_AUTO_POSITION=0, RELAY_LOG_FILE='$f', RELAY_LOG_POS=$p" && echo "  -> OK"
./rs.sh R1 | egrep "Auto_Position|Retrieved|Executed_Gtid|relay files" | tr -s ' '; q R1 "select @@relay_log_purge"
q R1 "CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION=1" && echo "  -> OK"
./rs.sh R1 | egrep "Auto_Position|Retrieved|Executed_Gtid|relay files" | tr -s ' '
q R1 "START REPLICA SQL_THREAD"; sleep 2; final
