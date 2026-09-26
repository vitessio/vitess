source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
N=3
echo "=== E4e: lock mode; STOP IO_THREAD only; CHANGE to R2 (receiver options only, no AUTO_POSITION clause); kill P; START IO from R2; release lock"
reset_env >/dev/null 2>&1; MODE=lock make_T 1000 2>&1 | egrep "commit returned|Retrieved|Executed_Gtid"
q R1 "STOP REPLICA IO_THREAD"
q R1 "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=45003, SOURCE_USER='vt_repl', SOURCE_PASSWORD='replpw', SOURCE_CONNECT_RETRY=10, GET_SOURCE_PUBLIC_KEY=1, SOURCE_HEARTBEAT_PERIOD=2" && echo "  -> OK"
./rs.sh R1 | egrep "Source_Port|SQL_Running:|Retrieved|Executed_Gtid|relay files" | tr -s ' '
$C kill9 P; echo "--- P killed"; q R1 "START REPLICA IO_THREAD"; sleep 2
echo "--- releasing lock (server-side KILL of lock-holder session)"; release_lock; sleep 3; final
