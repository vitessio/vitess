# usage: MODE=lock|sqlstop N=3 bash e1.sh graceful|kill9|prep  [extra R1 cnf lines...]
source /tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog/lib.sh
how=$1; shift
echo "=== MODE=${MODE:-lock} N=${N:-1} restart=$how R1 extra cnf: $*"
reset_env "$@" >/dev/null 2>&1; q R1 "select @@relay_log_recovery, @@sync_relay_log, @@replica_parallel_workers"
make_T 1000
echo "--- kill -9 P"; $C kill9 P
case $how in
graceful) echo "--- mysqladmin shutdown R1"; $C stop R1;;
prep) echo "--- Vitess prepareReplicaForShutdown then mysqladmin shutdown R1"; prep_shutdown; $C stop R1;;
kill9) echo "--- kill -9 R1"; $C kill9 R1;;
esac
ls -l R1/logs | grep relay | awk '{print $9"("$5")"}' | tr '\n' ' '; echo
$C start R1; echo "--- after restart, before START REPLICA"; ./rs.sh R1
echo "--- R1 error log (this start):"; awk '/Starting as process|starting as process/{buf=""} {buf=buf"\n"$0} END{print buf}' R1/logs/error.log | egrep -i "relay|recover|repl|sanit" | cut -c1-400
q R1 "${STARTCMD:-START REPLICA}"; sleep 4
final
