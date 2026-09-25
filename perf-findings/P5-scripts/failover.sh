#!/bin/bash
# failover.sh MODE   MODE = host (kill vttablet+mysqld_safe+mysqld of primary) | mysqld (kill mysqld_safe+mysqld only) | tablet (kill vttablet only)
# Env: ERS=1 to run EmergencyReparentShard manually right after the kill (instead of waiting for vtorc).
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
BIN=${BIN:-/home/vt/bin}
MODE=${1:-host}
V="/home/vt/bin/vtctldclient --server localhost:40021"
PRIM=$($V GetTablets --keyspace sbtest --shard 0 | awk '$4=="primary"{print $1}')
UID_=$((10#${PRIM#zone1-}))
D=/home/vt/perf/c40000/vt_$(printf '%010d' $UID_)
echo "primary $PRIM uid $UID_"
$S/prober/prober -duration ${DUR:-40s} -threads ${THREADS:-4} -interval ${INTERVAL:-10ms} -out $S/last_events.txt > $S/last_prober.txt 2>&1 &
PP=$!
sleep ${PRE:-4}
T0=$(date +%s.%N)
echo "kill at $(date +%T.%3N) mode=$MODE"
MPID=$(cat $D/mysql.pid)
SAFE=$(ps -eo pid,args | awk -v f="$D/my.cnf" '$0 ~ "mysqld_safe" && index($0, f) {print $1}')
case $MODE in
  host) kill -9 $(cat $D/vttablet.pid) $SAFE $MPID ;;
  mysqld) kill -9 $SAFE $MPID ;;
  tablet) kill -9 $(cat $D/vttablet.pid) ;;
esac
if [[ -n $ERS ]]; then
  $BIN/vtctldclient --server localhost:40021 EmergencyReparentShard sbtest/0 ${ERS_FLAGS:-} > $S/last_ers.txt 2>&1
  echo "ERS rc=$? done at $(date +%T.%3N) took $(echo "$(date +%s.%N) - $T0" | bc)s"
  cat $S/last_ers.txt | tail -3
fi
# wait until a new primary is in topo
for i in $(seq 1 600); do
  NP=$($V GetTablets --keyspace sbtest --shard 0 2>/dev/null | awk '$4=="primary"{print $1}')
  if [[ -n $NP && $NP != "$PRIM" ]]; then
    echo "new primary $NP in topo at $(date +%T.%3N), $(echo "$(date +%s.%N) - $T0" | bc)s after kill"
    break
  fi
  sleep 0.1
done
wait $PP
cat $S/last_prober.txt
