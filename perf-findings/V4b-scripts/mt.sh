#!/bin/bash
# Time one MoveTables copy phase src -> dst, with CPU per process and optional pprof.
# Usage: mt.sh LABEL [PROFILE_SECONDS]
# Env: MT_FLAGS extra flags for `MoveTables create` (e.g. --config-overrides ...).
set -uo pipefail
LABEL=${1:-run}
PROF=${2:-0}
D=/home/vt/perf/P4
OUT=$D/out/$LABEL
mkdir -p "$OUT"
VC="$D/c.sh vtctldclient"
TSOCK1=/home/vt/perf/c40000/vt_0000000101/mysql.sock
TSOCK2=/home/vt/perf/c40000/vt_0000000102/mysql.sock
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }

"$D/c.sh" cpu >"$OUT/cpu0"
uptime >"$OUT/uptime0"
t0=$(date +%s.%N)
$VC MoveTables --workflow wf --target-keyspace dst create --source-keyspace src --tables ${TABLES_LIST:-sbtest1,sbtest2,sbtest3,sbtest4} ${MT_FLAGS:-} >"$OUT/create.log" 2>&1 || { cat "$OUT/create.log"; exit 1; }
if [[ $PROF -gt 0 ]]; then
  sleep 3
  curl -s -o "$OUT/src.pprof" "http://localhost:40100/debug/pprof/profile?seconds=$PROF" &
  curl -s -o "$OUT/dst1.pprof" "http://localhost:40101/debug/pprof/profile?seconds=$PROF" &
fi
# Wait until both streams have finished copying (no copy_state rows and state Running).
while :; do
  n1=$(q $TSOCK1 "select count(*) from _vt.copy_state")
  n2=$(q $TSOCK2 "select count(*) from _vt.copy_state")
  s1=$(q $TSOCK1 "select state from _vt.vreplication where workflow='wf'")
  s2=$(q $TSOCK2 "select state from _vt.vreplication where workflow='wf'")
  p1=$(q $TSOCK1 "select length(pos) from _vt.vreplication where workflow='wf'")
  p2=$(q $TSOCK2 "select length(pos) from _vt.vreplication where workflow='wf'")
  if [[ $n1 == 0 && $n2 == 0 && $s1 == Running && $s2 == Running && ${p1:-0} -gt 0 && ${p2:-0} -gt 0 ]]; then break; fi
  if [[ $s1 == Error || $s2 == Error ]]; then echo "ERROR"; q $TSOCK1 "select message from _vt.vreplication"; break; fi
  sleep 0.25
done
t1=$(date +%s.%N)
"$D/c.sh" cpu >"$OUT/cpu1"
uptime >"$OUT/uptime1"
wait
cq="select 0"
TL=${TABLES_LIST:-}; for t in ${TL//,/ }; do cq+="+(select count(*) from vt_dst.$t)"; done
[[ -z ${TABLES_LIST:-} ]] && cq="select (select count(*) from vt_dst.sbtest1)+(select count(*) from vt_dst.sbtest2)+(select count(*) from vt_dst.sbtest3)+(select count(*) from vt_dst.sbtest4)"
rows=$(( $(q $TSOCK1 "$cq") + $(q $TSOCK2 "$cq") ))
dur=$(echo "$t1 - $t0" | bc)
echo "$LABEL rows=$rows dur=${dur}s rows/s=$(echo "$rows / $dur" | bc) load: $(cut -d, -f3- "$OUT/uptime1")" | tee "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v r="$rows" '{d=$4-$2; printf "  %-14s %7.2f s  %6.2f s/1M rows\n", $1, d, d*1e6/r}' | tee -a "$OUT/summary"
