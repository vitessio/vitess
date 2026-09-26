#!/bin/bash
# Time a MoveTables of every table of keyspace many -> mdst (one stream), CPU per process.
# Usage: mtmany.sh LABEL     Env: PROF=seconds (target vttablet pprof, taken 5 s in), NOCANCEL=1, MT_FLAGS
set -uo pipefail
LABEL=$1
D=/home/vt/perf/V4b
OUT=$D/out/mm-$LABEL
mkdir -p "$OUT"
VC="$D/c.sh vtctldclient"
DR=/home/vt/perf/c40000
SUID=${SUID:-103}
TUID=${TUID:-104}
TSOCK=$DR/vt_0000000$TUID/mysql.sock
SSOCK=$DR/vt_0000000$SUID/mysql.sock
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
ntab=$(q $SSOCK "select count(*) from information_schema.tables where table_schema='vt_many'")
"$D/c.sh" cpu >"$OUT/cpu0"
st0=$(q $SSOCK "show global status where variable_name in ('Questions','Connections')" | awk '{printf "%s=%s ", $1, $2}')
tt0=$(q $TSOCK "show global status where variable_name in ('Questions','Connections','Com_insert','Com_update','Com_select')" | awk '{printf "%s=%s ", $1, $2}')
t0=$(date +%s.%N)
$VC MoveTables --workflow wfm --target-keyspace mdst create --source-keyspace many --all-tables ${MT_FLAGS:-} >"$OUT/create.log" 2>&1 || { cat "$OUT/create.log"; exit 1; }
t0b=$(date +%s.%N)
if [[ ${PROF:-0} -gt 0 ]]; then
  (sleep 5; curl -s -o "$OUT/tgt.pprof" "http://localhost:$((40000 + TUID))/debug/pprof/profile?seconds=$PROF") &
  (sleep 5; curl -s -o "$OUT/src.pprof" "http://localhost:$((40000 + SUID))/debug/pprof/profile?seconds=$PROF") &
fi
last=""
while :; do
  r=$(q $TSOCK "select (select count(distinct table_name) from _vt.copy_state), (select state from _vt.vreplication where workflow='wfm'), (select length(pos) from _vt.vreplication where workflow='wfm')")
  set -- $r
  [[ ${1:-1} == 0 && ${2:-} == Running && ${3:-0} -gt 0 ]] && break
  [[ ${2:-} == Error ]] && { echo ERROR; q $TSOCK "select message from _vt.vreplication"; break; }
  now=$(date +%s)
  if [[ $now != "$last" && $((now % 10)) == 0 ]]; then echo "$(date +%T) remaining tables ${1:-?}" >>"$OUT/progress"; last=$now; fi
  sleep 0.5
done
t1=$(date +%s.%N)
"$D/c.sh" cpu >"$OUT/cpu1"
st1=$(q $SSOCK "show global status where variable_name in ('Questions','Connections')" | awk '{printf "%s=%s ", $1, $2}')
tt1=$(q $TSOCK "show global status where variable_name in ('Questions','Connections','Com_insert','Com_update','Com_select')" | awk '{printf "%s=%s ", $1, $2}')
wait
dur=$(echo "$t1 - $t0" | bc)
echo "$LABEL tables=$ntab create=$(echo "$t0b - $t0" | bc)s dur=${dur}s per-table=$(echo "scale=3; $dur / $ntab" | bc)s load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
echo "  src mysqld: $st0 -> $st1" | tee -a "$OUT/summary"
echo "  tgt mysqld: $tt0 -> $tt1" | tee -a "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v r="$ntab" '{d=$4-$2; if (d>0.05) printf "  %-14s %7.2f s  %6.1f ms/table\n", $1, d, d*1e3/r}' | tee -a "$OUT/summary"
if [[ ${NOCANCEL:-0} != 1 ]]; then
  t2=$(date +%s.%N)
  $VC MoveTables --workflow wfm --target-keyspace mdst cancel >"$OUT/cancel.log" 2>&1 || echo "cancel failed"
  echo "  cancel=$(echo "$(date +%s.%N) - $t2" | bc)s" | tee -a "$OUT/summary"
fi
