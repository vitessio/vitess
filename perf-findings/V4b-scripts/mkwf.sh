#!/bin/bash
# Create W MoveTables workflows many -> mdst, each moving TPER consecutive tables (t0001.. in keyspace many),
# and time how long until all are Running with an empty copy_state.
# Usage: mkwf.sh LABEL W TPER
set -uo pipefail
LABEL=$1
W=$2
TPER=$3
D=/home/vt/perf/V4b
OUT=$D/out/wf-$LABEL
mkdir -p "$OUT"
VC="$D/c.sh vtctldclient"
DR=/home/vt/perf/c40000
TSOCK=$DR/vt_0000000104/mysql.sock
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
"$D/c.sh" cpu >"$OUT/cpu0"
t0=$(date +%s.%N)
: >"$OUT/create.times"
for w in $(seq "${START:-1}" "$W"); do
  tl=""
  for j in $(seq 1 "$TPER"); do tl+="$(printf 't%04d' $(((w - 1) * TPER + j))),"; done
  c0=$(date +%s.%N)
  $VC MoveTables --workflow "$(printf 'mv%02d' "$w")" --target-keyspace mdst create --source-keyspace many --tables "${tl%,}" >>"$OUT/create.log" 2>&1 || echo "create $w failed"
  echo "$w $(echo "$(date +%s.%N) - $c0" | bc)" >>"$OUT/create.times"
done
t0b=$(date +%s.%N)
while :; do
  r=$(q $TSOCK "select (select count(*) from _vt.copy_state), (select count(*) from _vt.vreplication where workflow like 'mv%' and state='Running' and length(pos)>0), (select count(*) from _vt.vreplication where state='Error')")
  set -- $r
  [[ ${1:-1} == 0 && ${2:-0} == "$W" ]] && break
  [[ ${3:-0} != 0 ]] && { echo "ERROR"; q $TSOCK "select workflow, message from _vt.vreplication where state='Error'" | head -3; break; }
  sleep 0.5
done
t1=$(date +%s.%N)
"$D/c.sh" cpu >"$OUT/cpu1"
echo "$LABEL W=$W TPER=$TPER create_all=$(echo "$t0b - $t0" | bc)s (per wf: $(awk '{s+=$2; if ($2>m) m=$2} END{printf "avg %.2fs max %.2fs", s/NR, m}' "$OUT/create.times")) all_running=$(echo "$t1 - $t0" | bc)s load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk '{d=$4-$2; if (d>0.05) printf "  %-14s %7.2f s\n", $1, d}' | tee -a "$OUT/summary"
