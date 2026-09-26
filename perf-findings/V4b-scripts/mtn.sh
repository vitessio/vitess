#!/bin/bash
# Time one MoveTables copy phase src -> KS (N target shards), with CPU per process per 1M source rows.
# Usage: mtn.sh LABEL KS [SRCKS]      Env: TABLES_LIST (default sbtest1,sbtest2), MT_FLAGS, PROF=seconds, NOCANCEL=1
set -uo pipefail
LABEL=$1
KS=$2
SRCKS=${3:-src}
D=/home/vt/perf/V4b
OUT=$D/out/mt-$LABEL
mkdir -p "$OUT"
VC="$D/c.sh vtctldclient"
DR=/home/vt/perf/c40000
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
case $KS in
  dst2) T="101 102" ;;
  dst4) T="103 104 105 106" ;;
  *) T=${TGT_UIDS:?} ;;
esac
SSOCK=$DR/vt_0000000100/mysql.sock
TL=${TABLES_LIST:-sbtest1,sbtest2}
srcrows=0
for t in ${TL//,/ }; do srcrows=$((srcrows + $(q $SSOCK "select count(*) from vt_$SRCKS.$t"))); done
st0=$(q $SSOCK "show global status where variable_name in ('Handler_read_next','Bytes_sent','Handler_read_rnd_next')" | awk '{printf "%s=%s ", $1, $2}')
"$D/c.sh" cpu >"$OUT/cpu0"
t0=$(date +%s.%N)
$VC MoveTables --workflow "wf_$KS" --target-keyspace "$KS" create --source-keyspace "$SRCKS" --tables "$TL" ${MT_FLAGS:-} >"$OUT/create.log" 2>&1 || { cat "$OUT/create.log"; exit 1; }
t0b=$(date +%s.%N)
if [[ ${PROF:-0} -gt 0 ]]; then
  sleep 2
  curl -s -o "$OUT/src.pprof" "http://localhost:40100/debug/pprof/profile?seconds=$PROF" &
fi
while :; do
  done_all=1
  for u in $T; do
    s=$DR/vt_0000000$u/mysql.sock
    r=$(q $s "select (select count(*) from _vt.copy_state), (select group_concat(state) from _vt.vreplication where workflow='wf_$KS'), (select length(pos) from _vt.vreplication where workflow='wf_$KS')")
    set -- $r
    [[ ${1:-1} == 0 && ${2:-} == Running && ${3:-0} -gt 0 ]] || done_all=0
    [[ ${2:-} == Error ]] && { echo ERROR; q $s "select message from _vt.vreplication"; exit 1; }
  done
  ((done_all)) && break
  sleep 0.2
done
t1=$(date +%s.%N)
"$D/c.sh" cpu >"$OUT/cpu1"
st1=$(q $SSOCK "show global status where variable_name in ('Handler_read_next','Bytes_sent','Handler_read_rnd_next')" | awk '{printf "%s=%s ", $1, $2}')
wait
rows=0
for u in $T; do for t in ${TL//,/ }; do rows=$((rows + $(q $DR/vt_0000000$u/mysql.sock "select count(*) from vt_$KS.$t"))); done; done
dur=$(echo "$t1 - $t0" | bc)
echo "$LABEL ks=$KS srcrows=$srcrows tgtrows=$rows create=$(echo "$t0b - $t0" | bc)s dur=${dur}s load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
echo "  src status before: $st0" >>"$OUT/summary"
echo "  src status after:  $st1" >>"$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v r="$srcrows" '{d=$4-$2; if (d>0.05) printf "  %-14s %7.2f s  %6.2f s/1M src rows\n", $1, d, d*1e6/r}' | tee -a "$OUT/summary"
if [[ ${NOCANCEL:-0} != 1 ]]; then
  $VC MoveTables --workflow "wf_$KS" --target-keyspace "$KS" cancel >/dev/null 2>&1 || echo "cancel failed"
fi
