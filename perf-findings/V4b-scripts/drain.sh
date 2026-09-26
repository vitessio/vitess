#!/bin/bash
# Running-phase fan-out cost: stop all workflows, write EVENTS sysbench trx on the source mysqld, start the chosen
# workflows and time until every stream of them has reached the source GTID. CPU per source trx per process.
# Usage: drain.sh LABEL "dst2 dst4" [EVENTS] [SBTEST]    Env: PROF=seconds (source vttablet pprof)
LABEL=$1
KSS=$2
EVENTS=${3:-30000}
SBTEST=${4:-oltp_write_only}
D=/home/vt/perf/V4b
OUT=$D/out/dr-$LABEL
mkdir -p "$OUT"
DR=/home/vt/perf/c40000
SSOCK=$DR/vt_0000000100/mysql.sock
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
VC="$D/c.sh vtctldclient"
uids() { case $1 in dst2) echo "101 102" ;; esac; }

waitall() { # waitall GTID "ks ks"
  while :; do
    ok=1
    for k in $2; do for u in $(uids "$k"); do
      r=$(q "$DR/vt_0000000$u/mysql.sock" "select gtid_subset('$1', substring(pos, 9)) from _vt.vreplication where workflow='wf_$k'")
      [[ $r == 1 ]] || ok=0
    done; done
    ((ok)) && break
    sleep 0.1
  done
}
# Bring every workflow up to date first, so that only this run's transactions are drained.
$VC Workflow --keyspace dst2 start --workflow wf_dst2 >/dev/null 2>&1
waitall "$(q $SSOCK "select @@global.gtid_executed" | tr -d '\n')" "dst2"
$VC Workflow --keyspace dst2 stop --workflow wf_dst2 >/dev/null
sleep 1
sysbench --db-driver=mysql --mysql-socket=$SSOCK --mysql-user=vt_dba --mysql-db=vt_src --tables=2 --table-size=200000 \
  --threads=8 --events="$EVENTS" --time=0 --rand-type=uniform "$SBTEST" run >"$OUT/sysbench" 2>&1
srcgtid=$(q $SSOCK "select @@global.gtid_executed" | tr -d '\n')
"$D/c.sh" cpu >"$OUT/cpu0"
t0=$(date +%s.%N)
for k in $KSS; do $VC Workflow --keyspace "$k" start --workflow "wf_$k" >/dev/null; done
if [[ ${PROF:-0} -gt 0 ]]; then
  curl -s -o "$OUT/src.pprof" "http://localhost:40100/debug/pprof/profile?seconds=$PROF" &
  curl -s -o "$OUT/tgt.pprof" "http://localhost:40101/debug/pprof/profile?seconds=$PROF" &
fi
waitall "$srcgtid" "$KSS"
t1=$(date +%s.%N)
"$D/c.sh" cpu >"$OUT/cpu1"
wait
dur=$(echo "$t1 - $t0" | bc)
echo "$LABEL [$KSS] $SBTEST events=$EVENTS dur=${dur}s trx/s=$(echo "$EVENTS / $dur" | bc) load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v r="$EVENTS" '$1 ~ /_10[012]$/ {d=$4-$2; if (d>0.02) printf "  %-14s %7.2f s  %6.1f us/trx\n", $1, d, d*1e6/r}' | tee -a "$OUT/summary"
