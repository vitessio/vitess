#!/bin/bash
# bench.sh WORKLOAD MODE THREADS TIME LABEL [extra sysbench args...]
#   WORKLOAD: sysbench lua name (oltp_write_only, oltp_read_write, oltp_insert, ...) or a path to a lua script
#   MODE: ps (prepared, --db-ps-mode=auto) | text (--db-ps-mode=disable)
# Prints one line: label workload mode threads tps qps avg p95 p99 (latency per event = per transaction)
#   CPU µs per event per process (vtgate, tablets sum, mysqld sum), MySQL statements per event
#   (Questions, Com_begin, Com_commit summed over both mysqld) and load.
set -u
W=$1; MODE=$2; THREADS=$3; TIME=$4; LABEL=$5; shift 5
C=/home/vt/perf/P2/c.sh
WN=$(basename "$W" .lua); [[ -n ${TAG:-} ]] && WN="$WN+$TAG"; [[ $# -gt 0 ]] && WN="$WN+$(echo "$*" | sed 's/--//g; s/ /,/g')"
if [[ $MODE == ps ]]; then PSM=auto; else PSM=disable; fi
mstat() {
  for u in 100 101; do
    mysql -N -S /home/vt/perf/c30000/vt_0000000$u/mysql.sock -u vt_dba -e \
      "show global status where Variable_name in ('Questions','Com_begin','Com_commit')" 2>/dev/null
  done | awk '{s[$1]+=$2} END {printf "%d %d %d", s["Questions"], s["Com_begin"], s["Com_commit"]}'
}
V0=$(mktemp); V1=$(mktemp); /home/vt/perf/P2/vars.py snap > $V0
m0=$(mstat)
before=$($C cpu)
out=$($C sb "$W" --threads="$THREADS" --time="$TIME" --db-ps-mode=$PSM --histogram=on --report-interval=0 "$@" run 2>&1)
after=$($C cpu)
m1=$(mstat)
/home/vt/perf/P2/vars.py snap > $V1; vd=$(/home/vt/perf/P2/vars.py diff $V0 $V1); rm -f $V0 $V1
load=$(cut -d' ' -f1 /proc/loadavg)
ev=$(echo "$out" | awk '/total number of events:/ {print $5; exit}')
tps=$(echo "$out" | awk '/transactions:/ && /per sec/ {gsub(/[()]/,""); print $3; exit}')
qps=$(echo "$out" | awk '/queries:/ && /per sec/ {gsub(/[()]/,""); print $3; exit}')
errs=$(echo "$out" | awk '/ignored errors:/ {gsub(/[()]/,""); print $4; exit}')
avg=$(echo "$out" | awk '/avg:/ {print $2; exit}')
if [[ -z $ev || $ev == 0 ]]; then echo "$LABEL $W FAILED"; echo "$out" | tail -20 >&2; exit 1; fi
pcts=$(echo "$out" | awk '
  /Latency histogram/ {h=1; next}
  h && /\|/ { v=$1; c=$NF; if (c ~ /^[0-9]+$/) { vals[n]=v; cnt[n]=c; tot+=c; n++ } }
  h && /^$/ && n>0 {h=0}
  END { s=0; p95=""; p99="";
        for (i=0;i<n;i++){ s+=cnt[i]; if (p95=="" && s>=0.95*tot) p95=vals[i]; if (p99=="" && s>=0.99*tot) p99=vals[i]; }
        printf "%s %s", p95, p99 }')
cpu=$(paste <(echo "$before") <(echo "$after") | awk -v q="$ev" '
  { d=($4-$2)*1e6/q; if ($1 ~ /^vtgate/) g=d; else if ($1 ~ /^vttablet/) t+=d; else if ($1 ~ /^mysqld/) m+=d }
  END { printf "vtgate=%.0f tablets=%.0f mysqld=%.0f", g, t, m }')
my=$(echo "$m0 $m1" | awk -v q="$ev" '{printf "myq=%.2f mybegin=%.2f mycommit=%.2f", ($4-$1-4)/q, ($5-$2)/q, ($6-$3)/q}')
printf "%-14s %-22s %-4s thr=%-3s tps=%-8s qps=%-8s avg=%-6s p95=%-6s p99=%-6s %s %s err/s=%s load=%s %s\n" \
  "$LABEL" "$WN" "$MODE" "$THREADS" "$tps" "$qps" "$avg" $pcts "$cpu" "$my" "$errs" "$load" "$vd"
