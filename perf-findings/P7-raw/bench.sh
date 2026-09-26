#!/bin/bash
# bench.sh LABEL NAME THREADS TIME TEST [extra sysbench args...]
#   TEST: a p3.lua shape (p3:<shape>, text protocol) or a built-in sysbench test (e.g. oltp_read_only)
# Prints one line: label name threads tps qps avg p95 p99 (ms per sysbench event) and CPU us per query
# (vtgate, tablets summed, mysqld summed) + load average.
set -u
LABEL=$1; NAME=$2; THREADS=$3; TIME=$4; TEST=$5; shift 5
C=/home/vt/perf/P7/c.sh
if [[ $TEST == p3:* ]]; then TEST="/home/vt/perf/P7/lua/p3.lua --shape=${TEST#p3:}"; fi
before=$($C cpu)
# shellcheck disable=SC2086
out=$($C sb $TEST --threads="$THREADS" --time="$TIME" --histogram=on --report-interval=0 "$@" run 2>&1)
after=$($C cpu)
load=$(cut -d' ' -f1 /proc/loadavg)
q=$(echo "$out" | awk '/queries:/ && /per sec/ {gsub(/[()]/,""); print $2; exit}')
qps=$(echo "$out" | awk '/queries:/ && /per sec/ {gsub(/[()]/,""); print $3; exit}')
tps=$(echo "$out" | awk '/transactions:/ && /per sec/ {gsub(/[()]/,""); print $3; exit}')
avg=$(echo "$out" | awk '/avg:/ {print $2; exit}')
errs=$(echo "$out" | awk '/ignored errors:/ {print $3; exit}')
if [[ -z $q ]]; then echo "$LABEL $NAME FAILED: $(echo "$out" | grep -iE 'error|fatal' | head -3)"; exit 1; fi
pcts=$(echo "$out" | awk '
  /Latency histogram/ {h=1; next}
  h && /\|/ { v=$1; c=$NF; if (c ~ /^[0-9]+$/) { vals[n]=v; cnt[n]=c; tot+=c; n++ } }
  h && /^$/ && n>0 {h=0}
  END { s=0; p95=""; p99="";
        for (i=0;i<n;i++){ s+=cnt[i]; if (p95=="" && s>=0.95*tot) p95=vals[i]; if (p99=="" && s>=0.99*tot) p99=vals[i]; }
        if (p95=="") p95="0"; if (p99=="") p99=vals[n-1]; if (p99=="") p99="0";
        printf "%s %s", p95, p99 }')
cpu=$(paste <(echo "$before") <(echo "$after") | awk -v q="$q" '
  { d=($4-$2)*1e6/q; if ($1 ~ /^vtgate/) g=d; else if ($1 ~ /^vttablet/) t+=d; else if ($1 ~ /^mysqld/) m+=d }
  END { printf "vtgate=%.1f tablets=%.1f mysqld=%.1f", g, t, m }')
printf "%-10s %-16s thr=%-3s tps=%-9s qps=%-9s avg=%-7s p95=%-7s p99=%-7s %s errs=%s load=%s\n" "$LABEL" "$NAME" "$THREADS" "$tps" "$qps" "$avg" $pcts "$cpu" "${errs:-0}" "$load"
