#!/bin/bash
# bench.sh MODE THREADS TIME [LABEL]
#   MODE: ps (prepared, --db-ps-mode=auto) | text (--db-ps-mode=disable)
# Prints one line: label mode threads qps avg p95 p99 cpu_us/query per process (vtgate, tablets sum, mysqld sum) + load
set -u
MODE=$1; THREADS=$2; TIME=$3; LABEL=${4:-run}
C=/home/vt/perf/P1/c.sh
if [[ $MODE == ps ]]; then PSM=auto; else PSM=disable; fi
before=$($C cpu)
out=$($C sb oltp_point_select --threads="$THREADS" --time="$TIME" --db-ps-mode=$PSM --histogram=on --report-interval=0 run 2>&1)
after=$($C cpu)
load=$(cut -d' ' -f1 /proc/loadavg)
q=$(echo "$out" | awk '/queries:/ && /per sec/ {gsub(/[()]/,""); print $2; exit}')
qps=$(echo "$out" | awk '/queries:/ && /per sec/ {gsub(/[()]/,""); print $3; exit}')
avg=$(echo "$out" | awk '/avg:/ {print $2; exit}')
# percentiles from histogram: lines "   value |  ***  count"
pcts=$(echo "$out" | awk '
  /Latency histogram/ {h=1; next}
  h && /\|/ { v=$1; c=$NF; if (c ~ /^[0-9]+$/) { vals[n]=v; cnt[n]=c; tot+=c; n++ } }
  h && /^$/ && n>0 {h=0}
  END { s=0; p95=""; p99="";
        for (i=0;i<n;i++){ s+=cnt[i]; if (p95=="" && s>=0.95*tot) p95=vals[i]; if (p99=="" && s>=0.99*tot) p99=vals[i]; }
        printf "%s %s", p95, p99 }')
cpu=$(paste <(echo "$before") <(echo "$after") | awk -v q="$q" '
  { d=($4-$2)*1e6/q; if ($1 ~ /^vtgate/) g=d; else if ($1 ~ /^vttablet/) t+=d; else if ($1 ~ /^mysqld/) m+=d }
  END { printf "vtgate=%.0f tablets=%.0f mysqld=%.0f", g, t, m }')
printf "%-12s %-4s thr=%-3s qps=%-8s avg=%-5s p95=%-6s p99=%-6s %s load=%s\n" "$LABEL" "$MODE" "$THREADS" "$qps" "$avg" $pcts "$cpu" "$load"
