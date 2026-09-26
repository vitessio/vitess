#!/bin/bash
# Time a MoveTables SwitchTraffic / ReverseTraffic of wf_dst2 (src -> dst2) under a light write load and report the
# client-visible write gap, plus the vtctld step timeline of the switch.
# Usage: switch.sh LABEL switchtraffic|reversetraffic [extra flags]
LABEL=$1
ACTION=$2
shift 2
D=/home/vt/perf/V4b
OUT=$D/out/sw-$LABEL-$ACTION
mkdir -p "$OUT"
LOG=/home/vt/perf/c40000/logs/vtctld.log
"$D/prober" -dsn "root@tcp(127.0.0.1:40003)/src?timeout=2s&readTimeout=60s&writeTimeout=60s" -threads 4 -interval 10ms \
  -duration ${PDUR:-10s} -table-size ${TSIZE:-200000} -out "$OUT/events" >"$OUT/prober" 2>&1 &
pp=$!
sleep 4
l0=$(wc -l <"$LOG")
t0=$(date +%s.%N)
"$D/c.sh" vtctldclient MoveTables --workflow wf_dst2 --target-keyspace dst2 "$ACTION" "$@" >"$OUT/switch.log" 2>&1
rc=$?
t1=$(date +%s.%N)
wait $pp
tail -n +$((l0 + 1)) "$LOG" >"$OUT/vtctld.log"
gap=$(grep -o 'max gap between successful completions=[0-9.]*[mµ]*s' "$OUT/prober" | sed 's/.*=//')
echo "$LABEL $ACTION rc=$rc cmd=$(echo "$t1 - $t0" | bc)s gap=$gap $(grep -o 'err=[0-9]*' "$OUT/prober" | head -1) load=$(uptime | sed 's/.*average: //' | cut -d, -f1)"
