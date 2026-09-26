#!/bin/bash
# prof.sh OUTDIR KIND WORKLOAD MODE THREADS [extra]   KIND=profile|allocs|mutex|block
# Runs a 25s sysbench and captures a 15s profile from vtgate and tablet 100 concurrently.
set -u
OUT=$1; KIND=$2; shift 2
mkdir -p "$OUT"
W=$1; M=$2; T=$3; shift 3
/home/vt/perf/P2/bench.sh "$W" "$M" "$T" 25 "prof-$KIND" "$@" > "$OUT/bench-$KIND.txt" &
sleep 5
curl -s -o "$OUT/vtgate-$KIND.pprof" "http://localhost:30001/debug/pprof/$KIND?seconds=15" &
curl -s -o "$OUT/tablet-$KIND.pprof" "http://localhost:30100/debug/pprof/$KIND?seconds=15" &
wait
cat "$OUT/bench-$KIND.txt"
