#!/bin/bash
# PRS under light write load. Usage: prs.sh <new-primary-uid> [extra PRS flags]
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
BIN=${BIN:-/home/vt/bin}
NEW=$1; shift
DUR=${DUR:-14s}
$S/prober/prober -duration $DUR -threads ${THREADS:-4} -interval ${INTERVAL:-10ms} ${PROBER_FLAGS:-} -out $S/last_events.txt > $S/last_prober.txt 2>&1 &
PP=$!
sleep ${PRE:-4}
T0=$(date +%s.%N)
echo "PRS start $(date +%T.%3N)"
$BIN/vtctldclient --server localhost:40021 PlannedReparentShard sbtest/0 --new-primary zone1-0000000$NEW "$@" > $S/last_prs.txt 2>&1
RC=$?
T1=$(date +%s.%N)
echo "PRS end   $(date +%T.%3N) rc=$RC duration=$(echo "$T1 - $T0" | bc)s"
wait $PP
cat $S/last_prober.txt
