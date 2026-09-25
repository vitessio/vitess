#!/bin/bash
# vtorc_trial.sh MODE [vtorc flags...]: fresh cluster, start vtorc, kill primary, report.
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
MODE=$1; shift
NOPREP=1 $S/fresh.sh >/dev/null
$S/vtorc.sh start "$@" >/dev/null
sleep ${SETTLE:-12}; sleep $(awk "BEGIN{srand(); print rand()*6}")
DUR=${DUR:-25s} PROBER_TABLE=1000 $S/failover.sh $MODE 2>&1 | grep -vE "No such process"
grep -m1 -E "Analysis: (DeadPrimary|PrimaryTabletUnreachableByQuorum|UnreachablePrimary)" /home/vt/perf/c40000/logs/vtorc.log | cut -c12-23,40-200
grep -m1 "finished EmergencyReparentShard" /home/vt/perf/c40000/logs/vtorc.log | cut -c12-23
cp /home/vt/perf/c40000/logs/vtorc.log $S/logs/vtorc_${TAG:-last}.log; cp /home/vt/perf/c40000/logs/vtgate.log $S/logs/vtgate_${TAG:-last}.log
