#!/bin/bash
# PRS A/B: base vs P5 vttablet, semi_sync, no vtgate buffering (back-to-back PRS), max write gap.
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
export DURABILITY=semi_sync DUR=7s PRE=2
for r in 1 2; do
  for bin in /home/vt/bin /home/vt/bin-P5; do
    echo "=== r$r $bin REPLICAS=${REPLICAS:-2}"
    BIN=$bin REPLICAS=${REPLICAS:-2} $S/fresh.sh >/dev/null
    if [[ ${REPLICAS:-2} == 2 ]]; then targets="101 102 100 101 102 100"; else targets="101 100 101 100 101 100"; fi
    BIN=$bin $S/prs_loop.sh $targets | grep -E "PRS end|gap"
    uptime
  done
done
