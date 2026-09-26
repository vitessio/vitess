#!/bin/bash
# SwitchTraffic/ReverseTraffic A/B: for each round, for each arm, restart the vttablets+vtctld with the arm's binaries
# (outside the bench lock), then run PAIRS switch+reverse pairs, each under the lock.
# Usage: abswitch.sh "base ref new new2" ROUNDS PAIRS TAG
D=/home/vt/perf/V4b
ARMS=$1
ROUNDS=${2:-3}
PAIRS=${3:-2}
TAG=${4:-ab}
LOG=$D/out/$TAG.log
for r in $(seq 1 "$ROUNDS"); do
  for arm in $ARMS; do
    "$D/setarm.sh" "$arm" >>"$LOG.setarm" 2>&1
    for p in $(seq 1 "$PAIRS"); do
      for act in switchtraffic reversetraffic; do
        l="$TAG-$arm-r$r-p$p"
        line=$(flock -o /home/vt/perf/bench.lock "$D/switch.sh" "$l" "$act")
        st=$(python3 "$D/steps.py" "$D/out/sw-$l-$act")
        echo "$line | $st" | tee -a "$LOG"
        sleep 2
      done
    done
  done
done
