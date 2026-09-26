#!/bin/bash
# Many-tables MoveTables A/B: for each round and arm, restart tablets+vtctld with the arm (outside the lock), run
# mtmany.sh (MoveTables --all-tables many -> mdst) under the lock, then cancel the workflow outside the lock.
# Usage: abmany.sh "ref new new3" ROUNDS TAG
D=/home/vt/perf/V4b
ARMS=$1
ROUNDS=${2:-2}
TAG=${3:-mm}
for r in $(seq 1 "$ROUNDS"); do
  for arm in $ARMS; do
    "$D/setarm.sh" "$arm" >>"$D/out/$TAG.setarm" 2>&1
    "$D/c.sh" purge-binlogs 0 >/dev/null 2>&1
    NOCANCEL=1 flock -o /home/vt/perf/bench.lock "$D/mtmany.sh" "$TAG-$arm-r$r" | tee -a "$D/out/$TAG.log"
    c0=$(date +%s.%N)
    "$D/c.sh" vtctldclient MoveTables --workflow wfm --target-keyspace mdst cancel >"$D/out/$TAG-$arm-r$r.cancel" 2>&1 || echo "cancel failed" | tee -a "$D/out/$TAG.log"
    echo "  cancel=$(echo "$(date +%s.%N) - $c0" | bc)s" | tee -a "$D/out/$TAG.log"
  done
done
