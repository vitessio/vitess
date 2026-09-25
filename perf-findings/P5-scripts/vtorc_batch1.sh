#!/bin/bash
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
export DURABILITY=semi_sync VTGATE_EXTRA_FLAGS=--enable-buffer
for r in 1 2 3; do
  echo "=== round $r default poll (5s)"; uptime
  $S/vtorc_trial.sh ${MODE:-host}
  echo "=== round $r instance-poll-time=1s"; uptime
  $S/vtorc_trial.sh ${MODE:-host} --instance-poll-time=1s
done
