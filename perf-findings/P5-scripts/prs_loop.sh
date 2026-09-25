#!/bin/bash
# prs_loop.sh uid1 uid2 ... : run prs.sh for each target and print a summary line
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
for n in "$@"; do
  DUR=${DUR:-10s} PRE=${PRE:-3} $S/prs.sh $n | grep -E "PRS end|gap|errors from|n=|^ +[0-9]+ "
  echo ---
done
