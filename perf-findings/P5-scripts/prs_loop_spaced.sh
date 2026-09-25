#!/bin/bash
# prs_loop_spaced.sh uid1 uid2 ... : PRS with 62s spacing (vtgate --buffer-min-time-between-failovers default 1m)
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
first=1
for n in "$@"; do
  [[ $first == 1 ]] || sleep ${SPACE:-52}
  first=0
  DUR=${DUR:-10s} PRE=${PRE:-3} $S/prs.sh $n | grep -E "PRS end|gap|errors from|n=|^ +[0-9]+ "
  echo ---
done
