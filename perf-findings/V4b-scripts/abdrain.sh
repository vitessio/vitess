#!/bin/bash
# Alternate running-phase drains between vttablet binaries.
# Usage: abdrain.sh ROUNDS "label|bin" ...     Env: CASES (default "wo:30000:oltp_write_only upd:60000:oltp_update_non_index"),
#        NS (default "dst2 dst4 dst2+dst4")
ROUNDS=$1
shift
D=/home/vt/perf/V4b
CASES=${CASES:-"wo:30000:oltp_write_only upd:60000:oltp_update_non_index"}
NS=${NS:-"dst2"}
for r in $(seq 1 "$ROUNDS"); do
  for cfg in "$@"; do
    IFS='|' read -r label bin flags <<<"$cfg"
    VTTABLET_EXTRA_FLAGS="$flags" "$D/setarm.sh" "$bin" >/dev/null 2>&1
    for c in $CASES; do
      IFS=: read -r cn ev sb <<<"$c"
      for n in $NS; do
        flock -o /home/vt/perf/bench.lock "$D/drain.sh" "$label-$cn-${n//+/_}-r$r" "${n//+/ }" "$ev" "$sb" >/dev/null
        f=$D/out/dr-$label-$cn-${n//+/_}-r$r/summary
        echo "$(head -1 "$f" | cut -d' ' -f1,5,6) $(grep -E 'vttablet_100|mysqld_100' "$f" | awk '{printf "%s=%s ", $1, $4}') tgtvtt=$(awk '/vttablet_10[12]/{s+=$4} END{print s}' "$f") tgtmy=$(awk '/mysqld_10[12]/{s+=$4} END{print s}' "$f") load=$(uptime | sed 's/.*average: //' | cut -d, -f1)"
      done
      "$D/c.sh" purge-binlogs 30 >/dev/null 2>&1
    done
  done
done
