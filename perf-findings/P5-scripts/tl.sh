#!/bin/bash
# tl.sh <vtctldclient event log>: print relative timestamps of events, skipping per-file noise unless ALL=1
sed -E 's/.*seconds:([0-9]+) nanoseconds:([0-9]+).*value:(.*)/\1 \2 \3/' "$1" | awk -v all="$ALL" '{
  t = $1 + $2/1e9; if (t0 == 0) t0 = t;
  v = $0; sub(/^[0-9]+ [0-9]+ /, "", v);
  if (all == "" && v ~ /(Backing up file|Compressing backup|Closing compressor|Copying file|Decompressing|Restoring file|closing|Done taking|Closing|Done restoring|Restore: copying)/) next;
  printf "%7.3f %s\n", t - t0, substr(v, 1, 140)
}'
