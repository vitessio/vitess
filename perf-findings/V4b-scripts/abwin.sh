#!/bin/bash
# SwitchTraffic with and without the 20 ms empty-transaction coalescing window (new6 binaries), 2 rounds x 3 pairs.
D=/home/vt/perf/V4b
for r in 1 2; do
  VTTABLET_EXTRA_FLAGS="--vstream-coalesce-empty-transactions --vstream-coalesce-empty-transactions-window=20ms" "$D/abswitch.sh" "new6" 1 3 "abwin$r"
  "$D/abswitch.sh" "new6" 1 3 "abnowin$r"
done
