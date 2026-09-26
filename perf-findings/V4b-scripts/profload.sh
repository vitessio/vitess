#!/bin/bash
# manyload.sh with 12 s CPU profiles of the source (103) and target (104) vttablets, taken 3 s in.
# Usage: profload.sh LABEL RATE DUR NT
D=/home/vt/perf/V4b
mkdir -p "$D/out/prof"
(sleep 3; curl -s -o "$D/out/prof/$1-src.pprof" "http://localhost:40103/debug/pprof/profile?seconds=12") &
(sleep 3; curl -s -o "$D/out/prof/$1-tgt.pprof" "http://localhost:40104/debug/pprof/profile?seconds=12") &
"$D/manyload.sh" "$@"
wait
