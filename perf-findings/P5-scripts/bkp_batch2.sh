#!/bin/bash
# Per-file parallelism: backup concurrency 1 (emulates one dominant file), pargzip blocks 2 (default) vs 4.
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
run() {
  local label=$1 bin=$2 conc=$3; shift 3
  BIN=$bin $S/restart_tablet.sh 1 "$@" >/dev/null
  NORESTORE=1 $S/bkp.sh "$label" --concurrency=$conc
}
for r in 1 2; do
  run c1b2_$r /home/vt/bin 1
  run c1b4_$r /home/vt/bin 1 --backup-storage-number-blocks=4
  run c1zstd_$r /home/vt/bin 1 --compression-engine-name=zstd
  run c8b2_$r /home/vt/bin 8
done
BIN=/home/vt/bin $S/restart_tablet.sh 1 >/dev/null
