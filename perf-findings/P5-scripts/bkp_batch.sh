#!/bin/bash
# Backup/restore A/B on tablet 101 (index 1).
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
run() {
  local label=$1 bin=$2; shift 2
  BIN=$bin $S/restart_tablet.sh 1 "$@" >/dev/null
  $S/bkp.sh "$label"
}
for r in ${ROUNDS:-1 2 3}; do
  run base$r /home/vt/bin
  run p5$r /home/vt/bin-P5
  run f07$r /home/vt/bin-P5f07
  run pgzip$r /home/vt/bin --compression-engine-name=pgzip
  run zstd$r /home/vt/bin --compression-engine-name=zstd
  run lz4$r /home/vt/bin --compression-engine-name=lz4
done
BIN=/home/vt/bin $S/restart_tablet.sh 1 >/dev/null
