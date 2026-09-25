#!/bin/bash
# Interleaved VTOrc dead-primary detection A/B with randomized kill phase.
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
export DURABILITY=semi_sync VTGATE_EXTRA_FLAGS=--enable-buffer DUR=22s
for r in 1 2 3 4; do
  echo "=== r$r A base default"; TAG=A$r VTORC_BIN=/home/vt/bin $S/vtorc_trial.sh host
  echo "=== r$r B base poll1s"; TAG=B$r VTORC_BIN=/home/vt/bin $S/vtorc_trial.sh host --instance-poll-time=1s
  echo "=== r$r C patched poll1s"; TAG=C$r VTORC_BIN=/home/vt/bin-P5 $S/vtorc_trial.sh host --instance-poll-time=1s
  echo "=== r$r D patched default"; TAG=D$r VTORC_BIN=/home/vt/bin-P5 $S/vtorc_trial.sh host
  echo "=== r$r E patched poll1s recovery100ms"; TAG=E$r VTORC_BIN=/home/vt/bin-P5 $S/vtorc_trial.sh host --instance-poll-time=1s --recovery-poll-duration=100ms
  uptime
done
