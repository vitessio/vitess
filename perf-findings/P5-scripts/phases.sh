#!/bin/bash
# phases.sh label...: copy-phase durations from backup/restore event logs
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
for l in "$@"; do
  b=$($S/tl.sh $S/logs/bkp_$l.txt | awk '/found [0-9]+ files to backup/{s=$1} /Completed backing up MANIFEST/{e=$1} /Canceled|Completed/{} END{printf "%.2f", e-s}')
  bs=$($S/tl.sh $S/logs/bkp_$l.txt | awk '/using replication position/{s=$1} /found [0-9]+ files to backup/{e=$1} END{printf "%.2f", e-s}')
  r=$($S/tl.sh $S/logs/rst_$l.txt | awk '/reinit config file/{s=$1} /returning replication position/{e=$1} END{printf "%.2f", e-s}')
  rs=$($S/tl.sh $S/logs/rst_$l.txt | awk '/shutdown mysqld/{s=$1} /deleting existing files/{e=$1} END{printf "%.2f", e-s}')
  ru=$($S/tl.sh $S/logs/rst_$l.txt | awk '/returning replication position/{s=$1} /Restore: complete/{e=$1} END{printf "%.2f", e-s}')
  echo "$l backup: shutdown=${bs}s copy=${b}s | restore: shutdown=${rs}s copy=${r}s start+upgrade+restart=${ru}s"
done
