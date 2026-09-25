#!/bin/bash
# bkp.sh LABEL [backup flags...]: back up tablet 101, restore it, report wall time, vttablet+mysqld CPU and size.
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
LABEL=$1; shift
V="/home/vt/bin/vtctldclient --server localhost:40021 --action-timeout 1h"
cpu() { /home/vt/perf/P5/c.sh cpu | awk '$1=="vttablet_101"{print $2}'; }
rm -rf /home/vt/perf/c40000/backups/sbtest/0/* 2>/dev/null
c0=$(cpu); t0=$(date +%s.%N)
$V Backup "$@" zone1-0000000101 > $S/logs/bkp_$LABEL.txt 2>&1 || { echo "backup failed"; tail -5 $S/logs/bkp_$LABEL.txt; }
t1=$(date +%s.%N); c1=$(cpu)
SZ=$(du -sm /home/vt/perf/c40000/backups/sbtest/0 | cut -f1)
echo "$LABEL backup wall=$(echo "$t1 - $t0" | bc)s vttablet_cpu=$(echo "$c1 - $c0" | bc)s size=${SZ}MB load=$(cut -d' ' -f1 /proc/loadavg)"
[[ -n $NORESTORE ]] && exit 0
c0=$(cpu); t0=$(date +%s.%N)
$V RestoreFromBackup ${RESTORE_FLAGS:-} zone1-0000000101 > $S/logs/rst_$LABEL.txt 2>&1 || { echo "restore failed"; tail -5 $S/logs/rst_$LABEL.txt; }
t1=$(date +%s.%N); c1=$(cpu)
echo "$LABEL restore wall=$(echo "$t1 - $t0" | bc)s vttablet_cpu=$(echo "$c1 - $c0" | bc)s load=$(cut -d' ' -f1 /proc/loadavg)"
