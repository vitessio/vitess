#!/bin/bash
# Restart all vttablets and vtctld with the binaries of arm $1 (base|ref|new|<dir>), mysqld/vtgate kept.
# Then wait until vtgate can write to src and all workflows are Running.
D=/home/vt/perf/V4b
case $1 in
  base) B=/home/vt/bin ;;
  ref) B=/home/vt/bin-V4bref ;;
  new) B=/home/vt/bin-V4c ;;
  new2) B=/home/vt/bin-V4d ;;
  new3) B=/home/vt/bin-V4e ;;
  new4) B=/home/vt/bin-V4f ;;
  new5) B=/home/vt/bin-V4g ;;
  new6) B=/home/vt/bin-V4h ;;
  *) B=$1 ;;
esac
T=$(date +%s)
BIN=$B "$D/c.sh" restart-tablets >/dev/null 2>&1
BIN=$B "$D/c.sh" restart-vtctld >/dev/null 2>&1
for i in $(seq 1 120); do
  mysql -h 127.0.0.1 -P 40003 -u root -e "select 1 from sbtest1 limit 1" src >/dev/null 2>&1 && break
  sleep 0.5
done
for i in $(seq 1 200); do
  n=0
  for d in /home/vt/perf/c40000/vt_*; do
    # A Running stream only counts once it has recorded a heartbeat after the restart
    # (it may be waiting in the tablet picker).
    c=$(mysql -N -S "$d/mysql.sock" -u vt_dba -e "select count(*) from _vt.vreplication where state not in ('Running','Stopped') or (state='Running' and time_updated < $T + 2)" 2>/dev/null)
    n=$((n + ${c:-0}))
  done
  [[ $n == 0 ]] && break
  sleep 0.5
done
sleep ${SETTLE:-3}
echo "arm $1 bin=$B ready after $(( $(date +%s) - T ))s (not ready: $n)"
