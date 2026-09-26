#!/bin/bash
# Idle cost of the running workflows: CPU per process, target/source MySQL statement and connection counts,
# vttablet RSS and goroutines, over DUR seconds with no user traffic.
# Usage: idle.sh LABEL [DUR]
LABEL=$1
DUR=${2:-30}
D=/home/vt/perf/V4b
OUT=$D/out/idle-$LABEL
mkdir -p "$OUT"
DR=/home/vt/perf/c40000
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
S=$DR/vt_0000000103/mysql.sock
T=$DR/vt_0000000104/mysql.sock
stat() { q "$1" "show global status where variable_name in ('Questions','Com_update','Com_insert','Com_select','Com_commit','Threads_connected','Bytes_sent','Binlog_bytes_written')" | awk '{printf "%s=%s ", $1, $2}'; }
nwf=$(q $T "select count(*) from _vt.vreplication where state='Running'")
"$D/c.sh" cpu >"$OUT/cpu0"
s0=$(stat $S); t0s=$(stat $T)
tb0=$(q $T "select sum(file_size) from performance_schema.file_instances" 2>/dev/null)
b0=$(ls -l $DR/vt_0000000104/bin-logs/ 2>/dev/null | awk '{s+=$5} END{print s}')
sleep "$DUR"
"$D/c.sh" cpu >"$OUT/cpu1"
s1=$(stat $S); t1s=$(stat $T)
b1=$(ls -l $DR/vt_0000000104/bin-logs/ 2>/dev/null | awk '{s+=$5} END{print s}')
echo "$LABEL streams=$nwf dur=${DUR}s load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v d="$DUR" '{x=$4-$2; if (x>0.01) printf "  %-14s %6.3f cores\n", $1, x/d}' | tee -a "$OUT/summary"
echo "  src: $s0" >>"$OUT/summary"; echo "    -> $s1" >>"$OUT/summary"
echo "  tgt: $t0s" >>"$OUT/summary"; echo "    -> $t1s" >>"$OUT/summary"
python3 - "$s0" "$s1" "$t0s" "$t1s" "$DUR" <<'EOF' | tee -a "$OUT/summary"
import sys
def p(s): return {k: int(v) for k, v in (x.split('=') for x in s.split())}
s0, s1, t0, t1, d = p(sys.argv[1]), p(sys.argv[2]), p(sys.argv[3]), p(sys.argv[4]), float(sys.argv[5])
for name, a, b in (("src", s0, s1), ("tgt", t0, t1)):
    print("  %s mysqld per s: %s  Threads_connected=%d" % (name, " ".join("%s=%.1f" % (k, (b[k] - a[k]) / d) for k in sorted(a) if k != 'Threads_connected'), b['Threads_connected']))
EOF
for u in 103 104; do
  pid=$(cat $DR/vt_0000000$u/vttablet.pid)
  echo "  vttablet_$u RSS=$(awk '/VmRSS/{print $2}' /proc/$pid/status)kB goroutines=$(curl -s "http://localhost:$((40000 + u))/debug/pprof/goroutine?debug=1" | head -1 | grep -o '[0-9]*$') tcp_to_mysql=$(ls -l /proc/$pid/fd 2>/dev/null | grep -c socket)" | tee -a "$OUT/summary"
done
echo "  target binlog growth: $(( (${b1:-0} - ${b0:-0}) / DUR )) B/s ($(( (${b1:-0} - ${b0:-0}) * 3600 / DUR / 1048576 )) MB/h)" | tee -a "$OUT/summary"
