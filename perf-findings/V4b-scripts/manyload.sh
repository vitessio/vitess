#!/bin/bash
# Steady write load on keyspace many (single-row UPDATEs over NT tables at RATE trx/s for DUR s, directly on the
# source mysqld) while the workflows run; CPU per source transaction per process.
# Usage: manyload.sh LABEL [RATE] [DUR] [NT]
LABEL=$1
RATE=${2:-500}
DUR=${3:-30}
NT=${4:-1000}
D=/home/vt/perf/V4b
OUT=$D/out/ml-$LABEL
mkdir -p "$OUT"
DR=/home/vt/perf/c40000
S=$DR/vt_0000000103/mysql.sock
T=$DR/vt_0000000104/mysql.sock
q() { mysql -N -S "$1" -u vt_dba -e "$2" 2>/dev/null; }
nwf=$(q $T "select count(*) from _vt.vreplication where state='Running'")
binlog_bytes() { ls -l "$DR/vt_0000000$1/bin-logs/" 2>/dev/null | awk '{s+=$5} END{print s}'; }
"$D/c.sh" cpu >"$OUT/cpu0"
tb0=$(binlog_bytes 104)
sb0=$(binlog_bytes 103)
sysbench --db-driver=mysql --mysql-socket=$S --mysql-user=vt_dba --mysql-db=vt_many --threads=4 --rate="$RATE" \
  --time="$DUR" --ntables="$NT" "$D/many.lua" run >"$OUT/sysbench" 2>&1
"$D/c.sh" cpu >"$OUT/cpu1"
tb1=$(binlog_bytes 104)
sb1=$(binlog_bytes 103)
n=$(grep -o 'transactions: *[0-9]*' "$OUT/sysbench" | grep -o '[0-9]*$')
srcgtid=$(q $S "select @@global.gtid_executed" | tr -d '\n')
# Lag at the end of the load: streams not yet at the source position.
behind=$(q $T "select count(*) from _vt.vreplication where state='Running' and not gtid_subset('$srcgtid', substring(pos, 9))")
echo "$LABEL streams=$nwf trx=$n rate=$RATE behind_at_end=$behind load: $(uptime | sed 's/.*average: //')" | tee "$OUT/summary"
paste -d' ' "$OUT/cpu0" "$OUT/cpu1" | awk -v r="$n" '$1 ~ /_10[34]$/ {d=$4-$2; printf "  %-14s %7.2f s  %7.1f us/trx\n", $1, d, d*1e6/r}' | tee -a "$OUT/summary"
echo "  binlog bytes per source trx: source $(( (sb1 - sb0) / n )) target $(( (tb1 - tb0) / n ))" | tee -a "$OUT/summary"
