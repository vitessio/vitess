#!/bin/bash
# restart_tablet.sh <index> [extra vttablet flags...]   (BIN selects binaries; shard 0 of the P5 cluster)
I=$1; shift
BIN=${BIN:-/home/vt/bin}
BASE=40000
UID_=$((100 + I))
ALIAS=$(printf 'zone1-%010d' $UID_)
D=/home/vt/perf/c40000/vt_$(printf '%010d' $UID_)
LOG=/home/vt/perf/c40000/logs/vttablet_$UID_.log
[[ -f $D/vttablet.pid ]] && kill $(cat $D/vttablet.pid) 2>/dev/null
for _ in $(seq 1 100); do kill -0 $(cat $D/vttablet.pid) 2>/dev/null || break; sleep 0.1; done
setpriv --reuid=vt --regid=vt --init-groups env HOME=/home/vt VTDATAROOT=/home/vt/perf/c40000 VT_MYSQL_ROOT=/usr \
  EXTRA_MY_CNF=/home/vt/perf/c40000/extra.cnf PATH=$BIN:/usr/sbin:/usr/bin:/bin nohup $BIN/vttablet \
  --topo-implementation etcd2 --topo-global-server-address localhost:$((BASE + 10)) --topo-global-root /vitess/global \
  --tablet-path "$ALIAS" --tablet-hostname localhost --init-keyspace sbtest --init-shard 0 \
  --init-tablet-type replica --health-check-interval 5s --backup-storage-implementation file \
  --file-backup-storage-root /home/vt/perf/c40000/backups --port $((BASE + 100 + I)) --grpc-port $((BASE + 200 + I)) \
  --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  --pid-file "$D/vttablet.pid" \
  --heartbeat-on-demand-duration=5s --pprof-http --log-format text --config-file-not-found-handling=ignore \
  --queryserver-config-pool-size 64 --queryserver-config-transaction-cap 128 "$@" >>"$LOG" 2>&1 </dev/null &
for _ in $(seq 1 300); do curl -sf "http://localhost:$((BASE + 100 + I))/debug/status" >/dev/null 2>&1 && break; sleep 0.2; done
sleep 2
echo "restarted $ALIAS with $BIN $*"
