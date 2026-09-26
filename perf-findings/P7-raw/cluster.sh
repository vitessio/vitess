#!/bin/bash
# Local Vitess cluster for performance profiling.
#
# Usage (run as the unprivileged `vt` user, e.g. `runuser -u vt -- env BASE=20000 ./cluster.sh up`):
#   cluster.sh up        start etcd, vtctld, mysqld+vttablet per tablet, vtgate; elect primaries; load schema/vschema
#   cluster.sh down      stop everything and delete the data dir
#   cluster.sh status    show processes and tablets
#   cluster.sh env       print the ports of this cluster
#   cluster.sh prepare   (re)load sysbench data through vtgate: TABLES tables x TABLE_SIZE rows (default 100000)
#   cluster.sh cpu       print cumulative CPU seconds (user+sys) per process: vtgate, vttablet_<uid>, mysqld_<uid>
#                        (diff two snapshots around a run to get CPU per query)
#   cluster.sh sb ARGS   run sysbench against vtgate with the right connection flags, e.g.
#                        cluster.sh sb oltp_point_select --threads=8 --time=30 run
#
# Environment:
#   BASE      port base (default 20000). Use different bases (20000, 30000) for concurrent clusters.
#   NAME      cluster name / data dir suffix (default c$BASE)
#   KEYSPACE  keyspace name (default sbtest)
#   SHARDS    space separated shard list (default "-80 80-"; use "0" for unsharded)
#   REPLICAS  replicas per shard in addition to the primary (default 0)
#   TABLES    number of sysbench tables to declare in the vschema (default 4)
#   VTGATE_EXTRA_FLAGS / VTTABLET_EXTRA_FLAGS  extra flags
#   BIN       directory with the Vitess binaries (default /home/vt/bin)
#
# Ports (relative to BASE): etcd +10/+11, vtctld web +20 grpc +21, vtgate web +1 grpc +2 mysql +3,
# tablet i (0-based): web +100+i, grpc +200+i, mysqld +400+i.
# pprof is available at http://localhost:<web port>/debug/pprof/ on vtgate and every vttablet.

set -euo pipefail

BASE=${BASE:-20000}
NAME=${NAME:-c$BASE}
KEYSPACE=${KEYSPACE:-sbtest}
SHARDS=${SHARDS:-"-80 80-"}
REPLICAS=${REPLICAS:-0}
TABLES=${TABLES:-4}
BIN=${BIN:-/home/vt/bin}
CELL=zone1
export VTDATAROOT=${VTDATAROOT:-/home/vt/perf/$NAME}
export PATH="$BIN:/usr/sbin:$PATH"
export VT_MYSQL_ROOT=/usr

ETCD_PORT=$((BASE + 10))
ETCD_PEER=$((BASE + 11))
VTCTLD_WEB=$((BASE + 20))
VTCTLD_GRPC=$((BASE + 21))
VTGATE_WEB=$((BASE + 1))
VTGATE_GRPC=$((BASE + 2))
VTGATE_MYSQL=$((BASE + 3))
TOPO="--topo-implementation etcd2 --topo-global-server-address localhost:$ETCD_PORT --topo-global-root /vitess/global"
LOGDIR=$VTDATAROOT/logs

vtctldclient() { command vtctldclient --server "localhost:$VTCTLD_GRPC" "$@"; }

wait_http() {
  for _ in $(seq 1 600); do
    curl -sf "http://localhost:$1/debug/status" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "timeout waiting for http port $1" >&2
  return 1
}

# tablet index -> uid; uids are unique per cluster (VTDATAROOT is per cluster).
tablet_uid() { echo $((100 + $1)); }

write_mycnf() {
  cat >"$VTDATAROOT/extra.cnf" <<'EOF'
innodb_buffer_pool_size = 512M
innodb_flush_log_at_trx_commit = 2
sync_binlog = 0
innodb_log_file_size = 256M
max_connections = 2000
performance_schema = OFF
EOF
}

start_tablets() {
  i=0
  for shard in $SHARDS; do
    for r in $(seq 0 "$REPLICAS"); do
      local uid alias; uid=$(tablet_uid $i); printf -v alias '%s-%010d' "$CELL" "$uid"
      env ${TABLET_ENV:-} vttablet $TOPO --tablet-path "$alias" --tablet-hostname localhost --init-keyspace "$KEYSPACE" --init-shard "$shard" \
        --init-tablet-type replica --health-check-interval 5s --backup-storage-implementation file \
        --file-backup-storage-root "$VTDATAROOT/backups" --port $((BASE + 100 + i)) --grpc-port $((BASE + 200 + i)) \
        --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
        --pid-file "$VTDATAROOT/vt_$(printf '%010d' "$uid")/vttablet.pid" \
        --heartbeat-on-demand-duration=5s --pprof-http --log-format text --config-file-not-found-handling=ignore \
        --queryserver-config-pool-size 64 --queryserver-config-transaction-cap 128 \
        ${VTTABLET_EXTRA_FLAGS:-} >"$LOGDIR/vttablet_$uid.log" 2>&1 &
      i=$((i + 1))
    done
  done
  i=0
  for shard in $SHARDS; do
    for r in $(seq 0 "$REPLICAS"); do wait_http $((BASE + 100 + i)); i=$((i + 1)); done
  done

}

start_vtgate() {
  env ${GATE_ENV:-} vtgate $TOPO --cell "$CELL" --cells-to-watch "$CELL" --tablet-types-to-wait PRIMARY,REPLICA \
    --port "$VTGATE_WEB" --grpc-port "$VTGATE_GRPC" --mysql-server-port "$VTGATE_MYSQL" \
    --mysql-server-socket-path "$VTDATAROOT/vtgate.sock" --service-map 'grpc-vtgateservice' \
    --mysql-auth-server-impl none --pprof-http --log-format text --config-file-not-found-handling=ignore \
    ${VTGATE_EXTRA_FLAGS:-} >"$LOGDIR/vtgate.log" 2>&1 &
  echo $! >"$VTDATAROOT/vtgate.pid"
  wait_http "$VTGATE_WEB"
  for _ in $(seq 1 100); do
    mysql -h 127.0.0.1 -P "$VTGATE_MYSQL" -u root -e 'select 1' "$KEYSPACE" >/dev/null 2>&1 && break
    sleep 0.3
  done
}

# restart vtgate and vttablets only (keep mysqld, topo, data); picks up BIN / *_EXTRA_FLAGS / env (GOGC, GOMAXPROCS...)
restart() {
  [[ -f $VTDATAROOT/vtgate.pid ]] && kill "$(cat "$VTDATAROOT/vtgate.pid")" 2>/dev/null || true
  for f in "$VTDATAROOT"/vt_*/vttablet.pid; do [[ -f $f ]] && kill "$(cat "$f")" 2>/dev/null || true; done
  for _ in $(seq 1 100); do
    local alive=0
    for f in "$VTDATAROOT"/vt_*/vttablet.pid "$VTDATAROOT/vtgate.pid"; do [[ -f $f ]] && kill -0 "$(cat "$f")" 2>/dev/null && alive=1; done
    [[ $alive == 0 ]] && break; sleep 0.2
  done
  # also stop strays whose pid file was overwritten (e.g. a start that failed to bind its port)
  pkill -f -- "^(vttablet|vtgate) .*--topo-global-server-address localhost:$ETCD_PORT" 2>/dev/null || true
  for _ in $(seq 1 100); do
    pgrep -f -- "^(vttablet|vtgate) .*--topo-global-server-address localhost:$ETCD_PORT" >/dev/null || break
    sleep 0.2
  done
  export EXTRA_MY_CNF=$VTDATAROOT/extra.cnf
  start_tablets
  sleep 3
  start_vtgate
  for _ in $(seq 1 100); do
    [[ $(mysql -N -h 127.0.0.1 -P "$VTGATE_MYSQL" -u root -e 'select count(*) from sbtest1 where id in (1,2,3,4,5,6,7,8)' "$KEYSPACE" 2>/dev/null) == 8 ]] && break
    sleep 0.3
  done
}

up() {
  mkdir -p "$LOGDIR" "$VTDATAROOT/backups"
  write_mycnf
  export EXTRA_MY_CNF=$VTDATAROOT/extra.cnf

  etcd --name "$NAME" --data-dir "$VTDATAROOT/etcd" \
    --listen-client-urls "http://localhost:$ETCD_PORT" --advertise-client-urls "http://localhost:$ETCD_PORT" \
    --listen-peer-urls "http://localhost:$ETCD_PEER" --initial-advertise-peer-urls "http://localhost:$ETCD_PEER" \
    --initial-cluster "$NAME=http://localhost:$ETCD_PEER" >"$LOGDIR/etcd.log" 2>&1 &
  echo $! >"$VTDATAROOT/etcd.pid"
  for _ in $(seq 1 100); do curl -sf "http://localhost:$ETCD_PORT/health" >/dev/null && break; sleep 0.1; done
  command vtctldclient --server internal $TOPO AddCellInfo --root "/vitess/$CELL" --server-address "localhost:$ETCD_PORT" "$CELL" >/dev/null 2>&1 || true

  vtctld $TOPO --cell "$CELL" --service-map 'grpc-vtctl,grpc-vtctld' --backup-storage-implementation file \
    --file-backup-storage-root "$VTDATAROOT/backups" --port "$VTCTLD_WEB" --grpc-port "$VTCTLD_GRPC" \
    --log-format text --config-file-not-found-handling=ignore >"$LOGDIR/vtctld.log" 2>&1 &
  wait_http "$VTCTLD_WEB"

  vtctldclient CreateKeyspace --durability-policy=none "$KEYSPACE" >/dev/null 2>&1 || true

  local i=0 pids=()
  for shard in $SHARDS; do
    for r in $(seq 0 "$REPLICAS"); do
      local uid; uid=$(tablet_uid $i)
      mysqlctl --tablet-uid "$uid" --mysql-port $((BASE + 400 + i)) --log-format text \
        --config-file-not-found-handling=ignore init >"$LOGDIR/mysqlctl_$uid.log" 2>&1 &
      pids+=($!)
      i=$((i + 1))
    done
  done
  for p in "${pids[@]}"; do wait "$p"; done

  start_tablets

  # Elect the first tablet of every shard as primary.
  i=0
  for shard in $SHARDS; do
    local alias; printf -v alias '%s-%010d' "$CELL" "$(tablet_uid $i)"
    vtctldclient PlannedReparentShard "$KEYSPACE/$shard" --new-primary "$alias" >/dev/null
    i=$((i + REPLICAS + 1))
  done

  load_schema

  start_vtgate
  env_info
}

load_schema() {
  local sql="" vs_tables=""
  for t in $(seq 1 "$TABLES"); do
    sql+="CREATE TABLE IF NOT EXISTS sbtest$t (id INT NOT NULL, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', PRIMARY KEY (id), KEY k_$t (k)) ENGINE=InnoDB;"
    [[ -n $vs_tables ]] && vs_tables+=","
    vs_tables+="\"sbtest$t\":{\"column_vindexes\":[{\"column\":\"id\",\"name\":\"hash\"}]}"
  done
  vtctldclient ApplySchema --sql "$sql" "$KEYSPACE" >/dev/null
  if [[ $SHARDS == "0" ]]; then
    vtctldclient ApplyVSchema --vschema '{"sharded":false}' "$KEYSPACE" >/dev/null
  else
    vtctldclient ApplyVSchema --vschema "{\"sharded\":true,\"vindexes\":{\"hash\":{\"type\":\"hash\"}},\"tables\":{$vs_tables}}" "$KEYSPACE" >/dev/null
  fi
}

down() {
  [[ -f $VTDATAROOT/vtgate.pid ]] && kill "$(cat "$VTDATAROOT/vtgate.pid")" 2>/dev/null || true
  for f in "$VTDATAROOT"/vt_*/vttablet.pid; do [[ -f $f ]] && kill "$(cat "$f")" 2>/dev/null || true; done
  sleep 1
  for d in "$VTDATAROOT"/vt_*; do
    [[ -d $d ]] || continue
    local uid=$((10#${d##*vt_}))
    mysqlctl --tablet-uid "$uid" --log-format text --config-file-not-found-handling=ignore shutdown >/dev/null 2>&1 || true
  done
  pkill -f -- "--topo-global-server-address localhost:$ETCD_PORT" 2>/dev/null || true
  [[ -f $VTDATAROOT/etcd.pid ]] && kill "$(cat "$VTDATAROOT/etcd.pid")" 2>/dev/null || true
  sleep 1
  rm -rf "$VTDATAROOT"
}

env_info() {
  cat <<EOF
cluster $NAME (VTDATAROOT=$VTDATAROOT), keyspace $KEYSPACE shards [$SHARDS] replicas/shard $REPLICAS
  vtgate mysql: mysql -h 127.0.0.1 -P $VTGATE_MYSQL -u root $KEYSPACE
  vtgate web/pprof: http://localhost:$VTGATE_WEB/debug/pprof/
  vtctldclient: vtctldclient --server localhost:$VTCTLD_GRPC
  tablet i: web/pprof $((BASE + 100))+i, grpc $((BASE + 200))+i, mysqld $((BASE + 400))+i (socket $VTDATAROOT/vt_<uid>/mysql.sock, user vt_dba)
EOF
}

SB_ARGS() {
  echo "--db-driver=mysql --mysql-host=127.0.0.1 --mysql-port=$VTGATE_MYSQL --mysql-user=root --mysql-db=$KEYSPACE --tables=$TABLES --table-size=${TABLE_SIZE:-100000} --auto_inc=off"
}

prepare() {
  for t in $(seq 1 "$TABLES"); do
    mysql -h 127.0.0.1 -P "$VTGATE_MYSQL" -u root "$KEYSPACE" -e "DROP TABLE IF EXISTS sbtest$t"
  done
  # shellcheck disable=SC2046
  sysbench $(SB_ARGS) oltp_read_write --threads=4 prepare
}

pids() {
  [[ -f $VTDATAROOT/vtgate.pid ]] && echo "vtgate $(cat "$VTDATAROOT/vtgate.pid")"
  for d in "$VTDATAROOT"/vt_*; do
    [[ -d $d ]] || continue
    local uid=$((10#${d##*vt_}))
    [[ -f $d/vttablet.pid ]] && echo "vttablet_$uid $(cat "$d/vttablet.pid")"
    [[ -f $d/mysql.pid ]] && echo "mysqld_$uid $(cat "$d/mysql.pid")"
  done
}

cpu() {
  local tck; tck=$(getconf CLK_TCK)
  pids | while read -r name pid; do
    [[ -r /proc/$pid/stat ]] || continue
    awk -v n="$name" -v t="$tck" '{ printf "%s %.2f\n", n, ($14 + $15) / t }' "/proc/$pid/stat"
  done
}

status() {
  env_info
  vtctldclient GetTablets 2>/dev/null || true
}

case "${1:-}" in
  up) up ;;
  restart) restart ;;
  down) down ;;
  status) status ;;
  env) env_info ;;
  prepare) prepare ;;
  pids) pids ;;
  cpu) cpu ;;
  sb) shift; # shellcheck disable=SC2046
      sysbench $(SB_ARGS) "$@" ;;
  *) echo "usage: $0 up|down|status|env" >&2; exit 2 ;;
esac
