#!/bin/bash
# P4 VReplication harness: a copy of perf-findings/harness/cluster.sh that runs several keyspaces in one cluster.
#
# Usage (as the vt user): BASE=40000 ./cluster.sh up|down|cpu|pids|restart-tablets|load|purge-binlogs|env
#
# Environment (in addition to the base harness):
#   KEYSPACES  space separated "keyspace:shard,shard" list (default "src:0 dst:-80,80-").
#              The first keyspace is unsharded (vschema {"sharded":false}) and gets the sysbench schema;
#              the others get a hash-on-id vschema for the same tables (no schema: MoveTables creates it).
#   TABLES / TABLE_SIZE   sysbench tables loaded into the first keyspace by `load` (directly into its mysqld).
#   BIN        Vitess binaries. `restart-tablets` restarts only the vttablets (mysqld kept) with $BIN.
# Tablet i (0-based, in KEYSPACES order): uid 100+i, web BASE+100+i, grpc BASE+200+i, mysqld BASE+400+i.
set -euo pipefail

BASE=${BASE:-40000}
NAME=${NAME:-c$BASE}
KEYSPACES=${KEYSPACES:-"src:0 dst:-80,80-"}
TABLES=${TABLES:-4}
TABLE_SIZE=${TABLE_SIZE:-500000}
BIN=${BIN:-/home/vt/bin}
CELL=zone1
export VTDATAROOT=${VTDATAROOT:-/home/vt/perf/$NAME}
export PATH="$BIN:/usr/sbin:/usr/local/bin:/usr/bin:/bin"
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

tablet_uid() { echo $((100 + $1)); }

# Prints "index keyspace shard" for every tablet.
tablets() {
  local i=0
  for spec in $KEYSPACES; do
    local ks=${spec%%:*} shards=${spec#*:}
    for shard in ${shards//,/ }; do echo "$i $ks $shard"; i=$((i + 1)); done
  done
}

write_mycnf() {
  cat >"$VTDATAROOT/extra.cnf" <<EOC
innodb_buffer_pool_size = ${BUFPOOL:-256M}
innodb_flush_log_at_trx_commit = 2
sync_binlog = 0
innodb_log_file_size = ${REDO:-64M}
max_connections = 2000
performance_schema = OFF
EOC
}

start_vtctld() {
  # shellcheck disable=SC2086
  vtctld $TOPO --cell "$CELL" --service-map 'grpc-vtctl,grpc-vtctld' --backup-storage-implementation file \
    --file-backup-storage-root "$VTDATAROOT/backups" --port "$VTCTLD_WEB" --grpc-port "$VTCTLD_GRPC" \
    --log-format text --config-file-not-found-handling=ignore ${VTCTLD_EXTRA_FLAGS:-} >>"$LOGDIR/vtctld.log" 2>&1 &
  echo $! >"$VTDATAROOT/vtctld.pid"
  wait_http "$VTCTLD_WEB"
}

restart_vtctld() {
  local p
  p=$(cat "$VTDATAROOT/vtctld.pid" 2>/dev/null || pgrep -f -- "--port $VTCTLD_WEB " || true)
  for x in $p; do kill "$x" 2>/dev/null; while kill -0 "$x" 2>/dev/null; do sleep 0.1; done; done
  start_vtctld
}

start_tablet() {
  local i=$1 ks=$2 shard=$3 uid alias
  uid=$(tablet_uid "$i")
  printf -v alias '%s-%010d' "$CELL" "$uid"
  # shellcheck disable=SC2086
  vttablet $TOPO --tablet-path "$alias" --tablet-hostname localhost --init-keyspace "$ks" --init-shard "$shard" \
    --init-tablet-type replica --health-check-interval 5s --backup-storage-implementation file \
    --file-backup-storage-root "$VTDATAROOT/backups" --port $((BASE + 100 + i)) --grpc-port $((BASE + 200 + i)) \
    --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    --pid-file "$VTDATAROOT/vt_$(printf '%010d' "$uid")/vttablet.pid" \
    --heartbeat-on-demand-duration=5s --pprof-http --log-format text --config-file-not-found-handling=ignore \
    --queryserver-config-pool-size 64 --queryserver-config-transaction-cap 128 \
    ${VTTABLET_EXTRA_FLAGS:-} >>"$LOGDIR/vttablet_$uid.log" 2>&1 &
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

  start_vtctld

  for spec in $KEYSPACES; do vtctldclient CreateKeyspace --durability-policy=none "${spec%%:*}" >/dev/null 2>&1 || true; done

  local pids=()
  while read -r i ks shard; do
    local uid
    uid=$(tablet_uid "$i")
    mysqlctl --tablet-uid "$uid" --mysql-port $((BASE + 400 + i)) --log-format text \
      --config-file-not-found-handling=ignore init >"$LOGDIR/mysqlctl_$uid.log" 2>&1 &
    pids+=($!)
  done < <(tablets)
  for p in "${pids[@]}"; do wait "$p"; done

  while read -r i ks shard; do start_tablet "$i" "$ks" "$shard"; done < <(tablets)
  while read -r i ks shard; do wait_http $((BASE + 100 + i)); done < <(tablets)
  while read -r i ks shard; do
    local alias
    printf -v alias '%s-%010d' "$CELL" "$(tablet_uid "$i")"
    vtctldclient PlannedReparentShard "$ks/$shard" --new-primary "$alias" >/dev/null
  done < <(tablets)

  load_schema

  # shellcheck disable=SC2086
  vtgate $TOPO --cell "$CELL" --cells-to-watch "$CELL" --tablet-types-to-wait PRIMARY \
    --port "$VTGATE_WEB" --grpc-port "$VTGATE_GRPC" --mysql-server-port "$VTGATE_MYSQL" \
    --mysql-server-socket-path "$VTDATAROOT/vtgate.sock" --service-map 'grpc-vtgateservice' \
    --mysql-auth-server-impl none --pprof-http --log-format text --config-file-not-found-handling=ignore \
    ${VTGATE_EXTRA_FLAGS:-} >"$LOGDIR/vtgate.log" 2>&1 &
  echo $! >"$VTDATAROOT/vtgate.pid"
  wait_http "$VTGATE_WEB"
  env_info
}

load_schema() {
  local sql="" vs_tables="" first=1
  for t in $(seq 1 "$TABLES"); do
    sql+="CREATE TABLE IF NOT EXISTS sbtest$t (id INT NOT NULL AUTO_INCREMENT, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', PRIMARY KEY (id), KEY k_$t (k)) ENGINE=InnoDB;"
    [[ -n $vs_tables ]] && vs_tables+=","
    vs_tables+="\"sbtest$t\":{\"column_vindexes\":[{\"column\":\"id\",\"name\":\"hash\"}]}"
  done
  for spec in $KEYSPACES; do
    local ks=${spec%%:*}
    if ((first)); then
      vtctldclient ApplySchema --sql "$sql" "$ks" >/dev/null
      vtctldclient ApplyVSchema --vschema '{"sharded":false}' "$ks" >/dev/null
      first=0
    else
      vtctldclient ApplyVSchema --vschema "{\"sharded\":true,\"vindexes\":{\"hash\":{\"type\":\"hash\"}},\"tables\":{$vs_tables}}" "$ks" >/dev/null
    fi
  done
}

# Load sysbench tables into the first keyspace directly through its primary's socket (fast, no vtgate).
load() {
  local sock=$VTDATAROOT/vt_0000000100/mysql.sock ks=${KEYSPACES%%:*}
  for t in $(seq 1 "$TABLES"); do mysql -S "$sock" -u vt_dba "vt_$ks" -e "DROP TABLE IF EXISTS sbtest$t"; done
  sysbench --db-driver=mysql --mysql-socket="$sock" --mysql-user=vt_dba --mysql-db="vt_$ks" --tables="$TABLES" \
    --table-size="$TABLE_SIZE" --threads="$TABLES" oltp_write_only prepare
  purge_binlogs 0
}

# Rotate binlogs and purge the files older than $1 seconds (default 120: running streams may still need recent files).
purge_binlogs() {
  local age=${1:-120}
  for d in "$VTDATAROOT"/vt_*; do
    mysql -S "$d/mysql.sock" -u vt_dba -e "FLUSH BINARY LOGS; PURGE BINARY LOGS BEFORE NOW() - INTERVAL $age SECOND;" 2>&1 | grep -v Warning || true
  done
}

# Stop the vttablet with index $1: the one in its pid file and any other one still holding its web port.
kill_tablet() {
  local idx=$1 f pids p
  f=$VTDATAROOT/vt_$(printf '%010d' "$(tablet_uid "$idx")")/vttablet.pid
  pids="$(cat "$f" 2>/dev/null || true) $(pgrep -f -- "--port $((BASE + 100 + idx)) " || true)"
  for p in $pids; do kill "$p" 2>/dev/null || true; done
  for p in $pids; do while kill -0 "$p" 2>/dev/null; do sleep 0.2; done; done
}

restart_tablets() {
  while read -r i ks shard; do kill_tablet "$i" & done < <(tablets)
  wait
  while read -r i ks shard; do start_tablet "$i" "$ks" "$shard"; done < <(tablets)
  while read -r i ks shard; do wait_http $((BASE + 100 + i)); done < <(tablets)
}

# Restart the vttablet with index $1 only (mysqld kept).
restart_tablet() {
  local idx=$1
  kill_tablet "$idx"
  while read -r i ks shard; do [[ $i == "$idx" ]] && start_tablet "$i" "$ks" "$shard"; done < <(tablets)
  wait_http $((BASE + 100 + idx))
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
  echo "cluster $NAME (VTDATAROOT=$VTDATAROOT), keyspaces [$KEYSPACES]"
  echo "  vtgate mysql: mysql -h 127.0.0.1 -P $VTGATE_MYSQL -u root ; vtctldclient --server localhost:$VTCTLD_GRPC"
  tablets | while read -r i ks shard; do
    echo "  tablet $i uid $(tablet_uid "$i") $ks/$shard web $((BASE + 100 + i)) sock $VTDATAROOT/vt_$(printf '%010d' "$(tablet_uid "$i")")/mysql.sock"
  done
}

pids() {
  [[ -f $VTDATAROOT/vtgate.pid ]] && echo "vtgate $(cat "$VTDATAROOT/vtgate.pid")"
  [[ -f $VTDATAROOT/vtctld.pid ]] && echo "vtctld $(cat "$VTDATAROOT/vtctld.pid")"
  for d in "$VTDATAROOT"/vt_*; do
    [[ -d $d ]] || continue
    local uid=$((10#${d##*vt_}))
    [[ -f $d/vttablet.pid ]] && echo "vttablet_$uid $(cat "$d/vttablet.pid")"
    [[ -f $d/mysql.pid ]] && echo "mysqld_$uid $(cat "$d/mysql.pid")"
  done
}

cpu() {
  local tck
  tck=$(getconf CLK_TCK)
  pids | while read -r name pid; do
    [[ -r /proc/$pid/stat ]] || continue
    awk -v n="$name" -v t="$tck" '{ printf "%s %.2f\n", n, ($14 + $15) / t }' "/proc/$pid/stat"
  done
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  env) env_info ;;
  load) load ;;
  purge-binlogs) purge_binlogs "${2:-120}" ;;
  restart-tablets) restart_tablets ;;
  restart-tablet) restart_tablet "$2" ;;
  restart-vtctld) restart_vtctld ;;
  pids) pids ;;
  cpu) cpu ;;
  vtctldclient) shift; vtctldclient "$@" ;;
  *) echo "usage: $0 up|down|env|load|purge-binlogs|restart-tablets|pids|cpu|vtctldclient ARGS" >&2; exit 2 ;;
esac
