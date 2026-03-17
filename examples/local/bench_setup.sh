#!/bin/bash

# Bring up commerce keyspace + customer keyspace tablets with configurable
# parallel replication workers for benchmarking VReplication throughput.

source ../common/env.sh

PARALLEL_WORKERS=${PARALLEL_WORKERS:-1}
SIDECAR_DB_NAME=${SIDECAR_DB_NAME:-"_vt"}
# VReplication experimental flags: 1=OptimizeInserts, 4=VPlayerBatching, 8=AllowNoBlobBinlogRowImage
# Default 13 (all enabled). Set DISABLE_BATCHING=1 to test without multi-statement batching.
VREPL_FLAGS=${VREPL_FLAGS:-13}

echo "=== Bench Setup (parallel_workers=$PARALLEL_WORKERS) ==="

# Step 1: Bring up the commerce keyspace (topo, vtctld, commerce tablets, vtorc, vtgate)
./101_initial_cluster.sh || fail "Failed to bring up initial cluster"

# Step 2: Apply bench schema and vschema to commerce
vtctldclient ApplySchema --sql-file create_bench_schema.sql commerce || fail "Failed to apply bench schema"
vtctldclient ApplyVSchema --vschema-file vschema_bench.json commerce || fail "Failed to apply bench vschema"

echo "Bench schema and vschema applied to commerce keyspace."

# Step 3: Create customer keyspace
if vtctldclient GetKeyspace customer > /dev/null 2>&1; then
	vtctldclient SetKeyspaceDurabilityPolicy --durability-policy=none customer || fail "Failed to set durability policy on customer keyspace"
else
	vtctldclient CreateKeyspace --sidecar-db-name="${SIDECAR_DB_NAME}" --durability-policy=none customer || fail "Failed to create customer keyspace"
fi

# Step 4: Start mysqlctls for customer tablets with small buffer pool.
# We set innodb_buffer_pool_chunk_size=1M at startup so that the buffer pool
# can actually be reduced below the default 128MB chunk size.
BENCH_EXTRA_CNF="$VTDATAROOT/tmp/bench_target.cnf"
cat > "$BENCH_EXTRA_CNF" <<'EOF'
# Bench: small buffer pool to force disk I/O on secondary index access.
# 32MB gives each parallel worker ~8MB (matching serial's total), avoiding
# destructive cache thrashing between workers while keeping I/O significant.
innodb_buffer_pool_chunk_size = 1048576
innodb_buffer_pool_size = 33554432
EOF

for i in 200 201 202; do
	EXTRA_MY_CNF="$BENCH_EXTRA_CNF" CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh &
done

sleep 2
echo "Waiting for customer mysqlctls to start..."
wait
echo "Customer mysqlctls are running!"

# Step 5: Start customer vttablets with --vreplication-parallel-replication-workers flag
cell='zone1'
keyspace='customer'

for uid in 200 201 202; do
	mysql_port=$((17000 + uid))
	port=$((15000 + uid))
	grpc_port=$((16000 + uid))
	printf -v alias '%s-%010d' "$cell" "$uid"
	printf -v tablet_dir 'vt_%010d' "$uid"
	printf -v tablet_logfile 'vttablet_%010d_querylog.txt' "$uid"

	tablet_type=replica
	if [[ "${uid: -1}" -gt 1 ]]; then
		tablet_type=rdonly
	fi

	echo "Starting vttablet for $alias with vreplication-parallel-replication-workers=$PARALLEL_WORKERS..."

	# shellcheck disable=SC2086
	vttablet \
		$TOPOLOGY_FLAGS \
		--log-queries-to-file "$VTDATAROOT/tmp/$tablet_logfile" \
		--tablet-path "$alias" \
		--tablet-hostname "" \
		--init-keyspace "$keyspace" \
		--init-shard "0" \
		--init-tablet-type "$tablet_type" \
		--health-check-interval 5s \
		--backup-storage-implementation file \
		--file-backup-storage-root "$VTDATAROOT/backups" \
		--restore-from-backup \
		--port "$port" \
		--grpc-port "$grpc_port" \
		--service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
		--pid-file "$VTDATAROOT/$tablet_dir/vttablet.pid" \
		--heartbeat-on-demand-duration=5s \
		--pprof-http \
		--log-format text \
		--vreplication-parallel-replication-workers "$PARALLEL_WORKERS" \
		--relay-log-max-size 250000 \
		--relay-log-max-items 5000 \
		--vreplication-experimental-flags "$VREPL_FLAGS" \
		>"$VTDATAROOT/$tablet_dir/vttablet.out" 2>&1 &

	# Wait for tablet to be listening
	for _ in $(seq 0 300); do
		curl -I "http://$(hostname -f):$port/debug/status" >/dev/null 2>&1 && break
		sleep 0.1
	done
	curl -I "http://$(hostname -f):$port/debug/status" || fail "vttablet for $alias could not be started!"
	echo "vttablet for $alias is running!"
done

# Step 6: Wait for healthy shard
wait_for_healthy_shard customer 0 || fail "Customer shard not healthy"

# Step 7: Tune MySQL durability for benchmark throughput on all tablets.
# With innodb_flush_log_at_trx_commit=0 and sync_binlog=0, redo log fsyncs
# don't happen on every COMMIT. This removes fsync as a variable so we can
# isolate the applier throughput difference between serial and parallel.
echo ""
echo "Tuning MySQL settings for benchmark..."
for uid in 100 101 102 200 201 202; do
	printf -v tablet_dir 'vt_%010d' "$uid"
	sock="$VTDATAROOT/$tablet_dir/mysql.sock"
	if [[ -S "$sock" ]]; then
		command mysql --no-defaults -u vt_dba -S "$sock" -e \
			"SET GLOBAL innodb_flush_log_at_trx_commit = 0; SET GLOBAL sync_binlog = 0; SET GLOBAL rpl_semi_sync_source_enabled = 0;" 2>/dev/null && \
			echo "  Tuned tablet $uid (durability, semi-sync off)" || echo "  Warning: could not tune tablet $uid"
	fi
done
# Tune target tablets: disable change buffering to force immediate B-tree
# page reads on every INSERT/UPDATE/DELETE. Combined with the 8MB buffer pool
# set at startup, this makes each applier statement very expensive.
for uid in 200 201 202; do
	printf -v tablet_dir 'vt_%010d' "$uid"
	sock="$VTDATAROOT/$tablet_dir/mysql.sock"
	if [[ -S "$sock" ]]; then
		command mysql --no-defaults -u vt_dba -S "$sock" -e \
			"SET GLOBAL innodb_change_buffering = 'none';" 2>/dev/null && \
			echo "  Tuned tablet $uid (change buffering off, 8MB buffer pool)" || echo "  Warning: could not tune tablet $uid"
	fi
done

echo ""
echo "=== Bench Setup Complete ==="
echo "Commerce keyspace: bench tables loaded"
echo "Customer keyspace: 3 tablets (parallel_workers=$PARALLEL_WORKERS)"
